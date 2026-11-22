package io.github.jh_mmm.biliaccelerator.btr

import io.github.jh_mmm.biliaccelerator.core.BiliAcceleratorCore
import java.net.URI
import java.util.concurrent.ConcurrentHashMap
import kotlin.math.min
import kotlin.math.pow

data class LiveSegment(
    val name: String,
    val url: String,
    val num: Long,
    val duration: Double = 0.0
)

data class ParsedM3u8(
    val mapUrl: String?,
    val segments: List<LiveSegment>,
    val lastNum: Long
)

data class LiveHostStatus(
    val host: String,
    val state: String, // "healthy", "blocked", "banned", "untested"
    val bps: Long,
    val fbMs: Long
)

class HostHealth(
    @Volatile var fbMs: Double = 0.0,
    @Volatile var bps: Double = 0.0,
    @Volatile var failures: Int = 0,
    @Volatile var blockedUntil: Long = 0L,
    @Volatile var lastSuccessAt: Long = 0L,
    @Volatile var proven: Boolean = false,
    @Volatile var emptyReplies: Int = 0,
    @Volatile var banned: Boolean = false
)

class BtrLiveHostPool(
    private val onBan: ((String) -> Unit)? = null
) {
    private val health = ConcurrentHashMap<String, HostHealth>()

    private fun entry(host: String): HostHealth {
        val cleanHost = BiliAcceleratorCore.cleanHost(host).lowercase()
        return health.computeIfAbsent(cleanHost) { HostHealth() }
    }

    fun add(host: String, proven: Boolean = false) {
        if (host.isBlank()) return
        val clean = BiliAcceleratorCore.cleanHost(host).lowercase()
        val item = entry(clean)
        if (proven) item.proven = true
    }

    fun success(host: String, fbMs: Double, bps: Double) {
        val item = entry(host)
        synchronized(item) {
            item.proven = true
            item.banned = false
            item.emptyReplies = 0
            item.failures = 0
            item.blockedUntil = 0L
            item.lastSuccessAt = System.currentTimeMillis()
            if (fbMs > 0.0) {
                item.fbMs = if (item.fbMs > 0.0) item.fbMs * 0.6 + fbMs * 0.4 else fbMs
            }
            if (bps > 0.0) {
                item.bps = if (item.bps > 0.0) item.bps * 0.6 + bps * 0.4 else bps
            }
        }
    }

    fun failure(host: String, receivedBytes: Long = 0L) {
        val item = entry(host)
        synchronized(item) {
            item.failures += 1
            val now = System.currentTimeMillis()
            val penalty = min(20000.0, 1500.0 * (2.0.pow(min(item.failures, 3)))).toLong()
            item.blockedUntil = now + penalty

            if (receivedBytes <= 0L) {
                item.emptyReplies += 1
                if (item.emptyReplies >= 2 && !item.banned) {
                    item.banned = true
                    onBan?.invoke(host)
                }
            }
        }
    }

    fun pick(count: Int = 2): List<String> {
        val now = System.currentTimeMillis()
        val all = health.entries.toList()
        if (all.isEmpty()) return emptyList()

        val open = all.filter { !it.value.banned && it.value.blockedUntil <= now }
        val pool = when {
            open.isNotEmpty() -> open
            else -> {
                val unbanned = all.filter { !it.value.banned }
                if (unbanned.isNotEmpty()) unbanned else all
            }
        }

        val ranked = pool.sortedWith { a, b ->
            val aItem = a.value
            val bItem = b.value
            // 1. 验证通过节点优先
            val provenCompare = bItem.proven.compareTo(aItem.proven)
            if (provenCompare != 0) return@sortedWith provenCompare

            // 2. 首字节延迟低的优先
            val aFb = if (aItem.fbMs > 0.0) aItem.fbMs else 999999.0
            val bFb = if (bItem.fbMs > 0.0) bItem.fbMs else 999999.0
            val fbCompare = aFb.compareTo(bFb)
            if (fbCompare != 0) return@sortedWith fbCompare

            // 3. 吞吐速率高的优先
            bItem.bps.compareTo(aItem.bps)
        }

        return ranked.take(maxOf(1, count)).map { it.key }
    }

    fun unproven(): List<String> {
        val now = System.currentTimeMillis()
        return health.entries
            .filter { !it.value.proven && !it.value.banned && it.value.blockedUntil <= now }
            .map { it.key }
    }

    fun status(): List<LiveHostStatus> {
        val now = System.currentTimeMillis()
        return health.entries.map { (host, item) ->
            val state = when {
                item.banned -> "banned"
                item.blockedUntil > now -> "blocked"
                item.proven -> "healthy"
                else -> "untested"
            }
            LiveHostStatus(
                host = host,
                state = state,
                bps = item.bps.toLong(),
                fbMs = item.fbMs.toLong()
            )
        }
    }
}

object BtrLiveCore {

    /**
     * BTR 已知国内与海外 fMP4 HLS 官方镜像集群（对齐上游 live-core.js KNOWN_FMP4_HOSTS：
     * 集群内节点支持同源签名跨节点互认拉取；由 probeCandidates 首包 TTFB 探测与 EWMA 评分根据用户所在位置自然优选低延迟国内或海外节点）
     */
    val KNOWN_FMP4_HOSTS = listOf(
        "d1--cn-gotcha204.bilivideo.com",
        "d1--cn-gotcha208.bilivideo.com",
        "d1--ov-gotcha208.bilivideo.com",
        "d1--ov-gotcha208b.bilivideo.com"
    )

    private val LIVE_HOST_REGEX = Regex("""(?:\.|^)bilivideo\.(com|cn|net)$""", RegexOption.IGNORE_CASE)
    private val VOD_ONLY_HOST_PREFIX_REGEX = Regex("""^(?:upos|proxy)-""", RegexOption.IGNORE_CASE)
    private val LIVE_GOTCHA_HOST_REGEX = Regex(
        """^(?:[a-z0-9-]+--)?(?:cn|ov)-gotcha[a-z0-9-]*\.bilivideo\.(?:com|cn|net)$""",
        RegexOption.IGNORE_CASE
    )
    private val LIVE_BVC_NUMERIC_ROUTE_REGEX = Regex("""^/live-bvc/\d+/""", RegexOption.IGNORE_CASE)
    private val PROXY_WRAP_REGEX = Regex(
        """^(https?:)//[\w.-]+\.smtcdns\.(?:net|com)/((?:[a-z0-9](?:[a-z0-9-]{0,61}[a-z0-9])?\.)+bilivideo\.(?:com|cn|net)(?::\d{1,5})?)(/.*)$""",
        RegexOption.IGNORE_CASE
    )

    fun isLiveSegmentUrl(value: String): Boolean {
        return try {
            val uri = URI(value)
            val host = uri.host ?: ""
            val path = uri.path ?: ""
            uri.scheme?.equals("https", ignoreCase = true) == true &&
                    LIVE_HOST_REGEX.containsMatchIn(host) &&
                    path.contains("/live-bvc/") &&
                    path.endsWith(".m4s", ignoreCase = true)
        } catch (_: Throwable) {
            false
        }
    }

    fun isLivePlaylistUrl(value: String): Boolean {
        return try {
            val uri = URI(value)
            val host = uri.host ?: ""
            val path = uri.path ?: ""
            uri.scheme?.equals("https", ignoreCase = true) == true &&
                    LIVE_HOST_REGEX.containsMatchIn(host) &&
                    path.endsWith(".m3u8", ignoreCase = true)
        } catch (_: Throwable) {
            false
        }
    }

    private fun extractPortFromAuthority(authority: String): Int {
        val trimmed = authority.trim()
        if (trimmed.isEmpty()) return -1
        if (trimmed.startsWith("[")) {
            val closeIdx = trimmed.indexOf(']')
            if (closeIdx <= 0 || closeIdx + 1 >= trimmed.length) return -1
            val rest = trimmed.substring(closeIdx + 1)
            if (!rest.startsWith(":")) return -1
            return rest.substring(1).toIntOrNull()?.takeIf { it in 1..65535 } ?: -1
        }
        if (trimmed.count { it == ':' } == 1) {
            return trimmed.substringAfter(':').toIntOrNull()?.takeIf { it in 1..65535 } ?: -1
        }
        return -1
    }

    private fun isLoopbackBareHost(bareHost: String): Boolean {
        val h = bareHost.lowercase()
        return h == "localhost" ||
            h == "127.0.0.1" ||
            h.startsWith("127.") ||
            h == "::1" ||
            h == "0:0:0:0:0:0:0:1" ||
            h.startsWith("::ffff:127.")
    }

    fun isP2pUrl(value: String): Boolean {
        val trimmed = value.trim()
        if (trimmed.isEmpty()) return false
        val parsed = BiliAcceleratorCore.parseUri(trimmed)
        val parsedHostRaw = parsed?.host?.let { BiliAcceleratorCore.cleanHost(it).lowercase() }.orEmpty()
        val host: String
        val port: Int
        if (parsed != null && parsedHostRaw.isNotBlank()) {
            host = stripPortFromAuthority(parsedHostRaw)
            port = if (parsed.port > 0) parsed.port else extractPortFromAuthority(parsedHostRaw)
        } else {
            val cleanedAuth = BiliAcceleratorCore.cleanHost(trimmed).lowercase()
            host = stripPortFromAuthority(cleanedAuth)
            port = extractPortFromAuthority(cleanedAuth)
        }
        if (host.isBlank()) return false
        val bareHost = host.removeSurrounding("[", "]")
        if (isLoopbackBareHost(bareHost)) {
            return false
        }
        val hasNonStandardPort = port > 0 && port != 80 && port != 443
        val queryPart = trimmed.substringAfter('?', "").substringBefore('#')
        val hasOsMcdn = queryPart.isNotEmpty() && queryPart.split('&').any { param ->
            val eq = param.indexOf('=')
            eq > 0 &&
                param.substring(0, eq).equals("os", ignoreCase = true) &&
                param.substring(eq + 1).equals("mcdn", ignoreCase = true)
        }
        return hasNonStandardPort ||
            hasOsMcdn ||
            BiliAcceleratorCore.isIpAddress(host) ||
            BiliAcceleratorCore.isKnownP2pHost(host) ||
            BiliAcceleratorCore.isMcdnHost(host) ||
            host.split(".").firstOrNull()?.contains("302") == true
    }

    /**
     * 将商业中继代理包装的 URL 解包恢复至官方源节点 (对齐 BTR unwrapProxyUrl)
     * 仅允许严格 *.bilivideo.(com|cn|net) 内部官方域名，防止误拆普通点号路径或重定向至任意/回环主机。
     * e.g. https://xxx.smtcdns.net/d1--yy.bilivideo.com/... -> https://d1--yy.bilivideo.com/...
     */
    fun unwrapProxyUrl(value: String): String {
        val match = PROXY_WRAP_REGEX.matchEntire(value.trim()) ?: return ""
        val scheme = match.groupValues[1]
        val authority = match.groupValues[2]
        val rest = match.groupValues[3]
        if (!isValidUnwrappedAuthority(authority)) return ""
        return "$scheme//$authority$rest"
    }

    private fun isValidUnwrappedAuthority(authority: String): Boolean {
        val colonIdx = authority.lastIndexOf(':')
        val hostPart = if (colonIdx >= 0) {
            val port = authority.substring(colonIdx + 1).toIntOrNull() ?: return false
            if (port !in 1..65535) return false
            authority.substring(0, colonIdx)
        } else {
            authority
        }
        return hostPart.isNotEmpty() && LIVE_HOST_REGEX.containsMatchIn(hostPart)
    }

    /**
     * 归一化直播分片与 #EXT-X-MAP 初始化分片缓存键：
     * 剥离协议/主机/端口与易变鉴权 Query，并将 /live-bvc/<routeId>/ 归一化为 /live-bvc/，
     * 使播放列表跨节点/跨厂商故障切换后仍可命中已预取的分片与初始化头，同时保留不同清晰度子目录隔离。
     */
    fun normalizeCacheKey(url: String): String {
        val trimmed = url.trim()
        if (trimmed.isEmpty()) return ""
        val unwrapped = unwrapProxyUrl(trimmed).ifBlank { trimmed }
        val parts = parseRawHttpUrl(unwrapped)
        val rawPath = parts?.rawPath
            ?: unwrapped.substringBefore('#').substringBefore('?')
        val cleanPath = rawPath.ifBlank { return unwrapped }
        val liveBvcIdx = cleanPath.indexOf("/live-bvc/", ignoreCase = true)
        val canonical = if (liveBvcIdx >= 0) {
            val fromBvc = cleanPath.substring(liveBvcIdx)
            LIVE_BVC_NUMERIC_ROUTE_REGEX.replaceFirst(fromBvc, "/live-bvc/")
        } else {
            cleanPath
        }
        return canonical.lowercase()
    }

    fun segmentNumber(name: String): Long {
        val cleanName = name.substringBefore('?').substringBefore('#')
        val match = Regex("""(\d+)\.m4s$""", RegexOption.IGNORE_CASE).find(cleanName)
            ?: Regex("""(\d+)\.m4s""", RegexOption.IGNORE_CASE).find(cleanName)
        return match?.groupValues?.get(1)?.toLongOrNull() ?: 0L
    }

    fun isFmp4Playlist(text: String, parsed: ParsedM3u8): Boolean {
        if (!text.contains("#EXT-X-MAP", ignoreCase = true) || parsed.segments.isEmpty()) {
            return false
        }
        return parsed.segments.all { seg ->
            seg.name.substringBefore('?').substringBefore('#').endsWith(".m4s", ignoreCase = true)
        }
    }

    /**
     * 解析 m3u8 播放列表文本并提取分片与初始化分片 URL (对齐 BTR parseM3u8)
     */
    fun parseM3u8(text: String, playlistUrl: String): ParsedM3u8 {
        val lines = text.lines()
        val segments = mutableListOf<LiveSegment>()
        var mapUrl: String? = null
        var currentDuration = 0.0

        for (rawLine in lines) {
            val line = rawLine.trim()
            if (line.startsWith("#EXT-X-MAP", ignoreCase = true)) {
                val uriMatch = Regex("""URI="([^"]+)"""", RegexOption.IGNORE_CASE).find(line)
                val uriStr = uriMatch?.groupValues?.get(1)
                if (!uriStr.isNullOrBlank()) {
                    mapUrl = resolveUrl(uriStr, playlistUrl)
                }
                continue
            }
            if (line.startsWith("#EXTINF:", ignoreCase = true)) {
                val durStr = line.substringAfter("#EXTINF:").substringBefore(',').trim()
                currentDuration = durStr.toDoubleOrNull() ?: 0.0
                continue
            }
            if (line.isBlank() || line.startsWith("#")) continue

            val resolved = resolveUrl(line, playlistUrl)
            val num = segmentNumber(line)
            segments.add(LiveSegment(name = line, url = resolved, num = num, duration = currentDuration))
            currentDuration = 0.0
        }

        val lastNum = segments.lastOrNull()?.num ?: 0L
        return ParsedM3u8(mapUrl = mapUrl, segments = segments, lastNum = lastNum)
    }

    private fun stripPortFromAuthority(authority: String): String {
        val trimmed = authority.trim()
        if (trimmed.startsWith("[")) {
            val closeIdx = trimmed.indexOf(']')
            return if (closeIdx > 0) trimmed.substring(0, closeIdx + 1) else trimmed
        }
        return if (trimmed.count { it == ':' } == 1) trimmed.substringBefore(':') else trimmed
    }

    fun isKnownFmp4Host(host: String): Boolean {
        val clean = stripPortFromAuthority(BiliAcceleratorCore.cleanHost(host)).lowercase()
        if (clean.isBlank()) return false
        return KNOWN_FMP4_HOSTS.any { it.equals(clean, ignoreCase = true) }
    }

    fun isVodOnlyHost(host: String): Boolean {
        val clean = stripPortFromAuthority(BiliAcceleratorCore.cleanHost(host)).lowercase()
        if (clean.isBlank()) return false
        return VOD_ONLY_HOST_PREFIX_REGEX.containsMatchIn(clean)
    }

    fun isLiveCompatibleCustomHost(host: String): Boolean {
        val clean = stripPortFromAuthority(BiliAcceleratorCore.cleanHost(host)).lowercase()
        if (clean.isBlank()) return false
        if (isVodOnlyHost(clean) || isP2pUrl(clean)) return false
        if (!LIVE_HOST_REGEX.containsMatchIn(clean)) return false
        return isKnownFmp4Host(clean) || LIVE_GOTCHA_HOST_REGEX.matches(clean)
    }

    fun filterLiveCustomHosts(hosts: Collection<String>?): List<String> {
        if (hosts.isNullOrEmpty()) return emptyList()
        return hosts.asSequence()
            .map { stripPortFromAuthority(BiliAcceleratorCore.cleanHost(it)).lowercase() }
            .filter { it.isNotBlank() && isLiveCompatibleCustomHost(it) }
            .distinct()
            .toList()
    }

    fun areHostsSignatureCompatible(sourceHost: String, targetHost: String): Boolean {
        val cleanSource = stripPortFromAuthority(BiliAcceleratorCore.cleanHost(sourceHost)).lowercase()
        val cleanTarget = stripPortFromAuthority(BiliAcceleratorCore.cleanHost(targetHost)).lowercase()
        if (cleanSource.isBlank() || cleanTarget.isBlank()) return false
        if (cleanSource == cleanTarget) return true
        return isKnownFmp4Host(cleanSource) && isKnownFmp4Host(cleanTarget)
    }

    private data class RawHttpUrlParts(
        val scheme: String,
        val rawAuthority: String,
        val hostOnly: String,
        val rawPath: String,
        val rawQuery: String?,
        val rawFragment: String?
    )

    private fun parseRawHttpUrl(rawUrl: String): RawHttpUrlParts? {
        val unwrapped = unwrapProxyUrl(rawUrl).ifBlank { rawUrl.trim() }
        if (unwrapped.isEmpty()) return null
        val normalized = if (unwrapped.startsWith("//")) "https:$unwrapped" else unwrapped
        val fragIdx = normalized.indexOf('#')
        val noFrag = if (fragIdx >= 0) normalized.substring(0, fragIdx) else normalized
        val rawFragment = if (fragIdx >= 0) normalized.substring(fragIdx + 1) else null

        val queryIdx = noFrag.indexOf('?')
        val noQuery = if (queryIdx >= 0) noFrag.substring(0, queryIdx) else noFrag
        val rawQuery = if (queryIdx >= 0) noFrag.substring(queryIdx + 1) else null

        val schemeSep = noQuery.indexOf("://")
        if (schemeSep <= 0) return null
        val scheme = noQuery.substring(0, schemeSep).lowercase()
        if (scheme != "http" && scheme != "https") return null

        val afterScheme = noQuery.substring(schemeSep + 3)
        val pathIdx = afterScheme.indexOf('/')
        val rawAuthority = if (pathIdx >= 0) afterScheme.substring(0, pathIdx) else afterScheme
        if (rawAuthority.isBlank()) return null
        val rawPath = if (pathIdx >= 0) afterScheme.substring(pathIdx) else ""
        val hostWithoutUserInfo = rawAuthority.substringAfter('@')
        val hostOnly = stripPortFromAuthority(hostWithoutUserInfo).lowercase()
        if (hostOnly.isBlank()) return null

        return RawHttpUrlParts(
            scheme = scheme,
            rawAuthority = rawAuthority,
            hostOnly = hostOnly,
            rawPath = rawPath,
            rawQuery = rawQuery,
            rawFragment = rawFragment
        )
    }

    /**
     * 为指定候选直播节点构建目标分片/探测/播放列表 URL（解决多源异构 CDN 鉴权签名不互通引发 HTTP 403 误封问题）：
     * 1. 若 liveHostUrls 中存在目标节点的专属签名 URL，提取该节点的 scheme/host/port、基础路径前缀与专属鉴权 Query 参数，
     *    同时保留请求 URL (segmentUrl) 的分片文件名或相对子路径；
     * 2. 若目标节点与源节点同属 KNOWN_FMP4_HOSTS 同源签名互认集群（或 liveHostUrls 含同集群 Donor URL），使用 swapHost 跨节点复用签名；
     * 3. 若候选节点来自不同 CDN 厂商且在 liveHostUrls 中无专属签名 URL（亦不在同一 KNOWN_FMP4_HOSTS 互认集群内），
     *    返回 null 以拒绝发送不匹配的鉴权签名，避免触发无谓的 HTTP 403 节点封禁。
     */
    fun resolveTargetUrlForHost(
        segmentUrl: String,
        targetHost: String,
        liveHostUrls: Map<String, String> = emptyMap(),
        primaryUrl: String? = null,
        customPoolOnly: Boolean = false
    ): String? {
        val normalizedSegUrl = unwrapProxyUrl(segmentUrl).ifBlank { segmentUrl.trim() }
        val segParts = parseRawHttpUrl(normalizedSegUrl) ?: return null
        val cleanTargetAuth = BiliAcceleratorCore.cleanHost(targetHost)
        if (cleanTargetAuth.isBlank()) return null
        val targetHostOnly = stripPortFromAuthority(cleanTargetAuth).lowercase()
        if (targetHostOnly.isBlank()) return null
        val targetAuthLower = cleanTargetAuth.lowercase()
        val hasExplicitTargetPort = !cleanTargetAuth.equals(targetHostOnly, ignoreCase = true)

        val sourceHostOnly = segParts.hostOnly
        val sourceRefUrl = liveHostUrls[segParts.rawAuthority.lowercase()]
            ?: liveHostUrls[sourceHostOnly]
            ?: primaryUrl
        val sourceRefParts = sourceRefUrl?.let { parseRawHttpUrl(it) }

        val mappedHostUrl = liveHostUrls[targetAuthLower]
            ?: liveHostUrls[targetHostOnly]
            ?: liveHostUrls.entries.firstOrNull { (k, _) ->
                val kClean = BiliAcceleratorCore.cleanHost(k).lowercase()
                kClean == targetAuthLower || stripPortFromAuthority(kClean) == targetHostOnly
            }?.value

        fun buildFromTemplate(
            templateParts: RawHttpUrlParts,
            overrideAuthority: String? = null,
            keepSegPath: Boolean = false
        ): String {
            val finalScheme = templateParts.scheme
            val finalAuthority = overrideAuthority
                ?: if (hasExplicitTargetPort) cleanTargetAuth else templateParts.rawAuthority

            val isSameEntryPath =
                (segParts.rawPath.endsWith(".m3u8", ignoreCase = true) && templateParts.rawPath.endsWith(".m3u8", ignoreCase = true)) ||
                (segParts.rawPath.endsWith(".flv", ignoreCase = true) && templateParts.rawPath.endsWith(".flv", ignoreCase = true))
            val finalPath = if (keepSegPath) {
                segParts.rawPath
            } else if (
                isSameEntryPath &&
                (sourceRefParts == null || segParts.rawPath == sourceRefParts.rawPath)
            ) {
                templateParts.rawPath
            } else {
                val templateDir = if (templateParts.rawPath.endsWith("/")) {
                    templateParts.rawPath.trimEnd('/')
                } else {
                    templateParts.rawPath.substringBeforeLast('/', "")
                }
                val sourceDir = sourceRefParts?.rawPath?.let { sp ->
                    if (sp.endsWith("/")) sp.trimEnd('/') else sp.substringBeforeLast('/', "")
                }.orEmpty()
                val relSegPath = when {
                    sourceDir.isNotEmpty() && segParts.rawPath.startsWith("$sourceDir/") ->
                        segParts.rawPath.removePrefix("$sourceDir/")
                    templateDir.isNotEmpty() && segParts.rawPath.startsWith("$templateDir/") ->
                        segParts.rawPath.removePrefix("$templateDir/")
                    else ->
                        segParts.rawPath.substringAfterLast('/')
                }
                if (templateDir.isNotEmpty() && relSegPath.isNotEmpty()) {
                    "$templateDir/$relSegPath"
                } else {
                    segParts.rawPath
                }
            }

            val finalQuery = when {
                !templateParts.rawQuery.isNullOrBlank() -> templateParts.rawQuery
                areHostsSignatureCompatible(sourceHostOnly, targetHostOnly) -> segParts.rawQuery
                else -> null
            }
            val qPart = if (!finalQuery.isNullOrEmpty()) "?$finalQuery" else ""
            val fPart = if (!segParts.rawFragment.isNullOrEmpty()) "#${segParts.rawFragment}" else ""
            return "$finalScheme://$finalAuthority$finalPath$qPart$fPart"
        }

        // 1. 目标节点即为分片当前源节点
        if (targetHostOnly == sourceHostOnly) {
            if (!segParts.rawQuery.isNullOrBlank()) {
                return if (hasExplicitTargetPort) swapHost(normalizedSegUrl, cleanTargetAuth) else normalizedSegUrl
            }
            val sameHostTemplate = mappedHostUrl?.let { parseRawHttpUrl(it) } ?: sourceRefParts
            if (sameHostTemplate != null && !sameHostTemplate.rawQuery.isNullOrBlank()) {
                val auth = if (hasExplicitTargetPort) cleanTargetAuth else segParts.rawAuthority
                return buildFromTemplate(sameHostTemplate, overrideAuthority = auth, keepSegPath = true)
            }
            return if (hasExplicitTargetPort) swapHost(normalizedSegUrl, cleanTargetAuth) else normalizedSegUrl
        }

        // 2. liveHostUrls 存在该候选节点的专属签名 URL：保留分片相对路径 + 替换该节点专属 scheme/authority/baseDir/query
        if (!mappedHostUrl.isNullOrBlank()) {
            val hostParts = parseRawHttpUrl(mappedHostUrl)
            if (hostParts != null) {
                return buildFromTemplate(hostParts)
            }
        }

        // 严禁将点播专用节点（upos-*/proxy-*）用于直播流（/live-bvc/ 等），避免自定义点播 CDN 池破坏直播拉流
        if (isVodOnlyHost(targetHostOnly)) {
            return null
        }

        val isFlv = segParts.rawPath.endsWith(".flv", ignoreCase = true)

        // 3. 源节点与目标节点同属 KNOWN_FMP4_HOSTS 同源签名互认集群（仅限 fMP4 HLS，FLV 各线路签名独立）
        if (!isFlv && areHostsSignatureCompatible(sourceHostOnly, targetHostOnly)) {
            if (segParts.rawQuery.isNullOrBlank() && sourceRefParts != null && !sourceRefParts.rawQuery.isNullOrBlank()) {
                return buildFromTemplate(sourceRefParts, overrideAuthority = cleanTargetAuth, keepSegPath = true)
            }
            return swapHost(normalizedSegUrl, cleanTargetAuth)
        }

        // 4. 目标节点属于 KNOWN_FMP4_HOSTS，且 liveHostUrls 中存在另一 KNOWN_FMP4_HOSTS 节点的同源签名 Donor
        if (!isFlv && isKnownFmp4Host(targetHostOnly)) {
            val fmp4DonorParts = liveHostUrls.entries
                .firstOrNull { (k, _) -> isKnownFmp4Host(k) }
                ?.value
                ?.let { parseRawHttpUrl(it) }
            if (fmp4DonorParts != null) {
                return buildFromTemplate(fmp4DonorParts, overrideAuthority = cleanTargetAuth)
            }
            if (isP2pUrl(normalizedSegUrl) && liveHostUrls.isEmpty()) {
                return swapHost(normalizedSegUrl, cleanTargetAuth)
            }
        }

        // 5. 用户显式启用仅自定义节点池模式（仅放行直播兼容节点，拒绝点播 UPOS 池覆盖直播）
        if (customPoolOnly) {
            if (!isLiveCompatibleCustomHost(targetHostOnly)) {
                return null
            }
            return swapHost(normalizedSegUrl, cleanTargetAuth)
        }

        // 6. 完全无鉴权参数的流（如本地无签名单元测试）允许 swapHost；否则拒绝向异构无专属签名节点发送错配 Token
        val hasAnySignedQuery = !segParts.rawQuery.isNullOrBlank() ||
            !sourceRefParts?.rawQuery.isNullOrBlank() ||
            liveHostUrls.values.any { !parseRawHttpUrl(it)?.rawQuery.isNullOrBlank() }
        if (!hasAnySignedQuery) {
            return swapHost(normalizedSegUrl, cleanTargetAuth)
        }

        return null
    }

    fun swapHost(originalUrl: String, newHost: String): String {
        val normalized = unwrapProxyUrl(originalUrl).ifBlank { originalUrl }
        val cleanNewAuth = BiliAcceleratorCore.cleanHost(newHost)
        if (cleanNewAuth.isBlank()) return normalized
        val newHostOnly = stripPortFromAuthority(cleanNewAuth)
        val hasExplicitPort = cleanNewAuth != newHostOnly

        // 1. Android 运行时优先使用 Uri.Builder.encodedAuthority 保留原始百分比编码（如 %2F/%2B/%3D），防止破坏直播鉴权签名
        val androidUri = runCatching { android.net.Uri.parse(normalized) }.getOrNull()
        val androidHost = runCatching { androidUri?.host }.getOrNull()
        if (androidUri != null && !androidHost.isNullOrBlank()) {
            val androidPort = runCatching { androidUri.port }.getOrDefault(-1)
            if ((!hasExplicitPort && androidPort == -1 && newHostOnly.equals(androidHost, ignoreCase = true)) ||
                cleanNewAuth.equals(androidUri.encodedAuthority, ignoreCase = true)
            ) {
                return normalized
            }
            return androidUri.buildUpon()
                .encodedAuthority(cleanNewAuth)
                .build()
                .toString()
        }

        // 2. JVM 单元测试回退：直接拼接 rawPath / rawQuery / rawFragment，严禁调用会解码重编码的 7 参 URI(...) 构造器
        return try {
            val uri = URI(normalized)
            val origHost = uri.host ?: return normalized
            val origPort = uri.port
            if ((!hasExplicitPort && origPort == -1 && newHostOnly.equals(origHost, ignoreCase = true)) ||
                cleanNewAuth.equals(uri.rawAuthority, ignoreCase = true)
            ) {
                return normalized
            }
            val scheme = uri.scheme ?: "https"
            val userInfoPrefix = uri.rawUserInfo?.takeIf { it.isNotEmpty() }?.let { "$it@" } ?: ""
            val rawPath = uri.rawPath.orEmpty()
            val rawQuery = uri.rawQuery?.let { "?$it" } ?: ""
            val rawFragment = uri.rawFragment?.let { "#$it" } ?: ""
            "$scheme://$userInfoPrefix$cleanNewAuth$rawPath$rawQuery$rawFragment"
        } catch (_: Throwable) {
            normalized
        }
    }

    /**
     * 基于播放列表最后一个已知分片猜测下一未发布的分片 URL（对齐 BTR speculative prefetch: lastNum + 1）
     */
    fun guessNextSegmentUrl(lastSeg: LiveSegment): String? {
        if (lastSeg.num <= 0L) return null
        val nextNum = lastSeg.num + 1L
        val regex = Regex("""(\d+)(\.m4s)$""", RegexOption.IGNORE_CASE)

        val androidUri = runCatching { android.net.Uri.parse(lastSeg.url) }.getOrNull()
        val androidRawPath = runCatching { androidUri?.encodedPath }.getOrNull()
        if (androidUri != null && !androidRawPath.isNullOrBlank()) {
            val replacedPath = regex.replace(androidRawPath) { "$nextNum${it.groupValues[2]}" }
            if (replacedPath == androidRawPath) return null
            return androidUri.buildUpon()
                .encodedPath(replacedPath)
                .build()
                .toString()
        }

        return try {
            val uri = URI(lastSeg.url)
            val path = uri.rawPath ?: return null
            val replacedPath = regex.replace(path) { "$nextNum${it.groupValues[2]}" }
            if (replacedPath == path) return null
            val scheme = uri.scheme ?: "https"
            val rawAuth = uri.rawAuthority ?: return null
            val rawQuery = uri.rawQuery?.let { "?$it" } ?: ""
            val rawFragment = uri.rawFragment?.let { "#$it" } ?: ""
            "$scheme://$rawAuth$replacedPath$rawQuery$rawFragment"
        } catch (_: Throwable) {
            null
        }
    }

    private fun resolveUrl(relativeOrAbsolute: String, baseUrl: String): String {
        return try {
            val baseUri = URI(baseUrl)
            baseUri.resolve(relativeOrAbsolute).toASCIIString()
        } catch (_: Throwable) {
            relativeOrAbsolute
        }
    }
}
