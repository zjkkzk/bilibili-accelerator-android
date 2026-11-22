package io.github.jh_mmm.biliaccelerator.core

import android.net.Uri
import io.github.jh_mmm.biliaccelerator.BuildConfig
import java.net.URI
import java.net.URLDecoder
import java.net.URLEncoder

data class RewriteResult(
    val changed: Boolean,
    val originalUrl: String,
    val finalUrl: String,
    val originalHost: String,
    val targetHost: String,
    val reason: String,
    val isPcdn: Boolean,
    val isMcdn: Boolean,
    val segmentKey: String = "",
    val isBtr: Boolean = false,
    val btrChunks: Int = 0,
    val btrBytes: Long = 0L,
    val btrConcurrency: Int = 0,
    val btrCdnCount: Int = 0,
    val btrAvgLatencyMs: Long = 0L
)

data class AcceleratorConfig(
    val enabled: Boolean = true,
    val targetHost: String = "upos-sz-mirrorcos.bilivideo.com",
    val proxyHost: String = "proxy-tf-all-ws.bilivideo.com",
    val blockPcdn: Boolean = true,
    val proxyMcdn: Boolean = true,
    val mcdnStrategy: String = "proxy-all", // proxy-all | proxy-v1
    val forceUpos: Boolean = false,
    val rewriteAkamai: Boolean = false,
    val portHeuristic: Boolean = true,
    val enableMossHook: Boolean = false,
    val enableBtr: Boolean = true,
    val btrAutoConcurrency: Boolean = true,
    val btrConcurrency: Int = 6,
    val btrAutoChunkSize: Boolean = true,
    val btrChunkSizeKb: Int = 512,
    val customCdnPool: List<String> = emptyList(),
    val btrCdnPoolSize: Int = 8,
    val enableEpisodeAcceleration: Boolean = true,
    val enableLiveAcceleration: Boolean = false,
    val selectionMode: String = "auto", // auto | manual
    val stallRecovery: Boolean = true
)

object BiliAcceleratorCore {

    val MODULE_PACKAGE: String = BuildConfig.APPLICATION_ID

    val TARGET_PACKAGES = setOf(
        "tv.danmaku.bili",
        "com.bilibili.app.in",
        "com.bstar.intl",
        "tv.danmaku.bilipr",
        "tv.danmaku.bilibilihd",
        "com.bilibili.app.blue",
        "com.bilibili.studio",
        "com.bilibili.comic"
    )

    private val PACKAGE_NAME_REGEX = Regex("""^[a-z][a-z0-9_]*(\.[a-z][a-z0-9_]*)+$""")

    fun isTargetPackage(packageName: String?): Boolean {
        if (packageName.isNullOrBlank()) return false
        val p = packageName.trim().lowercase()
        if (!PACKAGE_NAME_REGEX.matches(p)) return false
        if (TARGET_PACKAGES.contains(p)) return true
        return p.startsWith("tv.danmaku.bili.") ||
                p.startsWith("com.bilibili.") ||
                p.startsWith("com.bstar.")
    }

    // 跨区统一交错候选池（对齐 bilibili-accelerator v0.5.0 与 BTR #44：不预设地域顺序，且保证默认 8 节点池包含 upos-sz-mirrorcoso1）
    val INTERLEAVED_CANDIDATE_POOL = listOf(
        "upos-sz-mirrorcos.bilivideo.com",
        "upos-sz-mirrorcosov.bilivideo.com",
        "upos-sz-mirrorali.bilivideo.com",
        "upos-sz-mirroraliov.bilivideo.com",
        "upos-sz-mirrorhw.bilivideo.com",
        "upos-sz-mirrorhwov.bilivideo.com",
        "upos-sz-mirrorcoso1.bilivideo.com",
        "upos-tf-all-hw.bilivideo.com",
        "upos-tf-all-tx.bilivideo.com",
        "upos-sz-mirrorbos.bilivideo.com",
        "upos-sz-mirror08c.bilivideo.com",
        "upos-sz-mirrorbd.bilivideo.com"
    )

    val MAINLAND_CANDIDATE_POOL = listOf(
        "upos-sz-mirrorcos.bilivideo.com",
        "upos-sz-mirrorali.bilivideo.com",
        "upos-sz-mirrorhw.bilivideo.com",
        "upos-sz-mirrorcoso1.bilivideo.com",
        "upos-tf-all-hw.bilivideo.com",
        "upos-tf-all-tx.bilivideo.com",
        "upos-sz-mirrorcosov.bilivideo.com",
        "upos-sz-mirroraliov.bilivideo.com",
        "upos-sz-mirrorhwov.bilivideo.com",
        "upos-sz-mirrorbos.bilivideo.com",
        "upos-sz-mirror08c.bilivideo.com",
        "upos-sz-mirrorbd.bilivideo.com"
    )

    val OVERSEAS_CANDIDATE_POOL = listOf(
        "upos-sz-mirrorcosov.bilivideo.com",
        "upos-sz-mirroraliov.bilivideo.com",
        "upos-sz-mirrorhwov.bilivideo.com",
        "upos-sz-mirrorali.bilivideo.com",
        "upos-tf-all-hw.bilivideo.com",
        "upos-sz-mirrorhw.bilivideo.com",
        "upos-sz-mirrorcos.bilivideo.com",
        "upos-tf-all-tx.bilivideo.com",
        "upos-sz-mirrorcoso1.bilivideo.com",
        "upos-sz-mirrorbos.bilivideo.com",
        "upos-sz-mirror08c.bilivideo.com",
        "upos-sz-mirrorbd.bilivideo.com"
    )

    val HK_REGIONAL_CANDIDATE_POOL = listOf(
        "cn-hk-eq-01-01.bilivideo.com",
        "cn-hk-eq-01-03.bilivideo.com"
    )

    val CANDIDATE_POOL = INTERLEAVED_CANDIDATE_POOL
    val DEFAULT_CDN_POOL = CANDIDATE_POOL

    /**
     * 判断当前系统时区是否处于海外环境（不依赖 Locale，防止海外简体中文系统被误判为国内；移除含糊的 cst 时区名）
     */
    fun isOverseasEnvironment(): Boolean {
        return try {
            val tzId = java.util.TimeZone.getDefault().id.lowercase()
            val isMainlandTz = tzId.contains("shanghai") || tzId.contains("chongqing") ||
                    tzId.contains("harbin") || tzId.contains("urumqi") || tzId == "prc"
            !isMainlandTz
        } catch (_: Throwable) {
            false
        }
    }

    /**
     * 校验用户自定义 CDN 池或目标节点是否为合法的 B 站官方 CDN 域名（对齐 BTR normalizeCdnHost）。
     * 严格拒绝任意第三方域名、IP、MCDN、PCDN、代理节点（proxy-* / 自定义 proxyHost）、调度域名与海外节点，
     * 防止在拉流时将含签名的请求泄露到非 B 站服务器或误把中继代理当作普通 CDN 节点。
     */
    fun isValidCustomCdnHost(rawHost: String, configuredProxyHost: String? = null): Boolean {
        val host = normalizeHost(rawHost)
        if (host.isBlank() || !isValidHost(host)) return false
        if (isIpAddress(host) || isAkamaiHost(host) || isMcdnHost(host) ||
            isKnownP2pHost(host) || isSchedulerHost(host) || isProxyHost(host, configuredProxyHost) ||
            host.endsWith(".bstarstatic.com")
        ) {
            return false
        }
        return isBiliCdnHost(host)
    }

    /**
     * 校验用户自定义 MCDN 中继代理域名是否为合法的 B 站官方域名（允许 proxy-* 及其他合法官方域名，匹配 SettingsScreen）。
     */
    fun isValidProxyHost(rawHost: String?): Boolean {
        val host = normalizeHost(rawHost)
        if (host.isBlank() || !isValidHost(host)) return false
        if (isIpAddress(host) || isAkamaiHost(host) || isMcdnHost(host) ||
            isKnownP2pHost(host) || isSchedulerHost(host) || host.endsWith(".bstarstatic.com")
        ) {
            return false
        }
        return isBiliCdnHost(host)
    }

    fun isProxyHost(rawHost: String?, configuredProxyHost: String? = null): Boolean {
        val host = normalizeHost(rawHost)
        if (host.isBlank()) return false
        if ((host.startsWith("proxy-tf-all-") || host.startsWith("proxy-")) && isBiliCdnHost(host)) {
            return true
        }
        val cfgProxy = normalizeHost(configuredProxyHost)
        return cfgProxy.isNotBlank() && host == cfgProxy
    }

    fun resolveProxyHost(config: AcceleratorConfig): String {
        val cleaned = normalizeHost(config.proxyHost)
        return if (isValidProxyHost(cleaned)) cleaned else "proxy-tf-all-ws.bilivideo.com"
    }

    /**
     * 从中继代理 URL（如 https://proxy-tf-all-ws.bilivideo.com/?url=... 或自定义官方 proxyHost/?url=...）
     * 中安全提取被包裹的内层媒体 URL。
     */
    fun extractProxyWrappedInnerUrl(url: String, configuredProxyHost: String? = null): String? {
        if (url.isBlank()) return null
        val parsed = parseUri(url) ?: return null
        val host = normalizeHost(parsed.host)
        if (host.isBlank()) return null
        val isKnownOrConfiguredProxy = isProxyHost(host, configuredProxyHost) ||
                (isBiliCdnHost(host) && (parsed.path.isEmpty() || parsed.path == "/"))
        if (!isKnownOrConfiguredProxy) return null
        val inner = parsed.queryParam("url")?.trim() ?: return null
        if (!inner.startsWith("http://", ignoreCase = true) && !inner.startsWith("https://", ignoreCase = true)) {
            return null
        }
        return inner
    }

    fun getEffectiveCdnPool(config: AcceleratorConfig): List<String> {
        val userList = config.customCdnPool
            .map { normalizeHost(it) }
            .filter { isValidCustomCdnHost(it, config.proxyHost) }
            .distinct()
        val basePool = if (userList.isNotEmpty()) userList else CANDIDATE_POOL
        val poolSize = config.btrCdnPoolSize.coerceIn(1, 32)
        return basePool.take(poolSize)
    }

    private val HOSTNAME_REGEX = Regex("""^[a-zA-Z0-9]([a-zA-Z0-9-]{0,61}[a-zA-Z0-9])?(\.[a-zA-Z0-9]([a-zA-Z0-9-]{0,61}[a-zA-Z0-9])?)+(:[0-9]{1,5})?$""")
    private val MEDIA_PATH_REGEX = Regex("""\.(m4s|mp4|flv|m3u8)(?:$|[?#])""", RegexOption.IGNORE_CASE)
    private val BILI_MEDIA_HOST_REGEX = Regex(
        """(?:^|\.)(?:bilivideo\.(?:com|cn|net)|akamaized\.net|bstarstatic\.com|szbdyd\.com|hdslb\.com|xycdn\.com|mountaintoys\.cn|nexusedgeio\.com|ahdohpiechei\.com|live\.bilibili\.com)$""",
        RegexOption.IGNORE_CASE
    )
    private val IPV6_GROUP_REGEX = Regex("""^[0-9a-fA-F]{1,4}$""")
    private val XY_MCDN_REGEX = Regex("""^xy(?:\d+x){3}\d+xy\.mcdn\.bilivideo\.(?:cn|com|net)$""", RegexOption.IGNORE_CASE)

    /**
     * IPv6 安全的去端口主机提取：
     * - `[240e:1234::1]:8080` -> `240e:1234::1`
     * - `[::1]` -> `::1`
     * - `240e:1234::1` -> `240e:1234::1`
     * - `host.example.com:8080` -> `host.example.com`
     */
    fun stripPortFromHost(rawHost: String?): String {
        val cleaned = cleanHost(rawHost)
        if (cleaned.isEmpty()) return ""
        if (cleaned.startsWith("[")) {
            val closeIdx = cleaned.indexOf(']')
            return if (closeIdx > 1) cleaned.substring(1, closeIdx) else cleaned
        }
        return if (cleaned.count { it == ':' } == 1) {
            cleaned.substringBefore(':')
        } else {
            cleaned
        }
    }

    fun normalizeHost(rawHost: String?): String = stripPortFromHost(rawHost).lowercase()

    fun extractPortFromHost(rawHost: String?): Int {
        val cleaned = cleanHost(rawHost)
        if (cleaned.isEmpty()) return -1
        if (cleaned.startsWith("[")) {
            val closeIdx = cleaned.indexOf(']')
            if (closeIdx != -1 && closeIdx + 1 < cleaned.length && cleaned[closeIdx + 1] == ':') {
                return cleaned.substring(closeIdx + 2).toIntOrNull()?.takeIf { it in 1..65535 } ?: -1
            }
            return -1
        }
        return if (cleaned.count { it == ':' } == 1) {
            cleaned.substringAfter(':').toIntOrNull()?.takeIf { it in 1..65535 } ?: -1
        } else {
            -1
        }
    }

    fun isLoopbackHost(rawHost: String?): Boolean {
        val h = normalizeHost(rawHost)
        if (h.isEmpty()) return false
        if (h == "localhost" || h == "::1" || h == "0:0:0:0:0:0:0:1") return true
        // IPv4 127.0.0.0/8
        if (h.startsWith("127.") && isIpAddress(h)) return true
        // IPv4-mapped IPv6 loopback (e.g. ::ffff:127.0.0.1, ::ffff:7f00:1, 0:0:0:0:0:ffff:127.0.0.1)
        val mappedPrefix = when {
            h.startsWith("::ffff:") -> h.removePrefix("::ffff:")
            h.startsWith("0:0:0:0:0:ffff:") -> h.removePrefix("0:0:0:0:0:ffff:")
            else -> null
        }
        if (mappedPrefix != null) {
            if (mappedPrefix.startsWith("127.") && isIpAddress(mappedPrefix)) return true
            val hexParts = mappedPrefix.split(':')
            if (hexParts.size == 2) {
                val hi = hexParts[0].toIntOrNull(16)
                val lo = hexParts[1].toIntOrNull(16)
                if (hi != null && lo != null && (hi ushr 8) == 0x7f && hi in 0..0xffff && lo in 0..0xffff) {
                    return true
                }
            }
        }
        return false
    }

    fun isIpAddress(rawHost: String): Boolean {
        val h = normalizeHost(rawHost)
        if (h.isEmpty() || h.startsWith("[")) return false

        val parts = h.split('.')
        if (parts.size == 4 && parts.all { part -> part.isNotEmpty() && part.all { it.isDigit() } && part.toIntOrNull()?.let { it in 0..255 } == true }) {
            return true
        }

        if (h == "::") return true
        if (h.count { it == ':' } < 2) return false
        if (h.split("::").size > 2) return false
        if (h.startsWith(":") && !h.startsWith("::")) return false
        if (h.endsWith(":") && !h.endsWith("::")) return false

        val groups = h.split(':')
        val lastGroup = groups.lastOrNull().orEmpty()
        if (lastGroup.contains('.')) {
            val v4Parts = lastGroup.split('.')
            val validV4 = v4Parts.size == 4 && v4Parts.all { part ->
                part.isNotEmpty() && part.all { it.isDigit() } && part.toIntOrNull()?.let { it in 0..255 } == true
            }
            if (!validV4) return false
            val prefixGroups = groups.dropLast(1)
            if (prefixGroups.size > 6) return false
            if (!h.contains("::") && prefixGroups.size != 6) return false
            val nonEmptyPrefix = prefixGroups.filter { it.isNotEmpty() }
            return nonEmptyPrefix.all { IPV6_GROUP_REGEX.matches(it) }
        }
        if (groups.size > 8) return false
        if (!h.contains("::") && groups.size != 8) return false
        val nonEmptyGroups = groups.filter { it.isNotEmpty() }
        return nonEmptyGroups.isNotEmpty() && nonEmptyGroups.all { IPV6_GROUP_REGEX.matches(it) }
    }

    fun cleanHost(raw: String?): String {
        if (raw.isNullOrBlank()) return ""
        var cleaned = raw.trim()
        cleaned = cleaned.replace(Regex("""^(?:https?:)?//""", RegexOption.IGNORE_CASE), "")
        val slashIdx = cleaned.indexOf('/')
        if (slashIdx != -1) {
            cleaned = cleaned.substring(0, slashIdx)
        }
        val atIdx = cleaned.indexOf('@')
        if (atIdx != -1) {
            cleaned = cleaned.substring(atIdx + 1)
        }
        val hashIdx = cleaned.indexOf('#')
        if (hashIdx != -1) {
            cleaned = cleaned.substring(0, hashIdx)
        }
        val qIdx = cleaned.indexOf('?')
        if (qIdx != -1) {
            cleaned = cleaned.substring(0, qIdx)
        }
        return cleaned.trim()
    }

    fun isValidHost(host: String): Boolean {
        if (host.isBlank() || host.length > 253) return false
        return HOSTNAME_REGEX.matches(host)
    }

    private val KNOWN_P2P_SUFFIXES = listOf(
        ".szbdyd.com",
        ".mountaintoys.cn",
        ".nexusedgeio.com",
        ".ahdohpiechei.com",
        ".xycdn.com"
    )

    private val KNOWN_P2P_HOSTS = setOf(
        "upos-sz-mirror14b.bilivideo.com"
    )

    /**
     * 识别版权保护/DRM 加密流（Widevine/ChinaDRM/PSSH/UGC付费等）：
     * 这类流需要客户端底层硬件解密管线直接处理，BTR 本地代理无法解密，必须原生直连放行。
     */
    fun hasDrmSignal(url: String): Boolean {
        if (url.isBlank()) return false
        val lower = url.lowercase()
        return lower.contains("drm_tech_type") ||
                (lower.contains("drm_type=") && !lower.contains("drm_type=0")) ||
                lower.contains("bvc-drm") ||
                lower.contains("pssh") ||
                lower.contains("widevine") ||
                lower.contains("/drm/") ||
                lower.contains("has_drm=1") ||
                lower.contains("is_drm=1") ||
                lower.contains("drm=1")
    }

    /**
     * 识别番剧、电影、纪录片等剧集（PGC / OGV）媒体流
     */
    fun isEpisodeMediaUrl(url: String, path: String = ""): Boolean {
        if (url.isBlank() && path.isBlank()) return false
        val lowerUrl = url.lowercase()
        val lowerPath = (if (path.isNotBlank()) path else parseUri(url)?.path ?: "").lowercase()
        return lowerPath.contains("/bangumi/") ||
                lowerPath.contains("/ogv/") ||
                lowerPath.contains("/pgc/") ||
                lowerUrl.contains("ep_id=") ||
                lowerUrl.contains("epid=") ||
                lowerUrl.contains("season_id=") ||
                lowerUrl.contains("season_type=") ||
                lowerUrl.contains("scene=ogv") ||
                lowerUrl.contains("scene=pgc") ||
                lowerUrl.contains("business=pgc") ||
                lowerUrl.contains("biz=pgc") ||
                lowerUrl.contains("is_pgc=1")
    }

    /**
     * 识别剧集试看片段（对齐 BTR 原版 unsupportedEpisode: is_preview=1，交给 B 站原生播放器处理）
     */
    fun isEpisodePreview(url: String): Boolean {
        if (url.isBlank()) return false
        val lower = url.lowercase()
        return lower.contains("is_preview=1") ||
                lower.contains("preview=1") ||
                lower.contains("trial=1")
    }

    fun isBiliMediaHost(rawHost: String?): Boolean {
        val h = normalizeHost(rawHost)
        if (h.isBlank()) return false
        return BILI_MEDIA_HOST_REGEX.containsMatchIn(h) || isKnownP2pHost(h) || isMcdnHost(h)
    }

    fun hasMediaSignal(url: String): Boolean {
        if (url.isBlank()) return false
        val lower = url.lowercase()
        val pathPart = lower.substringBefore('?').substringBefore('#')
        return lower.contains("bilivideo") ||
                lower.contains("akamaized.net") ||
                lower.contains("bstarstatic") ||
                lower.contains("szbdyd.com") ||
                lower.contains("mountaintoys") ||
                lower.contains("nexusedgeio") ||
                lower.contains("ahdohpiechei") ||
                lower.contains("xycdn.com") ||
                lower.contains("hdslb.com") ||
                lower.contains("live.bilibili.com") ||
                lower.contains("mcdn.bili") ||
                lower.contains("smtcdns.") ||
                lower.contains("os=mcdn") ||
                lower.contains("xy_usource=") ||
                pathPart.contains("/upgcxcode/") ||
                pathPart.contains("/v1/resource/") ||
                pathPart.contains("/live-bvc/") ||
                pathPart.contains("/live-stream/") ||
                pathPart.contains("/live-flv/")
    }

    fun isMediaUrl(url: String): Boolean {
        if (url.isBlank()) return false
        val unwrapped = unwrapSmtcdnsUrl(url).ifBlank { url }
        val parsed = parseUri(unwrapped) ?: return false
        val proxyInner = extractProxyWrappedInnerUrl(unwrapped)
        if (!proxyInner.isNullOrBlank()) {
            return isMediaUrl(proxyInner)
        }
        val host = normalizeHost(parsed.host)
        val path = parsed.path
        val hasBiliSpecificPath = path.startsWith("/upgcxcode/", ignoreCase = true) ||
                path.startsWith("/v1/resource/", ignoreCase = true) ||
                path.contains("/live-bvc/", ignoreCase = true) ||
                path.contains("/live-stream/", ignoreCase = true) ||
                path.contains("/live-flv/", ignoreCase = true)
        if (hasBiliSpecificPath) {
            return true
        }
        if (!MEDIA_PATH_REGEX.containsMatchIn(path)) {
            return false
        }
        return isBiliMediaHost(host) ||
                parsed.queryParam("os")?.equals("mcdn", ignoreCase = true) == true ||
                parsed.queryParam("xy_usource") != null ||
                !parsed.queryParam("live_id").isNullOrBlank()
    }

    internal fun isKnownP2pHost(hostname: String): Boolean {
        val clean = normalizeHost(hostname)
        if (clean.isBlank()) return false
        if (KNOWN_P2P_HOSTS.contains(clean)) return true
        for (suffix in KNOWN_P2P_SUFFIXES) {
            val bare = suffix.removePrefix(".")
            if (clean == bare || clean.endsWith(suffix, ignoreCase = true)) return true
        }
        if (clean.startsWith("upos-", ignoreCase = true)) {
            val firstLabel = clean.split(".").firstOrNull() ?: ""
            if (firstLabel.contains("302")) return true
        }
        return false
    }

    internal fun isMcdnHost(hostname: String): Boolean {
        val clean = normalizeHost(hostname)
        return XY_MCDN_REGEX.matches(clean) ||
                clean.endsWith(".mcdn.bilivideo.cn") ||
                clean.endsWith(".mcdn.bilivideo.com") ||
                clean.endsWith(".mcdn.bilivideo.net")
    }

    /**
     * 判断直播主机是否为慢速/劣质节点（对齐 realzza isSlowLiveHost）
     */
    fun isSlowLiveHost(hostValue: String, extra: String? = null, portHeuristic: Boolean = true): Boolean {
        if (hostValue.isBlank()) return false
        val hostOnly = normalizeHost(hostValue)
        val port = extractPortFromHost(hostValue)
        if (isIpAddress(hostOnly) || isMcdnHost(hostOnly) || isKnownP2pHost(hostOnly)) {
            return true
        }
        if (portHeuristic && port != -1 && port != 80 && port != 443) {
            return true
        }
        if (!extra.isNullOrBlank() && (extra.contains("os=mcdn", ignoreCase = true) || extra.contains("pcdn", ignoreCase = true))) {
            return true
        }
        return false
    }

    /**
     * 清洗直播候选节点列表（对齐 realzza filterLiveUrlInfo）
     * 剥离住宅 PCDN、MCDN 和非标端口，保留原生官方 CDN
     */
    fun filterLiveCandidates(rawList: List<String>, portHeuristic: Boolean = true): List<String> {
        if (rawList.isEmpty()) return rawList
        val kept = rawList.filter { candidate ->
            val parsed = parseUri(candidate)
            val host = parsed?.host ?: cleanHost(candidate)
            !isSlowLiveHost(host, parsed?.queryParam?.invoke("extra") ?: candidate, portHeuristic)
        }
        return if (kept.isNotEmpty()) kept.distinct() else rawList.distinct()
    }

    /**
     * 识别中继/MCDN/P2P资源流（对齐 BTR cdn-resolver.js isRelayUrl）：
     * 这类流（含 *.mcdn.*、/v1/resource/、以及 ?os=mcdn）的路径与鉴权签名仅由边缘中继节点接受，
     * 普通 UPOS 节点一律返回 HTTP 403；即使域名曾被改写为 upos-*，只要带 ?os=mcdn 仍属于中继签名。
     */
    fun isRelayUrl(url: String, configuredProxyHost: String? = null): Boolean {
        if (url.isBlank()) return false
        val parsed = parseUri(url) ?: return false
        val host = normalizeHost(parsed.host)
        val path = parsed.path.lowercase()
        if (isMcdnHost(host) ||
            isProxyHost(host, configuredProxyHost) ||
            extractProxyWrappedInnerUrl(url, configuredProxyHost) != null ||
            path.startsWith("/v1/resource/")
        ) {
            return true
        }
        return parsed.queryParam("os")?.equals("mcdn", ignoreCase = true) == true
    }

    fun isUposHost(hostname: String): Boolean {
        val h = normalizeHost(hostname)
        if (h.isBlank() || isKnownP2pHost(h)) return false
        return UPOS_HOST_REGEX.matches(h)
    }

    private val UPOS_HOST_REGEX = Regex("""^upos-[a-z0-9-]+\.bilivideo\.com$""", RegexOption.IGNORE_CASE)

    fun isAkamaiHost(hostname: String): Boolean {
        val h = normalizeHost(hostname)
        return h.endsWith(".akamaized.net")
    }

    fun isBiliAkamaiHost(hostname: String): Boolean {
        val h = normalizeHost(hostname)
        return h.endsWith(".akamaized.net") && (h.startsWith("upos-") || h.contains("bili"))
    }

    fun isBiliCdnHost(hostname: String): Boolean {
        val h = normalizeHost(hostname)
        if (isMcdnHost(h) || isKnownP2pHost(h)) return false
        return h.endsWith(".bilivideo.com") ||
                h.endsWith(".bilivideo.cn") ||
                h.endsWith(".bilivideo.net") ||
                isBiliAkamaiHost(h)
    }

    fun isSchedulerHost(hostname: String): Boolean {
        val h = normalizeHost(hostname)
        return h == "szbdyd.com" || h.endsWith(".szbdyd.com") ||
                h == "mountaintoys.cn" || h.endsWith(".mountaintoys.cn")
    }

    /**
     * 从 szbdyd.com / mountaintoys.cn 等调度器 URL 的 xy_usource 参数中提取真实原生 B 站 CDN 域名。
     * 仅当未携带 ?os=mcdn 中继签名且非 /v1/resource/ 边缘专属路径，且 xy_usource 为合法的官方 CDN 主机时返回。
     */
    fun extractSchedulerSourceHost(url: String): String? {
        if (url.isBlank()) return null
        val effective = unwrapSmtcdnsUrl(url).ifBlank { url }
        val parsed = parseUri(effective) ?: return null
        val hostname = normalizeHost(parsed.host)
        if (!isSchedulerHost(hostname)) return null
        val queryMcdn = parsed.queryParam("os")?.equals("mcdn", ignoreCase = true) == true
        val isV1Resource = parsed.path.startsWith("/v1/resource/", ignoreCase = true)
        if (queryMcdn || isV1Resource) return null
        val rawSource = parsed.queryParam("xy_usource") ?: return null
        val cleanedSource = cleanHost(rawSource)
        val hostOnly = normalizeHost(rawSource)
        if (!isValidHost(cleanedSource) || !isBiliCdnHost(hostOnly)) return null
        return hostOnly
    }

    /**
     * 将 szbdyd.com / mountaintoys.cn 调度器 URL 还原为 xy_usource 指定的官方 HTTPS CDN URL。
     * 若非调度器 URL 或无法安全还原，返回 null。
     */
    fun extractSchedulerSourceUrl(url: String): String? {
        if (url.isBlank()) return null
        val effective = unwrapSmtcdnsUrl(url).ifBlank { url }
        val parsed = parseUri(effective) ?: return null
        val hostOnly = extractSchedulerSourceHost(effective) ?: return null
        return parsed.replaceAuthority(hostOnly)
    }

    fun unwrapSchedulerUrl(url: String): String {
        return extractSchedulerSourceUrl(url) ?: url
    }

    data class HostClassification(
        val host: String,
        val port: Int,
        val kind: String, // scheduler | mcdn | pcdn | akamai | upos | unknown
        val isPcdn: Boolean,
        val isMcdn: Boolean,
        val isAkamai: Boolean,
        val isScheduler: Boolean,
        val isSlow: Boolean,
        val schedulerSource: String?
    )

    /**
     * 对齐上游 rewrite.js classify()，确保 szbdyd.com 与 mountaintoys.cn 在分类、PCDN/调度器识别及 xy_usource 提取上完全一致。
     */
    fun classify(url: String, config: AcceleratorConfig = AcceleratorConfig()): HostClassification? {
        if (url.isBlank()) return null
        val effective = unwrapSmtcdnsUrl(url).ifBlank { url }
        val parsed = parseUri(effective) ?: return null
        val hostname = normalizeHost(parsed.host)
        if (hostname.isBlank()) return null
        val port = parsed.port
        val isLive = isLiveMediaUrl(parsed.path, effective)
        val isBiliFamily = isBiliFamilyHost(hostname)
        val hasBiliMediaSignature = parsed.path.contains("/upgcxcode/") ||
                parsed.path.contains("/v1/resource/") ||
                parsed.queryParam("xy_usource") != null ||
                parsed.queryParam("os")?.equals("mcdn", ignoreCase = true) == true
        val isIp = isIpAddress(hostname)
        val hasNonDefaultPort = config.portHeuristic && port != -1 && port != 80 && port != 443
        val queryMcdn = parsed.queryParam("os")?.equals("mcdn", ignoreCase = true) == true
        val xyMcdn = XY_MCDN_REGEX.matches(hostname)
        val mcdn = isMcdnHost(hostname)
        val akamai = isAkamaiHost(hostname)
        val knownP2p = isKnownP2pHost(hostname)
        val scheduler = isSchedulerHost(hostname)
        val schedulerSource = extractSchedulerSourceHost(effective)

        val isPcdn = (isIp && (hasBiliMediaSignature || knownP2p || isLive)) ||
                xyMcdn ||
                knownP2p ||
                scheduler ||
                queryMcdn ||
                (hasNonDefaultPort && (isBiliFamily || hasBiliMediaSignature || isLive))

        val kind = when {
            schedulerSource != null || scheduler -> "scheduler"
            mcdn -> "mcdn"
            isPcdn -> "pcdn"
            akamai -> "akamai"
            isUposHost(hostname) || hostname.endsWith(".bilivideo.com") -> "upos"
            else -> "unknown"
        }
        val isSlow = isPcdn || (config.rewriteAkamai && akamai)
        return HostClassification(
            host = hostname,
            port = port,
            kind = kind,
            isPcdn = isPcdn,
            isMcdn = mcdn,
            isAkamai = akamai,
            isScheduler = scheduler,
            isSlow = isSlow,
            schedulerSource = schedulerSource
        )
    }

    private val HDNTS_EXP_REGEX = Regex("""(?:^|[~&])exp=(\d+)""", RegexOption.IGNORE_CASE)

    fun extractUrlDeadlineSeconds(url: String): Long {
        if (url.isBlank()) return 0L
        var effectiveUrl = unwrapSmtcdnsUrl(url).ifBlank { url }
        var parsed = parseUri(effectiveUrl) ?: return 0L
        val inner = extractProxyWrappedInnerUrl(effectiveUrl)
        if (!inner.isNullOrBlank()) {
            effectiveUrl = unwrapSmtcdnsUrl(inner).ifBlank { inner }
            parsed = parseUri(effectiveUrl) ?: return 0L
        }
        val directDeadline = listOf("deadline", "e", "expires")
            .firstNotNullOfOrNull { key -> parsed.queryParam(key)?.toLongOrNull()?.takeIf { it > 0L } }
        if (directDeadline != null) return directDeadline
        val hdnts = parsed.queryParam("hdnts")
        if (!hdnts.isNullOrBlank()) {
            val exp = HDNTS_EXP_REGEX.find(hdnts)?.groupValues?.getOrNull(1)?.toLongOrNull()
            if (exp != null && exp > 0L) return exp
        }
        return 0L
    }

    fun unwrapSmtcdnsUrl(url: String): String {
        return io.github.jh_mmm.biliaccelerator.btr.BtrLiveCore.unwrapProxyUrl(url.trim())
    }

    fun isBiliFamilyHost(hostname: String): Boolean {
        val h = normalizeHost(hostname)
        return isBiliCdnHost(h) ||
                isKnownP2pHost(h) ||
                isMcdnHost(h) ||
                h.endsWith(".bilibili.com") ||
                h.endsWith(".bstarstatic.com")
    }

    data class ParsedUri(
        val scheme: String,
        val host: String,
        val port: Int,
        val path: String,
        val queryParam: (String) -> String?,
        val replaceAuthority: (String) -> String,
        val proxyWrap: (String) -> String
    )

    fun parseUri(original: String): ParsedUri? {
        val trimmed = original.trim()
        if (trimmed.isEmpty()) return null
        val normalized = if (trimmed.startsWith("//")) "https:$trimmed" else trimmed

        try {
            val u = Uri.parse(normalized)
            val host = u.host
            if (!host.isNullOrBlank()) {
                val scheme = u.scheme?.lowercase() ?: "https"
                if (scheme == "http" || scheme == "https") {
                    return ParsedUri(
                        scheme = scheme,
                        host = host,
                        port = u.port,
                        path = u.path ?: "",
                        queryParam = { key -> u.getQueryParameter(key) },
                        replaceAuthority = { newAuth ->
                            val cleanAuth = cleanHost(newAuth)
                            u.buildUpon().scheme("https").encodedAuthority(cleanAuth).build().toString()
                        },
                        proxyWrap = { proxyHost ->
                            val cleanProxy = stripPortFromHost(proxyHost)
                            Uri.Builder()
                                .scheme("https")
                                .encodedAuthority(cleanProxy)
                                .path("/")
                                .appendQueryParameter("url", normalized)
                                .build().toString()
                        }
                    )
                }
            }
        } catch (_: Throwable) {}

        // JVM 单元测试回退（当 android.net.Uri 在非 Android 环境未被模拟时）
        return try {
            val trimmed = original.trim()
            if (trimmed.isEmpty()) return null
            val normalized = if (trimmed.startsWith("//")) "https:$trimmed" else trimmed
            val javaUri = URI(normalized)
            val scheme = javaUri.scheme?.lowercase() ?: return null
            if (scheme != "http" && scheme != "https") return null
            val host = javaUri.host ?: return null
            if (host.isBlank()) return null

            val rawQuery = javaUri.rawQuery
            ParsedUri(
                scheme = scheme,
                host = host,
                port = javaUri.port,
                path = javaUri.rawPath ?: "",
                queryParam = { key ->
                    if (rawQuery.isNullOrEmpty()) {
                        null
                    } else {
                        rawQuery.split("&").firstNotNullOfOrNull { pair ->
                            val eqIdx = pair.indexOf('=')
                            val k = if (eqIdx >= 0) pair.substring(0, eqIdx) else pair
                            if (k == key) {
                                val rawVal = if (eqIdx >= 0) pair.substring(eqIdx + 1) else ""
                                runCatching { URLDecoder.decode(rawVal, "UTF-8") }.getOrDefault(rawVal)
                            } else {
                                null
                            }
                        }
                    }
                },
                replaceAuthority = { newAuth ->
                    val cleanAuth = cleanHost(newAuth)
                    val rawPath = javaUri.rawPath?.takeIf { it.isNotEmpty() } ?: ""
                    val q = if (!rawQuery.isNullOrEmpty()) "?$rawQuery" else ""
                    val f = if (!javaUri.rawFragment.isNullOrEmpty()) "#${javaUri.rawFragment}" else ""
                    "https://$cleanAuth$rawPath$q$f"
                },
                proxyWrap = { proxyHost ->
                    val cleanProxy = stripPortFromHost(proxyHost)
                    val encoded = URLEncoder.encode(normalized, "UTF-8").replace("+", "%20")
                    "https://$cleanProxy/?url=$encoded"
                }
            )
        } catch (_: Throwable) {
            null
        }
    }

    fun isLiveMediaUrl(path: String, fullUrl: String = ""): Boolean {
        val parsed = if (fullUrl.isNotBlank()) parseUri(fullUrl) else null
        val lowerPath = (parsed?.path ?: path).lowercase()
        if (lowerPath.contains("/live-bvc/") ||
            lowerPath.contains("/live-stream/") ||
            lowerPath.contains("/live-flv/")
        ) {
            return true
        }
        val lowerHost = parsed?.host?.lowercase().orEmpty()
        if (lowerHost.contains("gotcha") ||
            lowerHost == "live.bilibili.com" ||
            lowerHost.endsWith(".live.bilibili.com") ||
            lowerHost.contains("live-play")
        ) {
            return true
        }
        return !parsed?.queryParam("live_id").isNullOrBlank()
    }

    private val AUDIO_REP_REGEX = Regex("""-(302\d{2}|3025\d)\.(m4s|mp4)""", RegexOption.IGNORE_CASE)
    private val VIDEO_REP_REGEX = Regex("""-(300\d{2}|301\d{2}|100\d{3})\.(m4s|mp4)""", RegexOption.IGNORE_CASE)

    fun isAudioTrack(url: String): Boolean {
        if (url.isBlank()) return false
        val lowerUrl = url.lowercase()
        val pathPart = lowerUrl.substringBefore('?').substringBefore('#')
        val parsed = parseUri(url)
        val codecs = parsed?.queryParam("codecs")?.lowercase()
        if (codecs != null && (codecs.startsWith("mp4a") || codecs.startsWith("flac") || codecs.startsWith("ec-3") || codecs.startsWith("ac-4"))) {
            return true
        }
        val idParam = parsed?.queryParam("id") ?: parsed?.queryParam("rep_id")
        if (idParam != null && (idParam.matches(Regex("""^30(2\d\d|25\d)$""")) || idParam.equals("dolby", ignoreCase = true) || idParam.equals("flac", ignoreCase = true))) {
            return true
        }
        if (AUDIO_REP_REGEX.containsMatchIn(pathPart)) return true
        if (VIDEO_REP_REGEX.containsMatchIn(pathPart)) return false
        val fileName = pathPart.substringAfterLast('/')
        return fileName.startsWith("audio") || fileName.startsWith("dolby") || fileName.startsWith("flac")
    }

    fun normalizeMcdnStrategy(raw: String?): String {
        return when (raw?.trim()?.lowercase()) {
            "proxy-v1" -> "proxy-v1"
            else -> "proxy-all"
        }
    }

    fun normalizeSelectionMode(raw: String?): String {
        return when (raw?.trim()?.lowercase()) {
            "manual", "fixed" -> "manual"
            else -> "auto"
        }
    }

    fun normalizeConfig(config: AcceleratorConfig): AcceleratorConfig {
        val rawMcdn: String? = config.mcdnStrategy
        val rawSelection: String? = config.selectionMode
        val rawTarget: String? = config.targetHost
        val rawProxy: String? = config.proxyHost
        val rawPool: List<String>? = config.customCdnPool
        return config.copy(
            targetHost = rawTarget?.takeIf { it.isNotBlank() } ?: "upos-sz-mirrorcos.bilivideo.com",
            proxyHost = rawProxy?.takeIf { it.isNotBlank() } ?: "proxy-tf-all-ws.bilivideo.com",
            mcdnStrategy = normalizeMcdnStrategy(rawMcdn),
            selectionMode = normalizeSelectionMode(rawSelection),
            customCdnPool = rawPool ?: emptyList()
        )
    }

    fun shouldProxyMcdn(isMcdn: Boolean, path: String, config: AcceleratorConfig): Boolean {
        if (!isMcdn || !config.proxyMcdn) return false
        return when (normalizeMcdnStrategy(config.mcdnStrategy)) {
            "proxy-all" -> true
            "proxy-v1" -> path.startsWith("/v1/resource/", ignoreCase = true)
            else -> true
        }
    }

    fun resolveTargetHost(config: AcceleratorConfig): String {
        val cleanedTarget = cleanHost(config.targetHost)
        val targetHostOnly = normalizeHost(config.targetHost)
        val effectivePool = getEffectiveCdnPool(config)
        return if (isValidHost(cleanedTarget) && isValidCustomCdnHost(targetHostOnly, config.proxyHost) &&
            (isBiliCdnHost(targetHostOnly) || effectivePool.contains(targetHostOnly) || CANDIDATE_POOL.contains(targetHostOnly))
        ) {
            targetHostOnly
        } else {
            config.customCdnPool
                .map { normalizeHost(it) }
                .firstOrNull { isValidCustomCdnHost(it, config.proxyHost) }
                ?: CANDIDATE_POOL[0]
        }
    }

    fun resolveTargetHost(rawTarget: String): String {
        return resolveTargetHost(AcceleratorConfig(targetHost = rawTarget))
    }

    fun extractSegmentKey(path: String): String {
        if (path.isBlank()) return ""
        val fileName = path.substringAfterLast('/').trim()
        return fileName.take(96)
    }

    fun rewriteUrl(original: String, config: AcceleratorConfig): RewriteResult {
        if (!config.enabled || !hasMediaSignal(original)) {
            return noChange(original, "ignored")
        }

        val parsed = parseUri(original) ?: return noChange(original, "parse-failed")
        val hostname = normalizeHost(parsed.host)
        if (isLoopbackHost(hostname)) {
            return noChange(original, "already-local-proxy")
        }
        if (!isMediaUrl(original)) {
            return noChange(original, "ignored")
        }
        val segKey = extractSegmentKey(parsed.path)
        val cleanProxyHost = resolveProxyHost(config)
        if (hostname == cleanProxyHost) {
            return noChange(original, "already-proxied", isMcdn = true)
        }

        // 0. 解包 .smtcdns.net/.com 商业中继代理 URL（兼容多级子域与点播/直播）
        val unwrappedProxy = unwrapSmtcdnsUrl(original)
        if (unwrappedProxy.isNotBlank() && unwrappedProxy != original) {
            val unwrappedUri = parseUri(unwrappedProxy)
            val unwrappedHost = normalizeHost(unwrappedUri?.host)
            if (unwrappedUri != null && unwrappedHost.isNotBlank()) {
                val unwrappedSegKey = extractSegmentKey(unwrappedUri.path)
                val innerRes = rewriteUrl(unwrappedProxy, config)
                if (isLiveMediaUrl(unwrappedUri.path, unwrappedProxy)) {
                    val isUnwrappedCleanLive = !isIpAddress(unwrappedHost) &&
                            !isKnownP2pHost(unwrappedHost) &&
                            !isMcdnHost(unwrappedHost)
                    return RewriteResult(
                        changed = true,
                        originalUrl = original,
                        finalUrl = if (innerRes.changed) innerRes.finalUrl else unwrappedProxy,
                        originalHost = hostname,
                        targetHost = if (innerRes.changed) innerRes.targetHost else unwrappedHost,
                        reason = if (innerRes.changed) innerRes.reason else if (isUnwrappedCleanLive) "live-smtcdns-unwrap" else "live-pcdn-detected",
                        isPcdn = true,
                        isMcdn = innerRes.isMcdn,
                        segmentKey = unwrappedSegKey
                    )
                }
                return RewriteResult(
                    changed = true,
                    originalUrl = original,
                    finalUrl = if (innerRes.changed) innerRes.finalUrl else unwrappedProxy,
                    originalHost = hostname,
                    targetHost = if (innerRes.changed) innerRes.targetHost else unwrappedHost,
                    reason = if (innerRes.changed) innerRes.reason else "smtcdns-unwrap",
                    isPcdn = true,
                    isMcdn = innerRes.isMcdn,
                    segmentKey = unwrappedSegKey
                )
            }
        }

        val isLive = isLiveMediaUrl(parsed.path, original)
        val port = parsed.port
        val isBiliFamily = isBiliFamilyHost(hostname)
        val hasBiliMediaSignature = parsed.path.contains("/upgcxcode/") ||
                parsed.path.contains("/v1/resource/") ||
                parsed.queryParam("xy_usource") != null ||
                parsed.queryParam("os")?.equals("mcdn", ignoreCase = true) == true

        val isIp = isIpAddress(hostname)
        val hasNonDefaultPort = config.portHeuristic && port != -1 && port != 80 && port != 443
        val queryMcdn = parsed.queryParam("os")?.equals("mcdn", ignoreCase = true) == true
        val explicitMcdnHost = isMcdnHost(hostname)
        // 对齐 realzza rewrite.js: 仅显式 *.mcdn.* 域名属于 MCDN 代理目标；裸 os=mcdn 归入 PCDN 拦截
        val isMcdn = explicitMcdnHost
        val isPcdn = (isIp && (hasBiliMediaSignature || isKnownP2pHost(hostname) || isLive)) ||
                XY_MCDN_REGEX.matches(hostname) ||
                isKnownP2pHost(hostname) ||
                queryMcdn ||
                (hasNonDefaultPort && (isBiliFamily || hasBiliMediaSignature || isLive))

        // 1. szbdyd / mountaintoys 调度器：仅在未携带 ?os=mcdn 中继签名且非 /v1/resource/ 时恢复 xy_usource 参数所携带的真实原生 CDN 域名
        val schedulerHostOnly = extractSchedulerSourceHost(original)
        if (schedulerHostOnly != null) {
            val rewritten = parsed.replaceAuthority(schedulerHostOnly)
            val isMountaintoys = hostname == "mountaintoys.cn" || hostname.endsWith(".mountaintoys.cn", ignoreCase = true)
            val baseReason = if (isMountaintoys) "mountaintoys-source" else "szbdyd-source"
            val reason = if (isLive) "live-$baseReason" else baseReason
            return if (rewritten != original) {
                RewriteResult(
                    changed = true,
                    originalUrl = original,
                    finalUrl = rewritten,
                    originalHost = hostname,
                    targetHost = schedulerHostOnly,
                    reason = reason,
                    isPcdn = true,
                    isMcdn = false,
                    segmentKey = segKey
                )
            } else {
                noChange(original, "ok", isPcdn = true, segmentKey = segKey)
            }
        }

        // 直播流 PCDN/MCDN 检测：若非 szbdyd/mountaintoys 还原，标记其为直播 PCDN，绝不盲目改写至 VOD UPOS 镜像
        if (isLive) {
            return if (isPcdn || isMcdn) {
                noChange(original, "live-pcdn-detected", isPcdn = true, isMcdn = isMcdn, segmentKey = segKey)
            } else {
                noChange(original, "live-ok", isPcdn = false, isMcdn = false, segmentKey = segKey)
            }
        }

        // 关键防护：版权保护/DRM 加密流（Widevine/ChinaDRM/PSSH/UGC付费等）必须原生直连放行，严禁改写域名
        if (hasDrmSignal(original)) {
            return noChange(original, "drm-protected", isPcdn = false, isMcdn = false, segmentKey = segKey)
        }

        // 2. 中继/MCDN 节点防护（对齐上游 BTR #47）：
        // *.mcdn.*、/v1/resource/、?os=mcdn 携带边缘中继专属签名，移植到普通 UPOS 节点 100% 返回 403；
        // 因此严禁将任何中继签名地址替换为 upos-* 主机：允许走官方中继代理时按策略代理，否则原样安全放行。
        val isV1Resource = parsed.path.startsWith("/v1/resource/", ignoreCase = true)
        if (isMcdn) {
            if (config.proxyMcdn && shouldProxyMcdn(true, parsed.path, config)) {
                val rewritten = parsed.proxyWrap(cleanProxyHost)
                if (rewritten != original) {
                    return RewriteResult(
                        changed = true,
                        originalUrl = original,
                        finalUrl = rewritten,
                        originalHost = hostname,
                        targetHost = cleanProxyHost,
                        reason = "mcdn-proxy",
                        isPcdn = false,
                        isMcdn = true,
                        segmentKey = segKey
                    )
                } else {
                    return noChange(original, "ok", isMcdn = true, segmentKey = segKey)
                }
            }
            val passReason = if (isV1Resource) "mcdn-v1-pass" else "mcdn-relay-pass"
            return noChange(original, passReason, isPcdn = isPcdn, isMcdn = true, segmentKey = segKey)
        }

        // /v1/resource/ 路径依赖边缘中继鉴权，普通 UPOS 节点不支持并会直接 403；即使非 *.mcdn.* 域名也禁止改写为普通 UPOS
        if (isV1Resource) {
            return noChange(original, "mcdn-v1-pass", isPcdn = isPcdn, isMcdn = false, segmentKey = segKey)
        }

        // 2.5 非 *.mcdn.* 主机但携带 ?os=mcdn 鉴权参数的伪装中继流：
        // 对齐上游 rewrite.js：仅显式 *.mcdn.* 域名走官方中继代理（proxy-tf-all-* 仅接受 *.mcdn.*）；
        // 非 *.mcdn.* 的 ?os=mcdn 流绝不包裹中继代理，也不盲目换普通 UPOS 报 403，原样放行并标记 isPcdn=true。
        if (queryMcdn) {
            return noChange(original, "mcdn-relay-pass", isPcdn = true, isMcdn = false, segmentKey = segKey)
        }

        // 3. Akamai 节点处理：默认放行官方 Akamai 鉴权线路；杜绝非 BTR 模式下直接改写 Host 导致 403 播放失败
        val isAkamai = isAkamaiHost(hostname)
        val isBstar = hostname.endsWith(".bstarstatic.com", ignoreCase = true)

        // 关键防护（对齐 realzza 与 BTR）：Akamai HMAC 鉴权签名与 UPOS 完全不兼容。
        // 在非 BTR 模式下（无法通过 BTR 多源对冲兜底），严禁直接替换 Host 为 UPOS，强制放行以杜绝 HTTP 403 播放失败。
        if (isAkamai && (!config.enableBtr || !config.rewriteAkamai)) {
            return noChange(original, "akamai-pass", isPcdn = false, isMcdn = false, segmentKey = segKey)
        }

        // 4. PCDN 拦截 / Force 模式 / Akamai 改写
        val shouldRewrite = (isPcdn && config.blockPcdn) ||
                (isAkamai && config.rewriteAkamai && config.enableBtr) ||
                (config.forceUpos && isBiliCdnHost(hostname) && !isAkamai && !isBstar)

        if (shouldRewrite) {
            val target = resolveTargetHost(config)
            val rewritten = parsed.replaceAuthority(target)
            if (rewritten != original) {
                val reason = when {
                    isAkamai -> "akamai-redirect"
                    isPcdn -> "pcdn-host"
                    else -> "force-upos"
                }
                return RewriteResult(
                    changed = true,
                    originalUrl = original,
                    finalUrl = rewritten,
                    originalHost = hostname,
                    targetHost = target,
                    reason = reason,
                    isPcdn = isPcdn,
                    isMcdn = false,
                    segmentKey = segKey
                )
            } else {
                return noChange(original, "ok", isPcdn = isPcdn, isMcdn = false, segmentKey = segKey)
            }
        }

        return noChange(original, "ok", isPcdn = isPcdn, isMcdn = false, segmentKey = segKey)
    }

    internal fun isCleanUposDonor(url: String, requireUpgcxcode: Boolean = false): Boolean {
        if (url.contains("hdnts=", ignoreCase = true)) return false
        val parsed = parseUri(url) ?: return false
        val host = normalizeHost(parsed.host)
        if (host.isBlank() || isIpAddress(host) || isKnownP2pHost(host) || isMcdnHost(host) ||
            isAkamaiHost(host) || isSchedulerHost(host) || host.endsWith(".bstarstatic.com")
        ) {
            return false
        }
        if (parsed.port != -1 && parsed.port != 80 && parsed.port != 443) return false
        if (isRelayUrl(url) || hasDrmSignal(url) || isEpisodePreview(url)) return false
        if (requireUpgcxcode && !parsed.path.contains("/upgcxcode/")) return false
        return isUposHost(host) || (host.startsWith("cn-hk-eq-") && host.endsWith(".bilivideo.com"))
    }

    fun filterAndFillBackupUrls(
        rawList: List<String>,
        config: AcceleratorConfig,
        onRewrite: ((RewriteResult) -> Unit)? = null
    ): List<String> {
        if (!config.enabled) return rawList.distinct()
        val isLiveList = rawList.any { isLiveMediaUrl("", it) }
        val resolvedTarget = resolveTargetHost(config)
        val filtered = mutableListOf<String>()

        if (isLiveList) {
            // 直播流候选过滤：
            // 1. 若带 xy_usource 则恢复为官方源站
            // 2. 剥离 PCDN / MCDN / 高位端口 / 裸 IP 节点，保留原生官方 CDN (gotcha / live-bvc 等)
            for (url in rawList) {
                val res = rewriteUrl(url, config)
                if (res.changed) {
                    onRewrite?.invoke(res)
                    filtered.add(res.finalUrl)
                } else if (!res.isPcdn && !res.isMcdn) {
                    filtered.add(url)
                }
            }
            // 容错兜底：若所有节点均被判定为 PCDN（极端情况），保留原列表避免直播黑屏
            val cleaned = filterLiveCandidates(filtered, config.portHeuristic)
            return if (cleaned.isNotEmpty()) cleaned else (if (filtered.isNotEmpty()) filtered.distinct() else rawList.distinct())
        }

        // VOD 点播流候选过滤与补全
        val nonRelayUrls = mutableListOf<String>()
        val relayFallbackUrls = mutableListOf<String>()
        for (url in rawList) {
            val res = rewriteUrl(url, config)
            if (res.changed) {
                onRewrite?.invoke(res)
                if (isRelayUrl(res.finalUrl, config.proxyHost)) {
                    relayFallbackUrls.add(res.finalUrl)
                } else {
                    nonRelayUrls.add(res.finalUrl)
                }
                // 对齐 BTR / realzza：改写 Akamai 或 forceUpos 强制改写官方节点时，保留原地址及独立签名作为 403 兜底
                if (res.reason == "akamai-redirect" || res.reason == "force-upos") {
                    nonRelayUrls.add(url)
                }
            } else if (res.reason == "mcdn-v1-pass" || res.reason == "mcdn-relay-pass") {
                relayFallbackUrls.add(url)
            } else if (!res.isPcdn || !config.blockPcdn) {
                if (isRelayUrl(url, config.proxyHost)) {
                    relayFallbackUrls.add(url)
                } else {
                    nonRelayUrls.add(url)
                }
            }
        }
        filtered.addAll(nonRelayUrls)

        // 优先从原始列表中挑选真正干净的官方 UPOS URL 作为 Donor；仅当原始列表全为 /upgcxcode/ PCDN 并已被改写为 UPOS 时才从已改写列表补充
        val donorUrl = rawList.firstOrNull { isCleanUposDonor(it) }
            ?: filtered.firstOrNull { isCleanUposDonor(it, requireUpgcxcode = true) }
        if (filtered.distinct().size <= 1 && donorUrl != null) {
            val existingHosts = filtered.mapNotNull { parseUri(it)?.host?.let { h -> normalizeHost(h) } }.toSet()
            val pool = getEffectiveCdnPool(config)
            val candidates = pool.filter {
                !it.equals(resolvedTarget, ignoreCase = true) && !existingHosts.contains(it.lowercase())
            }
            val fillCount = config.btrCdnPoolSize.coerceIn(2, 6)
            for (candidate in candidates.take(fillCount)) {
                val backupRes = rewriteUrl(
                    donorUrl,
                    config.copy(targetHost = candidate, forceUpos = true)
                )
                if (backupRes.changed) {
                    filtered.add(backupRes.finalUrl)
                }
            }
        }

        filtered.addAll(relayFallbackUrls)
        return filtered.distinct()
    }

    fun noChange(
        url: String,
        reason: String,
        isPcdn: Boolean = false,
        isMcdn: Boolean = false,
        host: String? = null,
        segmentKey: String = ""
    ): RewriteResult {
        val resolvedHost = host ?: (parseUri(url)?.host?.let { normalizeHost(it) } ?: "")
        return RewriteResult(
            changed = false,
            originalUrl = url,
            finalUrl = url,
            originalHost = resolvedHost,
            targetHost = resolvedHost,
            reason = reason,
            isPcdn = isPcdn,
            isMcdn = isMcdn,
            segmentKey = segmentKey
        )
    }
}
