package io.github.jh_mmm.biliaccelerator.core

import com.google.gson.Gson
import com.google.gson.JsonArray
import com.google.gson.JsonObject
import com.google.gson.JsonParser
import io.github.jh_mmm.biliaccelerator.btr.BtrHttpClient
import io.github.jh_mmm.biliaccelerator.hook.RemoteClient
import okhttp3.Request
import java.io.File
import java.util.Locale
import java.util.TimeZone
import java.util.concurrent.CopyOnWriteArrayList
import java.util.concurrent.CountDownLatch
import java.util.concurrent.Executor
import java.util.concurrent.Executors
import java.util.concurrent.ScheduledExecutorService
import java.util.concurrent.ScheduledFuture
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicBoolean
import kotlin.math.abs
import kotlin.math.exp
import kotlin.math.ln
import kotlin.math.max
import kotlin.math.min
import kotlin.math.pow
import kotlin.math.roundToLong
import kotlin.random.Random

/**
 * 非 BTR 模式下的点播智能路由与卡顿竞速恢复引擎（完整移植自 bilibili-accelerator v0.5.0 routing.js & bili-accelerator.page.js）。
 *
 * 核心特性：
 * 1. "auto" vs "manual" 模式：在 "auto" 模式下不盲目替换健康的官方 CDN（包括海外 mirror*ov 与 Akamai 原生节点），
 *    仅拦截/清洗 PCDN、MCDN 与调度器节点（优先提权 playinfo 下发的干净官方备份源）。
 * 2. Shaka Player 双半衰期 EWMA 带宽估计器（快半衰期 2s、慢半衰期 5s，取较低值以快速响应跌落、缓慢恢复）。
 * 3. 卡顿 (stuck: 首字节超时 / 慢分片无法跑赢剩余缓冲 / 播放器 BUFFERING_START 701) 与持续带宽不足 (shortfall) 判定。
 * 4. 2 候选节点 768KB HTTP Range 异步竞速，仅当获胜节点达到当前吞吐 1.5 倍以上增益 (SWITCH_GAIN = 1.5) 或当前节点故障时切换。
 * 5. 按时区/语言地域 (regionKey) 持久化各节点历史测速得分（3 天半衰期指数衰减 + 1 小时近期故障降权）。
 */
object AutoVodRouter {

    const val RACE_BYTES: Int = 768 * 1024
    const val RACE_TIMEOUT_MS: Long = 4000L

    const val EWMA_FAST_HALF_LIFE_S: Double = 2.0
    const val EWMA_SLOW_HALF_LIFE_S: Double = 5.0
    const val MIN_SAMPLE_BYTES: Long = 16_000L
    const val MIN_TOTAL_BYTES: Long = 128_000L

    const val STUCK_MIN_FRAGMENT_BYTES: Long = 128L * 1024L
    const val STUCK_MIN_TRANSFER_MS: Long = 500L
    const val STUCK_NO_FIRST_BYTE_MS: Long = 1000L
    const val STUCK_URGENT_BUFFER_S: Double = 3.0
    const val STUCK_RATE_FACTOR: Double = 1.3
    const val STUCK_MIN_REMAINING_MS: Double = 1000.0
    const val STUCK_MARGIN_MS: Double = 2000.0

    const val SHORTFALL_RATE_FACTOR: Double = 1.2
    const val SHORTFALL_MIN_BYTES: Long = 4L * 1024L * 1024L
    const val SHORTFALL_MIN_MS: Long = 8000L
    const val SHORTFALL_BUFFER_S: Double = 30.0

    const val ERROR_WINDOW_MS: Long = 30_000L
    const val ERROR_LIMIT: Int = 2

    const val SWITCH_GAIN: Double = 1.5
    const val MAX_SWITCHES: Int = 4
    const val SWITCH_BACKOFF_MS: Long = 10_000L
    const val NO_SWITCH_COOLDOWN_MS: Long = 15_000L
    const val MAX_COOLDOWN_MS: Long = 120_000L
    const val LOST_RACE_REST_MS: Long = 60_000L

    const val HISTORY_HALF_LIFE_MS: Long = 3L * 24L * 60L * 60L * 1000L
    const val RECENT_FAILURE_MS: Long = 60L * 60L * 1000L
    const val EXPLORE_RATE: Double = 0.2
    const val UNKNOWN_HOST_MBPS: Double = 10.0

    const val AVOID_MS: Long = 3000L
    const val STALL_GRACE_MS: Long = 2500L
    const val RACE_CUT_GRACE_MS: Long = 1000L
    const val MANUAL_SPACING_MS: Long = 10_000L
    const val MAX_SESSIONS: Int = 4
    const val DEFAULT_REQUIRED_BPS: Double = 3_750_000.0

    private val AUDIO_ID_REGEX = Regex("""^30(2\d\d|25\d)$""")
    private val FILE_EXT_REGEX = Regex("""\.(m4s|mp4|flv)$""", RegexOption.IGNORE_CASE)
    private val CID_PREFIX_REGEX = Regex("""^(\d+)""")
    private val REP_ID_REGEX = Regex("""-(\d+)\.(?:m4s|mp4|flv)$""", RegexOption.IGNORE_CASE)
    private val RANGE_HEADER_REGEX = Regex("""bytes=(\d+)-(\d*)""", RegexOption.IGNORE_CASE)
    private val MIRROR_VENDOR_REGEX = Regex("""^upos-[a-z]+-mirror([a-z0-9]+?)(ov)?\.bilivideo\.com$""", RegexOption.IGNORE_CASE)
    private val TF_VENDOR_REGEX = Regex("""^upos-tf-all-([a-z]+)\.bilivideo\.com$""", RegexOption.IGNORE_CASE)

    private val VENDORS = mapOf(
        "cos" to "tencent",
        "coso1" to "tencent",
        "ali" to "alibaba",
        "hw" to "huawei",
        "tx" to "tencent",
        "bos" to "baidu",
        "bd" to "baidu",
        "akam" to "akamai"
    )

    // ---- Data models --------------------------------------------------------

    data class ByteRangeSpec(
        val start: Long,
        val end: Long?,
        val length: Long?
    )

    data class VodRepresentation(
        val key: String,
        val id: Int = 0,
        val kind: String = "video",
        var bandwidth: Long = 0L,
        val codecs: String = "",
        val urls: LinkedHashMap<String, String> = LinkedHashMap(),
        val issued: MutableList<String> = mutableListOf()
    )

    data class VodSessionTable(
        val cid: String,
        val reps: LinkedHashMap<String, VodRepresentation> = LinkedHashMap()
    )

    data class ProgressSample(
        val timeMs: Long,
        val loadedBytes: Long
    )

    data class InflightRequestSnapshot(
        val total: Long,
        val loaded: Long = 0L,
        val startedAt: Long = 0L,
        val firstByteAt: Long? = null,
        val rateBps: Double = 0.0,
        val key: String = "",
        val host: String = "",
        val startByte: Long = 0L,
        val endByte: Long = -1L,
        val retry: Boolean = false
    )

    data class StuckVerdict(
        val reason: String,
        val waitedMs: Long = 0L,
        val rateBps: Double = 0.0,
        val remainingMs: Double = 0.0
    )

    data class EvaluationInput(
        val nowMs: Long,
        val requiredBps: Double,
        val bufferAheadS: Double,
        val inflight: List<InflightRequestSnapshot> = emptyList(),
        val estimateBps: Double? = null,
        val measuredBytes: Long = 0L,
        val measuredMs: Long = 0L,
        val recentErrors: Int = 0,
        val playerStalling: Boolean = false
    )

    data class EvaluationVerdict(
        val trigger: String,
        val req: InflightRequestSnapshot? = null
    )

    data class HostHistoryRecord(
        val mbps: Double = 0.0,
        val n: Double = 0.0,
        val at: Long = 0L,
        val lastFailAt: Long? = null
    )

    data class SpeedHistory(
        val hosts: MutableMap<String, HostHistoryRecord> = LinkedHashMap()
    )

    data class PickChallengersInput(
        val candidates: List<String>,
        val current: String,
        val issued: List<String> = emptyList(),
        val measured: Set<String> = emptySet(),
        val failed: Map<String, Int> = emptyMap(),
        val lost: Map<String, Long> = emptyMap(),
        val history: SpeedHistory? = null,
        val nowMs: Long = 0L,
        val random: () -> Double = { Random.nextDouble() }
    )

    data class RaceContenderResult(
        val host: String,
        val ok: Boolean,
        val ms: Long,
        val bytes: Int = RACE_BYTES,
        val status: Int = if (ok) 206 else 0,
        val incumbent: Boolean = false,
        val cut: Boolean = false,
        val settled: Boolean = true
    )

    data class RaceVerdict(
        val winner: String?,
        val runnerUp: String?,
        val switchTo: String?,
        val hostRateBps: Double = 0.0,
        val retryOn: String? = null
    )

    data class HostDescription(
        val region: String?,
        val vendor: String?,
        val id: String
    )

    data class SwitchRecord(
        val atMs: Long,
        val from: String,
        val to: String,
        val trigger: String,
        val beforeMbps: Double
    )

    data class RaceRecord(
        val atMs: Long,
        val trigger: String,
        val from: String,
        val currentMbps: Double,
        val contenders: List<RaceContenderResult>,
        val switchTo: String?,
        val retryOn: String?
    )

    class HostStats {
        val estimator = BandwidthEstimator()
        var requests: Int = 0
        var bytes: Long = 0L
        val errors: MutableList<Long> = mutableListOf()
        val ttfbs: MutableList<Long> = mutableListOf()
    }

    class InflightRequestHandle internal constructor(
        val sessionCid: String,
        val key: String,
        val kind: String,
        val host: String,
        val startByte: Long,
        val endByte: Long,
        val totalBytes: Long,
        val startedAtMs: Long,
        val retry: Boolean
    ) {
        @Volatile
        var firstByteAtMs: Long? = null
        @Volatile
        var loadedBytes: Long = 0L
        val samples: MutableList<ProgressSample> = mutableListOf()
        @Volatile
        var raced: Boolean = false
        @Volatile
        var ended: Boolean = false
    }

    fun interface ActiveHostListener {
        fun onActiveHostChanged(
            cid: String,
            oldHost: String,
            newHost: String,
            newUrl: String?,
            trigger: String
        )
    }

    private data class ActiveHostNotification(
        val cid: String,
        val oldHost: String,
        val newHost: String,
        val newUrl: String?,
        val trigger: String
    )

    class VodRoutingSession(val cid: String) {
        val table = VodSessionTable(cid)
        var requiredBps: Double = DEFAULT_REQUIRED_BPS
        var hasExplicitRequiredBps: Boolean = false
        var assignedHost: String? = null
        var lastHost: String? = null
        var activeHost: String? = null
        var fallbackHost: String? = null
        var avoidHost: String? = null
        var avoidUntilMs: Long = 0L
        var detourHost: String? = null
        var detourUntilMs: Long = 0L
        var lastVideoKey: String? = null
        val ends: MutableMap<String, Long> = LinkedHashMap()
        val fileSizes: MutableMap<String, Long> = LinkedHashMap()
        val hosts: MutableMap<String, HostStats> = LinkedHashMap()
        val requestedSlots: MutableSet<String> = HashSet()
        val inflight: MutableList<InflightRequestHandle> = mutableListOf()
        val isRacing = AtomicBoolean(false)
        var nextRaceAtMs: Long = 0L
        var cooldownMs: Long = 0L
        var manualAtMs: Long = Long.MIN_VALUE / 2
        val failed: MutableMap<String, Int> = LinkedHashMap()
        val lost: MutableMap<String, Long> = LinkedHashMap()
        val measured: MutableSet<String> = HashSet()
        val races: MutableList<RaceRecord> = mutableListOf()
        val switches: MutableList<SwitchRecord> = mutableListOf()
        var stalls: Int = 0
        var lastBufferAheadS: Double = 10.0
        var pendingStallStartMs: Long? = null
        var pendingStallTask: ScheduledFuture<*>? = null
        var isStalling: Boolean = false
    }

    // ---- Pluggable SpeedHistoryStore & RangeRaceFetcher ---------------------

    interface SpeedHistoryStore {
        fun load(regionKey: String): SpeedHistory
        fun save(regionKey: String, history: SpeedHistory)
    }

    class InMemorySpeedHistoryStore : SpeedHistoryStore {
        private val map = LinkedHashMap<String, SpeedHistory>()

        @Synchronized
        override fun load(regionKey: String): SpeedHistory {
            val existing = map[regionKey] ?: return SpeedHistory()
            return SpeedHistory( LinkedHashMap(existing.hosts) )
        }

        @Synchronized
        override fun save(regionKey: String, history: SpeedHistory) {
            map[regionKey] = SpeedHistory(LinkedHashMap(history.hosts))
        }

        @Synchronized
        fun clear() {
            map.clear()
        }
    }

    class FileSpeedHistoryStore(private val baseDir: File) : SpeedHistoryStore {
        private val gson = Gson()
        private val lock = Any()

        private fun fileFor(regionKey: String): File {
            val safeName = regionKey.replace(Regex("""[^a-zA-Z0-9._-]"""), "_")
            val dir = File(baseDir, "auto_vod_history")
            if (!dir.exists()) dir.mkdirs()
            return File(dir, "hosts_v1_$safeName.json")
        }

        override fun load(regionKey: String): SpeedHistory {
            synchronized(lock) {
                return try {
                    val file = fileFor(regionKey)
                    if (!file.isFile) return SpeedHistory()
                    val text = file.readText(Charsets.UTF_8)
                    val parsed = gson.fromJson(text, SpeedHistory::class.java)
                    if (parsed?.hosts != null) {
                        SpeedHistory(LinkedHashMap(parsed.hosts))
                    } else {
                        SpeedHistory()
                    }
                } catch (_: Throwable) {
                    SpeedHistory()
                }
            }
        }

        override fun save(regionKey: String, history: SpeedHistory) {
            synchronized(lock) {
                try {
                    val file = fileFor(regionKey)
                    val tmp = File(file.parentFile, "${file.name}.tmp")
                    tmp.writeText(gson.toJson(history), Charsets.UTF_8)
                    if (!tmp.renameTo(file)) {
                        file.writeText(gson.toJson(history), Charsets.UTF_8)
                        tmp.delete()
                    }
                } catch (_: Throwable) {}
            }
        }
    }

    data class RangeFetchResponse(
        val status: Int,
        val bytes: Int,
        val durationMs: Long
    ) {
        val isOk: Boolean
            get() = (status == 206 || status == 200) && bytes >= 64 * 1024
    }

    class RaceAbortController {
        private val aborted = AtomicBoolean(false)
        private val onAbortCallbacks = CopyOnWriteArrayList<() -> Unit>()

        val isAborted: Boolean
            get() = aborted.get()

        fun addOnAbort(callback: () -> Unit) {
            if (aborted.get()) {
                runCatching { callback() }
            } else {
                onAbortCallbacks.add(callback)
                if (aborted.get()) {
                    runCatching { callback() }
                }
            }
        }

        fun abort() {
            if (aborted.compareAndSet(false, true)) {
                for (cb in onAbortCallbacks) {
                    runCatching { cb() }
                }
            }
        }
    }

    fun interface RangeRaceFetcher {
        fun fetchRange(url: String, startByte: Long, endByte: Long, timeoutMs: Long): RangeFetchResponse
    }

    interface CancellableRangeRaceFetcher : RangeRaceFetcher {
        fun fetchRange(
            url: String,
            startByte: Long,
            endByte: Long,
            timeoutMs: Long,
            abortController: RaceAbortController,
            onProgress: (Int) -> Unit
        ): RangeFetchResponse

        override fun fetchRange(url: String, startByte: Long, endByte: Long, timeoutMs: Long): RangeFetchResponse {
            return fetchRange(url, startByte, endByte, timeoutMs, RaceAbortController()) {}
        }
    }

    object OkHttpRangeRaceFetcher : CancellableRangeRaceFetcher {
        override fun fetchRange(
            url: String,
            startByte: Long,
            endByte: Long,
            timeoutMs: Long,
            abortController: RaceAbortController,
            onProgress: (Int) -> Unit
        ): RangeFetchResponse {
            val t0 = System.currentTimeMillis()
            if (abortController.isAborted) {
                return RangeFetchResponse(status = 0, bytes = 0, durationMs = 1L)
            }
            val expectedBytes = (endByte - startByte + 1L).coerceAtLeast(1L).toInt()
            val client = BtrHttpClient.probeClient.newBuilder()
                .callTimeout(timeoutMs, TimeUnit.MILLISECONDS)
                .connectTimeout(min(timeoutMs, 3500L), TimeUnit.MILLISECONDS)
                .readTimeout(timeoutMs, TimeUnit.MILLISECONDS)
                .build()
            val request = Request.Builder()
                .url(url)
                .get()
                .header("Range", "bytes=$startByte-$endByte")
                .header("User-Agent", "Bilibili Freedoooooom/MarkII")
                .build()
            val call = client.newCall(request)
            abortController.addOnAbort { call.cancel() }
            var capturedStatus = 0
            var totalRead = 0
            return try {
                call.execute().use { response ->
                    capturedStatus = response.code
                    val body = response.body
                    if (body != null && (capturedStatus == 206 || capturedStatus == 200)) {
                        val stream = body.byteStream()
                        val buf = ByteArray(16 * 1024)
                        while (totalRead < expectedBytes && !abortController.isAborted) {
                            val toRead = min(buf.size, expectedBytes - totalRead)
                            val n = stream.read(buf, 0, toRead)
                            if (n < 0) break
                            totalRead += n
                            onProgress(totalRead)
                        }
                    }
                    val elapsed = (System.currentTimeMillis() - t0).coerceAtLeast(1L)
                    RangeFetchResponse(status = capturedStatus, bytes = totalRead, durationMs = elapsed)
                }
            } catch (_: Throwable) {
                val elapsed = (System.currentTimeMillis() - t0).coerceAtLeast(1L)
                RangeFetchResponse(status = capturedStatus, bytes = totalRead, durationMs = elapsed)
            }
        }
    }

    // ---- Shaka Dual-Half-Life EWMA & BandwidthEstimator ---------------------

    class Ewma(halfLifeS: Double) {
        private val alpha: Double = exp(ln(0.5) / halfLifeS)
        private var estimate: Double = 0.0
        private var totalWeight: Double = 0.0

        fun sample(weightS: Double, value: Double) {
            if (weightS <= 0.0) return
            val adjAlpha = alpha.pow(weightS)
            estimate = value * (1.0 - adjAlpha) + adjAlpha * estimate
            totalWeight += weightS
        }

        fun get(): Double {
            val zeroFactor = 1.0 - alpha.pow(totalWeight)
            return if (zeroFactor > 0.0) estimate / zeroFactor else 0.0
        }
    }

    class BandwidthEstimator {
        private val fast = Ewma(EWMA_FAST_HALF_LIFE_S)
        private val slow = Ewma(EWMA_SLOW_HALF_LIFE_S)
        private var totalBytes: Long = 0L
        private var totalMs: Long = 0L

        fun sample(durationMs: Long, numBytes: Long): Boolean {
            if (durationMs <= 0L || numBytes < MIN_SAMPLE_BYTES) {
                return false
            }
            val bps = 8000.0 * numBytes.toDouble() / durationMs.toDouble()
            val weightS = durationMs.toDouble() / 1000.0
            fast.sample(weightS, bps)
            slow.sample(weightS, bps)
            totalBytes += numBytes
            totalMs += durationMs
            return true
        }

        fun estimate(): Double? {
            return if (totalBytes >= MIN_TOTAL_BYTES) min(fast.get(), slow.get()) else null
        }

        fun bytes(): Long = totalBytes
        fun ms(): Long = totalMs
    }

    fun createEstimator(): BandwidthEstimator = BandwidthEstimator()

    // ---- Pure routing.js functions ------------------------------------------

    fun fileKey(url: String?): String? {
        if (url.isNullOrBlank()) return null
        val unwrapped = BiliAcceleratorCore.unwrapSmtcdnsUrl(url).ifBlank { url }
        val parsed = BiliAcceleratorCore.parseUri(unwrapped) ?: return null
        val name = parsed.path.substringAfterLast('/')
        return if (FILE_EXT_REGEX.containsMatchIn(name)) name else null
    }

    fun cidOf(key: String?): String? {
        if (key.isNullOrBlank()) return null
        return CID_PREFIX_REGEX.find(key)?.groupValues?.getOrNull(1)
    }

    fun repIdOf(key: String?): String? {
        if (key.isNullOrBlank()) return null
        return REP_ID_REGEX.find(key)?.groupValues?.getOrNull(1)
    }

    fun parseRange(value: String?): ByteRangeSpec? {
        if (value.isNullOrBlank()) return null
        val match = RANGE_HEADER_REGEX.find(value) ?: return null
        val start = match.groupValues[1].toLongOrNull() ?: return null
        val endStr = match.groupValues[2]
        val end = if (endStr.isEmpty()) null else endStr.toLongOrNull()
        val length = if (end != null && end >= start) end - start + 1L else null
        return ByteRangeSpec(start = start, end = end, length = length)
    }

    fun hostOf(url: String?): String {
        if (url.isNullOrBlank()) return ""
        val parsed = BiliAcceleratorCore.parseUri(url) ?: return ""
        return BiliAcceleratorCore.normalizeHost(parsed.host)
    }

    fun isUsableVodUrl(url: String, config: AcceleratorConfig = AcceleratorConfig(enableBtr = false)): Boolean {
        if (url.isBlank()) return false
        val classification = BiliAcceleratorCore.classify(url, config) ?: return false
        if (classification.isPcdn || classification.isMcdn || classification.isScheduler) {
            return false
        }
        return BiliAcceleratorCore.isBiliCdnHost(classification.host)
    }

    fun buildTableFromJson(
        jsonText: String,
        isUsable: (String) -> Boolean = { isUsableVodUrl(it) }
    ): VodSessionTable? {
        if (jsonText.isBlank()) return null
        val root = runCatching { JsonParser.parseString(jsonText) }.getOrNull() ?: return null
        if (!root.isJsonObject) return null
        val rootObj = root.asJsonObject
        val dataObj = rootObj.getAsJsonObjectOrNull("data")
        val resultObj = rootObj.getAsJsonObjectOrNull("result")
        val videoInfoObj = dataObj?.getAsJsonObjectOrNull("video_info")
            ?: resultObj?.getAsJsonObjectOrNull("video_info")
        val dashContainers = listOfNotNull(
            dataObj?.getAsJsonObjectOrNull("dash"),
            resultObj?.getAsJsonObjectOrNull("dash"),
            videoInfoObj?.getAsJsonObjectOrNull("dash"),
            rootObj.getAsJsonObjectOrNull("dash")
        ).distinct()
        if (dashContainers.isEmpty()) return null

        val reps = LinkedHashMap<String, VodRepresentation>()
        var detectedCid: String? = null

        for (dash in dashContainers) {
            val groups = mutableListOf<Pair<String, JsonArray?>>()
            groups.add("video" to dash.getAsJsonArrayOrNull("video"))
            groups.add("audio" to dash.getAsJsonArrayOrNull("audio"))
            dash.getAsJsonObjectOrNull("dolby")?.getAsJsonArrayOrNull("audio")?.let {
                groups.add("audio" to it)
            }
            dash.getAsJsonObjectOrNull("flac")?.getAsJsonObjectOrNull("audio")?.let { single ->
                val arr = JsonArray().apply { add(single) }
                groups.add("audio" to arr)
            }
            for ((kind, arr) in groups) {
                if (arr == null) continue
                for (elem in arr) {
                    if (!elem.isJsonObject) continue
                    val entry = elem.asJsonObject
                    val base = entry.getStringOrNull("baseUrl") ?: entry.getStringOrNull("base_url")
                    val backups = mutableListOf<String>()
                    for (bKey in listOf("backupUrl", "backup_url")) {
                        entry.getAsJsonArrayOrNull(bKey)?.forEach { bElem ->
                            val s = if (bElem.isJsonPrimitive && bElem.asJsonPrimitive.isString) bElem.asString else null
                            if (!s.isNullOrBlank()) backups.add(s)
                        }
                    }
                    val rawUrls = listOfNotNull(base) + backups
                    val normalizedUrls = rawUrls.map { u ->
                        val step1 = BiliAcceleratorCore.unwrapSmtcdnsUrl(u).ifBlank { u }
                        BiliAcceleratorCore.extractSchedulerSourceUrl(step1) ?: step1
                    }
                    val key = normalizedUrls.firstNotNullOfOrNull { fileKey(it) } ?: continue
                    val rep = reps[key] ?: VodRepresentation(
                        key = key,
                        id = entry.getIntOrNull("id") ?: 0,
                        kind = kind,
                        bandwidth = entry.getLongOrNull("bandwidth") ?: 0L,
                        codecs = entry.getStringOrNull("codecs") ?: ""
                    )
                    for (u in normalizedUrls) {
                        val h = hostOf(u)
                        if (h.isBlank() || fileKey(u) != key || !isUsable(u)) continue
                        if (!rep.urls.containsKey(h)) {
                            rep.urls[h] = u
                            rep.issued.add(h)
                        }
                    }
                    reps[key] = rep
                    if (detectedCid == null) {
                        detectedCid = cidOf(key)
                    }
                }
            }
        }
        if (reps.isEmpty()) return null
        return VodSessionTable(cid = detectedCid ?: reps.keys.first(), reps = reps)
    }

    fun urlFor(rep: VodRepresentation?, host: String?): String? {
        if (rep == null || host.isNullOrBlank()) return null
        val targetHost = BiliAcceleratorCore.normalizeHost(host)
        if (targetHost.isBlank()) return null
        rep.urls[targetHost]?.let { return it }
        if (!BiliAcceleratorCore.isUposHost(targetHost)) {
            return null
        }
        val donorSource = rep.issued
            .firstOrNull { h ->
                BiliAcceleratorCore.isUposHost(h) &&
                    rep.urls[h]?.contains("hdnts=", ignoreCase = true) == false
            }
            ?.let { rep.urls[it] }
            ?: return null
        val parsed = BiliAcceleratorCore.parseUri(donorSource) ?: return null
        return parsed.replaceAuthority(targetHost)
    }

    fun candidatesFor(rep: VodRepresentation?, pool: List<String>): List<String> {
        if (rep == null) return emptyList()
        val out = rep.issued.toMutableList()
        for (rawHost in pool) {
            val h = BiliAcceleratorCore.normalizeHost(rawHost)
            if (h.isNotBlank() && !out.contains(h) && urlFor(rep, h) != null) {
                out.add(h)
            }
        }
        return out
    }

    fun recentRate(samples: List<ProgressSample>, nowMs: Long, windowMs: Long = 1000L): Double {
        if (samples.isEmpty()) return 0.0
        val last = samples.last()
        var first = samples.first()
        for (i in samples.indices.reversed()) {
            if (nowMs - samples[i].timeMs <= windowMs) {
                first = samples[i]
            } else {
                break
            }
        }
        if (first === last) {
            first = samples.first()
        }
        val dt = last.timeMs - first.timeMs
        return if (dt > 0L) 8000.0 * (last.loadedBytes - first.loadedBytes).toDouble() / dt.toDouble() else 0.0
    }

    fun stuckVerdict(
        req: InflightRequestSnapshot?,
        nowMs: Long,
        requiredBps: Double,
        bufferAheadS: Double
    ): StuckVerdict? {
        if (req == null || req.total < STUCK_MIN_FRAGMENT_BYTES || !(requiredBps > 0.0)) {
            return null
        }
        val bufferMs = max(0.0, bufferAheadS) * 1000.0
        val firstByteAt = req.firstByteAt
        if (firstByteAt == null) {
            val waited = nowMs - req.startedAt
            if (waited >= STUCK_NO_FIRST_BYTE_MS && bufferAheadS < STUCK_URGENT_BUFFER_S) {
                return StuckVerdict(reason = "no-first-byte", waitedMs = waited)
            }
            return null
        }
        val transferMs = nowMs - firstByteAt
        if (transferMs < STUCK_MIN_TRANSFER_MS && req.loaded < STUCK_MIN_FRAGMENT_BYTES) {
            return null
        }
        val rate = if (req.rateBps > 0.0) {
            req.rateBps
        } else if (transferMs > 0L) {
            8000.0 * req.loaded.toDouble() / transferMs.toDouble()
        } else {
            0.0
        }
        val remainingMs = if (rate > 0.0) {
            (req.total - req.loaded).toDouble() * 8000.0 / rate
        } else {
            Double.POSITIVE_INFINITY
        }
        if (rate < STUCK_RATE_FACTOR * requiredBps &&
            remainingMs > STUCK_MIN_REMAINING_MS &&
            remainingMs > bufferMs - STUCK_MARGIN_MS
        ) {
            return StuckVerdict(reason = "slow-fragment", rateBps = rate, remainingMs = remainingMs)
        }
        return null
    }

    fun evaluate(input: EvaluationInput): EvaluationVerdict? {
        if (!(input.requiredBps > 0.0)) {
            return null
        }
        if (input.recentErrors >= ERROR_LIMIT) {
            return EvaluationVerdict(trigger = "errors")
        }
        for (req in input.inflight) {
            if (stuckVerdict(req, input.nowMs, input.requiredBps, input.bufferAheadS) != null) {
                return EvaluationVerdict(trigger = "stuck", req = req)
            }
        }
        if (input.playerStalling) {
            return EvaluationVerdict(trigger = "stuck", req = input.inflight.firstOrNull())
        }
        val enough = input.measuredBytes >= SHORTFALL_MIN_BYTES || input.measuredMs >= SHORTFALL_MIN_MS
        val est = input.estimateBps
        if (enough && est != null && est < SHORTFALL_RATE_FACTOR * input.requiredBps &&
            input.bufferAheadS < SHORTFALL_BUFFER_S
        ) {
            return EvaluationVerdict(trigger = "shortfall")
        }
        return null
    }

    fun historyScore(history: SpeedHistory?, host: String): Double? {
        val rec = history?.hosts?.get(host.lowercase()) ?: return null
        return if (rec.n > 0.0) rec.mbps else null
    }

    fun recentlyFailed(history: SpeedHistory?, host: String, nowMs: Long): Boolean {
        val rec = history?.hosts?.get(host.lowercase()) ?: return false
        val lastFail = rec.lastFailAt ?: return false
        return nowMs - lastFail < RECENT_FAILURE_MS
    }

    fun recordSample(history: SpeedHistory?, host: String, mbps: Double, nowMs: Long): SpeedHistory {
        val h = history ?: SpeedHistory()
        val clean = host.lowercase()
        val prev = h.hosts[clean] ?: HostHistoryRecord(mbps = 0.0, n = 0.0, at = nowMs)
        val age = max(0L, nowMs - (if (prev.at > 0L) prev.at else nowMs))
        val nEff = prev.n * 0.5.pow(age.toDouble() / HISTORY_HALF_LIFE_MS.toDouble())
        val nextMbps = (prev.mbps * nEff + mbps) / (nEff + 1.0)
        h.hosts[clean] = prev.copy(
            mbps = nextMbps,
            n = nEff + 1.0,
            at = nowMs
        )
        return h
    }

    fun recordFailure(history: SpeedHistory?, host: String, nowMs: Long): SpeedHistory {
        val h = history ?: SpeedHistory()
        val clean = host.lowercase()
        val prev = h.hosts[clean] ?: HostHistoryRecord(mbps = 0.0, n = 0.0, at = nowMs)
        h.hosts[clean] = prev.copy(lastFailAt = nowMs)
        return h
    }

    fun describeHost(host: String?): HostDescription {
        val h = BiliAcceleratorCore.normalizeHost(host)
        if (BiliAcceleratorCore.isAkamaiHost(h)) {
            return HostDescription(region = "overseas", vendor = "akamai", id = "akamai")
        }
        MIRROR_VENDOR_REGEX.matchEntire(h)?.let { m ->
            val code = m.groupValues[1].lowercase()
            val ov = m.groupValues[2].lowercase()
            return HostDescription(
                region = if (ov.isNotEmpty()) "overseas" else "mainland",
                vendor = VENDORS[code],
                id = code + ov
            )
        }
        TF_VENDOR_REGEX.matchEntire(h)?.let { m ->
            val code = m.groupValues[1].lowercase()
            return HostDescription(
                region = "mainland",
                vendor = VENDORS[code],
                id = "tf-$code"
            )
        }
        return HostDescription(region = null, vendor = null, id = h.substringBefore('.').ifBlank { h })
    }

    fun pickChallengers(input: PickChallengersInput): List<String> {
        val current = input.current.lowercase()
        val pool = input.candidates
            .map { it.lowercase() }
            .filter { h ->
                h.isNotBlank() &&
                    h != current &&
                    (input.failed[h] ?: 0) < ERROR_LIMIT &&
                    !(input.lost.containsKey(h) && input.nowMs - (input.lost[h] ?: 0L) < LOST_RACE_REST_MS)
            }
            .distinct()
        if (pool.isEmpty()) return emptyList()

        fun failedLately(h: String): Int = if (recentlyFailed(input.history, h, input.nowMs)) 1 else 0

        val known = pool
            .filter { failedLately(it) == 0 }
            .mapNotNull { historyScore(input.history, it) }
            .sorted()
        val typical = if (known.isNotEmpty()) known[(known.size - 1) / 2] else UNKNOWN_HOST_MBPS

        fun score(h: String): Double = historyScore(input.history, h) ?: typical
        fun overseas(h: String): Int = if (describeHost(h).region == "overseas") 1 else 0

        val ordered = pool.sortedWith { a, b ->
            val failCmp = failedLately(a).compareTo(failedLately(b))
            if (failCmp != 0) return@sortedWith failCmp
            val scoreCmp = score(b).compareTo(score(a))
            if (scoreCmp != 0) return@sortedWith scoreCmp
            overseas(a).compareTo(overseas(b))
        }

        val picks = mutableListOf<String>()
        for (rawIssued in input.issued) {
            val h = rawIssued.lowercase()
            if (picks.isEmpty() && pool.contains(h) && !input.measured.contains(h)) {
                picks.add(h)
            }
        }
        for (h in ordered) {
            if (picks.size < 2 && !picks.contains(h)) {
                picks.add(h)
            }
        }
        if (picks.size == 2 && input.random() < EXPLORE_RATE) {
            val rest = pool.filter { !picks.contains(it) }
            if (rest.isNotEmpty()) {
                val idx = (input.random() * rest.size).toInt().coerceIn(0, rest.lastIndex)
                picks[1] = rest[idx]
            }
        }
        return picks
    }

    fun raceVerdict(
        results: List<RaceContenderResult>,
        currentRateBps: Double,
        raceBytes: Int = RACE_BYTES
    ): RaceVerdict {
        val done = results
            .filter { it.ok && it.ms > 0L }
            .sortedBy { it.ms }
        val bytes = if (raceBytes > 0) raceBytes else RACE_BYTES
        val currentMs = if (currentRateBps > 0.0) {
            bytes.toDouble() * 8000.0 / currentRateBps
        } else {
            Double.POSITIVE_INFINITY
        }
        if (done.isEmpty()) {
            return RaceVerdict(winner = null, runnerUp = null, switchTo = null)
        }
        val winner = done[0]
        val better = winner.ms.toDouble() * SWITCH_GAIN <= currentMs
        return RaceVerdict(
            winner = winner.host,
            runnerUp = done.getOrNull(1)?.host,
            switchTo = if (better) winner.host else null
        )
    }

    fun stuckRaceVerdict(
        results: List<RaceContenderResult>,
        stuckRateBps: Double,
        sustainedBps: Double?,
        recentErrors: Int,
        raceBytes: Int = RACE_BYTES
    ): RaceVerdict {
        val fragment = raceVerdict(results, stuckRateBps, raceBytes)
        val safeStuck = max(0.0, stuckRateBps)
        val hostRateBps = if (recentErrors > 0) {
            safeStuck
        } else {
            max(safeStuck, sustainedBps ?: 0.0)
        }
        val video = raceVerdict(results, hostRateBps, raceBytes)
        val retryOn = if (video.switchTo == null && fragment.switchTo != null) fragment.switchTo else null
        return video.copy(
            hostRateBps = hostRateBps,
            retryOn = retryOn
        )
    }

    fun nextCooldown(kind: String, switches: Int, previousMs: Long = 0L): Long {
        if (kind == "switch") {
            val exp = max(0, switches - 1)
            return (SWITCH_BACKOFF_MS.toDouble() * 2.0.pow(exp.toDouble())).roundToLong()
        }
        val candidate = if (previousMs > 0L) previousMs * 2L else NO_SWITCH_COOLDOWN_MS
        return min(MAX_COOLDOWN_MS, candidate)
    }

    // ---- Stateful runtime engine & PlayerHook integration -------------------

    private val stateLock = Any()
    private val sessions = LinkedHashMap<String, VodRoutingSession>()
    @Volatile
    private var currentSession: VodRoutingSession? = null

    @Volatile
    private var customHistoryStore: SpeedHistoryStore? = null
    private val defaultMemoryHistoryStore = InMemorySpeedHistoryStore()
    @Volatile
    private var cachedFileHistoryStore: FileSpeedHistoryStore? = null
    @Volatile
    private var loadedHistoryRegion: String? = null
    private var activeHistory: SpeedHistory = SpeedHistory()

    @Volatile
    private var rangeFetcher: RangeRaceFetcher = OkHttpRangeRaceFetcher
    @Volatile
    private var randomProvider: () -> Double = { Random.nextDouble() }
    @Volatile
    private var clock: () -> Long = { System.currentTimeMillis() }
    @Volatile
    private var synchronousRaceForTesting: Boolean = false
    @Volatile
    private var stallGraceMsForTesting: Long = STALL_GRACE_MS
    @Volatile
    private var raceCutGraceMsForTesting: Long = RACE_CUT_GRACE_MS

    private val activeHostListeners = CopyOnWriteArrayList<ActiveHostListener>()

    private val raceExecutor: Executor = Executors.newCachedThreadPool { r ->
        Thread(r, "AutoVodRouter-RaceWorker").apply { isDaemon = true }
    }

    private val scheduler: ScheduledExecutorService = Executors.newSingleThreadScheduledExecutor { r ->
        Thread(r, "AutoVodRouter-Scheduler").apply { isDaemon = true }
    }

    fun addActiveHostListener(listener: ActiveHostListener) {
        activeHostListeners.addIfAbsent(listener)
    }

    fun removeActiveHostListener(listener: ActiveHostListener) {
        activeHostListeners.remove(listener)
    }

    private fun dispatchActiveHostNotification(notification: ActiveHostNotification?) {
        if (notification == null) return
        for (listener in activeHostListeners) {
            runCatching {
                listener.onActiveHostChanged(
                    cid = notification.cid,
                    oldHost = notification.oldHost,
                    newHost = notification.newHost,
                    newUrl = notification.newUrl,
                    trigger = notification.trigger
                )
            }
        }
    }

    fun regionKey(): String {
        return try {
            val tz = TimeZone.getDefault().id.ifBlank { "unknown" }
            val lang = Locale.getDefault().toLanguageTag().ifBlank { "unknown" }
            "$tz|$lang"
        } catch (_: Throwable) {
            "unknown"
        }
    }

    fun configureHistoryStore(store: SpeedHistoryStore?) {
        synchronized(stateLock) {
            customHistoryStore = store
            loadedHistoryRegion = null
            activeHistory = resolveHistoryStoreLocked().load(regionKey())
            loadedHistoryRegion = regionKey()
        }
    }

    fun configureHistoryDir(baseDir: File?) {
        if (baseDir == null) return
        configureHistoryStore(FileSpeedHistoryStore(baseDir))
    }

    fun configureRangeFetcher(fetcher: RangeRaceFetcher) {
        rangeFetcher = fetcher
    }

    fun setSynchronousRaceForTesting(sync: Boolean) {
        synchronousRaceForTesting = sync
    }

    fun setRandomProviderForTesting(provider: () -> Double) {
        randomProvider = provider
    }

    fun setClockForTesting(provider: () -> Long) {
        clock = provider
    }

    fun setStallGraceMsForTesting(ms: Long) {
        stallGraceMsForTesting = ms.coerceAtLeast(0L)
    }

    fun setRaceCutGraceMsForTesting(ms: Long) {
        raceCutGraceMsForTesting = ms.coerceAtLeast(0L)
    }

    fun resetForTesting() {
        synchronized(stateLock) {
            for (s in sessions.values) {
                s.pendingStallTask?.cancel(false)
                s.pendingStallTask = null
                s.pendingStallStartMs = null
            }
            sessions.clear()
            currentSession = null
            defaultMemoryHistoryStore.clear()
            customHistoryStore = null
            cachedFileHistoryStore = null
            loadedHistoryRegion = null
            activeHistory = SpeedHistory()
            rangeFetcher = OkHttpRangeRaceFetcher
            randomProvider = { Random.nextDouble() }
            clock = { System.currentTimeMillis() }
            synchronousRaceForTesting = false
            stallGraceMsForTesting = STALL_GRACE_MS
            raceCutGraceMsForTesting = RACE_CUT_GRACE_MS
            activeHostListeners.clear()
        }
    }

    private fun resolveHistoryStoreLocked(): SpeedHistoryStore {
        customHistoryStore?.let { return it }
        cachedFileHistoryStore?.let { return it }
        val ctx = runCatching { RemoteClient.getContext() }.getOrNull()
        val dir = runCatching { ctx?.noBackupFilesDir ?: ctx?.cacheDir }.getOrNull()
        if (dir != null && dir.isDirectory) {
            return FileSpeedHistoryStore(dir).also { cachedFileHistoryStore = it }
        }
        return defaultMemoryHistoryStore
    }

    private fun ensureHistoryLoadedLocked(): SpeedHistory {
        val reg = regionKey()
        if (loadedHistoryRegion != reg) {
            activeHistory = resolveHistoryStoreLocked().load(reg)
            loadedHistoryRegion = reg
        }
        return activeHistory
    }

    private fun saveHistoryLocked() {
        val reg = loadedHistoryRegion ?: regionKey()
        resolveHistoryStoreLocked().save(reg, activeHistory)
    }

    fun getSpeedHistorySnapshot(): SpeedHistory {
        synchronized(stateLock) {
            val h = ensureHistoryLoadedLocked()
            return SpeedHistory(LinkedHashMap(h.hosts))
        }
    }

    fun getCurrentSessionForTesting(): VodRoutingSession? {
        synchronized(stateLock) {
            return currentSession
        }
    }

    fun getSessionForTesting(cid: String): VodRoutingSession? {
        synchronized(stateLock) {
            return sessions[cid]
        }
    }

    /**
     * 判断当前配置是否应启用非 BTR 智能点播路由。
     * - 仅在模块启用、BTR 关闭、且 selectionMode 为 "auto" (默认) 时启用。
     * - 当 selectionMode 为 "manual"/"fixed" 时退回静态节点替换模式。
     */
    fun shouldUseAutoRouting(config: AcceleratorConfig): Boolean {
        return config.enabled &&
            !config.enableBtr &&
            BiliAcceleratorCore.normalizeSelectionMode(config.selectionMode) == "auto"
    }

    /**
     * 对齐 bili-accelerator.page.js rewriteConfig()：
     * 在 auto 模式下仅拦截 PCDN / MCDN / 调度器，不强制将所有健康官方节点改写为静态 targetHost，也不在无 BTR 时改写 Akamai。
     */
    fun effectiveRewriteConfig(config: AcceleratorConfig): AcceleratorConfig {
        if (!shouldUseAutoRouting(config)) return config
        return config.copy(
            forceUpos = false,
            rewriteAkamai = false
        )
    }

    private fun sessionKeyAndFileKey(url: String, playinfoTrack: PlayinfoTrackEntry? = null): Pair<String, String>? {
        val unwrapped = BiliAcceleratorCore.unwrapSmtcdnsUrl(url).ifBlank { url }
        val recovered = BiliAcceleratorCore.extractSchedulerSourceUrl(unwrapped) ?: unwrapped
        val fKey = fileKey(recovered)
            ?: playinfoTrack?.fileKey?.takeIf { it.isNotBlank() }
            ?: BiliAcceleratorCore.parseUri(recovered)?.path?.substringAfterLast('/')?.takeIf { it.isNotBlank() }
            ?: return null
        val cid = playinfoTrack?.cid?.takeIf { it > 0L }?.toString()
            ?: cidOf(fKey)
            ?: playinfoTrack?.pathKey?.takeIf { it.isNotBlank() }
            ?: fKey
        return cid to fKey
    }

    private fun getOrCreateSessionLocked(cid: String): VodRoutingSession {
        var session = sessions[cid]
        if (session == null) {
            session = VodRoutingSession(cid)
            sessions[cid] = session
            if (sessions.size > MAX_SESSIONS) {
                val oldestKey = sessions.keys.firstOrNull { sessions[it] !== currentSession }
                if (oldestKey != null) {
                    sessions.remove(oldestKey)
                }
            }
        }
        return session
    }

    private fun hostStatsLocked(session: VodRoutingSession, host: String): HostStats {
        val clean = host.lowercase()
        return session.hosts.getOrPut(clean) { HostStats() }
    }

    private fun recentErrorsCountLocked(stats: HostStats, nowMs: Long, beforeMs: Long? = null): Int {
        return stats.errors.count { t ->
            nowMs - t < ERROR_WINDOW_MS && (beforeMs == null || t < beforeMs)
        }
    }

    private fun updateSessionRequiredBpsLocked(
        session: VodRoutingSession,
        bandwidthBps: Double,
        isSelectedTrack: Boolean = false
    ) {
        if (!(bandwidthBps > 0.0)) return
        if (isSelectedTrack || !session.hasExplicitRequiredBps) {
            session.requiredBps = bandwidthBps
            session.hasExplicitRequiredBps = true
        } else if (bandwidthBps > session.requiredBps) {
            session.requiredBps = bandwidthBps
        }
    }

    fun ingestPlayinfoJson(jsonText: String, config: AcceleratorConfig = AcceleratorConfig(enableBtr = false)) {
        val autoCfg = effectiveRewriteConfig(config)
        val table = buildTableFromJson(jsonText) { isUsableVodUrl(it, autoCfg) } ?: return
        synchronized(stateLock) {
            val session = getOrCreateSessionLocked(table.cid)
            for ((k, rep) in table.reps) {
                val existing = session.table.reps[k]
                if (existing != null && rep.bandwidth <= 0L && existing.bandwidth > 0L) {
                    rep.bandwidth = existing.bandwidth
                }
                session.table.reps[k] = rep
                if (rep.kind == "video" && rep.bandwidth > 0L) {
                    val isSelected = session.lastVideoKey == k
                    if (isSelected || !session.hasExplicitRequiredBps) {
                        updateSessionRequiredBpsLocked(session, rep.bandwidth.toDouble(), isSelectedTrack = isSelected)
                    }
                }
            }
        }
    }

    fun ingestPlayinfoTracks(
        entries: List<PlayinfoTrackEntry>,
        config: AcceleratorConfig = AcceleratorConfig(enableBtr = false)
    ) {
        if (entries.isEmpty()) return
        val autoCfg = effectiveRewriteConfig(config)
        synchronized(stateLock) {
            for (entry in entries) {
                if (entry.hasDrm || entry.isPreview || entry.primaryUrl.isBlank()) continue
                if (BiliAcceleratorCore.isLiveMediaUrl("", entry.primaryUrl)) continue
                val keyPair = sessionKeyAndFileKey(entry.primaryUrl, entry) ?: continue
                val (cid, fKey) = keyPair
                val session = getOrCreateSessionLocked(cid)
                val urls = listOf(entry.primaryUrl) + entry.backupUrls
                val rep = registerTrackUrlsLocked(
                    session = session,
                    fKey = fKey,
                    urls = urls,
                    isAudio = entry.isAudio,
                    config = autoCfg,
                    updateLastVideoKey = false
                )
                if (entry.bandwidth > 0L) {
                    rep.bandwidth = max(rep.bandwidth, entry.bandwidth)
                    if (!entry.isAudio) {
                        val isSelected = session.lastVideoKey == fKey
                        if (isSelected || !session.hasExplicitRequiredBps) {
                            updateSessionRequiredBpsLocked(session, rep.bandwidth.toDouble(), isSelectedTrack = isSelected)
                        }
                    }
                }
            }
        }
    }

    private fun registerTrackUrlsLocked(
        session: VodRoutingSession,
        fKey: String,
        urls: List<String>,
        isAudio: Boolean,
        config: AcceleratorConfig,
        updateLastVideoKey: Boolean = true
    ): VodRepresentation {
        val kind = if (isAudio) "audio" else "video"
        val rep = session.table.reps.getOrPut(fKey) {
            VodRepresentation(key = fKey, kind = kind)
        }
        for (raw in urls) {
            if (raw.isBlank()) continue
            val step1 = BiliAcceleratorCore.unwrapSmtcdnsUrl(raw).ifBlank { raw }
            val normalized = BiliAcceleratorCore.extractSchedulerSourceUrl(step1) ?: step1
            val h = hostOf(normalized)
            if (h.isBlank()) continue
            if (!isUsableVodUrl(normalized, config)) continue
            if (!rep.urls.containsKey(h)) {
                rep.urls[h] = normalized
                rep.issued.add(h)
            }
        }
        if (kind == "video" && updateLastVideoKey) {
            session.lastVideoKey = fKey
        }
        return rep
    }

    private fun findPromotableCleanBackup(
        originalUrl: String,
        rawBackups: List<String>,
        playinfoTrack: PlayinfoTrackEntry?,
        nowSec: Long = clock() / 1000L
    ): String? {
        val origDeadlineSec = BiliAcceleratorCore.extractUrlDeadlineSeconds(originalUrl)
        val isPlayinfoFresh = playinfoTrack != null && (
            playinfoTrack.deadlineEpochSec <= 0L ||
                (playinfoTrack.deadlineEpochSec > nowSec &&
                    (origDeadlineSec <= 0L || playinfoTrack.deadlineEpochSec >= origDeadlineSec))
            )
        val playinfoUrls = if (isPlayinfoFresh) {
            listOfNotNull(playinfoTrack?.primaryUrl) + playinfoTrack?.backupUrls.orEmpty()
        } else {
            emptyList()
        }
        val candidates = (rawBackups + playinfoUrls)
            .map { u ->
                val step1 = BiliAcceleratorCore.unwrapSmtcdnsUrl(u).ifBlank { u }
                BiliAcceleratorCore.extractSchedulerSourceUrl(step1) ?: step1
            }
            .distinct()
        return candidates.firstOrNull { backup ->
            if (!BiliAcceleratorCore.isCleanUposDonor(backup)) return@firstOrNull false
            val candDeadline = BiliAcceleratorCore.extractUrlDeadlineSeconds(backup)
            if (candDeadline > 0L && candDeadline <= nowSec) return@firstOrNull false
            if (origDeadlineSec > 0L && candDeadline in 1 until origDeadlineSec) return@firstOrNull false
            true
        }
    }

    private fun resolveRoutedTargetHostLocked(session: VodRoutingSession, nowMs: Long): Pair<String, String>? {
        val detour = session.detourHost
        if (!detour.isNullOrBlank() && nowMs < session.detourUntilMs) {
            return detour to "force-upos"
        }
        val active = session.activeHost ?: return null
        val target = if (session.avoidHost == active && nowMs < session.avoidUntilMs) {
            session.fallbackHost ?: active
        } else {
            active
        }
        return target to "force-upos"
    }

    /**
     * 非 BTR 点播主 URL 选择入口（供 PlayerHook.rewriteArgIfMedia 与 rewriteFallbackDataSource 调用）。
     */
    fun selectVodUrl(
        originalUrl: String,
        rawBackups: List<String> = emptyList(),
        playinfoTrack: PlayinfoTrackEntry? = null,
        config: AcceleratorConfig,
        startRaceIfNeeded: Boolean = true,
        nowMs: Long = clock()
    ): RewriteResult {
        if (!config.enabled || !BiliAcceleratorCore.hasMediaSignal(originalUrl)) {
            return BiliAcceleratorCore.noChange(originalUrl, "ignored")
        }
        if (!BiliAcceleratorCore.isMediaUrl(originalUrl)) {
            return BiliAcceleratorCore.noChange(originalUrl, "ignored")
        }
        if (BiliAcceleratorCore.isLiveMediaUrl("", originalUrl)) {
            return BiliAcceleratorCore.rewriteUrl(originalUrl, config)
        }
        if (playinfoTrack?.hasDrm == true || BiliAcceleratorCore.hasDrmSignal(originalUrl)) {
            return BiliAcceleratorCore.noChange(originalUrl, "drm-protected")
        }
        if (!shouldUseAutoRouting(config)) {
            return BiliAcceleratorCore.rewriteUrl(originalUrl, config)
        }

        val autoConfig = effectiveRewriteConfig(config)
        val baseResult = BiliAcceleratorCore.rewriteUrl(originalUrl, autoConfig)
        if (baseResult.reason == "already-local-proxy" ||
            baseResult.reason == "already-proxied" ||
            baseResult.reason == "parse-failed" ||
            baseResult.reason == "drm-protected"
        ) {
            return baseResult
        }

        val isAudio = playinfoTrack?.isAudio == true || BiliAcceleratorCore.isAudioTrack(originalUrl)
        val isSchedulerUnwrap = baseResult.reason == "szbdyd-source" || baseResult.reason == "mountaintoys-source"
        val cleanOfficialBackup = if (
            !isSchedulerUnwrap &&
            (baseResult.changed || baseResult.isPcdn || baseResult.isMcdn || BiliAcceleratorCore.isRelayUrl(originalUrl))
        ) {
            findPromotableCleanBackup(originalUrl, rawBackups, playinfoTrack, nowMs / 1000L)
        } else {
            null
        }

        // 选择当前基准干净 URL（对齐上游 promoteIssued -> rewriteObject 顺序）：
        // - 若原 URL 是调度器 (szbdyd/mountaintoys) 且成功提取 xy_usource，则优先采用还原后的官方 URL
        // - 若原 URL 是 PCDN/MCDN/中继且存在干净官方备份源，优先提权干净官方备份源 (pcdn-promote)，早于 mcdn-proxy 包裹
        // - 仅当无干净官方备份源时，显式 MCDN 才走 mcdn-proxy 或其他 per-URL 规则
        val candidateBaseUrl = when {
            isSchedulerUnwrap -> baseResult.finalUrl
            cleanOfficialBackup != null -> cleanOfficialBackup
            baseResult.changed -> baseResult.finalUrl
            else -> originalUrl
        }

        val keyPair = sessionKeyAndFileKey(candidateBaseUrl, playinfoTrack)
            ?: return if (cleanOfficialBackup != null && cleanOfficialBackup != originalUrl) {
                val promotedHost = hostOf(cleanOfficialBackup)
                baseResult.copy(
                    changed = true,
                    finalUrl = cleanOfficialBackup,
                    targetHost = promotedHost,
                    reason = "pcdn-promote",
                    isMcdn = false
                )
            } else {
                baseResult
            }

        val (cid, fKey) = keyPair
        synchronized(stateLock) {
            ensureHistoryLoadedLocked()
            val session = getOrCreateSessionLocked(cid)
            if (!isAudio) {
                currentSession = session
            }

            // 仅登记 B 站原始下发的 URL（严禁将改写后的 baseResult.finalUrl 写入 rep.issued）
            val allIssuedUrls = buildList {
                add(originalUrl)
                if (cleanOfficialBackup != null) add(cleanOfficialBackup)
                addAll(rawBackups)
                if (playinfoTrack != null) {
                    add(playinfoTrack.primaryUrl)
                    addAll(playinfoTrack.backupUrls)
                }
            }
            val rep = registerTrackUrlsLocked(session, fKey, allIssuedUrls, isAudio, autoConfig)
            val trackBandwidth = playinfoTrack?.bandwidth ?: 0L
            if (trackBandwidth > 0L) {
                rep.bandwidth = max(rep.bandwidth, trackBandwidth)
            }
            if (!isAudio && rep.bandwidth > 0L) {
                updateSessionRequiredBpsLocked(session, rep.bandwidth.toDouble(), isSelectedTrack = true)
            }

            val candidateHost = hostOf(candidateBaseUrl)
            if (!isAudio && candidateHost.isNotBlank() && isUsableVodUrl(candidateBaseUrl, autoConfig)) {
                if (session.assignedHost == null) {
                    session.assignedHost = candidateHost
                }
                session.lastHost = candidateHost
            }

            // 若 session 已经通过竞速选出了更优的 activeHost / detourHost，且当前流可安全换源到该节点，则路由至竞速获胜节点
            val routedTarget = resolveRoutedTargetHostLocked(session, nowMs)
            if (routedTarget != null && !BiliAcceleratorCore.isRelayUrl(candidateBaseUrl)) {
                val (targetHost, switchReason) = routedTarget
                val swappedUrl = urlFor(rep, targetHost)
                if (!swappedUrl.isNullOrBlank()) {
                    if (!isAudio) {
                        session.lastHost = targetHost
                    }
                    val origHost = hostOf(originalUrl)
                    val effectiveReason = when {
                        baseResult.isPcdn -> if (cleanOfficialBackup != null) "pcdn-promote" else "pcdn-host"
                        else -> switchReason
                    }
                    return RewriteResult(
                        changed = swappedUrl != originalUrl,
                        originalUrl = originalUrl,
                        finalUrl = swappedUrl,
                        originalHost = origHost,
                        targetHost = targetHost,
                        reason = if (swappedUrl != originalUrl) effectiveReason else "ok",
                        isPcdn = baseResult.isPcdn,
                        isMcdn = false,
                        segmentKey = baseResult.segmentKey
                    )
                }
            }
        }

        if (cleanOfficialBackup != null && cleanOfficialBackup != originalUrl) {
            val promotedHost = hostOf(cleanOfficialBackup)
            return baseResult.copy(
                changed = true,
                finalUrl = cleanOfficialBackup,
                targetHost = promotedHost,
                reason = "pcdn-promote",
                isMcdn = false
            )
        }
        return baseResult
    }

    /**
     * 根据当前会话的竞速结果与历史得分对非 BTR 备用 URL 列表排序，确保播放器内部重试优先命中竞速胜出节点。
     */
    fun prioritizeBackups(
        primaryUrl: String,
        backups: List<String>,
        config: AcceleratorConfig
    ): List<String> {
        if (!shouldUseAutoRouting(config) || backups.size <= 1) return backups
        synchronized(stateLock) {
            val history = ensureHistoryLoadedLocked()
            val keyPair = sessionKeyAndFileKey(primaryUrl)
            val session = keyPair?.let { sessions[it.first] } ?: currentSession
            val active = session?.activeHost
            val fallback = session?.fallbackHost
            val now = clock()
            return backups.sortedWith { a, b ->
                val ha = hostOf(a)
                val hb = hostOf(b)
                val rankA = when {
                    active != null && ha == active -> 0
                    fallback != null && ha == fallback -> 1
                    recentlyFailed(history, ha, now) -> 3
                    else -> 2
                }
                val rankB = when {
                    active != null && hb == active -> 0
                    fallback != null && hb == fallback -> 1
                    recentlyFailed(history, hb, now) -> 3
                    else -> 2
                }
                if (rankA != rankB) return@sortedWith rankA.compareTo(rankB)
                val scoreA = historyScore(history, ha) ?: -1.0
                val scoreB = historyScore(history, hb) ?: -1.0
                scoreB.compareTo(scoreA)
            }
        }
    }

    // ---- Inflight request tracking & throughput / stall reporting -----------

    fun beginInflightRequest(
        url: String,
        rangeHeader: String? = null,
        totalBytes: Long = -1L,
        requiredBps: Double = 0.0,
        nowMs: Long = clock()
    ): InflightRequestHandle? {
        if (BiliAcceleratorCore.isLiveMediaUrl("", url)) return null
        val keyPair = sessionKeyAndFileKey(url) ?: return null
        val (cid, fKey) = keyPair
        val host = hostOf(url)
        if (host.isBlank()) return null

        val parsedRange = parseRange(rangeHeader)
        val startByte = parsedRange?.start ?: 0L
        val endByte = parsedRange?.end ?: if (totalBytes > 0L) startByte + totalBytes - 1L else -1L
        val resolvedTotal = parsedRange?.length ?: totalBytes
        val isAudio = AUDIO_ID_REGEX.matches(repIdOf(fKey).orEmpty()) || BiliAcceleratorCore.isAudioTrack(url)
        val kind = if (isAudio) "audio" else "video"
        val slot = "$fKey|$startByte"

        synchronized(stateLock) {
            val session = getOrCreateSessionLocked(cid)
            val rep = session.table.reps.getOrPut(fKey) { VodRepresentation(key = fKey, kind = kind) }
            if (isUsableVodUrl(url) && !rep.urls.containsKey(host)) {
                rep.urls[host] = url
                rep.issued.add(host)
            }
            val isRetry = !session.requestedSlots.add(slot)
            val handle = InflightRequestHandle(
                sessionCid = cid,
                key = fKey,
                kind = kind,
                host = host,
                startByte = startByte,
                endByte = endByte,
                totalBytes = resolvedTotal,
                startedAtMs = nowMs,
                retry = isRetry
            )
            if (kind == "video") {
                currentSession = session
                session.lastVideoKey = fKey
                session.lastHost = host
                if (session.assignedHost == null) {
                    session.assignedHost = host
                }
                if (requiredBps > 0.0) {
                    updateSessionRequiredBpsLocked(session, requiredBps, isSelectedTrack = true)
                } else if (rep.bandwidth > 0L) {
                    updateSessionRequiredBpsLocked(session, rep.bandwidth.toDouble(), isSelectedTrack = true)
                }
                session.inflight.add(handle)
            }
            return handle
        }
    }

    fun reportKnownFileSize(url: String, fileSizeBytes: Long) {
        if (fileSizeBytes <= 0L) return
        val keyPair = sessionKeyAndFileKey(url) ?: return
        synchronized(stateLock) {
            val session = sessions[keyPair.first] ?: return
            session.fileSizes[keyPair.second] = fileSizeBytes
        }
    }

    fun reportInflightProgress(
        handle: InflightRequestHandle,
        loadedBytes: Long,
        bufferAheadS: Double = 0.0,
        config: AcceleratorConfig = AcceleratorConfig(enableBtr = false),
        nowMs: Long = clock()
    ): EvaluationVerdict? {
        val outcome: Pair<EvaluationVerdict?, RacePlan?>
        synchronized(stateLock) {
            if (handle.ended) return null
            if (handle.firstByteAtMs == null && loadedBytes > 0L) {
                handle.firstByteAtMs = nowMs
            }
            handle.loadedBytes = loadedBytes
            handle.samples.add(ProgressSample(nowMs, loadedBytes))
            if (handle.samples.size > 60) {
                handle.samples.subList(0, handle.samples.size - 60).clear()
            }
            val session = sessions[handle.sessionCid] ?: return null
            session.lastBufferAheadS = max(0.0, bufferAheadS)
            outcome = evaluateAndPrepareRaceLocked(session, max(0.0, bufferAheadS), playerStalling = false, config = config, nowMs = nowMs)
        }
        outcome.second?.let { executeRacePlan(it) }
        return outcome.first
    }

    fun endInflightRequest(
        handle: InflightRequestHandle,
        httpStatus: Int,
        loadedBytes: Long,
        aborted: Boolean = false,
        bufferAheadS: Double = 10.0,
        config: AcceleratorConfig = AcceleratorConfig(enableBtr = false),
        nowMs: Long = clock()
    ): EvaluationVerdict? {
        val outcome: Pair<EvaluationVerdict?, RacePlan?>
        synchronized(stateLock) {
            handle.ended = true
            val session = sessions[handle.sessionCid] ?: return null
            session.inflight.remove(handle)
            session.lastBufferAheadS = max(0.0, bufferAheadS)

            val missedDeadline = aborted && handle.firstByteAtMs == null && (nowMs - handle.startedAtMs) >= 1800L
            if (aborted && !missedDeadline) {
                return null
            }

            val stats = hostStatsLocked(session, handle.host)
            stats.requests += 1
            val success = !missedDeadline && (httpStatus == 200 || httpStatus == 206) && loadedBytes > 0L
            if (!success) {
                stats.errors.add(nowMs)
                session.failed[handle.host] = (session.failed[handle.host] ?: 0) + 1
                activeHistory = recordFailure(ensureHistoryLoadedLocked(), handle.host, nowMs)
                saveHistoryLocked()
                if (session.activeHost != null && handle.host == session.activeHost) {
                    session.avoidHost = handle.host
                    session.avoidUntilMs = nowMs + AVOID_MS
                }
                outcome = evaluateAndPrepareRaceLocked(session, max(0.0, bufferAheadS), playerStalling = false, config = config, nowMs = nowMs)
            } else {
                stats.bytes += loadedBytes
                if (loadedBytes >= STUCK_MIN_FRAGMENT_BYTES) {
                    session.measured.add(handle.host)
                }
                handle.firstByteAtMs?.let { fb ->
                    stats.ttfbs.add((fb - handle.startedAtMs).coerceAtLeast(0L))
                    if (stats.ttfbs.size > 30) {
                        stats.ttfbs.removeAt(0)
                    }
                }
                if (handle.kind == "video") {
                    val durationMs = (nowMs - handle.startedAtMs).coerceAtLeast(1L)
                    stats.estimator.sample(durationMs, loadedBytes)
                    if (loadedBytes >= STUCK_MIN_FRAGMENT_BYTES) {
                        val mbps = (loadedBytes.toDouble() * 8.0 / 1000.0) / durationMs.toDouble()
                        activeHistory = recordSample(ensureHistoryLoadedLocked(), handle.host, mbps, nowMs)
                        saveHistoryLocked()
                    }
                    if (handle.endByte >= 0L) {
                        val prevEnd = session.ends[handle.key] ?: -1L
                        val knownSize = session.fileSizes[handle.key] ?: -1L
                        val nextEnd = max(prevEnd, handle.endByte)
                        session.ends[handle.key] = if (knownSize > 0L) min(nextEnd, knownSize - 1L) else nextEnd
                    }
                }
                outcome = evaluateAndPrepareRaceLocked(session, max(0.0, bufferAheadS), playerStalling = false, config = config, nowMs = nowMs)
            }
        }
        outcome.second?.let { executeRacePlan(it) }
        return outcome.first
    }

    /**
     * 直接上报一次传输采样（耗时与字节数），更新 Shaka 双半衰期 EWMA 并在满足 shortfall 条件时触发竞速。
     * 注意：默认不累加 session.ends，避免纯带宽估算把探测偏移推进至超过真实文件长度导致 HTTP 416。
     */
    fun reportTransferSample(
        urlOrHost: String,
        durationMs: Long,
        bytes: Long,
        requiredBps: Double = 0.0,
        bufferAheadS: Double = 10.0,
        config: AcceleratorConfig = AcceleratorConfig(enableBtr = false),
        nowMs: Long = clock(),
        advanceByteOffset: Boolean = false
    ): EvaluationVerdict? {
        if (durationMs <= 0L || bytes <= 0L) return null
        val outcome: Pair<EvaluationVerdict?, RacePlan?>
        synchronized(stateLock) {
            val session = resolveTargetSessionLocked(urlOrHost, createIfMissing = urlOrHost.contains("://")) ?: return null
            val host = resolveHostFromUrlOrHost(urlOrHost, session) ?: return null
            if (requiredBps > 0.0) {
                updateSessionRequiredBpsLocked(session, requiredBps, isSelectedTrack = !session.hasExplicitRequiredBps)
            }
            session.lastBufferAheadS = max(0.0, bufferAheadS)
            if (session.assignedHost == null) {
                session.assignedHost = host
            }
            session.lastHost = host

            val stats = hostStatsLocked(session, host)
            stats.requests += 1
            stats.bytes += bytes
            stats.estimator.sample(durationMs, bytes)
            if (bytes >= STUCK_MIN_FRAGMENT_BYTES) {
                session.measured.add(host)
                val mbps = (bytes.toDouble() * 8.0 / 1000.0) / durationMs.toDouble()
                activeHistory = recordSample(ensureHistoryLoadedLocked(), host, mbps, nowMs)
                saveHistoryLocked()
            }
            if (advanceByteOffset) {
                val key = session.lastVideoKey
                if (key != null) {
                    val prevEnd = session.ends[key] ?: -1L
                    val knownSize = session.fileSizes[key] ?: -1L
                    val nextEnd = prevEnd + bytes
                    session.ends[key] = if (knownSize > 0L) min(nextEnd, knownSize - 1L) else nextEnd
                }
            }
            outcome = evaluateAndPrepareRaceLocked(session, max(0.0, bufferAheadS), playerStalling = false, config = config, nowMs = nowMs)
        }
        outcome.second?.let { executeRacePlan(it) }
        return outcome.first
    }

    /**
     * 播放器卡顿事件上报（IjkMediaPlayer MEDIA_INFO_BUFFERING_START = 701）。
     * 对齐上游 bili-accelerator.page.js：启动 2.5s STALL_GRACE_MS 宽限期，若期间收到 702 (MEDIA_INFO_BUFFERING_END) 则取消。
     */
    fun reportPlayerStall(
        urlOrHost: String? = null,
        bufferAheadS: Double = 0.0,
        config: AcceleratorConfig = AcceleratorConfig(enableBtr = false),
        nowMs: Long = clock(),
        immediate: Boolean = false
    ): Boolean {
        if (!shouldUseAutoRouting(config) || !config.stallRecovery) return false
        var immediatePlan: RacePlan? = null
        val triggeredOrScheduled: Boolean
        synchronized(stateLock) {
            val session = resolveTargetSessionLocked(urlOrHost, createIfMissing = false) ?: return false
            val clampedBuf = max(0.0, bufferAheadS)
            session.lastBufferAheadS = clampedBuf
            session.pendingStallTask?.cancel(false)
            session.pendingStallTask = null
            session.pendingStallStartMs = null

            val graceMs = if (immediate) 0L else stallGraceMsForTesting
            if (graceMs <= 0L) {
                session.isStalling = true
                session.stalls += 1
                val (verdict, plan) = evaluateAndPrepareRaceLocked(
                    session = session,
                    bufferAheadS = clampedBuf,
                    playerStalling = true,
                    config = config,
                    nowMs = nowMs
                )
                immediatePlan = plan
                triggeredOrScheduled = verdict != null
            } else {
                session.pendingStallStartMs = nowMs
                session.pendingStallTask = scheduler.schedule(
                    { handleStallGraceTimeout(session, expectedStartMs = nowMs, config = config) },
                    graceMs,
                    TimeUnit.MILLISECONDS
                )
                triggeredOrScheduled = true
            }
        }
        immediatePlan?.let { executeRacePlan(it) }
        return triggeredOrScheduled
    }

    /**
     * 播放器缓冲恢复上报（IjkMediaPlayer MEDIA_INFO_BUFFERING_END = 702 / VIDEO_RENDERING_START）：
     * 若在 2.5s STALL_GRACE_MS 宽限期内恢复，取消待触发的卡顿竞速，避免短暂波动误判。
     */
    fun reportPlayerBufferingEnd(
        urlOrHost: String? = null,
        nowMs: Long = clock()
    ): Boolean {
        synchronized(stateLock) {
            val session = resolveTargetSessionLocked(urlOrHost, createIfMissing = false) ?: return false
            val hadPending = session.pendingStallTask != null || session.pendingStallStartMs != null
            val wasStalling = session.isStalling
            session.pendingStallTask?.cancel(false)
            session.pendingStallTask = null
            session.pendingStallStartMs = null
            session.isStalling = false
            return hadPending || wasStalling
        }
    }

    private fun handleStallGraceTimeout(
        session: VodRoutingSession,
        expectedStartMs: Long,
        config: AcceleratorConfig
    ) {
        var plan: RacePlan? = null
        synchronized(stateLock) {
            if (session.pendingStallStartMs != expectedStartMs) return
            if (sessions[session.cid] !== session) return
            session.pendingStallStartMs = null
            session.pendingStallTask = null
            session.isStalling = true
            session.stalls += 1
            val (_, racePlan) = evaluateAndPrepareRaceLocked(
                session = session,
                bufferAheadS = session.lastBufferAheadS,
                playerStalling = true,
                config = config,
                nowMs = clock()
            )
            plan = racePlan
        }
        plan?.let { executeRacePlan(it) }
    }

    /**
     * 上报播放器当前真实超前缓冲时长（秒），仅更新 bufferAheadS 并评估 shortfall/inflight，绝不将其误标为 playerStalling。
     */
    fun reportPlayerBufferAhead(
        bufferAheadS: Double,
        urlOrHost: String? = null,
        config: AcceleratorConfig = AcceleratorConfig(enableBtr = false),
        nowMs: Long = clock()
    ): Boolean {
        if (!shouldUseAutoRouting(config) || !config.stallRecovery) return false
        val clamped = max(0.0, bufferAheadS)
        val outcome: Pair<EvaluationVerdict?, RacePlan?>
        synchronized(stateLock) {
            val session = resolveTargetSessionLocked(urlOrHost, createIfMissing = false) ?: return false
            session.lastBufferAheadS = clamped
            outcome = evaluateAndPrepareRaceLocked(
                session = session,
                bufferAheadS = clamped,
                playerStalling = false,
                config = config,
                nowMs = nowMs
            )
        }
        outcome.second?.let { executeRacePlan(it) }
        return outcome.first != null
    }

    /**
     * 播放器缓冲进度更新上报（IjkMediaPlayer MEDIA_BUFFERING_UPDATE）：
     * percent 为占总视频时长的 0..100%，严禁直接当作 0..15s 缓冲池或把 percent<=2 判定为卡顿。
     * 仅在提供真实 bufferAheadS 或有效的 (currentPositionMs, durationMs) 时计算实际超前缓冲秒数。
     */
    fun reportPlayerBufferHunger(
        percent: Int = 0,
        urlOrHost: String? = null,
        config: AcceleratorConfig = AcceleratorConfig(enableBtr = false),
        nowMs: Long = clock(),
        currentPositionMs: Long = -1L,
        durationMs: Long = -1L,
        bufferAheadS: Double? = null
    ): Boolean {
        if (!shouldUseAutoRouting(config) || !config.stallRecovery) return false
        val resolvedBufferAheadS = when {
            bufferAheadS != null -> max(0.0, bufferAheadS)
            durationMs > 0L && currentPositionMs >= 0L -> {
                val bufferedPosMs = durationMs.toDouble() * percent.coerceIn(0, 100) / 100.0
                max(0.0, (bufferedPosMs - currentPositionMs.toDouble()) / 1000.0)
            }
            else -> return false
        }
        return reportPlayerBufferAhead(
            bufferAheadS = resolvedBufferAheadS,
            urlOrHost = urlOrHost,
            config = config,
            nowMs = nowMs
        )
    }

    /**
     * 播放器底层带宽提示上报（IjkMediaPlayer MEDIA_INFO_NETWORK_BANDWIDTH = 703，单位 kbps）。
     * 仅更新带宽估计器，严禁推进 session.ends 字节偏移，防止后续 Range 竞速超出真实文件长度引发 HTTP 416。
     */
    fun reportBandwidthHint(
        kbps: Int,
        urlOrHost: String? = null,
        config: AcceleratorConfig = AcceleratorConfig(enableBtr = false),
        nowMs: Long = clock()
    ) {
        if (kbps <= 0 || !shouldUseAutoRouting(config)) return
        val bytesPerSec = (kbps.toLong() * 1000L) / 8L
        if (bytesPerSec < MIN_SAMPLE_BYTES) return
        val bufferAhead = synchronized(stateLock) {
            resolveTargetSessionLocked(urlOrHost, createIfMissing = false)?.lastBufferAheadS
        } ?: return
        reportTransferSample(
            urlOrHost = urlOrHost.orEmpty(),
            durationMs = 1000L,
            bytes = bytesPerSec,
            bufferAheadS = bufferAhead,
            config = config,
            nowMs = nowMs,
            advanceByteOffset = false
        )
    }

    /**
     * 传输错误上报：记录节点错误并在达到阈值 (30s 内 >= 2 次错误) 时触发竞速切源。
     */
    fun reportTransferError(
        urlOrHost: String? = null,
        config: AcceleratorConfig = AcceleratorConfig(enableBtr = false),
        nowMs: Long = clock()
    ): Boolean {
        val outcome: Pair<EvaluationVerdict?, RacePlan?>
        synchronized(stateLock) {
            val session = resolveTargetSessionLocked(urlOrHost, createIfMissing = !urlOrHost.isNullOrBlank() && urlOrHost.contains("://")) ?: return false
            val host = resolveHostFromUrlOrHost(urlOrHost, session) ?: return false
            val stats = hostStatsLocked(session, host)
            stats.requests += 1
            stats.errors.add(nowMs)
            session.failed[host] = (session.failed[host] ?: 0) + 1
            activeHistory = recordFailure(ensureHistoryLoadedLocked(), host, nowMs)
            saveHistoryLocked()
            if (session.activeHost != null && host == session.activeHost) {
                session.avoidHost = host
                session.avoidUntilMs = nowMs + AVOID_MS
            }
            outcome = evaluateAndPrepareRaceLocked(
                session = session,
                bufferAheadS = session.lastBufferAheadS,
                playerStalling = false,
                config = config,
                nowMs = nowMs
            )
        }
        outcome.second?.let { executeRacePlan(it) }
        return outcome.first != null
    }

    /**
     * 手动触发当前会话的线路竞速（对齐 bili-accelerator.page.js retest()，间隔至少 10 秒）。
     */
    fun triggerManualRetest(
        config: AcceleratorConfig = AcceleratorConfig(enableBtr = false),
        nowMs: Long = clock()
    ): Boolean {
        if (!shouldUseAutoRouting(config)) return false
        val plan: RacePlan?
        synchronized(stateLock) {
            val session = currentSession ?: return false
            if (session.isRacing.get() || nowMs - session.manualAtMs < MANUAL_SPACING_MS) {
                return false
            }
            val inflightHandle = session.inflight.firstOrNull {
                it.kind == "video" && it.totalBytes >= STUCK_MIN_FRAGMENT_BYTES
            }
            plan = prepareRaceLocked(session, "manual", inflightHandle, config, nowMs)
        }
        if (plan == null) return false
        executeRacePlan(plan)
        return true
    }

    private fun resolveTargetSessionLocked(
        urlOrHost: String?,
        createIfMissing: Boolean = true
    ): VodRoutingSession? {
        if (!urlOrHost.isNullOrBlank() && urlOrHost.contains("://")) {
            val keyPair = sessionKeyAndFileKey(urlOrHost)
            if (keyPair != null) {
                val s = if (createIfMissing) {
                    getOrCreateSessionLocked(keyPair.first)
                } else {
                    sessions[keyPair.first] ?: return currentSession
                }
                val isAudio = BiliAcceleratorCore.isAudioTrack(urlOrHost)
                val h = hostOf(urlOrHost)
                if (h.isNotBlank()) {
                    registerTrackUrlsLocked(
                        session = s,
                        fKey = keyPair.second,
                        urls = listOf(urlOrHost),
                        isAudio = isAudio,
                        config = AcceleratorConfig(enableBtr = false)
                    )
                    if (!isAudio) {
                        if (s.assignedHost == null) s.assignedHost = h
                        s.lastHost = h
                    }
                }
                if (!isAudio) {
                    currentSession = s
                }
                return s
            }
        }
        return currentSession ?: if (createIfMissing) {
            getOrCreateSessionLocked("default").also { currentSession = it }
        } else {
            null
        }
    }

    private fun resolveHostFromUrlOrHost(urlOrHost: String?, session: VodRoutingSession): String? {
        if (!urlOrHost.isNullOrBlank()) {
            val h = if (urlOrHost.contains("://")) hostOf(urlOrHost) else BiliAcceleratorCore.normalizeHost(urlOrHost)
            if (h.isNotBlank()) return h
        }
        return session.activeHost ?: session.lastHost ?: session.assignedHost
    }

    private fun evaluateAndPrepareRaceLocked(
        session: VodRoutingSession,
        bufferAheadS: Double,
        playerStalling: Boolean,
        config: AcceleratorConfig,
        nowMs: Long
    ): Pair<EvaluationVerdict?, RacePlan?> {
        if (!shouldUseAutoRouting(config) || !config.stallRecovery) {
            return null to null
        }
        if (session.isRacing.get() || nowMs < session.nextRaceAtMs || session.switches.size >= MAX_SWITCHES) {
            return null to null
        }
        val currentHost = session.activeHost ?: session.lastHost ?: session.assignedHost ?: return null to null
        val stats = hostStatsLocked(session, currentHost)
        val inflightSnapshots = session.inflight
            .filter { it.kind == "video" && !it.raced && it.host == currentHost }
            .map { h ->
                InflightRequestSnapshot(
                    total = h.totalBytes,
                    loaded = h.loadedBytes,
                    startedAt = h.startedAtMs,
                    firstByteAt = h.firstByteAtMs,
                    rateBps = recentRate(h.samples, nowMs, 1000L),
                    key = h.key,
                    host = h.host,
                    startByte = h.startByte,
                    endByte = h.endByte,
                    retry = h.retry
                )
            }
        val verdict = evaluate(
            EvaluationInput(
                nowMs = nowMs,
                requiredBps = session.requiredBps,
                bufferAheadS = bufferAheadS,
                inflight = inflightSnapshots,
                estimateBps = stats.estimator.estimate(),
                measuredBytes = stats.estimator.bytes(),
                measuredMs = stats.estimator.ms(),
                recentErrors = recentErrorsCountLocked(stats, nowMs),
                playerStalling = playerStalling
            )
        ) ?: return null to null

        val stuckHandle = if (verdict.trigger == "stuck") {
            session.inflight.firstOrNull { !it.raced && it.host == currentHost && (verdict.req == null || (it.key == verdict.req.key && it.startByte == verdict.req.startByte)) }
        } else {
            null
        }
        val plan = prepareRaceLocked(session, verdict.trigger, stuckHandle, config, nowMs)
        return verdict to plan
    }

    private data class ContenderSpec(
        val host: String,
        val url: String,
        val incumbent: Boolean = false
    )

    private data class RacePlan(
        val session: VodRoutingSession,
        val repKey: String,
        val trigger: String,
        val contenders: List<ContenderSpec>,
        val startByte: Long,
        val endByte: Long,
        val raceBytes: Int,
        val currentRateBps: Double,
        val currentHost: String,
        val stuckHandle: InflightRequestHandle?,
        val raceBaseNowMs: Long,
        val raceStartClockMs: Long
    )

    private class MutableContenderState(val spec: ContenderSpec) {
        var ok: Boolean = false
        var status: Int = 0
        var bytes: Int = 0
        var ms: Long = 0L
        var cut: Boolean = false
        var settled: Boolean = false
        val abortController = RaceAbortController()

        fun snapshot(): RaceContenderResult = RaceContenderResult(
            host = spec.host,
            ok = ok,
            ms = ms,
            bytes = bytes,
            status = status,
            incumbent = spec.incumbent,
            cut = cut,
            settled = settled
        )
    }

    /**
     * 仅返回会话中真实登记过合法签名的表征；严禁伪造无签名的 `/upgcxcode/00/00/...` URL 导致 403/404 污染节点历史分。
     */
    private fun findRaceRepresentationLocked(
        session: VodRoutingSession,
        stuckHandle: InflightRequestHandle?
    ): VodRepresentation? {
        stuckHandle?.key?.let { key ->
            session.table.reps[key]?.takeIf { it.urls.isNotEmpty() }?.let { return it }
        }
        session.lastVideoKey?.let { key ->
            session.table.reps[key]?.takeIf { it.urls.isNotEmpty() }?.let { return it }
        }
        return session.table.reps.values.firstOrNull { it.kind == "video" && it.urls.isNotEmpty() }
            ?: session.table.reps.values.firstOrNull { it.urls.isNotEmpty() }
    }

    private fun prepareRaceLocked(
        session: VodRoutingSession,
        trigger: String,
        stuckHandle: InflightRequestHandle?,
        config: AcceleratorConfig,
        nowMs: Long
    ): RacePlan? {
        val currentHost = session.activeHost ?: session.lastHost ?: session.assignedHost ?: return null
        val rep = findRaceRepresentationLocked(session, stuckHandle) ?: return null

        if (stuckHandle != null && trigger == "stuck") {
            stuckHandle.raced = true
        }
        var startByte = when {
            stuckHandle != null -> stuckHandle.startByte
            session.ends[rep.key] != null && (session.ends[rep.key] ?: -1L) >= 0L -> (session.ends[rep.key] ?: -1L) + 1L
            else -> 0L
        }
        val knownSize = session.fileSizes[rep.key] ?: -1L
        if (knownSize > 0L && startByte >= knownSize) {
            startByte = max(0L, knownSize - RACE_BYTES.toLong())
        }
        if (startByte < 0L) return null
        var endByte = startByte + RACE_BYTES - 1L
        if (knownSize > 0L) {
            endByte = min(endByte, knownSize - 1L)
        }
        if (stuckHandle != null && stuckHandle.endByte >= startByte) {
            endByte = min(endByte, stuckHandle.endByte)
        }
        val raceBytes = (endByte - startByte + 1L).coerceAtLeast(1L).toInt()

        val history = ensureHistoryLoadedLocked()
        val effectivePool = BiliAcceleratorCore.getEffectiveCdnPool(config)
        val challengers = pickChallengers(
            PickChallengersInput(
                candidates = candidatesFor(rep, effectivePool),
                current = currentHost,
                issued = rep.issued,
                measured = session.measured,
                failed = session.failed,
                lost = session.lost,
                history = history,
                nowMs = nowMs,
                random = randomProvider
            )
        )

        var currentRateBps = 0.0
        if (trigger != "errors") {
            currentRateBps = when {
                stuckHandle != null -> {
                    if (stuckHandle.loadedBytes > 0L) {
                        8000.0 * stuckHandle.loadedBytes.toDouble() / max(1L, nowMs - stuckHandle.startedAtMs).toDouble()
                    } else {
                        0.0
                    }
                }
                trigger == "stuck" -> 0.0
                else -> hostStatsLocked(session, currentHost).estimator.estimate() ?: 0.0
            }
        }

        val contenders = challengers.mapNotNull { host ->
            val u = urlFor(rep, host)
            if (u != null) ContenderSpec(host = host, url = u, incumbent = false) else null
        }.toMutableList()

        val currentUrl = urlFor(rep, currentHost)
        if (trigger == "manual" && !(currentRateBps > 0.0) && currentUrl != null && contenders.isNotEmpty()) {
            contenders.add(ContenderSpec(host = currentHost, url = currentUrl, incumbent = true))
        }

        if (contenders.isEmpty()) {
            concludeRaceLocked(
                session = session,
                repKey = rep.key,
                trigger = trigger,
                results = emptyList(),
                currentRateBps = currentRateBps,
                raceBytes = raceBytes,
                currentHost = currentHost,
                stuckHandle = stuckHandle,
                nowMs = nowMs
            )
            return null
        }

        if (!session.isRacing.compareAndSet(false, true)) {
            return null
        }

        return RacePlan(
            session = session,
            repKey = rep.key,
            trigger = trigger,
            contenders = contenders,
            startByte = startByte,
            endByte = endByte,
            raceBytes = raceBytes,
            currentRateBps = currentRateBps,
            currentHost = currentHost,
            stuckHandle = stuckHandle,
            raceBaseNowMs = nowMs,
            raceStartClockMs = clock()
        )
    }

    /**
     * 并发竞速执行器（对齐 bili-accelerator.page.js runRace）：
     * - 所有候选节点同时发起 HTTP Range 请求；
     * - 最快挑战者（或含 incumbent 时达到 SWITCH_GAIN 判定条件）完成即刻决断，使用最新时钟时间戳结束竞速；
     * - 仍在进行的其余候选获得 RACE_CUT_GRACE_MS (1s) 缓冲期采集历史测速，超时后立即取消 (abort) 并在结束后补录历史得分。
     */
    private fun executeRacePlan(plan: RacePlan) {
        val fetcher = rangeFetcher
        val states = plan.contenders.map { MutableContenderState(it) }
        val incumbentState = states.firstOrNull { it.spec.incumbent }
        val hasIncumbent = incumbentState != null
        val waitAllForLegacyMock = synchronousRaceForTesting && fetcher !is CancellableRangeRaceFetcher

        val raceLock = Any()
        var remaining = states.size
        var decided = false
        var decideTimer: ScheduledFuture<*>? = null
        var cutTimer: ScheduledFuture<*>? = null
        var pendingAtDecision: Set<String> = emptySet()
        val decisionLatch = CountDownLatch(1)

        fun freshNowMs(): Long {
            val elapsed = (clock() - plan.raceStartClockMs).coerceAtLeast(0L)
            return plan.raceBaseNowMs + elapsed
        }

        fun decideIfNeeded() {
            val snapshot: List<RaceContenderResult>
            val hasPending: Boolean
            synchronized(raceLock) {
                if (decided) return
                decided = true
                decideTimer?.cancel(false)
                decideTimer = null
                if (incumbentState != null && !incumbentState.settled) {
                    incumbentState.ms = max(1L, clock() - plan.raceStartClockMs)
                }
                pendingAtDecision = states.filter { !it.settled }.map { it.spec.host }.toSet()
                hasPending = pendingAtDecision.isNotEmpty()
                snapshot = states.map { it.snapshot() }
                if (hasPending) {
                    val cutDelay = raceCutGraceMsForTesting
                    val cutAction = Runnable {
                        val toAbort = mutableListOf<RaceAbortController>()
                        synchronized(raceLock) {
                            for (s in states) {
                                if (!s.settled) {
                                    s.cut = true
                                    toAbort.add(s.abortController)
                                }
                            }
                        }
                        for (ctl in toAbort) {
                            ctl.abort()
                        }
                    }
                    if (cutDelay <= 0L) {
                        raceExecutor.execute(cutAction)
                    } else {
                        cutTimer = scheduler.schedule(cutAction, cutDelay, TimeUnit.MILLISECONDS)
                    }
                }
            }

            val notification: ActiveHostNotification?
            try {
                val concludeNowMs = freshNowMs()
                notification = synchronized(stateLock) {
                    concludeRaceLocked(
                        session = plan.session,
                        repKey = plan.repKey,
                        trigger = plan.trigger,
                        results = snapshot,
                        currentRateBps = plan.currentRateBps,
                        raceBytes = plan.raceBytes,
                        currentHost = plan.currentHost,
                        stuckHandle = plan.stuckHandle,
                        nowMs = concludeNowMs
                    )
                }
            } catch (_: Throwable) {
                plan.session.isRacing.set(false)
                decisionLatch.countDown()
                return
            }
            dispatchActiveHostNotification(notification)
            decisionLatch.countDown()
        }

        val minOkBytes = min(plan.raceBytes, 64 * 1024)
        for (state in states) {
            raceExecutor.execute {
                val timeoutTask = scheduler.schedule(
                    { state.abortController.abort() },
                    RACE_TIMEOUT_MS,
                    TimeUnit.MILLISECONDS
                )
                val resp = try {
                    if (fetcher is CancellableRangeRaceFetcher) {
                        fetcher.fetchRange(
                            url = state.spec.url,
                            startByte = plan.startByte,
                            endByte = plan.endByte,
                            timeoutMs = RACE_TIMEOUT_MS,
                            abortController = state.abortController
                        ) { loadedBytes ->
                            synchronized(raceLock) {
                                if (!state.settled) {
                                    state.bytes = loadedBytes
                                    state.ms = max(1L, clock() - plan.raceStartClockMs)
                                }
                            }
                        }
                    } else {
                        fetcher.fetchRange(
                            url = state.spec.url,
                            startByte = plan.startByte,
                            endByte = plan.endByte,
                            timeoutMs = RACE_TIMEOUT_MS
                        )
                    }
                } catch (_: Throwable) {
                    RangeFetchResponse(status = 0, bytes = state.bytes, durationMs = max(1L, clock() - plan.raceStartClockMs))
                } finally {
                    timeoutTask.cancel(false)
                }

                var shouldDecideNow = false
                var lateSnapshot: List<RaceContenderResult>? = null
                synchronized(raceLock) {
                    val elapsed = max(1L, clock() - plan.raceStartClockMs)
                    state.status = resp.status
                    state.bytes = resp.bytes
                    state.ms = if (resp.durationMs > 0L) resp.durationMs else elapsed
                    state.ok = !state.cut && (resp.status == 206 || resp.status == 200) && resp.bytes >= minOkBytes
                    state.settled = true
                    remaining -= 1

                    if (!decided && !waitAllForLegacyMock) {
                        if (state.spec.incumbent || (state.ok && !hasIncumbent)) {
                            shouldDecideNow = true
                        } else if (state.ok && decideTimer == null) {
                            val delayMs = max(
                                0L,
                                (state.ms.toDouble() * SWITCH_GAIN).roundToLong() - (clock() - plan.raceStartClockMs)
                            )
                            decideTimer = scheduler.schedule({ decideIfNeeded() }, delayMs, TimeUnit.MILLISECONDS)
                        }
                    }

                    if (remaining == 0) {
                        cutTimer?.cancel(false)
                        cutTimer = null
                        decideTimer?.cancel(false)
                        decideTimer = null
                        if (!decided) {
                            shouldDecideNow = true
                        } else if (pendingAtDecision.isNotEmpty()) {
                            lateSnapshot = states
                                .filter { it.spec.host in pendingAtDecision }
                                .map { it.snapshot() }
                        }
                    }
                }

                if (shouldDecideNow) {
                    decideIfNeeded()
                } else if (!lateSnapshot.isNullOrEmpty()) {
                    val lateNowMs = freshNowMs()
                    synchronized(stateLock) {
                        learnFromRaceLocked(
                            session = plan.session,
                            repKey = plan.repKey,
                            results = lateSnapshot!!,
                            nowMs = lateNowMs,
                            late = true
                        )
                    }
                }
            }
        }

        if (synchronousRaceForTesting) {
            runCatching {
                decisionLatch.await(RACE_TIMEOUT_MS + 2000L, TimeUnit.MILLISECONDS)
            }
        }
    }

    private fun learnFromRaceLocked(
        session: VodRoutingSession,
        repKey: String,
        results: List<RaceContenderResult>,
        nowMs: Long,
        late: Boolean = false
    ) {
        var history = ensureHistoryLoadedLocked()
        var historyChanged = false
        for (r in results) {
            if (!r.settled) continue
            if (r.status == 416) {
                // HTTP 416 说明探测字节偏移超出文件尾，重置偏移至 0 且绝不污染节点故障记录
                session.ends[repKey] = -1L
                continue
            }
            if (r.ok || (r.cut && r.bytes >= 64 * 1024)) {
                val mbps = (r.bytes.toDouble() * 8.0 / 1000.0) / max(1L, r.ms).toDouble()
                history = recordSample(history, r.host, mbps, nowMs)
                session.measured.add(r.host)
                historyChanged = true
                if (late && r.ok && session.activeHost != null && r.host != session.activeHost && session.fallbackHost == null) {
                    session.fallbackHost = r.host
                }
            } else if (!r.cut) {
                history = recordFailure(history, r.host, nowMs)
                session.failed[r.host] = (session.failed[r.host] ?: 0) + 1
                historyChanged = true
            }
        }
        if (historyChanged) {
            activeHistory = history
            saveHistoryLocked()
        }
    }

    private fun concludeRaceLocked(
        session: VodRoutingSession,
        repKey: String,
        trigger: String,
        results: List<RaceContenderResult>,
        currentRateBps: Double,
        raceBytes: Int,
        currentHost: String,
        stuckHandle: InflightRequestHandle?,
        nowMs: Long
    ): ActiveHostNotification? {
        session.isRacing.set(false)
        learnFromRaceLocked(session, repKey, results, nowMs, late = false)

        val challengers = results.filter { !it.incumbent }
        val incumbent = results.firstOrNull { it.incumbent && it.ok }
        var baseRateBps = if (incumbent != null) {
            incumbent.bytes.toDouble() * 8000.0 / max(1L, incumbent.ms).toDouble()
        } else {
            currentRateBps
        }

        val verdict = if (trigger == "stuck" && incumbent == null) {
            val stats = hostStatsLocked(session, currentHost)
            val errorsBeforeStuck = recentErrorsCountLocked(stats, nowMs, stuckHandle?.startedAtMs)
            val v = stuckRaceVerdict(
                results = challengers,
                stuckRateBps = currentRateBps,
                sustainedBps = stats.estimator.estimate(),
                recentErrors = errorsBeforeStuck,
                raceBytes = raceBytes
            )
            baseRateBps = v.hostRateBps
            v
        } else {
            raceVerdict(challengers, baseRateBps, raceBytes)
        }

        val retryOn = verdict.retryOn
        for (r in results) {
            if (!r.incumbent && r.host != verdict.switchTo && r.host != retryOn && r.status != 416) {
                session.lost[r.host] = nowMs
            }
        }

        session.races.add(
            RaceRecord(
                atMs = nowMs,
                trigger = trigger,
                from = currentHost,
                currentMbps = ((baseRateBps / 1e5).roundToLong() / 10.0),
                contenders = results,
                switchTo = verdict.switchTo,
                retryOn = retryOn
            )
        )
        if (session.races.size > 10) {
            session.races.removeAt(0)
        }

        var notification: ActiveHostNotification? = null
        val rep = session.table.reps[repKey]
            ?: session.lastVideoKey?.let { session.table.reps[it] }
            ?: session.table.reps.values.firstOrNull { it.kind == "video" }

        if (verdict.switchTo != null) {
            if (currentHost != verdict.switchTo) {
                session.measured.add(currentHost)
                session.lost[currentHost] = nowMs
                if (baseRateBps > 0.0) {
                    activeHistory = recordSample(ensureHistoryLoadedLocked(), currentHost, baseRateBps / 1e6, nowMs)
                    saveHistoryLocked()
                }
            }
            session.activeHost = verdict.switchTo
            session.fallbackHost = verdict.runnerUp?.takeIf { it != verdict.switchTo }
            session.avoidHost = null
            session.avoidUntilMs = 0L
            session.detourHost = null
            session.detourUntilMs = 0L
            session.switches.add(
                SwitchRecord(
                    atMs = nowMs,
                    from = currentHost,
                    to = verdict.switchTo,
                    trigger = trigger,
                    beforeMbps = ((baseRateBps / 1e5).roundToLong() / 10.0)
                )
            )
            session.cooldownMs = 0L
            session.nextRaceAtMs = nowMs + nextCooldown("switch", session.switches.size)
            notification = ActiveHostNotification(
                cid = session.cid,
                oldHost = currentHost,
                newHost = verdict.switchTo,
                newUrl = urlFor(rep, verdict.switchTo),
                trigger = trigger
            )
        } else if (retryOn != null) {
            // 仅将当前单次卡顿分片的重试临时绕行至胜出节点 (detour)，主视频流保留在原健康节点，同时记录一次悬挂错误
            if (session.activeHost == null) {
                session.activeHost = currentHost
            }
            session.fallbackHost = retryOn
            session.detourHost = retryOn
            session.detourUntilMs = nowMs + AVOID_MS
            hostStatsLocked(session, stuckHandle?.host ?: currentHost).errors.add(nowMs)
            session.nextRaceAtMs = nowMs + AVOID_MS
            notification = ActiveHostNotification(
                cid = session.cid,
                oldHost = currentHost,
                newHost = retryOn,
                newUrl = urlFor(rep, retryOn),
                trigger = "$trigger-detour"
            )
        } else {
            session.cooldownMs = nextCooldown("none", 0, session.cooldownMs)
            session.nextRaceAtMs = nowMs + session.cooldownMs
        }

        if (trigger == "manual") {
            session.manualAtMs = nowMs
        }
        return notification
    }

    // ---- JSON helpers -------------------------------------------------------

    private fun JsonObject.getAsJsonObjectOrNull(key: String): JsonObject? {
        val elem = get(key) ?: return null
        return if (elem.isJsonObject) elem.asJsonObject else null
    }

    private fun JsonObject.getAsJsonArrayOrNull(key: String): JsonArray? {
        val elem = get(key) ?: return null
        return if (elem.isJsonArray) elem.asJsonArray else null
    }

    private fun JsonObject.getStringOrNull(key: String): String? {
        val elem = get(key) ?: return null
        return if (elem.isJsonPrimitive && elem.asJsonPrimitive.isString) elem.asString else null
    }

    private fun JsonObject.getIntOrNull(key: String): Int? {
        val elem = get(key) ?: return null
        return if (elem.isJsonPrimitive) runCatching { elem.asInt }.getOrNull() else null
    }

    private fun JsonObject.getLongOrNull(key: String): Long? {
        val elem = get(key) ?: return null
        return if (elem.isJsonPrimitive) runCatching { elem.asLong }.getOrNull() else null
    }
}
