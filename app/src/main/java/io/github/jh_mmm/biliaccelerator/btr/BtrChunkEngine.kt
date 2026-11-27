package io.github.jh_mmm.biliaccelerator.btr

import android.util.Log
import io.github.jh_mmm.biliaccelerator.core.BiliAcceleratorCore
import io.github.jh_mmm.biliaccelerator.core.PlayinfoRegistry
import io.github.jh_mmm.biliaccelerator.core.RewriteResult
import io.github.jh_mmm.biliaccelerator.core.SharedConcurrencyState
import io.github.jh_mmm.biliaccelerator.hook.RemoteClient
import okhttp3.Call
import okhttp3.Request
import java.io.IOException
import java.io.OutputStream
import java.net.SocketException
import java.util.Locale
import java.util.concurrent.CompletableFuture
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.RejectedExecutionException
import java.util.concurrent.SynchronousQueue
import java.util.concurrent.ThreadFactory
import java.util.concurrent.ThreadPoolExecutor
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.atomic.AtomicInteger
import java.util.concurrent.atomic.AtomicLong
import java.util.concurrent.atomic.AtomicReference

object BtrChunkEngine {

    private const val TAG = "BiliAccelerator-BTR"
    const val CHUNK_SIZE_AUDIO = 128 * 1024
    const val CHUNK_SIZE_VIDEO = 512 * 1024
    const val AUDIO_MAX_CONCURRENCY = 2
    const val PROBE_PREFIX_BYTES = 64 * 1024

    private const val MAX_PREFETCH_CHUNKS = 8           // 基础滑动窗口参考值
    private const val MAX_BUFFER_BYTES = 16L * 1024 * 1024 // 单流内存缓存上限 16MB，防 OOM 同时避免队头阻塞
    internal const val FALLBACK_PLAYER_USER_AGENT = "Bilibili Freedoooooom/MarkII"
    internal const val WEB_COMPAT_USER_AGENT = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36"

    internal fun resolveDefaultUserAgent(): String {
        return RemoteClient.resolveHostPlayerUserAgent().takeIf { it.isNotBlank() }
            ?: FALLBACK_PLAYER_USER_AGENT
    }

    private fun buildUpstreamRequest(
        url: String,
        userAgent: String,
        referer: String?,
        extraHeaders: Map<String, String> = emptyMap(),
        rangeHeader: String? = null,
        closeConnection: Boolean = false,
        isHeadOnly: Boolean = false
    ): Request {
        val builder = Request.Builder().url(url)
        for ((k, v) in extraHeaders) {
            if (k.isNotBlank() && v.isNotBlank()) {
                builder.header(k, v)
            }
        }
        if (!rangeHeader.isNullOrBlank()) {
            builder.header("Range", rangeHeader)
        }
        if (!referer.isNullOrBlank()) {
            builder.header("Referer", referer)
        }
        builder.header("User-Agent", userAgent)
        if (closeConnection) {
            builder.header("Connection", "close")
        }
        if (isHeadOnly) {
            builder.head()
        }
        return builder.build()
    }

    /**
     * 403 请求头兼容性备选画像：当首片探针遭遇 403 且 URL 未过期时，
     * 有限度切换 Referer 有无及 UA（BiliDroid / MarkII / 桌面 Web Chrome），避免因客户端头与 CDN 校验口径不匹配导致全节点误熔断。
     */
    internal fun computeAlternateHeaderProfiles(
        session: BtrMediaSession,
        currentUserAgent: String,
        currentReferer: String?
    ): List<Pair<String, String?>> {
        val defaultWebRef = BtrSessionManager.defaultWebRefererFor(
            isEpisode = session.isEpisode,
            isLive = session.isLive
        )
        val defaultPlayerUa = resolveDefaultUserAgent()
        val rawProfiles = if (currentReferer.isNullOrBlank()) {
            listOf(
                currentUserAgent to defaultWebRef,
                FALLBACK_PLAYER_USER_AGENT to null,
                WEB_COMPAT_USER_AGENT to defaultWebRef
            )
        } else {
            listOf(
                FALLBACK_PLAYER_USER_AGENT to null,
                WEB_COMPAT_USER_AGENT to currentReferer,
                defaultPlayerUa to null
            )
        }
        val currentKey = "$currentUserAgent\u0000${currentReferer ?: ""}"
        val seen = LinkedHashSet<String>()
        seen.add(currentKey)
        val result = ArrayList<Pair<String, String?>>(3)
        for (pair in rawProfiles) {
            val key = "${pair.first}\u0000${pair.second ?: ""}"
            if (seen.add(key)) {
                result.add(pair)
                if (result.size >= 3) break
            }
        }
        return result
    }
    private const val RESUME_MIN_BYTES = 32 * 1024      // 断点续传拼接最低字节门槛（对齐 BTR 原版）
    internal const val SPEED_SAMPLE_MIN_BYTES = 48 * 1024L // 测速最小字节门槛（对齐 BTR SPEED_SAMPLE_MIN_BYTES = 48 KiB）
    private const val CANCEL_SPEED_MIN_BYTES = SPEED_SAMPLE_MIN_BYTES // 被取消竞争请求记录历史带宽门槛（对齐 BTR 48KB）
    private const val DEFAULT_HEDGE_DELAY_MS = 1200L    // 对冲请求最大延迟触发阈值（毫秒）
    internal const val MIN_HEDGE_DELAY_MS = 250L        // 动态对冲最小宽限/触发延迟（对齐 BTR 250ms）
    internal const val HEDGE_PACE_RATIO = 0.55          // 主请求速率低于均速 55% 时触发早对冲救援
    internal const val HEDGE_FASTER_RATIO = 1.5         // 备选节点已知速率 >= 主路 1.5x 时触发对冲
    internal const val HEDGE_STALL_IDLE_MS = 350L       // 主路收到部分字节后无新增进度超过 350ms 视为停滞
    internal const val HEDGE_EARLY_MIN_ETA_MS = 400.0   // 预计剩余耗时 >= 400ms 才值得提前唤醒对冲
    internal const val FIRST_BYTE_TIMEOUT_MS = 5_500L   // 对齐 BTR range-core.js firstByteTimeoutMs = 5500ms
    internal const val STALL_TIMEOUT_MS = 4_000L        // 对齐 BTR range-core.js stallTimeoutMs = 4000ms
    internal const val AUTO_INITIAL_CONCURRENCY = 6
    internal const val AUTO_MAX_CONCURRENCY = 16
    private const val ADAPTIVE_MIN_CHUNK_BYTES = 128 * 1024L
    private const val ADAPTIVE_CHUNK_STEP_BYTES = 64 * 1024L
    internal const val UNMEASURED_WARMUP_TARGET_NODES = 6
    internal const val UNMEASURED_TRIAL_INTERVAL_CHUNKS = 4L
    private val CRLF_BYTES = "\r\n".toByteArray(Charsets.US_ASCII)
    private val CHUNK_TERMINATOR_BYTES = "0\r\n\r\n".toByteArray(Charsets.US_ASCII)
    private const val PIECE_ROUNDS = 3                  // 单分片最大重排竞速轮次（对齐 BTR PIECE_ROUNDS）
    private const val SPEED_CUTOFF_RATIO = 12.0         // 淘汰低于最高带宽 1/12 的慢节点（对齐 BTR）
    private const val CONCURRENCY_PERSIST_TTL_MS = 30 * 60 * 1000L
    private const val CONCURRENCY_DISK_THROTTLE_MS = 3_000L
    private val CONCURRENCY_LADDER = intArrayOf(2, 4, 6, 8, 12, 16)

    @Volatile
    internal var firstByteTimeoutMsForTesting: Long = FIRST_BYTE_TIMEOUT_MS

    @Volatile
    internal var stallTimeoutMsForTesting: Long = STALL_TIMEOUT_MS

    private class ClientDisconnectException(cause: IOException) : IOException(cause)

    /**
     * 显式追踪调用是被主动取消（胜出取消/客户端断开）还是超时中止（OkHttp callTimeout / 首字节 / 停滞看门狗）。
     * OkHttp 4.12.0 中 callTimeout 触发后会调用 call.cancel() 使 call.isCanceled() == true，
     * 因此严禁直接用 !call.isCanceled() 判断是否需要惩罚慢/黑洞节点。
     */
    private class TrackedCall(
        val call: Call,
        val callerCanceled: AtomicBoolean = AtomicBoolean(false),
        val timedOut: AtomicBoolean = AtomicBoolean(false),
        val timeoutReason: AtomicReference<String?> = AtomicReference(null)
    ) {
        fun cancelByCaller() {
            callerCanceled.set(true)
            runCatching { call.cancel() }
        }

        fun cancelByTimeout(reason: String) {
            if (!callerCanceled.get()) {
                timedOut.set(true)
                timeoutReason.compareAndSet(null, reason)
            }
            runCatching { call.cancel() }
        }

        fun isCallerCanceled(): Boolean = callerCanceled.get()
        fun isTimedOut(): Boolean = timedOut.get()
    }

    internal fun isTimeoutError(
        e: Throwable,
        callCanceled: Boolean,
        callerCanceled: Boolean,
        timedOutFlag: Boolean
    ): Boolean {
        if (timedOutFlag) return true
        if (callerCanceled) return false
        if (e is java.net.SocketTimeoutException || e is java.io.InterruptedIOException) return true
        if (callCanceled) return true // OkHttp callTimeout 内部触发 call.cancel()，但 callerCanceled 为 false
        var cur: Throwable? = e.cause
        while (cur != null) {
            if (cur is java.net.SocketTimeoutException || cur is java.io.InterruptedIOException) return true
            cur = cur.cause
        }
        return false
    }

    /**
     * 首字节超时（默认 5.5s）与传输停滞看门狗（默认 4s），对齐 BTR range-core.js / idm-downloader.js：
     * - 收到响应头/首字节前仅挂载一次首字节定时器；
     * - 收到响应头后切换为低开销停滞检查（读循环仅更新 AtomicLong，不逐包重建调度任务）；
     * - 超时触发 trackedCall.cancelByTimeout(...)，明确区分超时与主动取消。
     */
    private class AttemptTimeoutWatchdog(
        private val trackedCall: TrackedCall,
        private val firstByteTimeoutMs: Long = firstByteTimeoutMsForTesting,
        private val stallTimeoutMs: Long = stallTimeoutMsForTesting
    ) : AutoCloseable {
        private val closed = AtomicBoolean(false)
        private val gotFirstByte = AtomicBoolean(false)
        private val lastProgressAtMs = AtomicLong(System.currentTimeMillis())
        @Volatile
        private var scheduledFuture: java.util.concurrent.ScheduledFuture<*>? = null

        init {
            if (firstByteTimeoutMs > 0L) {
                scheduledFuture = runCatching {
                    watchdogScheduler.schedule({
                        if (!closed.get() && !gotFirstByte.get()) {
                            trackedCall.cancelByTimeout("first_byte_timeout")
                        }
                    }, firstByteTimeoutMs, TimeUnit.MILLISECONDS)
                }.getOrNull()
            }
        }

        fun onResponseHeaders() {
            if (closed.get()) return
            lastProgressAtMs.set(System.currentTimeMillis())
            if (gotFirstByte.compareAndSet(false, true)) {
                scheduledFuture?.cancel(false)
                scheduledFuture = null
                if (stallTimeoutMs > 0L) {
                    scheduleStallCheck(stallTimeoutMs)
                }
            }
        }

        fun onBytesRead() {
            if (closed.get()) return
            lastProgressAtMs.set(System.currentTimeMillis())
            if (gotFirstByte.compareAndSet(false, true)) {
                scheduledFuture?.cancel(false)
                scheduledFuture = null
                if (stallTimeoutMs > 0L) {
                    scheduleStallCheck(stallTimeoutMs)
                }
            }
        }

        private fun scheduleStallCheck(delayMs: Long) {
            if (closed.get()) return
            scheduledFuture = runCatching {
                watchdogScheduler.schedule({
                    if (closed.get()) return@schedule
                    val idleMs = System.currentTimeMillis() - lastProgressAtMs.get()
                    if (idleMs >= stallTimeoutMs) {
                        trackedCall.cancelByTimeout("stall_timeout")
                    } else {
                        scheduleStallCheck((stallTimeoutMs - idleMs).coerceAtLeast(50L))
                    }
                }, delayMs.coerceAtLeast(50L), TimeUnit.MILLISECONDS)
            }.getOrNull()
        }

        override fun close() {
            if (closed.compareAndSet(false, true)) {
                scheduledFuture?.cancel(false)
                scheduledFuture = null
            }
        }
    }

    internal fun shouldAllowUnmeasuredTrial(
        taskIndex: Long,
        totalTasks: Long,
        measuredCount: Int = 1,
        unmeasuredCount: Int = 1
    ): Boolean {
        if (unmeasuredCount <= 0 || totalTasks <= 1L || taskIndex <= 0L) return false
        // 暖机期（已测速健康节点数 < 6）：对齐上游 cdn-resolver.js warm-up，
        // 保护第 0 片及每第 3 片走已验证快节点，其余并发起播切片快速分散探明未测速节点，避免单节点被并发打爆 412/429
        if (measuredCount < UNMEASURED_WARMUP_TARGET_NODES) {
            if (totalTasks <= 3L) {
                return measuredCount <= 1 && taskIndex == totalTasks - 1L
            }
            return taskIndex % 3L != 0L
        }
        // 稳态探索期：对齐上游 idm-downloader.js floor(count / 4) 配额（每 4 片 + 尾窗试探）
        if (totalTasks < UNMEASURED_TRIAL_INTERVAL_CHUNKS) return false
        if (taskIndex % UNMEASURED_TRIAL_INTERVAL_CHUNKS == 0L) return true
        val tailQuota = maxOf(1L, minOf(3L, totalTasks / UNMEASURED_TRIAL_INTERVAL_CHUNKS))
        return taskIndex >= totalTasks - tailQuota
    }

    @Volatile
    private var persistedConcurrencyState: SharedConcurrencyState? = null
    private val lastConcurrencyDiskWriteMs = AtomicLong(0L)

    internal fun loadPersistedConcurrencyState(nowMs: Long = System.currentTimeMillis()): SharedConcurrencyState? {
        val fromDisk = PlayinfoRegistry.loadSharedConcurrencyStateIfUpdated()
            ?: if (persistedConcurrencyState == null) PlayinfoRegistry.loadSharedConcurrencyState() else null
        if (fromDisk != null && (persistedConcurrencyState == null || fromDisk.updatedAtMs >= (persistedConcurrencyState?.updatedAtMs ?: 0L))) {
            persistedConcurrencyState = fromDisk
        }
        val state = persistedConcurrencyState ?: return null
        if (state.updatedAtMs > 0L && (nowMs - state.updatedAtMs) > CONCURRENCY_PERSIST_TTL_MS) {
            return null
        }
        return state
    }

    internal fun savePersistedConcurrencyState(
        state: SharedConcurrencyState,
        forceDiskWrite: Boolean = false
    ) {
        persistedConcurrencyState = state
        val now = System.currentTimeMillis()
        val prev = lastConcurrencyDiskWriteMs.get()
        if (forceDiskWrite || (now - prev) >= CONCURRENCY_DISK_THROTTLE_MS) {
            lastConcurrencyDiskWriteMs.set(now)
            PlayinfoRegistry.saveSharedConcurrencyState(state)
        }
    }

    internal fun resetConcurrencyPersistenceForTesting(clearDisk: Boolean = true) {
        persistedConcurrencyState = null
        lastConcurrencyDiskWriteMs.set(0L)
        if (clearDisk) {
            PlayinfoRegistry.clearSharedConcurrencyState()
        }
    }

    private fun daemonThreadFactory(prefix: String): ThreadFactory {
        val seq = AtomicInteger(0)
        return ThreadFactory { r ->
            Thread(r, "$prefix-${seq.incrementAndGet()}").apply { isDaemon = true }
        }
    }

    // 视频分片专用线程池：使用 SynchronousQueue 直接弹性扩容至 max=96，避免有界队列卡死在 corePoolSize
    private val videoWorkerPool = ThreadPoolExecutor(
        8,
        96,
        60L,
        TimeUnit.SECONDS,
        SynchronousQueue(),
        daemonThreadFactory("BTR-VideoWorker")
    ).apply {
        allowCoreThreadTimeOut(true)
    }

    // 音频分片隔离线程池：独立于视频高并发队列，保证音频帧低延迟不饥饿
    private val audioWorkerPool = ThreadPoolExecutor(
        2,
        24,
        60L,
        TimeUnit.SECONDS,
        SynchronousQueue(),
        daemonThreadFactory("BTR-AudioWorker")
    ).apply {
        allowCoreThreadTimeOut(true)
    }

    private fun workerPoolFor(isAudio: Boolean): ThreadPoolExecutor =
        if (isAudio) audioWorkerPool else videoWorkerPool

    // 对冲竞速专属独立执行线程池：与主拉取线程隔离，彻底杜绝冲刺期对冲排队饥饿
    private val hedgeWorkerPool = ThreadPoolExecutor(
        0,
        96,
        60L,
        TimeUnit.SECONDS,
        SynchronousQueue(),
        daemonThreadFactory("BTR-HedgeWorker")
    )

    // 全局分发调度线程池
    private val globalDispatcherPool = ThreadPoolExecutor(
        0,
        64,
        60L,
        TimeUnit.SECONDS,
        SynchronousQueue(),
        daemonThreadFactory("BTR-Dispatcher")
    )

    // 首字节超时（5.5s）与传输停滞超时（4s）看门狗定时池
    private val watchdogScheduler = java.util.concurrent.ScheduledThreadPoolExecutor(1) { r ->
        Thread(r, "BTR-TimeoutWatchdog").apply { isDaemon = true }
    }.apply {
        setKeepAliveTime(30L, TimeUnit.SECONDS)
        allowCoreThreadTimeOut(true)
        removeOnCancelPolicy = true
    }

    // 活跃网络调用与对冲等待锁映射表：支持按 streamCallKey 细粒度隔离与按 sessionId 会话级全量清理
    private val streamActiveCalls = ConcurrentHashMap<String, MutableSet<TrackedCall>>()
    private val sessionStreamKeys = ConcurrentHashMap<String, MutableSet<String>>()
    private val streamSessionMap = ConcurrentHashMap<String, String>()
    private val streamHedgeLocks = ConcurrentHashMap<String, MutableSet<Object>>()
    private val sessionSwrrWeights = ConcurrentHashMap<String, MutableMap<String, Double>>()
    private val sessionTrialCursors = ConcurrentHashMap<String, AtomicInteger>()
    private val sessionInFlightTrials = ConcurrentHashMap<String, MutableSet<String>>()
    private val transferSeq = AtomicLong(0L)

    private fun bindStreamSession(streamCallKey: String, sessionId: String) {
        streamSessionMap[streamCallKey] = sessionId
        sessionStreamKeys.computeIfAbsent(sessionId) { ConcurrentHashMap.newKeySet() }.add(streamCallKey)
    }

    private fun registerStreamCall(streamCallKey: String, sessionId: String, call: TrackedCall) {
        bindStreamSession(streamCallKey, sessionId)
        streamActiveCalls.computeIfAbsent(streamCallKey) { ConcurrentHashMap.newKeySet() }.add(call)
    }

    private fun unregisterStreamCall(streamCallKey: String, call: TrackedCall) {
        streamActiveCalls[streamCallKey]?.remove(call)
    }

    private fun registerHedgeLock(streamCallKey: String, hedgeLock: Object) {
        streamHedgeLocks.computeIfAbsent(streamCallKey) { ConcurrentHashMap.newKeySet() }.add(hedgeLock)
    }

    private fun unregisterHedgeLock(streamCallKey: String, hedgeLock: Object) {
        streamHedgeLocks[streamCallKey]?.remove(hedgeLock)
    }

    private fun cancelStreamCalls(streamCallKey: String) {
        streamActiveCalls.remove(streamCallKey)?.forEach { tracked ->
            tracked.cancelByCaller()
        }
        streamHedgeLocks.remove(streamCallKey)?.forEach { lockObj ->
            synchronized(lockObj) { lockObj.notifyAll() }
        }
        streamSessionMap.remove(streamCallKey)?.let { sid ->
            sessionStreamKeys[sid]?.let { keys ->
                keys.remove(streamCallKey)
                if (keys.isEmpty()) {
                    sessionStreamKeys.remove(sid, keys)
                }
            }
        }
    }

    fun cancelSessionCalls(sessionId: String) {
        sessionStreamKeys.remove(sessionId)?.toList()?.forEach { streamCallKey ->
            cancelStreamCalls(streamCallKey)
        }
        sessionSwrrWeights.remove(sessionId)
        sessionTrialCursors.remove(sessionId)
        sessionInFlightTrials.remove(sessionId)
    }

    // 节点 / 路由 EWMA 测速追踪器（对齐 BTR routeKey = host + path 隔离策略）
    private class HostSpeedInfo(
        @Volatile var avgBps: Double = 0.0,
        @Volatile var lastMeasuredAt: Long = 0L,
        @Volatile var sampleCount: Int = 0
    )

    private val hostSpeeds = ConcurrentHashMap<String, HostSpeedInfo>()
    private const val SPEED_MEASURE_TTL_MS = 90_000L
    private const val MAX_TRACKED_HOSTS = 512

    internal fun routeKey(url: String): String {
        val uri = BiliAcceleratorCore.parseUri(url) ?: return extractHostname(url).lowercase()
        val host = BiliAcceleratorCore.normalizeHost(uri.host)
        val path = uri.path
        return if (path.isNotBlank() && path != "/") "$host$path" else host
    }

    fun recordRouteSpeed(url: String, bytes: Long, latencyMs: Long) {
        val key = routeKey(url)
        if (key.isNotBlank()) {
            recordHostSpeed(key, bytes, latencyMs)
        }
    }

    fun getRouteSpeed(url: String): Double {
        val key = routeKey(url)
        return if (key.isNotBlank()) getHostSpeed(key) else 0.0
    }

    fun recordHostSpeed(host: String, bytes: Long, latencyMs: Long) {
        if (host.isBlank() || bytes < SPEED_SAMPLE_MIN_BYTES || latencyMs <= 0) return
        val bps = (bytes * 1000.0) / latencyMs
        val now = System.currentTimeMillis()
        if (hostSpeeds.size > MAX_TRACKED_HOSTS) {
            hostSpeeds.entries.removeIf { now - it.value.lastMeasuredAt > SPEED_MEASURE_TTL_MS }
        }
        val info = hostSpeeds.getOrPut(host) { HostSpeedInfo() }
        synchronized(info) {
            if (now - info.lastMeasuredAt > SPEED_MEASURE_TTL_MS || info.avgBps <= 0.0) {
                info.avgBps = bps
            } else {
                // EWMA 衰减平滑：70% 历史 + 30% 新测速
                info.avgBps = info.avgBps * 0.7 + bps * 0.3
            }
            info.lastMeasuredAt = now
            info.sampleCount++
        }
    }

    fun getHostSpeed(host: String): Double {
        val info = hostSpeeds[host] ?: return 0.0
        val now = System.currentTimeMillis()
        return if (now - info.lastMeasuredAt < SPEED_MEASURE_TTL_MS) info.avgBps else 0.0
    }

    internal fun resetRouteSpeedsAndPenaltiesForTesting() {
        hostSpeeds.clear()
        cdnPenalties.clear()
        sessionSwrrWeights.clear()
        sessionTrialCursors.clear()
        sessionInFlightTrials.clear()
    }

    // 全局分片耗时与单连接速率 EWMA 计量器（对齐 BTR idm-downloader.js meter）
    private class TransferMeter {
        private val lock = Any()
        private var connectionBps: Double = 0.0
        private var pieceMs: Double = 0.0
        private var lastMeasuredAt: Long = 0L

        fun record(bytes: Long, elapsedMs: Long, nowMs: Long = System.currentTimeMillis()) {
            if (bytes < CANCEL_SPEED_MIN_BYTES || elapsedMs <= 0L) return
            val bps = (bytes * 1000.0) / elapsedMs
            synchronized(lock) {
                if (lastMeasuredAt <= 0L || (nowMs - lastMeasuredAt) > SPEED_MEASURE_TTL_MS || connectionBps <= 0.0) {
                    connectionBps = bps
                    pieceMs = elapsedMs.toDouble()
                } else {
                    connectionBps = connectionBps * 0.7 + bps * 0.3
                    pieceMs = pieceMs * 0.7 + elapsedMs * 0.3
                }
                lastMeasuredAt = nowMs
            }
        }

        fun snapshot(nowMs: Long = System.currentTimeMillis()): Pair<Double, Double> {
            synchronized(lock) {
                if (lastMeasuredAt <= 0L || (nowMs - lastMeasuredAt) > SPEED_MEASURE_TTL_MS) {
                    return 0.0 to 0.0
                }
                return connectionBps to pieceMs
            }
        }

        fun reset() {
            synchronized(lock) {
                connectionBps = 0.0
                pieceMs = 0.0
                lastMeasuredAt = 0L
            }
        }
    }

    private val videoTransferMeter = TransferMeter()
    private val audioTransferMeter = TransferMeter()

    internal fun recordTransferMeter(
        isAudio: Boolean,
        bytes: Long,
        elapsedMs: Long,
        nowMs: Long = System.currentTimeMillis()
    ) {
        (if (isAudio) audioTransferMeter else videoTransferMeter).record(bytes, elapsedMs, nowMs)
    }

    internal fun getTransferMeterSnapshot(
        isAudio: Boolean,
        nowMs: Long = System.currentTimeMillis()
    ): Pair<Double, Double> {
        return (if (isAudio) audioTransferMeter else videoTransferMeter).snapshot(nowMs)
    }

    internal fun resetTransferMetersForTesting() {
        videoTransferMeter.reset()
        audioTransferMeter.reset()
    }

    /**
     * 动态对冲延迟计算（对齐 BTR idm-downloader.js hedgeDelayMs）：
     * `max(250ms, min(configuredHedgeDelayMs, round(ewmaPieceMs * 1.5)))`
     */
    internal fun computeDynamicHedgeDelayMs(
        ewmaPieceMs: Double,
        configuredHedgeDelayMs: Long = DEFAULT_HEDGE_DELAY_MS,
        isStartupChunk: Boolean = false
    ): Long {
        val cap = if (isStartupChunk) minOf(800L, configuredHedgeDelayMs) else configuredHedgeDelayMs
        val effectiveCap = cap.coerceAtLeast(MIN_HEDGE_DELAY_MS)
        if (ewmaPieceMs <= 0.0) return effectiveCap
        val dynamic = Math.round(ewmaPieceMs * 1.5)
        return dynamic.coerceIn(MIN_HEDGE_DELAY_MS, effectiveCap)
    }

    /**
     * 实时进度与速率监控的早对冲/慢节点救援判定（对齐 BTR observeFirst + hedgeDue）：
     * - 至少等待 minGraceMs (250ms) 宽限期，避免 TCP/HTTP 正常握手期间误触发
     * - 到达动态延迟阈值后：若主路已完成 >75% 且速率健康且无明显更快备选节点则放行，否则触发对冲
     * - 宽限期后、动态延迟到达前：若主路已开始传输但中途停滞 >= 350ms、或实时速率显著低于单连接均速 (<55%)
     *   且预计剩余耗时 >= 400ms、或备选节点已知 >=1.5x 更快且主路预计超时，立即提前唤醒对冲
     */
    internal fun shouldTriggerEarlyHedge(
        elapsedMs: Long,
        receivedBytes: Long,
        expectedBytes: Long,
        msSinceLastProgress: Long,
        dynamicHedgeDelayMs: Long,
        avgConnectionBps: Double,
        hedgeNodeBps: Double,
        minGraceMs: Long = MIN_HEDGE_DELAY_MS
    ): Boolean {
        if (elapsedMs < minGraceMs) return false
        val expected = expectedBytes.coerceAtLeast(1L)
        val liveBps = if (receivedBytes > 0L) (receivedBytes * 1000.0) / elapsedMs.coerceAtLeast(1L) else 0.0
        val remaining = (expected - receivedBytes).coerceAtLeast(0L)

        if (elapsedMs >= dynamicHedgeDelayMs) {
            val fastAlmostDone = receivedBytes > (expected * 3L) / 4L &&
                msSinceLastProgress < minGraceMs &&
                liveBps >= maxOf(256.0 * 1024.0, avgConnectionBps * HEDGE_PACE_RATIO) &&
                !(hedgeNodeBps > 0.0 && hedgeNodeBps >= liveBps * HEDGE_FASTER_RATIO)
            return !fastAlmostDone
        }

        if (receivedBytes <= 0L || remaining <= 0L) return false

        val stallThresholdMs = minOf(dynamicHedgeDelayMs, maxOf(minGraceMs, HEDGE_STALL_IDLE_MS))
        if (msSinceLastProgress >= stallThresholdMs) {
            return true
        }

        val projectedEtaMs = if (liveBps > 0.0) (remaining * 1000.0) / liveBps else Double.POSITIVE_INFINITY
        if (projectedEtaMs < HEDGE_EARLY_MIN_ETA_MS) return false

        if (avgConnectionBps > 0.0 && liveBps < avgConnectionBps * HEDGE_PACE_RATIO) {
            return true
        }
        if (hedgeNodeBps > 0.0 && liveBps > 0.0 &&
            hedgeNodeBps >= liveBps * HEDGE_FASTER_RATIO &&
            (elapsedMs + projectedEtaMs) > dynamicHedgeDelayMs * 1.25
        ) {
            return true
        }
        return false
    }

    internal fun writeHttpChunk(out: OutputStream, data: ByteArray, offset: Int = 0, length: Int = data.size) {
        if (length <= 0) return
        val sizeLine = "${length.toString(16)}\r\n".toByteArray(Charsets.US_ASCII)
        out.write(sizeLine)
        out.write(data, offset, length)
        out.write(CRLF_BYTES)
    }

    internal fun writeHttpChunkTerminator(out: OutputStream) {
        out.write(CHUNK_TERMINATOR_BYTES)
        out.flush()
    }

    private val cdnPenalties = ConcurrentHashMap<String, Long>()

    private fun penalizeHost(host: String, durationMs: Long) {
        val now = System.currentTimeMillis()
        if (cdnPenalties.size > MAX_TRACKED_HOSTS) {
            cdnPenalties.entries.removeIf { it.value <= now }
        }
        cdnPenalties[host] = now + durationMs
    }

    data class ChunkTask(
        val index: Long,
        val startByte: Long,
        val endByte: Long,
        @Volatile var data: ByteArray? = null,
        @Volatile var isSuccess: Boolean = false,
        @Volatile var isFailed: Boolean = false,
        @Volatile var errorMessage: String? = null,
        @Volatile var receivedBytes: Int = 0,
        @Volatile var prefixData: ByteArray? = null
    ) {
        @Volatile
        var lastHttpCode: Int = 0
        private var prefixOwnerId: Long = 0L
        private var prefixOwnerFailed: Boolean = false
        private var liveBuffer: ByteArray? = null
        private var liveBufferPos: Int = 0
        private var prefixDataLen: Int = 0

        fun tryClaimPrefixProducer(
            attemptId: Long,
            basePrefix: ByteArray? = null,
            expectedTotal: Int = 0
        ): Boolean {
            synchronized(this) {
                if (isSuccess) return false
                if (prefixOwnerId == attemptId) return true
                val canTakeOver = prefixOwnerId == 0L || prefixOwnerFailed
                if (!canTakeOver) return false
                prefixOwnerId = attemptId
                prefixOwnerFailed = false
                if (basePrefix != null && basePrefix.isNotEmpty()) {
                    val baseLen = basePrefix.size
                    val allocSize = maxOf(expectedTotal, baseLen)
                    val dest = ByteArray(allocSize)
                    System.arraycopy(basePrefix, 0, dest, 0, baseLen)
                    liveBuffer = dest
                    liveBufferPos = baseLen
                    receivedBytes = maxOf(liveBufferPos, prefixDataLen)
                } else {
                    liveBuffer = null
                    liveBufferPos = 0
                }
                return true
            }
        }

        fun appendLivePrefix(attemptId: Long, buf: ByteArray, len: Int, expectedTotal: Int) {
            if (len <= 0 || expectedTotal <= 0) return
            synchronized(this) {
                if (prefixOwnerId != attemptId || isSuccess) return
                var dest = liveBuffer
                if (dest == null || dest.size < expectedTotal) {
                    val newDest = ByteArray(expectedTotal)
                    if (dest != null && liveBufferPos > 0) {
                        System.arraycopy(dest, 0, newDest, 0, minOf(liveBufferPos, newDest.size))
                    }
                    dest = newDest
                    liveBuffer = dest
                }
                val writable = minOf(len, expectedTotal - liveBufferPos)
                if (writable > 0) {
                    System.arraycopy(buf, 0, dest, liveBufferPos, writable)
                    liveBufferPos += writable
                    receivedBytes = maxOf(liveBufferPos, prefixDataLen)
                }
            }
        }

        fun saveFallbackPrefixIfBetter(bytes: ByteArray, length: Int) {
            if (length < RESUME_MIN_BYTES) return
            synchronized(this) {
                if (isSuccess) return
                val currentBest = maxOf(liveBufferPos, prefixDataLen)
                if (length > currentBest) {
                    prefixData = if (bytes.size == length) bytes.copyOf() else bytes.copyOf(length)
                    prefixDataLen = length
                    receivedBytes = length
                }
            }
        }

        fun consumeOwnedLiveBuffer(attemptId: Long): ByteArray? {
            synchronized(this) {
                if (prefixOwnerId != attemptId) return null
                val buf = liveBuffer
                val size = liveBufferPos
                liveBuffer = null
                liveBufferPos = 0
                prefixOwnerId = 0L
                prefixOwnerFailed = false
                receivedBytes = prefixDataLen
                if (buf == null || size <= 0) return null
                return if (buf.size == size) buf else buf.copyOf(size)
            }
        }

        fun releasePrefixProducer(attemptId: Long, keepPartial: Boolean) {
            synchronized(this) {
                if (prefixOwnerId == attemptId) {
                    val buf = liveBuffer
                    val size = liveBufferPos
                    if (keepPartial && buf != null && size >= RESUME_MIN_BYTES && size > prefixDataLen && !isSuccess) {
                        prefixData = buf.copyOf(size)
                        prefixDataLen = size
                    }
                    prefixOwnerId = 0L
                    prefixOwnerFailed = true
                    liveBuffer = null
                    liveBufferPos = 0
                    receivedBytes = prefixDataLen
                }
            }
        }

        fun takePrefixSnapshot(minResumeBytes: Int, expectedTotal: Int): ByteArray? {
            synchronized(this) {
                if (isSuccess) return null
                val liveBuf = liveBuffer
                val liveSize = if (liveBuf != null && liveBufferPos in minResumeBytes until expectedTotal) liveBufferPos else 0
                val external = prefixData
                val savedSize = if (external != null && prefixDataLen in minResumeBytes until expectedTotal && external.size >= prefixDataLen) prefixDataLen else 0
                return when {
                    liveSize >= savedSize && liveSize > 0 -> liveBuf!!.copyOf(liveSize)
                    savedSize > 0 -> if (external!!.size == savedSize) external else external.copyOf(savedSize)
                    else -> null
                }
            }
        }

        fun clearPrefixBuffersOnWin() {
            synchronized(this) {
                prefixData = null
                prefixDataLen = 0
                liveBuffer = null
                liveBufferPos = 0
                prefixOwnerId = 0L
                prefixOwnerFailed = false
            }
        }

        fun resetPrefix() {
            synchronized(this) {
                prefixData = null
                prefixDataLen = 0
                liveBuffer = null
                liveBufferPos = 0
                prefixOwnerId = 0L
                prefixOwnerFailed = false
                receivedBytes = 0
            }
        }

        fun releaseBuffers() {
            synchronized(this) {
                data = null
                prefixData = null
                prefixDataLen = 0
                liveBuffer = null
                liveBufferPos = 0
                prefixOwnerId = 0L
                prefixOwnerFailed = false
                receivedBytes = 0
            }
        }
    }

    /**
     * 自适应动态并发控制器（对齐 BTR / idm-downloader.js createAutoConcurrency）：
     * - 维护 [2, 4, 6, 8, 12, 16] 并发阶梯，严格受限于 [minConcurrency, maxConcurrency]
     * - 开播 / Seek 冲刺（startSprint / onSeekSprint）自动上探至 8~12 线程（受用户上限与 412/429 硬限制约束），
     *   待首屏突发完成或队列非饱和后按冷却步进回落至基准并发
     * - 仅在真实需求压力下升档：播放器卡顿 (onStall)、低缓冲饥饿 (onBufferHunger)、
     *   队列高饱和排队超时 (onSlow) 或持续队列饱和 (active >= limit && queued > 0 >= 1s)，严禁无压力盲目升档
     * - 采用 5 秒多桶（250ms）滑动窗口测量吞吐；试探须满足真实 >=8% 吞吐增益 (1.08x)，否则回退并软休息 90s
     * - 412/429 立即降档并设置硬休息上限且跨会话/跨进程持久化；持续空闲 (>=8s) 平滑降档不污染学习基线
     */
    internal class DynamicConcurrencyController(
        initialConcurrency: Int,
        internal val autoScale: Boolean,
        internal val minConcurrency: Int = 2,
        internal val maxConcurrency: Int = 16,
        initialNowMs: Long = System.currentTimeMillis(),
        private val enablePersistence: Boolean = false
    ) : PlayerConcurrencySink {
        internal companion object {
            const val SAT_WINDOW_MS = 5_000L
            const val THROUGHPUT_WINDOW_MS = 5_000L
            const val THROUGHPUT_BUCKET_MS = 250L
            const val STEP_COOLDOWN_MS = 2_500L
            const val SUSTAINED_SAT_MS = 1_000L
            const val TRIAL_WINDOW_MS = 5_000L
            const val TRIAL_MIN_GAIN_RATIO = 1.08
            const val TRIAL_REST_MS = 90_000L
            const val STALL_DEBOUNCE_MS = 3_000L
            const val PUSHBACK_DEBOUNCE_MS = 2_000L
            const val IDLE_STEP_DOWN_MS = 8_000L
            const val STARTUP_SPRINT_TARGET = 12
            const val STARTUP_SPRINT_MAX_MS = 30_000L
            const val STARTUP_BURST_BYTES = 4L * 1024 * 1024
            const val STARTUP_DECAY_MIN_MS = 3_000L
        }

        private data class ThroughputBucket(val atMs: Long, var bytes: Long)
        private data class StartupSprintState(
            val baseIdx: Int,
            val startedAtMs: Long,
            val untilMs: Long,
            var deliveredBytes: Long = 0L
        )

        private val lock = Any()
        private var ladderIdx: Int
        @Volatile
        var currentConcurrency: Int
            private set
        @Volatile
        var peakConcurrency: Int
            private set

        internal val isStartupSprintActive: Boolean
            get() = synchronized(lock) { startupSprint != null }

        private var intervalStartMs: Long = initialNowMs
        private var intervalBytes: Long = 0L
        private val throughputBuckets = ArrayDeque<ThroughputBucket>()
        private var bestBps: Double = 0.0
        private var lastMeasuredBps: Double = 0.0
        private var trialStartedAtMs: Long = 0L
        private var trialBaselineBps: Double = 0.0
        private var lastTrialDirection: Int = 0
        private var trialStalled: Boolean = false
        private var prevIndexBeforeTrial: Int = 0
        private var pushbackCooldownUntilMs: Long = 0L
        private var lastPushbackAtMs: Long = 0L
        private var lastPushbackWasHard: Boolean = false
        private var lastChangedAtMs: Long = 0L
        private var hasDemandMetrics: Boolean = false
        private var lastSaturatedAtMs: Long = 0L
        private var idleSinceMs: Long = initialNowMs
        private var isSaturated: Boolean = false
        private var saturatedSinceMs: Long = 0L
        private val saturatedSpans = ArrayList<Pair<Long, Long>>()
        private val softRestUntil = LongArray(CONCURRENCY_LADDER.size)
        // 按阶梯级记录 412/429 硬休息到期时间（对齐上游 idm-downloader.js resting Map）：
        // 过期条目自动移除，避免历史低档位 429 永久钳制后续高档位并发上限
        private val hardRestUntil = LongArray(CONCURRENCY_LADDER.size)
        private var pushbackStreak: Int = 0
        private var lastSlowAtMs: Long = 0L
        private var lastStallAtMs: Long = 0L
        private var lastHungerStepAtMs: Long = 0L
        private var persistedLearnedConcurrency: Int
        private var startupSprint: StartupSprintState? = null

        init {
            val loaded = if (enablePersistence && autoScale) loadPersistedConcurrencyState(initialNowMs) else null
            var effectiveTarget = initialConcurrency.coerceIn(minConcurrency, maxConcurrency)
            if (loaded != null) {
                var effectiveCeil = maxConcurrency
                if (loaded.hardCapConcurrency > 0 && loaded.hardCapUntilMs > initialNowMs) {
                    val clampedCap = loaded.hardCapConcurrency.coerceIn(minConcurrency, maxConcurrency)
                    val capIdx = maxIdxForLimit(clampedCap)
                    applyHardCapLocked(capIdx, loaded.hardCapUntilMs)
                    effectiveCeil = minOf(effectiveCeil, clampedCap)
                }
                if (loaded.learnedConcurrency > 0) {
                    effectiveTarget = loaded.learnedConcurrency.coerceIn(minConcurrency, effectiveCeil)
                } else {
                    effectiveTarget = effectiveTarget.coerceIn(minConcurrency, effectiveCeil)
                }
            }
            val idx = CONCURRENCY_LADDER.indexOfFirst { it >= effectiveTarget }
                .let { if (it == -1) CONCURRENCY_LADDER.lastIndex else it }
                .coerceAtMost(allowedMaxIndex(initialNowMs))
            ladderIdx = idx
            currentConcurrency = CONCURRENCY_LADDER[idx].coerceIn(minConcurrency, maxConcurrency)
            peakConcurrency = currentConcurrency
            persistedLearnedConcurrency = currentConcurrency
        }

        private fun maxIdxForLimit(limit: Int): Int {
            var best = 0
            for (i in CONCURRENCY_LADDER.indices) {
                if (CONCURRENCY_LADDER[i] <= limit) best = i
            }
            return best
        }

        private fun applyHardCapLocked(capIdx: Int, untilMs: Long) {
            val refusedIdx = (capIdx + 1).coerceIn(0, CONCURRENCY_LADDER.lastIndex)
            if (capIdx < CONCURRENCY_LADDER.lastIndex) {
                hardRestUntil[refusedIdx] = maxOf(hardRestUntil[refusedIdx], untilMs)
            }
        }

        private fun pruneAndGetActiveHardCapLocked(nowMs: Long): Pair<Int, Long> {
            val limitIdx = maxIdxForLimit(maxConcurrency)
            var capIdx = limitIdx
            var capUntil = 0L
            for (i in hardRestUntil.indices) {
                val until = hardRestUntil[i]
                if (until <= 0L) continue
                if (until <= nowMs) {
                    hardRestUntil[i] = 0L
                    continue
                }
                val allowedForLevel = (i - 1).coerceAtLeast(0)
                if (allowedForLevel < capIdx) {
                    capIdx = allowedForLevel
                    capUntil = until
                } else if (allowedForLevel == capIdx) {
                    capUntil = maxOf(capUntil, until)
                }
            }
            return capIdx to capUntil
        }

        internal fun activeHardCapForTesting(nowMs: Long = System.currentTimeMillis()): Int = synchronized(lock) {
            val (capIdx, until) = pruneAndGetActiveHardCapLocked(nowMs)
            if (until > nowMs) CONCURRENCY_LADDER[capIdx].coerceIn(minConcurrency, maxConcurrency) else 0
        }

        private fun allowedMaxIndex(nowMs: Long): Int {
            val limitIdx = maxIdxForLimit(maxConcurrency)
            val (capIdx, capUntil) = pruneAndGetActiveHardCapLocked(nowMs)
            return if (capUntil > nowMs) minOf(limitIdx, capIdx) else limitIdx
        }

        private fun pruneBucketsLocked(nowMs: Long) {
            while (throughputBuckets.isNotEmpty() && (nowMs - throughputBuckets.first().atMs) > THROUGHPUT_WINDOW_MS) {
                throughputBuckets.removeFirst()
            }
        }

        private fun recordBucketLocked(bytes: Long, nowMs: Long) {
            if (bytes <= 0L) return
            val last = throughputBuckets.lastOrNull()
            if (last != null && nowMs >= last.atMs && (nowMs - last.atMs) < THROUGHPUT_BUCKET_MS) {
                last.bytes += bytes
            } else {
                throughputBuckets.addLast(ThroughputBucket(nowMs, bytes))
            }
            pruneBucketsLocked(nowMs)
        }

        private fun computeWindowThroughputBpsLocked(nowMs: Long, minAtMs: Long = 0L): Double {
            pruneBucketsLocked(nowMs)
            var totalBytes = 0L
            var firstAt = Long.MAX_VALUE
            var lastAt = Long.MIN_VALUE
            var count = 0
            for (b in throughputBuckets) {
                if (b.atMs >= minAtMs) {
                    totalBytes += b.bytes
                    if (b.atMs < firstAt) firstAt = b.atMs
                    if (b.atMs > lastAt) lastAt = b.atMs
                    count++
                }
            }
            if (count == 0 || totalBytes <= 0L) return 0.0
            val spanMs = if (count == 1) {
                val refStart = maxOf(minAtMs, intervalStartMs)
                val elapsed = if (refStart in 1L until nowMs) (nowMs - refStart) else 1_000L
                elapsed.coerceIn(1_000L, THROUGHPUT_WINDOW_MS)
            } else {
                maxOf(1_000L, lastAt - firstAt + THROUGHPUT_BUCKET_MS)
            }
            return (totalBytes * 1000.0) / spanMs
        }

        private fun saturationLocked(nowMs: Long): Double {
            val from = nowMs - SAT_WINDOW_MS
            val it = saturatedSpans.iterator()
            while (it.hasNext()) {
                if (it.next().second <= from) it.remove()
            }
            var busyMs = 0L
            for ((sFrom, sTo) in saturatedSpans) {
                busyMs += maxOf(0L, sTo - maxOf(sFrom, from))
            }
            if (isSaturated) {
                busyMs += maxOf(0L, nowMs - maxOf(saturatedSinceMs, from))
            }
            return minOf(1.0, busyMs.toDouble() / SAT_WINDOW_MS)
        }

        private fun maybeDecayStartupSprintLocked(nowMs: Long): Boolean {
            val sprint = startupSprint ?: return false
            if (nowMs >= sprint.untilMs) {
                startupSprint = null
                return false
            }
            if (ladderIdx <= sprint.baseIdx) {
                startupSprint = null
                return false
            }
            if (lastChangedAtMs > 0L && (nowMs - lastChangedAtMs) < STEP_COOLDOWN_MS) return false
            val burstDoneAndHealthy = sprint.deliveredBytes >= STARTUP_BURST_BYTES && !isSaturated
            val unsaturatedLongEnough = hasDemandMetrics && !isSaturated && (nowMs - sprint.startedAtMs) >= STARTUP_DECAY_MIN_MS
            if (!burstDoneAndHealthy && !unsaturatedLongEnough) return false

            ladderIdx = maxOf(sprint.baseIdx, ladderIdx - 1)
            currentConcurrency = CONCURRENCY_LADDER[ladderIdx].coerceIn(minConcurrency, maxConcurrency)
            lastChangedAtMs = nowMs
            idleSinceMs = nowMs
            if (ladderIdx <= sprint.baseIdx) {
                startupSprint = null
            }
            return true
        }

        private fun syncCrossProcessHardCapLocked(nowMs: Long) {
            if (!enablePersistence || !autoScale) return
            val remote = loadPersistedConcurrencyState(nowMs) ?: return
            if (remote.hardCapConcurrency > 0 && remote.hardCapUntilMs > nowMs) {
                val clampedCap = remote.hardCapConcurrency.coerceIn(minConcurrency, maxConcurrency)
                applyHardCapLocked(maxIdxForLimit(clampedCap), remote.hardCapUntilMs)
                val maxIdx = allowedMaxIndex(nowMs)
                if (ladderIdx > maxIdx) {
                    ladderIdx = maxIdx
                    currentConcurrency = CONCURRENCY_LADDER[ladderIdx].coerceIn(minConcurrency, maxConcurrency)
                    lastTrialDirection = 0
                    startupSprint = null
                }
            }
        }

        private fun persistState(nowMs: Long, forceDiskWrite: Boolean = false) {
            if (!enablePersistence || !autoScale) return
            val (capIdx, capUntil) = pruneAndGetActiveHardCapLocked(nowMs)
            val hardCap = if (capUntil > nowMs) CONCURRENCY_LADDER[capIdx].coerceIn(minConcurrency, maxConcurrency) else 0
            val hardUntil = if (capUntil > nowMs) capUntil else 0L
            savePersistedConcurrencyState(
                SharedConcurrencyState(
                    learnedConcurrency = persistedLearnedConcurrency,
                    hardCapConcurrency = hardCap,
                    hardCapUntilMs = hardUntil,
                    updatedAtMs = nowMs
                ),
                forceDiskWrite = forceDiskWrite
            )
        }

        private fun stepUpLocked(nowMs: Long, strong: Boolean): Boolean {
            syncCrossProcessHardCapLocked(nowMs)
            if (lastChangedAtMs > 0L && (nowMs - lastChangedAtMs) < STEP_COOLDOWN_MS) return false
            if (!strong && nowMs < pushbackCooldownUntilMs) return false
            val hardMax = allowedMaxIndex(nowMs)
            if (ladderIdx >= hardMax) return false
            var next = ladderIdx + 1
            while (next <= hardMax) {
                val resting = nowMs < softRestUntil[next]
                if (!resting) break
                if (!strong) return false
                next++
            }
            if (next > hardMax || CONCURRENCY_LADDER[next] > maxConcurrency) return false
            val baseline = computeWindowThroughputBpsLocked(nowMs).takeIf { it > 0.0 } ?: lastMeasuredBps
            prevIndexBeforeTrial = ladderIdx
            ladderIdx = next
            currentConcurrency = CONCURRENCY_LADDER[ladderIdx].coerceIn(minConcurrency, maxConcurrency)
            if (currentConcurrency > peakConcurrency) peakConcurrency = currentConcurrency
            startupSprint = null
            lastTrialDirection = 1
            trialStalled = false
            trialStartedAtMs = nowMs
            trialBaselineBps = baseline
            lastChangedAtMs = nowMs
            intervalStartMs = nowMs
            intervalBytes = 0L
            return true
        }

        fun recordDemand(
            active: Int,
            limit: Int,
            queued: Int,
            nowMs: Long = System.currentTimeMillis()
        ) {
            if (!autoScale) return
            synchronized(lock) {
                val firstDemand = !hasDemandMetrics
                hasDemandMetrics = true
                if (firstDemand && active > 0) {
                    idleSinceMs = nowMs
                }
                val saturatedNow = active >= limit && queued > 0
                if (saturatedNow != isSaturated) {
                    if (isSaturated && nowMs > saturatedSinceMs) {
                        saturatedSpans.add(saturatedSinceMs to nowMs)
                    }
                    isSaturated = saturatedNow
                    if (saturatedNow) {
                        saturatedSinceMs = nowMs
                    }
                    saturationLocked(nowMs)
                }
                if (saturatedNow) {
                    lastSaturatedAtMs = nowMs
                    idleSinceMs = nowMs
                } else {
                    if (maybeDecayStartupSprintLocked(nowMs)) {
                        return
                    }
                    if (
                        (nowMs - idleSinceMs) >= IDLE_STEP_DOWN_MS &&
                        nowMs >= pushbackCooldownUntilMs &&
                        ladderIdx > 0 &&
                        CONCURRENCY_LADDER[ladderIdx - 1] >= minConcurrency
                    ) {
                        ladderIdx--
                        currentConcurrency = CONCURRENCY_LADDER[ladderIdx].coerceIn(minConcurrency, maxConcurrency)
                        lastTrialDirection = 0
                        trialStalled = false
                        startupSprint = null
                        idleSinceMs = nowMs
                    }
                }
            }
        }

        fun recordProgress(bytes: Long, nowMs: Long = System.currentTimeMillis()) {
            if (!autoScale || bytes <= 0L) return
            synchronized(lock) {
                syncCrossProcessHardCapLocked(nowMs)
                intervalBytes += bytes
                recordBucketLocked(bytes, nowMs)
                startupSprint?.let { it.deliveredBytes += bytes }
                val elapsed = nowMs - intervalStartMs

                if (nowMs < pushbackCooldownUntilMs) {
                    if (elapsed >= 1000L && intervalBytes >= 256 * 1024L) {
                        val currentBps = computeWindowThroughputBpsLocked(nowMs)
                        intervalStartMs = nowMs
                        intervalBytes = 0L
                        lastMeasuredBps = currentBps
                        if (bestBps <= 0.0 || currentBps > bestBps) bestBps = currentBps
                    }
                    return
                }

                if (lastTrialDirection > 0) {
                    val trialElapsed = nowMs - trialStartedAtMs
                    if (trialElapsed < TRIAL_WINDOW_MS || elapsed < 1000L || intervalBytes < 256 * 1024L) {
                        return
                    }
                    val trialBps = computeWindowThroughputBpsLocked(nowMs, minAtMs = trialStartedAtMs)
                    intervalStartMs = nowMs
                    intervalBytes = 0L
                    lastMeasuredBps = trialBps
                    if (trialBps > bestBps) bestBps = trialBps

                    val gained = trialBaselineBps <= 0.0 || trialBps >= trialBaselineBps * TRIAL_MIN_GAIN_RATIO
                    if (gained || trialStalled) {
                        lastTrialDirection = 0
                        trialStalled = false
                        lastChangedAtMs = nowMs
                        pushbackStreak = 0
                        persistedLearnedConcurrency = currentConcurrency
                        persistState(nowMs)
                    } else if (!hasDemandMetrics || isSaturated || saturationLocked(nowMs) >= 0.5) {
                        val failedIdx = ladderIdx
                        softRestUntil[failedIdx] = nowMs + TRIAL_REST_MS
                        ladderIdx = prevIndexBeforeTrial
                        currentConcurrency = CONCURRENCY_LADDER[ladderIdx].coerceIn(minConcurrency, maxConcurrency)
                        lastTrialDirection = 0
                        trialStalled = false
                        lastChangedAtMs = nowMs
                        pushbackCooldownUntilMs = nowMs + 6_000L
                        persistedLearnedConcurrency = currentConcurrency
                        persistState(nowMs)
                    } else {
                        lastTrialDirection = 0
                        trialStalled = false
                    }
                    return
                }

                if (maybeDecayStartupSprintLocked(nowMs)) {
                    return
                }

                if (elapsed < 1000L || intervalBytes < 256 * 1024L) return

                val currentBps = computeWindowThroughputBpsLocked(nowMs)
                intervalStartMs = nowMs
                intervalBytes = 0L
                lastMeasuredBps = currentBps

                if (currentBps > bestBps) {
                    bestBps = currentBps
                }

                // 仅在存在真实需求压力（已采集到队列指标且持续饱和）时触发升档；严禁无压力盲目爬升至满档
                val sustainedSaturated = isSaturated && (nowMs - saturatedSinceMs) >= SUSTAINED_SAT_MS
                val recentHighSat = lastSaturatedAtMs > 0L &&
                    (nowMs - lastSaturatedAtMs) <= 2_500L &&
                    saturationLocked(nowMs) >= 0.6
                val hasGenuinePressure = hasDemandMetrics && (sustainedSaturated || recentHighSat)
                if (hasGenuinePressure) {
                    stepUpLocked(nowMs, strong = false)
                } else if (hasDemandMetrics &&
                    !isSaturated &&
                    (nowMs - idleSinceMs) >= IDLE_STEP_DOWN_MS &&
                    ladderIdx > 0 &&
                    CONCURRENCY_LADDER[ladderIdx - 1] >= minConcurrency
                ) {
                    ladderIdx--
                    currentConcurrency = CONCURRENCY_LADDER[ladderIdx].coerceIn(minConcurrency, maxConcurrency)
                    startupSprint = null
                    idleSinceMs = nowMs
                }
            }
        }

        fun onPushback(nowMs: Long = System.currentTimeMillis(), isHard: Boolean = true): Int {
            synchronized(lock) {
                // 并发 412/429 防抖：同一突发窗口内多条线程同时收到 429 时合并为单次降档，防止雪崩直落至 2 线程并打满 180s
                if (lastPushbackAtMs > 0L && (nowMs - lastPushbackAtMs) < PUSHBACK_DEBOUNCE_MS && (!isHard || lastPushbackWasHard)) {
                    return currentConcurrency
                }
                val (_, activeHardUntil) = pruneAndGetActiveHardCapLocked(nowMs)
                if (activeHardUntil <= nowMs) {
                    pushbackStreak = 0
                }
                lastPushbackAtMs = nowMs
                lastPushbackWasHard = isHard
                val prevIdx = ladderIdx
                if (ladderIdx > 0 && CONCURRENCY_LADDER[ladderIdx - 1] >= minConcurrency) {
                    ladderIdx--
                }
                currentConcurrency = CONCURRENCY_LADDER[ladderIdx].coerceIn(minConcurrency, maxConcurrency)
                startupSprint = null
                lastTrialDirection = 0
                trialStalled = false
                trialStartedAtMs = 0L
                trialBaselineBps = 0.0
                lastChangedAtMs = nowMs
                pushbackCooldownUntilMs = maxOf(pushbackCooldownUntilMs, nowMs + 15_000L)
                if (isHard) {
                    pushbackStreak++
                    val restDurationMs = minOf(180_000L, 45_000L * (1L shl minOf(pushbackStreak - 1, 2)))
                    val capIdx = if (ladderIdx < prevIdx) ladderIdx else prevIdx
                    applyHardCapLocked(capIdx, nowMs + restDurationMs)
                }
                persistedLearnedConcurrency = currentConcurrency
                intervalStartMs = nowMs
                intervalBytes = 0L
                persistState(nowMs, forceDiskWrite = true)
                return currentConcurrency
            }
        }

        override fun onStall(nowMs: Long) {
            if (!autoScale) return
            synchronized(lock) {
                if (lastStallAtMs > 0L && (nowMs - lastStallAtMs) < STALL_DEBOUNCE_MS) return
                lastStallAtMs = nowMs
                if (lastTrialDirection > 0) {
                    trialStalled = true
                }
                if (stepUpLocked(nowMs, strong = true)) {
                    persistedLearnedConcurrency = currentConcurrency
                    persistState(nowMs, forceDiskWrite = true)
                }
            }
        }

        override fun onSlow(nowMs: Long) {
            if (!autoScale) return
            synchronized(lock) {
                if (lastSlowAtMs > 0L && (nowMs - lastSlowAtMs) < STEP_COOLDOWN_MS) return
                lastSlowAtMs = nowMs
                val saturated = hasDemandMetrics && (isSaturated || saturationLocked(nowMs) >= 0.6)
                if (saturated && stepUpLocked(nowMs, strong = false)) {
                    persistedLearnedConcurrency = currentConcurrency
                    persistState(nowMs)
                }
            }
        }

        override fun onBufferHunger(bufferedBytes: Long, nowMs: Long) {
            if (!autoScale) return
            synchronized(lock) {
                hasDemandMetrics = true
                lastSaturatedAtMs = nowMs
                idleSinceMs = nowMs
                if (lastHungerStepAtMs > 0L && (nowMs - lastHungerStepAtMs) < STEP_COOLDOWN_MS) return
                if (stepUpLocked(nowMs, strong = false)) {
                    lastHungerStepAtMs = nowMs
                    persistedLearnedConcurrency = currentConcurrency
                    persistState(nowMs)
                }
            }
        }

        fun startSprint(targetConcurrency: Int, nowMs: Long = System.currentTimeMillis()) {
            if (!autoScale) return
            synchronized(lock) {
                syncCrossProcessHardCapLocked(nowMs)
                val hardMax = allowedMaxIndex(nowMs)
                val clampedTarget = targetConcurrency.coerceIn(minConcurrency, maxConcurrency)
                val targetIdx = maxIdxForLimit(clampedTarget).coerceAtMost(hardMax)
                val baseIdx = startupSprint?.baseIdx ?: ladderIdx
                if (targetIdx > ladderIdx) {
                    ladderIdx = targetIdx
                    currentConcurrency = CONCURRENCY_LADDER[ladderIdx].coerceIn(minConcurrency, maxConcurrency)
                    if (currentConcurrency > peakConcurrency) peakConcurrency = currentConcurrency
                    lastTrialDirection = 0
                    trialStalled = false
                    trialStartedAtMs = 0L
                    trialBaselineBps = 0.0
                    lastChangedAtMs = nowMs
                    idleSinceMs = nowMs
                    intervalStartMs = nowMs
                    intervalBytes = 0L
                }
                startupSprint = if (ladderIdx > baseIdx) {
                    StartupSprintState(
                        baseIdx = baseIdx,
                        startedAtMs = nowMs,
                        untilMs = nowMs + STARTUP_SPRINT_MAX_MS,
                        deliveredBytes = 0L
                    )
                } else {
                    null
                }
            }
        }

        fun onSeekSprint(targetConcurrency: Int, nowMs: Long = System.currentTimeMillis()) {
            startSprint(targetConcurrency, nowMs)
        }
    }

    private class StreamStatsTracker(
        val sessionId: String,
        val primaryHost: String,
        @Volatile var concurrency: Int,
        val proxyPort: Int,
        val isEpisode: Boolean = false,
        val isAudio: Boolean = false
    ) {
        private val transferId = transferSeq.incrementAndGet()
        val totalChunks = AtomicInteger(0)
        val totalBytes = AtomicLong(0L)
        val totalLatency = AtomicLong(0L)
        val hitHosts = ConcurrentHashMap.newKeySet<String>()

        fun recordChunk(host: String, bytes: Long, latencyMs: Long) {
            totalChunks.incrementAndGet()
            totalBytes.addAndGet(bytes)
            totalLatency.addAndGet(latencyMs)
            hitHosts.add(host)
        }

        fun finishAndReport() {
            val count = totalChunks.get()
            if (count == 0) return

            val bytes = totalBytes.get()
            val avgLatency = totalLatency.get() / count
            val mbStr = String.format(Locale.getDefault(), "%.1f", bytes / (1024.0 * 1024.0))

            val prefix = when {
                isEpisode -> "BTR剧集加速(兼容模式)"
                isAudio || sessionId.startsWith("audio_") || primaryHost.contains("audio") -> "BTR音频优化"
                else -> "BTR并发加速"
            }
            val summaryReason = "$prefix · ${count}分片(${mbStr}MB) · ${hitHosts.size}个CDN · 均时${avgLatency}ms"

            val result = RewriteResult(
                changed = true,
                originalUrl = "",
                finalUrl = "",
                originalHost = primaryHost,
                targetHost = "127.0.0.1:$proxyPort (${concurrency}线程)",
                reason = summaryReason,
                isPcdn = false,
                isMcdn = false,
                segmentKey = "btr_stream_${sessionId}_$transferId",
                isBtr = true,
                btrChunks = count,
                btrBytes = bytes,
                btrConcurrency = concurrency,
                btrCdnCount = hitHosts.size,
                btrAvgLatencyMs = avgLatency
            )
            RemoteClient.notifyRewrite(result)
        }
    }

    internal data class ConcurrencyBounds(
        val minCon: Int,
        val initialCon: Int,
        val maxCon: Int,
        val useAuto: Boolean,
        val startupSprintTarget: Int
    )

    /**
     * 解析并发边界（对齐上游 range-core.js）：
     * - 开启自动并发（非音频/非番剧）时，自动并发阶梯允许突破默认手动滑块 6 线程，
     *   在 [2, AUTO_MAX_CONCURRENCY(16)] 间自适应爬升，并支持开播/Seek 冲刺至 12 线程；
     * - 关闭自动并发时严格遵守用户手动滑块值。
     */
    internal fun resolveConcurrencyBounds(
        concurrency: Int,
        autoConcurrency: Boolean,
        isAudio: Boolean,
        isEpisode: Boolean
    ): ConcurrencyBounds {
        val userMaxCon = concurrency.coerceIn(2, AUTO_MAX_CONCURRENCY)
        val minCon = 2
        val useAuto = !isAudio && !isEpisode && autoConcurrency
        val maxCon = when {
            isAudio -> AUDIO_MAX_CONCURRENCY
            isEpisode -> minOf(userMaxCon, 6).coerceIn(minCon, 8)
            useAuto -> maxOf(userMaxCon, AUTO_MAX_CONCURRENCY)
            else -> userMaxCon
        }
        val initialCon = when {
            isAudio -> AUDIO_MAX_CONCURRENCY
            isEpisode -> maxCon
            useAuto -> AUTO_INITIAL_CONCURRENCY.coerceIn(minCon, maxCon)
            else -> userMaxCon
        }
        val sprintTarget = if (useAuto) {
            minOf(maxCon, DynamicConcurrencyController.STARTUP_SPRINT_TARGET)
        } else {
            initialCon
        }
        return ConcurrencyBounds(
            minCon = minCon,
            initialCon = initialCon,
            maxCon = maxCon,
            useAuto = useAuto,
            startupSprintTarget = sprintTarget
        )
    }

    /**
     * 自适应切片尺寸计算（对齐上游 idm-downloader.js adaptiveMinChunk）：
     * - 音频流固定保持 128KB 极低延时；
     * - 未开启自动时严格采用 [baseChunkSizeKb]；
     * - 开启自动时：
     *   1. 以 [totalLength] 确定基础上限 (256KB~2048KB)；
     *   2. 若已测得单连接吞吐 [measuredConnectionBps]，按 `0.6s * bps` 对齐 64KB 步长限制上限，避免弱网 2MB 队头阻塞；
     *   3. 若存在有界 [requestedRangeBytes]，按 `requestedRangeBytes / min(max(4, hosts), concurrency)` 切分，
     *      杜绝 1.5MB DASH Range 塌缩成 totalTasks=1 丧失多源并发。
     */
    fun calculateDynamicChunkSize(
        totalLength: Long,
        baseChunkSizeKb: Int,
        autoChunkSize: Boolean,
        isAudio: Boolean,
        requestedRangeBytes: Long = -1L,
        measuredConnectionBps: Double = 0.0,
        concurrency: Int = 6,
        candidateHostCount: Int = 4
    ): Long {
        if (isAudio) return 128 * 1024L
        val baseBytes = (baseChunkSizeKb.coerceIn(128, 2048) * 1024).toLong()
        if (!autoChunkSize) return baseBytes
        val effectiveLen = if (totalLength > 0L) totalLength else requestedRangeBytes
        if (effectiveLen <= 0L) return baseBytes
        val scaled = when {
            effectiveLen < 15 * 1024 * 1024L -> baseBytes / 2
            effectiveLen < 60 * 1024 * 1024L -> baseBytes
            effectiveLen < 250 * 1024 * 1024L -> baseBytes * 2
            else -> baseBytes * 4
        }
        var target = scaled.coerceIn(256 * 1024L, 2048 * 1024L)
        if (measuredConnectionBps > 0.0) {
            val bpsAligned = ((measuredConnectionBps * 0.6).toLong() / ADAPTIVE_CHUNK_STEP_BYTES) * ADAPTIVE_CHUNK_STEP_BYTES
            val bpsCap = bpsAligned.coerceIn(ADAPTIVE_MIN_CHUNK_BYTES, 2048 * 1024L)
            target = minOf(target, bpsCap)
        }
        if (requestedRangeBytes > 0L) {
            val desiredPieces = minOf(maxOf(4, candidateHostCount.coerceAtLeast(1)), concurrency.coerceAtLeast(1)).coerceAtLeast(1)
            if (desiredPieces > 1) {
                val spreadRaw = requestedRangeBytes / desiredPieces
                val spreadAligned = (spreadRaw / ADAPTIVE_CHUNK_STEP_BYTES) * ADAPTIVE_CHUNK_STEP_BYTES
                val spreadCap = spreadAligned.coerceAtLeast(ADAPTIVE_MIN_CHUNK_BYTES)
                target = minOf(target, spreadCap)
            }
        }
        return target.coerceIn(ADAPTIVE_MIN_CHUNK_BYTES, 2048 * 1024L)
    }

    fun pipeStream(
        session: BtrMediaSession,
        requestedStart: Long = 0L,
        requestedEnd: Long? = null, // null 表示直至文件尾部
        isHeadOnly: Boolean = false,
        clientOut: OutputStream,
        concurrency: Int = 6,
        chunkSizeKb: Int = 512,
        autoConcurrency: Boolean = true,
        autoChunkSize: Boolean = true,
        proxyPort: Int = -1,
        parsedRange: ParsedRange = ParsedRange(
            hasRange = requestedStart > 0 || requestedEnd != null,
            start = requestedStart,
            end = requestedEnd
        ),
        keepAlive: Boolean = false,
        requestUserAgent: String? = null,
        requestReferer: String? = null,
        requestExtraHeaders: Map<String, String> = emptyMap()
    ): Boolean {
        if (parsedRange.isInvalid) {
            sendHttp416(clientOut, session.totalLength.takeIf { it > 0L })
            return false
        }

        val isAudio = session.isAudio || BiliAcceleratorCore.isAudioTrack(session.primaryUrl)
        var effectiveUserAgent = requestUserAgent?.takeIf { BtrLocalProxyServer.isUsableClientUserAgent(it) }
            ?: session.userAgent?.takeIf { BtrLocalProxyServer.isUsableClientUserAgent(it) }
            ?: resolveDefaultUserAgent()
        var effectiveReferer: String? = requestReferer?.takeIf { it.isNotBlank() } ?: session.referer
        var effectiveExtraHeaders: Map<String, String> = if (requestExtraHeaders.isNotEmpty()) {
            requestExtraHeaders
        } else {
            session.extraHeaders
        }

        val requestedBytes = if (parsedRange.hasRange && parsedRange.end != null && parsedRange.end >= parsedRange.start) {
            parsedRange.end - parsedRange.start + 1
        } else -1L

        val baseProbeSize = PROBE_PREFIX_BYTES.toLong()
        val initialProbeChunkSize = if (requestedBytes in 1 until baseProbeSize) {
            requestedBytes
        } else {
            baseProbeSize
        }

        val isEpisode = session.isEpisode
        val bounds = resolveConcurrencyBounds(
            concurrency = concurrency,
            autoConcurrency = autoConcurrency,
            isAudio = isAudio,
            isEpisode = isEpisode
        )
        val minCon = bounds.minCon
        val maxCon = bounds.maxCon
        val initialCon = bounds.initialCon
        val useAuto = bounds.useAuto
        val targetMaxCon = if (useAuto) maxCon else initialCon
        val startupSprintTarget = bounds.startupSprintTarget
        val existingController = session.concurrencyController as? DynamicConcurrencyController
        val isNewController = existingController == null ||
            existingController.autoScale != useAuto ||
            existingController.minConcurrency != minCon ||
            existingController.maxConcurrency != targetMaxCon
        val concurrencyController = if (!isNewController) {
            existingController!!
        } else {
            DynamicConcurrencyController(
                initialConcurrency = initialCon,
                autoScale = useAuto,
                minConcurrency = minCon,
                maxConcurrency = targetMaxCon,
                enablePersistence = useAuto
            ).also {
                if (useAuto && !isHeadOnly) {
                    it.startSprint(startupSprintTarget)
                }
                session.concurrencyController = it
            }
        }

        BtrSessionManager.maybeTriggerProactiveRefresh(session)
        var candidateUrls = session.currentCandidateUrls()
        val myStreamSeq = session.activeStreamSeq.incrementAndGet()
        val primaryHost = BiliAcceleratorCore.cleanHost(extractHostname(session.primaryUrl))
        val streamCallKey = "${session.sessionId}_${System.nanoTime()}"
        bindStreamSession(streamCallKey, session.sessionId)

        val statsTracker = StreamStatsTracker(
            sessionId = session.sessionId,
            primaryHost = primaryHost,
            concurrency = concurrencyController.currentConcurrency,
            proxyPort = proxyPort,
            isEpisode = isEpisode,
            isAudio = isAudio
        )

        try {
            val hasCachedMetadata = session.totalLength > 0L
            val totalLength: Long
            val contentType: String
            var firstChunk: ByteArray? = null
            var probeServingHost = primaryHost
            var probeLatencyMs = 20L
            var currentOffset: Long
            val effectiveStart: Long
            val effectiveEnd: Long?
            val actualEnd: Long
            var deferHeadersUntilFirstChunk = false

            fun probeWith403Recovery(probeStart: Long, probeSize: Long): ProbeOutcome {
                var outcome = probeMediaOutcome(
                    urls = candidateUrls,
                    session = session,
                    startOffset = probeStart,
                    chunkSize = probeSize,
                    userAgent = effectiveUserAgent,
                    referer = effectiveReferer,
                    extraHeaders = effectiveExtraHeaders,
                    concurrencyController = concurrencyController,
                    streamCallKey = streamCallKey
                )
                if (outcome.result != null || !outcome.sawForbidden403) {
                    return outcome
                }

                // 1. 链接已过期或候选签名已更新时优先触发一次签名刷新重试
                var didRefresh = false
                if (session.currentCandidateUrls() != candidateUrls || isUrlExpired(session)) {
                    didRefresh = true
                    if (session.currentCandidateUrls() == candidateUrls) {
                        BtrSessionManager.maybeTriggerProactiveRefresh(session, forceOnAuthFailure = true)
                    }
                    val refreshedUrls = session.currentCandidateUrls()
                    if (refreshedUrls != candidateUrls) {
                        candidateUrls = refreshedUrls
                        outcome = probeMediaOutcome(
                            urls = candidateUrls,
                            session = session,
                            startOffset = probeStart,
                            chunkSize = probeSize,
                            userAgent = effectiveUserAgent,
                            referer = effectiveReferer,
                            extraHeaders = effectiveExtraHeaders,
                            concurrencyController = concurrencyController,
                            streamCallKey = streamCallKey
                        )
                        if (outcome.result != null || !outcome.sawForbidden403) {
                            return outcome
                        }
                    }
                }

                // 2. 链接未过期但全节点 403：尝试有限次请求头兼容性重试（切换 Referer / UA）
                if (!isUrlExpired(session)) {
                    val altProfiles = computeAlternateHeaderProfiles(session, effectiveUserAgent, effectiveReferer)
                    for ((altUa, altRef) in altProfiles) {
                        val altExtra = if (altRef.isNullOrBlank()) {
                            effectiveExtraHeaders.filterKeys { !it.equals("Origin", ignoreCase = true) }
                        } else {
                            effectiveExtraHeaders
                        }
                        val altOutcome = probeMediaOutcome(
                            urls = candidateUrls,
                            session = session,
                            startOffset = probeStart,
                            chunkSize = probeSize,
                            userAgent = altUa,
                            referer = altRef,
                            extraHeaders = altExtra,
                            concurrencyController = concurrencyController,
                            streamCallKey = streamCallKey
                        )
                        if (altOutcome.result != null) {
                            effectiveUserAgent = altUa
                            effectiveReferer = altRef
                            effectiveExtraHeaders = altExtra
                            synchronized(session.urlLock) {
                                session.userAgent = altUa
                                session.referer = altRef
                                session.extraHeaders = altExtra
                            }
                            session.banList.clearTransient4xxRefusals()
                            Log.i(
                                TAG,
                                "BTR 403 header-compat recovery succeeded: session=${session.sessionId}, referer=${altRef ?: "<none>"}, ua=${altUa.take(48)}"
                            )
                            return altOutcome
                        }
                    }
                }

                // 3. 若此前尚未执行签名刷新（未过期 403 且请求头切换仍失败），最后触发一次强制签名刷新重试
                if (!didRefresh &&
                    (session.currentCandidateUrls() != candidateUrls ||
                        BtrSessionManager.maybeTriggerProactiveRefresh(session, forceOnAuthFailure = true) ||
                        session.currentCandidateUrls() != candidateUrls)
                ) {
                    candidateUrls = session.currentCandidateUrls()
                    outcome = probeMediaOutcome(
                        urls = candidateUrls,
                        session = session,
                        startOffset = probeStart,
                        chunkSize = probeSize,
                        userAgent = effectiveUserAgent,
                        referer = effectiveReferer,
                        extraHeaders = effectiveExtraHeaders,
                        concurrencyController = concurrencyController,
                        streamCallKey = streamCallKey
                    )
                }
                return outcome
            }

            if (parsedRange.isSuffix) {
                if (parsedRange.suffixLength <= 0L) {
                    sendHttp416(clientOut, session.totalLength.takeIf { it > 0L })
                    return false
                }
                // Suffix Range 模式: bytes=-suffixLength (如 bytes=-500)
                if (hasCachedMetadata) {
                    totalLength = session.totalLength
                    contentType = session.contentType ?: if (isAudio) "audio/mp4" else "video/mp4"
                } else {
                    val outcome = probeWith403Recovery(0L, initialProbeChunkSize)
                    val probeResult = outcome.result
                    if (probeResult == null) {
                        if (outcome.sawRangeNotSatisfiable416 && !outcome.sawForbidden403) {
                            val knownTotal = outcome.totalLengthFrom416?.takeIf { it > 0L }
                                ?: session.totalLength.takeIf { it > 0L }
                            if (knownTotal != null && session.totalLength <= 0L) {
                                session.totalLength = knownTotal
                            }
                            sendHttp416(clientOut, knownTotal)
                            return false
                        }
                        sendProbeFailureError(clientOut, session, outcome.sawForbidden403)
                        return false
                    }
                    if (probeResult.isNonRange) {
                        sendHttpError(clientOut, 502, "Bad Gateway - Upstream Ignored Suffix Range")
                        return false
                    }
                    totalLength = probeResult.totalLength
                    contentType = probeResult.contentType ?: if (isAudio) "audio/mp4" else "video/mp4"
                    probeServingHost = probeResult.servingHost.ifBlank { primaryHost }
                    probeLatencyMs = probeResult.probeLatencyMs
                    if (totalLength > 0L) {
                        session.totalLength = totalLength
                        session.contentType = contentType
                    }
                    if (totalLength > 0L && probeResult.firstChunkData != null && totalLength <= probeResult.firstChunkData.size) {
                        firstChunk = probeResult.firstChunkData
                    }
                }

                if (totalLength <= 0L) {
                    sendHttp416(clientOut, null)
                    return false
                }
                effectiveStart = maxOf(0L, totalLength - parsedRange.suffixLength)
                effectiveEnd = totalLength - 1L
                actualEnd = effectiveEnd
                if (effectiveStart >= totalLength) {
                    sendHttp416(clientOut, totalLength)
                    return false
                }

                if (firstChunk != null && effectiveStart < firstChunk.size) {
                    val offsetInFirst = effectiveStart.toInt()
                    val len = (actualEnd - effectiveStart + 1).toInt()
                    firstChunk = firstChunk.copyOfRange(offsetInFirst, minOf(firstChunk.size, offsetInFirst + len))
                    currentOffset = effectiveStart + firstChunk.size
                } else {
                    firstChunk = null
                    currentOffset = effectiveStart
                }
            } else {
                effectiveStart = parsedRange.start
                effectiveEnd = parsedRange.end

                if (effectiveEnd != null && effectiveEnd < effectiveStart) {
                    sendHttp416(clientOut, session.totalLength.takeIf { it > 0L })
                    return false
                }

                if (hasCachedMetadata && effectiveStart >= session.totalLength) {
                    sendHttp416(clientOut, session.totalLength)
                    return false
                }

                if (hasCachedMetadata && effectiveStart > 0L && !isHeadOnly) {
                    // Seek 延迟写头优化：立即并行拉取分片，待第 0 片成功后再写 206 响应头；若 403/502 仍可完整返回错误码
                    totalLength = session.totalLength
                    contentType = session.contentType ?: if (isAudio) "audio/mp4" else "video/mp4"
                    firstChunk = null
                    currentOffset = effectiveStart
                    actualEnd = when {
                        effectiveEnd != null -> minOf(effectiveEnd, totalLength - 1L)
                        else -> totalLength - 1L
                    }
                    deferHeadersUntilFirstChunk = true
                } else {
                    // 首片 64KB 竞速探针 (多源并发对冲探针，快速返回 HTTP 响应头)
                    val outcome = probeWith403Recovery(effectiveStart, initialProbeChunkSize)
                    val probeResult = outcome.result
                    if (probeResult == null) {
                        if (outcome.sawRangeNotSatisfiable416 && !outcome.sawForbidden403) {
                            val knownTotal = outcome.totalLengthFrom416?.takeIf { it > 0L }
                                ?: session.totalLength.takeIf { it > 0L }
                            if (knownTotal != null && session.totalLength <= 0L) {
                                session.totalLength = knownTotal
                            }
                            sendHttp416(clientOut, knownTotal)
                            return false
                        }
                        sendProbeFailureError(clientOut, session, outcome.sawForbidden403)
                        return false
                    }

                    if (probeResult.isNonRange) {
                        if (effectiveStart > 0L) {
                            sendHttpError(clientOut, 502, "Bad Gateway - Upstream Ignored Range Request")
                            return false
                        }
                        return pipeNonRangeStream(
                            url = probeResult.url,
                            session = session,
                            isHeadOnly = isHeadOnly,
                            userAgent = effectiveUserAgent,
                            referer = effectiveReferer,
                            extraHeaders = effectiveExtraHeaders,
                            clientOut = clientOut,
                            statsTracker = statsTracker,
                            streamCallKey = streamCallKey
                        )
                    }

                    totalLength = probeResult.totalLength
                    contentType = probeResult.contentType ?: if (isAudio) "audio/mp4" else "video/mp4"
                    probeServingHost = probeResult.servingHost.ifBlank { primaryHost }
                    probeLatencyMs = probeResult.probeLatencyMs
                    if (totalLength > 0L) {
                        session.totalLength = totalLength
                        session.contentType = contentType
                        if (effectiveStart >= totalLength) {
                            sendHttp416(clientOut, totalLength)
                            return false
                        }
                    }

                    firstChunk = probeResult.firstChunkData
                    actualEnd = when {
                        effectiveEnd != null -> if (totalLength > 0L) minOf(effectiveEnd, totalLength - 1L) else effectiveEnd
                        totalLength > 0L -> totalLength - 1L
                        else -> -1L
                    }
                    currentOffset = effectiveStart + (firstChunk?.size ?: 0)
                }
            }

            if (effectiveEnd != null && actualEnd >= 0L && actualEnd < effectiveStart) {
                sendHttp416(clientOut, totalLength.takeIf { it > 0L })
                return false
            }

            // Seek 并发瞬时冲刺恢复：兼容已完成片段的 Range 跳跃，及客户端主动断开旧流后重连的 Seek
            val prevWarmEnd = session.warmEnd.get()
            val abortedSeekHint = session.lastAbortedSeekHint.getAndSet(false)
            val prevObservedStart = session.lastObservedStart.getAndSet(effectiveStart)
            val isDisjointSeek = (prevWarmEnd > 0L && effectiveStart != prevWarmEnd) ||
                (abortedSeekHint && prevObservedStart >= 0L && effectiveStart != prevObservedStart) ||
                (prevWarmEnd == 0L && effectiveStart > 0L && prevObservedStart >= 0L && effectiveStart != prevObservedStart)
            if (!isHeadOnly && !isAudio && !isEpisode && useAuto && isDisjointSeek) {
                concurrencyController.onSeekSprint(startupSprintTarget)
            }

            // 根据探针测得或已缓存的媒体总长度、本次 Range 长度及实时单连接吞吐，自适应计算后续分片拉取的最佳 ChunkSize
            val remainingRangeHint = when {
                actualEnd >= currentOffset -> actualEnd - currentOffset + 1L
                requestedBytes > 0L -> (requestedBytes - (currentOffset - effectiveStart)).coerceAtLeast(1L)
                else -> -1L
            }
            val measuredConnBps = getTransferMeterSnapshot(isAudio).first
            val activeChunkSize = calculateDynamicChunkSize(
                totalLength = totalLength,
                baseChunkSizeKb = chunkSizeKb,
                autoChunkSize = autoChunkSize,
                isAudio = isAudio,
                requestedRangeBytes = remainingRangeHint,
                measuredConnectionBps = measuredConnBps,
                concurrency = concurrencyController.currentConcurrency,
                candidateHostCount = candidateUrls.size
            )

            // 2. 构造 HTTP 206 Partial Content (或 200 OK) 响应头（严格符合 RFC 9110，杜绝无 Content-Range 的 206）
            val hasExactTotalRange = totalLength > 0L && actualEnd >= effectiveStart
            val hasBoundedClientRange = effectiveEnd != null && actualEnd >= effectiveStart
            if (effectiveStart > 0L && !hasExactTotalRange && !hasBoundedClientRange) {
                sendHttpError(clientOut, 502, "Bad Gateway - Unknown Total Length for Open Range Seek")
                return false
            }
            val isPartial = (parsedRange.hasRange || effectiveStart > 0L || effectiveEnd != null) &&
                (hasExactTotalRange || hasBoundedClientRange)
            val useChunkedTransfer = !isPartial && !hasExactTotalRange && !hasBoundedClientRange && totalLength <= 0L
            val canKeepAliveResponse = keepAlive && hasExactTotalRange

            fun buildResponseHeadersBytes(): ByteArray {
                val statusLine = if (isPartial) "HTTP/1.1 206 Partial Content\r\n" else "HTTP/1.1 200 OK\r\n"
                val sb = StringBuilder(statusLine)
                sb.append("Content-Type: $contentType\r\n")
                sb.append("Accept-Ranges: bytes\r\n")

                if (canKeepAliveResponse) {
                    sb.append("Connection: keep-alive\r\n")
                    sb.append("Keep-Alive: timeout=15, max=100\r\n")
                } else {
                    sb.append("Connection: close\r\n")
                }

                if (hasExactTotalRange) {
                    val contentLength = actualEnd - effectiveStart + 1L
                    sb.append("Content-Length: $contentLength\r\n")
                    if (isPartial) {
                        sb.append("Content-Range: bytes $effectiveStart-$actualEnd/$totalLength\r\n")
                    }
                } else if (hasBoundedClientRange) {
                    val contentLength = actualEnd - effectiveStart + 1L
                    sb.append("Content-Length: $contentLength\r\n")
                    if (isPartial) {
                        sb.append("Content-Range: bytes $effectiveStart-$actualEnd/*\r\n")
                    }
                } else if (totalLength > 0L && !isPartial) {
                    sb.append("Content-Length: $totalLength\r\n")
                } else if (useChunkedTransfer) {
                    sb.append("Transfer-Encoding: chunked\r\n")
                }
                sb.append("\r\n")
                return sb.toString().toByteArray(Charsets.UTF_8)
            }

            var headersWritten = false
            if (!deferHeadersUntilFirstChunk) {
                clientOut.write(buildResponseHeadersBytes())
                clientOut.flush()
                headersWritten = true
            }

            if (isHeadOnly) return canKeepAliveResponse

            // 3. 写出首片探测数据（仅在未跳过探针且有首片时）
            if (firstChunk != null && firstChunk.isNotEmpty()) {
                val bytesToWrite = if (actualEnd >= effectiveStart) {
                    val needed = (actualEnd - effectiveStart + 1).toInt()
                    if (needed < firstChunk.size) firstChunk.copyOf(needed) else firstChunk
                } else firstChunk

                if (useChunkedTransfer) {
                    writeHttpChunk(clientOut, bytesToWrite)
                } else {
                    clientOut.write(bytesToWrite)
                }
                clientOut.flush()
                statsTracker.recordChunk(probeServingHost, bytesToWrite.size.toLong(), probeLatencyMs)
                concurrencyController.recordProgress(bytesToWrite.size.toLong())
                currentOffset = effectiveStart + bytesToWrite.size
            }

            // 未知总长且无客户端结束边界：采用 HTTP/1.1 chunked 顺序流式拉取；仅在上游真实 EOF 时写入 0\r\n\r\n 终结块，
            // 若中途失败则直接断开连接而不写终结块，确保客户端能可靠识别截断并重试而非误判正常 EOF
            if (actualEnd < 0L) {
                val completedCleanly = pipeRemainingSequential(
                    session = session,
                    startOffset = currentOffset,
                    chunkSize = activeChunkSize,
                    userAgent = effectiveUserAgent,
                    referer = effectiveReferer,
                    extraHeaders = effectiveExtraHeaders,
                    clientOut = clientOut,
                    statsTracker = statsTracker,
                    streamCallKey = streamCallKey,
                    useChunkedTransfer = useChunkedTransfer
                )
                if (completedCleanly && useChunkedTransfer) {
                    runCatching { writeHttpChunkTerminator(clientOut) }
                }
                return false
            }

            // 已在首片探针中完成全部范围传输
            if (currentOffset > actualEnd) {
                if (!isHeadOnly && hasExactTotalRange && session.activeStreamSeq.get() == myStreamSeq) {
                    session.warmEnd.set(actualEnd + 1L)
                }
                return canKeepAliveResponse
            }

            // 4. 懒加载切片任务切分与自适应并发流水线
            val remainingBytes = actualEnd - currentOffset + 1L
            val totalTasks = (remainingBytes + activeChunkSize - 1L) / activeChunkSize
            val taskMap = ConcurrentHashMap<Long, ChunkTask>()
            val completionLock = Object()
            val workerGate = Object()
            val activeWorkers = AtomicInteger(0)
            val isClientAborted = AtomicBoolean(false)
            val deliveredIndex = AtomicLong(0L)
            val workerPool = workerPoolFor(isAudio)

            val maxWindowByBytes = (MAX_BUFFER_BYTES / activeChunkSize.coerceAtLeast(64 * 1024L))
                .toInt()
                .coerceIn(MAX_PREFETCH_CHUNKS, 32)
            val dispatchedIndex = AtomicLong(0L)

            // 对齐上游 Semaphore.drainQueue() 同步交接语义：
            // 当工作线程退出且窗口内仍有待派发分片 (ready > 0) 时，计入即将立即补位的槽位 (refill)，
            // 杜绝每次分片完成瞬间 active 降至 limit-1 导致 isSaturated 抖动为 false 破坏持续饱和计时与冲刺回落判定。
            fun computeEffectiveDemandLocked(delivIdx: Long = deliveredIndex.get()): Triple<Int, Int, Int> {
                val lim = concurrencyController.currentConcurrency
                val act = activeWorkers.get()
                val win = minOf(maxWindowByBytes, maxOf(12, lim * 2))
                val readyLimit = minOf(totalTasks, delivIdx + win)
                val ready = if (isClientAborted.get()) 0L else (readyLimit - dispatchedIndex.get()).coerceAtLeast(0L)
                val refill = if (act < lim && ready > 0L) minOf((lim - act).toLong(), ready) else 0L
                val effectiveActive = act + refill.toInt()
                val remainingQueued = (ready - refill).coerceAtLeast(0L).coerceAtMost(Int.MAX_VALUE.toLong()).toInt()
                return Triple(effectiveActive, lim, remainingQueued)
            }

            fun recordEffectiveDemand(nowMs: Long = System.currentTimeMillis(), delivIdx: Long = deliveredIndex.get()) {
                val (effActive, lim, remQueued) = synchronized(workerGate) {
                    computeEffectiveDemandLocked(delivIdx)
                }
                concurrencyController.recordDemand(
                    active = effActive,
                    limit = lim,
                    queued = remQueued,
                    nowMs = nowMs
                )
            }

            val notifyCompletion: () -> Unit = {
                synchronized(completionLock) {
                    completionLock.notifyAll()
                }
            }

            val dispatchRunnable = Runnable {
                var dispatchIdx = 0L
                var lastGateDemandMs = System.currentTimeMillis()
                while (dispatchIdx < totalTasks && !isClientAborted.get()) {
                    var demandToRecord: Triple<Int, Int, Int>? = null
                    var demandAtMs = 0L
                    synchronized(workerGate) {
                        while (!isClientAborted.get() &&
                            (activeWorkers.get() >= concurrencyController.currentConcurrency ||
                                (dispatchIdx - deliveredIndex.get()) >= minOf(maxWindowByBytes, maxOf(12, concurrencyController.currentConcurrency * 2)))
                        ) {
                            val waitNow = System.currentTimeMillis()
                            if ((waitNow - lastGateDemandMs) >= 500L) {
                                lastGateDemandMs = waitNow
                                demandToRecord = computeEffectiveDemandLocked()
                                demandAtMs = waitNow
                                break
                            }
                            try {
                                workerGate.wait(25)
                            } catch (_: InterruptedException) {
                                break
                            }
                        }
                    }
                    if (demandToRecord != null) {
                        val (ea, lm, rq) = demandToRecord!!
                        concurrencyController.recordDemand(active = ea, limit = lm, queued = rq, nowMs = demandAtMs)
                        continue
                    }
                    if (isClientAborted.get()) break

                    val chunkStart = currentOffset + dispatchIdx * activeChunkSize
                    val chunkEnd = minOf(chunkStart + activeChunkSize - 1L, actualEnd)
                    val task = ChunkTask(dispatchIdx, chunkStart, chunkEnd)
                    taskMap[dispatchIdx] = task

                    val now = System.currentTimeMillis()
                    lastGateDemandMs = now
                    val (effActive, curLimit, remQueued) = synchronized(workerGate) {
                        activeWorkers.incrementAndGet()
                        dispatchIdx++
                        dispatchedIndex.set(dispatchIdx)
                        computeEffectiveDemandLocked()
                    }
                    concurrencyController.recordDemand(
                        active = effActive,
                        limit = curLimit,
                        queued = remQueued,
                        nowMs = now
                    )

                    try {
                        workerPool.execute {
                            try {
                                if (!isClientAborted.get()) {
                                    fetchChunkWithFailover(
                                        task = task,
                                        session = session,
                                        userAgent = effectiveUserAgent,
                                        referer = effectiveReferer,
                                        extraHeaders = effectiveExtraHeaders,
                                        statsTracker = statsTracker,
                                        concurrencyController = concurrencyController,
                                        isClientAborted = isClientAborted,
                                        streamCallKey = streamCallKey,
                                        totalTasks = totalTasks,
                                        onTaskCompleted = notifyCompletion
                                    )
                                }
                            } finally {
                                val (ea, lm, rq) = synchronized(workerGate) {
                                    activeWorkers.decrementAndGet()
                                    val snapshot = computeEffectiveDemandLocked()
                                    workerGate.notifyAll()
                                    snapshot
                                }
                                concurrencyController.recordDemand(
                                    active = ea,
                                    limit = lm,
                                    queued = rq,
                                    nowMs = System.currentTimeMillis()
                                )
                                notifyCompletion()
                            }
                        }
                    } catch (t: Throwable) {
                        task.isFailed = true
                        task.errorMessage = "Worker pool rejection: ${t.message}"
                        val (ea, lm, rq) = synchronized(workerGate) {
                            activeWorkers.decrementAndGet()
                            val snapshot = computeEffectiveDemandLocked()
                            workerGate.notifyAll()
                            snapshot
                        }
                        concurrencyController.recordDemand(
                            active = ea,
                            limit = lm,
                            queued = rq,
                            nowMs = System.currentTimeMillis()
                        )
                        notifyCompletion()
                    }
                }
            }

            val dispatchFuture = try {
                globalDispatcherPool.submit(dispatchRunnable)
            } catch (_: RejectedExecutionException) {
                val t = Thread(dispatchRunnable, "BTR-Dispatcher-Overflow").apply { isDaemon = true; start() }
                CompletableFuture<Void?>().also { f ->
                    Thread { runCatching { t.join() }; f.complete(null) }.apply { isDaemon = true; start() }
                }
            }

            try {
                var nextDeliverIndex = 0L
                var waitingChunkIdx = -1L
                var waitChunkStartedMs = 0L
                var waitChunkLastBytes = -1
                var waitChunkLastProgressMs = 0L
                var lastDeliveryDemandMs = System.currentTimeMillis()
                var lastStarvationSlowMs = 0L
                while (nextDeliverIndex < totalTasks && !isClientAborted.get()) {
                    val currentTask = taskMap[nextDeliverIndex]
                    if (currentTask == null || (currentTask.data == null && !currentTask.isFailed)) {
                        val nowWait = System.currentTimeMillis()
                        val curBytes = currentTask?.receivedBytes ?: 0
                        if (waitingChunkIdx != nextDeliverIndex) {
                            waitingChunkIdx = nextDeliverIndex
                            waitChunkStartedMs = nowWait
                            waitChunkLastBytes = curBytes
                            waitChunkLastProgressMs = nowWait
                        } else if (curBytes > waitChunkLastBytes) {
                            waitChunkLastBytes = curBytes
                            waitChunkLastProgressMs = nowWait
                        }
                        if ((nowWait - lastDeliveryDemandMs) >= 500L) {
                            lastDeliveryDemandMs = nowWait
                            recordEffectiveDemand(nowWait, nextDeliverIndex)
                        }
                        // 杜绝单分片正常耗时 >=1.2s 时误触 onBufferHunger（仅真实播放器低缓冲/卡顿信号触发 onBufferHunger/onStall）；
                        // 此处仅在队头分片长时间停滞无字节推进时上报 onSlow（内部校验真实队列饱和 >= 0.6）。
                        if ((nowWait - waitChunkStartedMs) >= 3_500L &&
                            (nowWait - waitChunkLastProgressMs) >= 2_500L &&
                            (nowWait - lastStarvationSlowMs) >= 3_000L &&
                            activeWorkers.get() > 0
                        ) {
                            lastStarvationSlowMs = nowWait
                            concurrencyController.onSlow(nowWait)
                            synchronized(workerGate) { workerGate.notifyAll() }
                        }
                        synchronized(completionLock) {
                            val task = taskMap[nextDeliverIndex]
                            if (!isClientAborted.get() && (task == null || (task.data == null && !task.isFailed))) {
                                completionLock.wait(400)
                            }
                        }
                        continue
                    }

                    if (currentTask.isFailed || currentTask.data == null) {
                        Log.e(TAG, "Chunk task #$nextDeliverIndex failed (${currentTask.errorMessage}). Aborting stream delivery.")
                        isClientAborted.set(true)
                        if (!headersWritten) {
                            sendProbeFailureError(
                                clientOut = clientOut,
                                session = session,
                                sawForbidden403 = currentTask.lastHttpCode == 403
                            )
                            headersWritten = true
                        }
                        return false
                    }

                    try {
                        if (!headersWritten) {
                            clientOut.write(buildResponseHeadersBytes())
                            headersWritten = true
                        }
                        val data = currentTask.data
                        if (data != null) {
                            clientOut.write(data)
                            clientOut.flush()
                            statsTracker.concurrency = concurrencyController.peakConcurrency
                        }
                        currentTask.releaseBuffers()
                        taskMap.remove(nextDeliverIndex)
                        nextDeliverIndex++
                        waitingChunkIdx = -1L
                        deliveredIndex.set(nextDeliverIndex)
                        val delivNow = System.currentTimeMillis()
                        lastDeliveryDemandMs = delivNow
                        val (ea, lm, rq) = synchronized(workerGate) {
                            val snapshot = computeEffectiveDemandLocked(nextDeliverIndex)
                            workerGate.notifyAll()
                            snapshot
                        }
                        concurrencyController.recordDemand(
                            active = ea,
                            limit = lm,
                            queued = rq,
                            nowMs = delivNow
                        )
                    } catch (_: IOException) {
                        Log.i(TAG, "Client disconnected (Seek or stop). Session=${session.sessionId}")
                        isClientAborted.set(true)
                        if (session.activeStreamSeq.get() == myStreamSeq) {
                            session.warmEnd.set(0L)
                            session.lastAbortedSeekHint.set(true)
                        }
                        return false
                    }
                }
                val completed = nextDeliverIndex >= totalTasks && !isClientAborted.get()
                if (session.activeStreamSeq.get() == myStreamSeq) {
                    if (completed && !isHeadOnly && hasExactTotalRange) {
                        session.warmEnd.set(actualEnd + 1L)
                    } else if (!completed) {
                        session.warmEnd.set(0L)
                    }
                }
                return (completed && canKeepAliveResponse)
            } finally {
                isClientAborted.set(true)
                synchronized(workerGate) {
                    workerGate.notifyAll()
                }
                dispatchFuture.cancel(true)
                cancelStreamCalls(streamCallKey)
                taskMap.values.forEach { it.releaseBuffers() }
                taskMap.clear()
            }
        } finally {
            cancelStreamCalls(streamCallKey)
            statsTracker.finishAndReport()
        }
    }

    private data class ProbeResult(
        val url: String,
        val totalLength: Long,
        val contentLength: Long,
        val contentType: String?,
        val firstChunkData: ByteArray?,
        val servingHost: String,
        val probeLatencyMs: Long,
        val isNonRange: Boolean = false
    )

    private data class ProbeOutcome(
        val result: ProbeResult?,
        val sawForbidden403: Boolean,
        val sawRangeNotSatisfiable416: Boolean = false,
        val totalLengthFrom416: Long? = null
    )

    private data class ContentRangeInfo(
        val start: Long,
        val end: Long?,
        val total: Long?
    )

    private val attemptSeq = AtomicLong(0L)
    private val CONTENT_RANGE_REGEX = Regex("""bytes\s+(\d+)-(\d+|\*)/(\d+|\*)""", RegexOption.IGNORE_CASE)

    private fun parseContentRange(cr: String?): ContentRangeInfo? {
        if (cr.isNullOrBlank()) return null
        val full = CONTENT_RANGE_REGEX.find(cr)
        if (full != null) {
            val start = full.groupValues[1].toLongOrNull() ?: return null
            val end = full.groupValues[2].toLongOrNull()
            val total = full.groupValues[3].toLongOrNull()
            return ContentRangeInfo(start, end, total)
        }
        val fallbackStart = parseRangeStart(cr) ?: return null
        val fallbackTotal = cr.substringAfterLast('/', "").trim().toLongOrNull()
        return ContentRangeInfo(fallbackStart, null, fallbackTotal)
    }

    internal fun isValidContentRange(
        code: Int,
        cr: String?,
        expectedStart: Long,
        session: BtrMediaSession,
        expectedEnd: Long? = null
    ): Boolean {
        if (code != 206) return cr.isNullOrBlank()
        if (cr.isNullOrBlank()) return false
        val info = parseContentRange(cr) ?: return false
        if (info.start != expectedStart) return false
        if (info.total != null && session.totalLength > 0L && info.total != session.totalLength) {
            return false
        }
        if (expectedEnd != null && info.end != null) {
            val eofEnd = info.total?.let { it - 1L }
            if (info.end != expectedEnd && info.end != eofEnd) {
                return false
            }
        }
        return true
    }

    private fun isUrlExpired(session: BtrMediaSession): Boolean {
        val dl = session.urlDeadlineEpochSec
        return dl > 0L && (System.currentTimeMillis() / 1000L) >= dl
    }

    private fun sendProbeFailureError(
        clientOut: OutputStream,
        session: BtrMediaSession,
        sawForbidden403: Boolean
    ) {
        if (sawForbidden403 || isUrlExpired(session)) {
            sendHttpError(clientOut, 403, "Forbidden - Upstream URL Expired or Refused")
        } else {
            sendHttpError(clientOut, 502, "Bad Gateway - Upstream CDN Unreachable")
        }
    }

    private fun pipeNonRangeStream(
        url: String,
        session: BtrMediaSession,
        isHeadOnly: Boolean,
        userAgent: String,
        referer: String?,
        extraHeaders: Map<String, String> = emptyMap(),
        clientOut: OutputStream,
        statsTracker: StreamStatsTracker,
        streamCallKey: String
    ): Boolean {
        val host = extractHostname(url)
        val startTime = System.currentTimeMillis()
        val okClient = if (session.isAudio) BtrHttpClient.audioStreamClient else BtrHttpClient.client
        val request = buildUpstreamRequest(
            url = url,
            userAgent = userAgent,
            referer = referer,
            extraHeaders = extraHeaders,
            closeConnection = true,
            isHeadOnly = isHeadOnly
        )

        val call = okClient.newCall(request)
        val trackedCall = TrackedCall(call)
        registerStreamCall(streamCallKey, session.sessionId, trackedCall)
        return try {
            call.execute().use { response ->
                val code = response.code
                val cr = response.header("Content-Range")
                if (code !in 200..299 || (code == 200 && !cr.isNullOrBlank())) {
                    val status = if (code == 403) 403 else 502
                    val msg = if (code == 403) "Forbidden" else "Bad Gateway"
                    sendHttpError(clientOut, status, msg)
                    return false
                }
                val cl = response.body?.contentLength() ?: -1L
                val useChunked = cl <= 0L && code != 204 && code != 304
                val ct = response.header("Content-Type") ?: if (session.isAudio) "audio/mp4" else "video/mp4"
                val reason = if (code == 206) "Partial Content" else "OK"

                val sb = StringBuilder("HTTP/1.1 $code $reason\r\n")
                sb.append("Content-Type: $ct\r\n")
                if (cl > 0L) {
                    sb.append("Content-Length: $cl\r\n")
                } else if (useChunked) {
                    sb.append("Transfer-Encoding: chunked\r\n")
                }
                sb.append("Connection: close\r\n\r\n")
                clientOut.write(sb.toString().toByteArray(Charsets.UTF_8))
                clientOut.flush()

                if (!isHeadOnly) {
                    var totalBytes = 0L
                    response.body?.byteStream()?.use { input ->
                        val buffer = ByteArray(PROBE_PREFIX_BYTES)
                        var read: Int
                        while (input.read(buffer).also { read = it } != -1) {
                            if (useChunked) {
                                writeHttpChunk(clientOut, buffer, 0, read)
                            } else {
                                clientOut.write(buffer, 0, read)
                            }
                            totalBytes += read
                        }
                    }
                    if (useChunked) {
                        writeHttpChunkTerminator(clientOut)
                    } else {
                        clientOut.flush()
                    }
                    statsTracker.recordChunk(host, totalBytes, System.currentTimeMillis() - startTime)
                }
                false
            }
        } catch (_: Exception) {
            false
        } finally {
            unregisterStreamCall(streamCallKey, trackedCall)
        }
    }

    private fun pipeRemainingSequential(
        session: BtrMediaSession,
        startOffset: Long,
        chunkSize: Long,
        userAgent: String,
        referer: String?,
        extraHeaders: Map<String, String> = emptyMap(),
        clientOut: OutputStream,
        statsTracker: StreamStatsTracker,
        streamCallKey: String,
        useChunkedTransfer: Boolean = false
    ): Boolean {
        var offset = startOffset
        val okClient = if (session.isAudio) BtrHttpClient.chunkAudioClient else BtrHttpClient.chunkClient
        val buffer = ByteArray(16 * 1024)
        var didRefresh403 = false

        while (true) {
            val reqEnd = offset + chunkSize - 1L
            val activeUrls = session.currentCandidateUrls()
            val candidates = sortUrlsByHealthAndSpeed(activeUrls, offset / chunkSize.coerceAtLeast(1L), session)
            var progressed = false
            var reachedEof = false
            var saw403 = false

            for (url in candidates) {
                val host = extractHostname(url)
                val startTime = System.currentTimeMillis()
                val req = buildUpstreamRequest(
                    url = url,
                    userAgent = userAgent,
                    referer = referer,
                    extraHeaders = extraHeaders,
                    rangeHeader = "bytes=$offset-$reqEnd"
                )
                val call = okClient.newCall(req)
                val trackedCall = TrackedCall(call)
                registerStreamCall(streamCallKey, session.sessionId, trackedCall)
                var chunkBytes = 0L
                try {
                    AttemptTimeoutWatchdog(trackedCall).use { watchdog ->
                        call.execute().use { response ->
                            watchdog.onResponseHeaders()
                            val code = response.code
                            if (code == 416) {
                                reachedEof = true
                                return@use
                            }
                            if (code != 206) {
                                if (code == 403) saw403 = true
                                session.banList.recordHttpFailure(url, host, if (code == 200) 410 else code)
                                return@use
                            }
                            val cr = response.header("Content-Range")
                            if (!isValidContentRange(code, cr, offset, session)) {
                                session.banList.recordHttpFailure(url, host, 410)
                                return@use
                            }
                            val crInfo = parseContentRange(cr)
                            crInfo?.total?.let { knownTotal ->
                                if (knownTotal > 0L && session.totalLength <= 0L) {
                                    session.totalLength = knownTotal
                                }
                            }
                            val expectedBodyLen = response.body?.contentLength() ?: -1L
                            val bodyStream = response.body?.byteStream() ?: return@use
                            var read: Int
                            while (bodyStream.read(buffer).also { read = it } != -1) {
                                watchdog.onBytesRead()
                                try {
                                    if (useChunkedTransfer) {
                                        writeHttpChunk(clientOut, buffer, 0, read)
                                    } else {
                                        clientOut.write(buffer, 0, read)
                                    }
                                } catch (ce: IOException) {
                                    throw ClientDisconnectException(ce)
                                }
                                chunkBytes += read
                            }
                            try {
                                clientOut.flush()
                            } catch (ce: IOException) {
                                throw ClientDisconnectException(ce)
                            }
                            if (chunkBytes == 0L) {
                                session.banList.recordTransportFailure(host, 15_000L, url = url, receivedBytes = 0L)
                                penalizeHost(host, 12_000L)
                                return@use
                            }
                            val latency = maxOf(1L, System.currentTimeMillis() - startTime)
                            statsTracker.recordChunk(host, chunkBytes, latency)
                            offset += chunkBytes
                            progressed = true
                            val knownTotal = crInfo?.total
                            if (expectedBodyLen > 0L && chunkBytes < expectedBodyLen) {
                                session.banList.recordTransportFailure(host, 15_000L, url = url, receivedBytes = chunkBytes)
                                penalizeHost(host, 12_000L)
                            } else {
                                recordRouteSpeed(url, chunkBytes, latency)
                                session.banList.recordSuccess(url, host)
                                if (knownTotal != null && offset >= knownTotal) {
                                    reachedEof = true
                                } else if (chunkBytes < chunkSize) {
                                    reachedEof = true
                                }
                            }
                        }
                    }
                } catch (_: ClientDisconnectException) {
                    return false
                } catch (e: IOException) {
                    if (chunkBytes > 0L) {
                        try {
                            clientOut.flush()
                        } catch (_: IOException) {
                            return false
                        }
                        val latency = maxOf(1L, System.currentTimeMillis() - startTime)
                        statsTracker.recordChunk(host, chunkBytes, latency)
                        if (chunkBytes >= CANCEL_SPEED_MIN_BYTES) {
                            recordRouteSpeed(url, chunkBytes, latency)
                        }
                        offset += chunkBytes
                        progressed = true
                    }
                    val callerCanceled = trackedCall.isCallerCanceled()
                    val timedOut = isTimeoutError(e, call.isCanceled(), callerCanceled, trackedCall.isTimedOut())
                    if (!callerCanceled || timedOut) {
                        session.banList.recordTransportFailure(host, 15_000L, url = url, receivedBytes = chunkBytes)
                        penalizeHost(host, 12_000L)
                    }
                } catch (e: Exception) {
                    if (!trackedCall.isCallerCanceled()) {
                        session.banList.recordTransportFailure(host, 15_000L, url = url, receivedBytes = chunkBytes)
                    }
                } finally {
                    unregisterStreamCall(streamCallKey, trackedCall)
                }
                if (reachedEof || progressed) break
            }

            if (!reachedEof && !progressed && saw403 && !didRefresh403 &&
                BtrSessionManager.maybeTriggerProactiveRefresh(session, forceOnAuthFailure = true)
            ) {
                didRefresh403 = true
                continue
            }
            if (reachedEof) return true
            if (!progressed) return false
        }
    }

    private fun executeSingleProbe(
        url: String,
        session: BtrMediaSession,
        startOffset: Long,
        chunkSize: Long,
        userAgent: String,
        referer: String?,
        extraHeaders: Map<String, String> = emptyMap(),
        okClient: okhttp3.OkHttpClient,
        primaryHost: String,
        concurrencyController: DynamicConcurrencyController?,
        activeCalls: MutableSet<TrackedCall>?,
        isDecided: AtomicBoolean?,
        streamCallKey: String?,
        sawForbidden403: AtomicBoolean?,
        sawRangeNotSatisfiable416: AtomicBoolean? = null,
        totalLengthFrom416: AtomicLong? = null,
        primaryFailReason: AtomicReference<String?>? = null
    ): ProbeResult? {
        if (isDecided?.get() == true) return null
        val host = extractHostname(url)
        val startTime = System.currentTimeMillis()
        val endOffset = startOffset + chunkSize - 1
        val request = buildUpstreamRequest(
            url = url,
            userAgent = userAgent,
            referer = referer,
            extraHeaders = extraHeaders,
            rangeHeader = "bytes=$startOffset-$endOffset"
        )

        val call = okClient.newCall(request)
        val trackedCall = TrackedCall(call)
        activeCalls?.add(trackedCall)
        if (streamCallKey != null) {
            registerStreamCall(streamCallKey, session.sessionId, trackedCall)
        }
        var localBytesRead = 0L
        var completedNormally = false
        return try {
            AttemptTimeoutWatchdog(trackedCall).use { watchdog ->
                call.execute().use { response ->
                    watchdog.onResponseHeaders()
                    if (isDecided?.get() == true) return null
                    val code = response.code
                    val responseHost = BiliAcceleratorCore.normalizeHost(response.request.url.host).ifBlank { host }
                    if (code == 200 || code == 206) {
                        val cr = response.header("Content-Range")
                        if (code == 200 && !cr.isNullOrBlank()) {
                            session.banList.recordHttpFailure(url, host, 410)
                            if (host == primaryHost) {
                                primaryFailReason?.compareAndSet(null, "首片 HTTP 200 携带异常 Content-Range ($cr)")
                            }
                            return null
                        }
                        val isNonRange = (code == 200 && cr.isNullOrBlank())
                        if (isNonRange && startOffset > 0L) {
                            session.banList.recordHttpFailure(url, host, 410)
                            return null
                        }
                        if (!isNonRange && !isValidContentRange(code, cr, startOffset, session)) {
                            session.banList.recordHttpFailure(url, host, 410)
                            if (host == primaryHost) {
                                primaryFailReason?.compareAndSet(null, "首片 Content-Range 异常 ($cr)")
                            }
                            return null
                        }
                        val crInfo = parseContentRange(cr)
                        val cl = response.body?.contentLength() ?: -1L
                        val total = if (isNonRange) cl else (crInfo?.total ?: cr?.substringAfterLast("/")?.trim()?.toLongOrNull() ?: -1L)
                        val ct = response.header("Content-Type")?.takeIf { it.isNotBlank() }

                        val data = if (isNonRange) {
                            ByteArray(0)
                        } else {
                            val stream = response.body?.byteStream()
                            if (stream == null) {
                                ByteArray(0)
                            } else {
                                val outBuf = java.io.ByteArrayOutputStream(chunkSize.coerceIn(1024L, 256 * 1024L).toInt())
                                val tmp = ByteArray(16 * 1024)
                                var read: Int
                                while (stream.read(tmp).also { read = it } != -1) {
                                    watchdog.onBytesRead()
                                    outBuf.write(tmp, 0, read)
                                    localBytesRead += read
                                    if (isDecided?.get() == true) {
                                        return null
                                    }
                                }
                                outBuf.toByteArray()
                            }
                        }
                        if (!isNonRange && data.isEmpty()) {
                            session.banList.recordTransportFailure(host, 20_000L, url = url)
                            penalizeHost(host, 20_000L)
                            if (host == primaryHost) {
                                primaryFailReason?.compareAndSet(null, "首片探测返回 0KB 空数据熔断")
                            }
                            return null
                        }
                        completedNormally = true
                        val latency = maxOf(1L, System.currentTimeMillis() - startTime)
                        session.banList.recordSuccess(url, host)
                        if (data.isNotEmpty()) {
                            recordRouteSpeed(url, data.size.toLong(), latency)
                        }
                        ProbeResult(url, total, cl, ct, data, responseHost, latency, isNonRange = isNonRange)
                    } else if (code == 416) {
                        sawRangeNotSatisfiable416?.set(true)
                        val total416 = response.header("Content-Range")
                            ?.substringAfterLast('/', "")
                            ?.trim()
                            ?.toLongOrNull()
                        if (total416 != null && total416 > 0L) {
                            totalLengthFrom416?.compareAndSet(-1L, total416)
                        }
                        null
                    } else {
                        if (code == 403) sawForbidden403?.set(true)
                        val isNodeFault = session.banList.recordHttpFailure(url, host, code)
                        if (code == 412 || code == 429) {
                            concurrencyController?.onPushback(isHard = true)
                        }
                        if (isNodeFault) {
                            val penaltyTime = if (code == 429) 45_000L else if (code == 412) 30_000L else 20_000L
                            penalizeHost(host, penaltyTime)
                        }
                        if (host == primaryHost) {
                            val detail = if (!responseHost.equals(host, ignoreCase = true)) {
                                "HTTP $code 错误 (跳转 $responseHost)"
                            } else {
                                "HTTP $code 错误"
                            }
                            primaryFailReason?.compareAndSet(null, detail)
                        }
                        null
                    }
                }
            }
        } catch (e: Exception) {
            val callerCanceled = trackedCall.isCallerCanceled()
            val timedOut = isTimeoutError(
                e = e,
                callCanceled = call.isCanceled(),
                callerCanceled = callerCanceled,
                timedOutFlag = trackedCall.isTimedOut()
            )
            if (!callerCanceled || timedOut) {
                if (timedOut && localBytesRead == 0L) {
                    concurrencyController?.onSlow()
                }
                session.banList.recordTransportFailure(host, 15_000L, url = url, receivedBytes = localBytesRead)
                penalizeHost(host, 12_000L)
                if (host == primaryHost) {
                    primaryFailReason?.compareAndSet(null, trackedCall.timeoutReason.get() ?: e.message ?: "连接超时")
                }
            }
            null
        } finally {
            // 对齐上游 idm-downloader.js：被胜出者取消的竞争探针若已读入 >= 48 KiB，记录其有效局部速率以加速节点测速收敛
            if (!completedNormally && localBytesRead >= SPEED_SAMPLE_MIN_BYTES) {
                val partialElapsed = maxOf(1L, System.currentTimeMillis() - startTime)
                recordRouteSpeed(url, localBytesRead, partialElapsed)
            }
            activeCalls?.remove(trackedCall)
            if (streamCallKey != null) {
                unregisterStreamCall(streamCallKey, trackedCall)
            }
        }
    }

    private fun raceProbeBatch(
        batch: List<String>,
        delaysMs: LongArray,
        waitTimeoutMs: Long,
        session: BtrMediaSession,
        startOffset: Long,
        chunkSize: Long,
        userAgent: String,
        referer: String?,
        extraHeaders: Map<String, String> = emptyMap(),
        okClient: okhttp3.OkHttpClient,
        primaryHost: String,
        concurrencyController: DynamicConcurrencyController?,
        streamCallKey: String?,
        sawForbidden403: AtomicBoolean,
        saw416: AtomicBoolean,
        total416: AtomicLong,
        primaryFailReason: AtomicReference<String?>
    ): ProbeResult? {
        if (batch.isEmpty()) return null
        if (batch.size == 1) {
            return executeSingleProbe(
                url = batch[0],
                session = session,
                startOffset = startOffset,
                chunkSize = chunkSize,
                userAgent = userAgent,
                referer = referer,
                extraHeaders = extraHeaders,
                okClient = okClient,
                primaryHost = primaryHost,
                concurrencyController = concurrencyController,
                activeCalls = null,
                isDecided = null,
                streamCallKey = streamCallKey,
                sawForbidden403 = sawForbidden403,
                sawRangeNotSatisfiable416 = saw416,
                totalLengthFrom416 = total416,
                primaryFailReason = primaryFailReason
            )
        }
        val isDecided = AtomicBoolean(false)
        val resultFuture = CompletableFuture<ProbeResult?>()
        val activeCalls = ConcurrentHashMap.newKeySet<TrackedCall>()
        val failedCount = AtomicInteger(0)
        val staggerLock = Object()

        for (i in batch.indices) {
            val url = batch[i]
            val delayMs = delaysMs.getOrElse(i) { delaysMs.lastOrNull() ?: 200L }
            try {
                hedgeWorkerPool.execute {
                    var outcomeRecorded = false
                    try {
                        if (delayMs > 0L) {
                            val deadline = System.currentTimeMillis() + delayMs
                            synchronized(staggerLock) {
                                while (!isDecided.get() && failedCount.get() < i) {
                                    val rem = deadline - System.currentTimeMillis()
                                    if (rem <= 0L) break
                                    try {
                                        staggerLock.wait(rem)
                                    } catch (_: InterruptedException) {
                                        break
                                    }
                                }
                            }
                        }
                        if (isDecided.get()) {
                            outcomeRecorded = true
                            return@execute
                        }

                        val probeRes = executeSingleProbe(
                            url = url,
                            session = session,
                            startOffset = startOffset,
                            chunkSize = chunkSize,
                            userAgent = userAgent,
                            referer = referer,
                            extraHeaders = extraHeaders,
                            okClient = okClient,
                            primaryHost = primaryHost,
                            concurrencyController = concurrencyController,
                            activeCalls = activeCalls,
                            isDecided = isDecided,
                            streamCallKey = streamCallKey,
                            sawForbidden403 = sawForbidden403,
                            sawRangeNotSatisfiable416 = saw416,
                            totalLengthFrom416 = total416,
                            primaryFailReason = primaryFailReason
                        )
                        if (probeRes != null && isDecided.compareAndSet(false, true)) {
                            outcomeRecorded = true
                            resultFuture.complete(probeRes)
                            synchronized(staggerLock) { staggerLock.notifyAll() }
                            activeCalls.forEach { tracked -> tracked.cancelByCaller() }
                        } else if (!isDecided.get()) {
                            outcomeRecorded = true
                            val failures = failedCount.incrementAndGet()
                            synchronized(staggerLock) { staggerLock.notifyAll() }
                            if (failures >= batch.size && isDecided.compareAndSet(false, true)) {
                                resultFuture.complete(null)
                            }
                        } else {
                            outcomeRecorded = true
                        }
                    } catch (_: Throwable) {
                    } finally {
                        if (!outcomeRecorded && !isDecided.get()) {
                            val failures = failedCount.incrementAndGet()
                            synchronized(staggerLock) { staggerLock.notifyAll() }
                            if (failures >= batch.size && isDecided.compareAndSet(false, true)) {
                                resultFuture.complete(null)
                            }
                        }
                    }
                }
            } catch (_: Throwable) {
                val failures = failedCount.incrementAndGet()
                synchronized(staggerLock) { staggerLock.notifyAll() }
                if (failures >= batch.size && isDecided.compareAndSet(false, true)) {
                    resultFuture.complete(null)
                }
            }
        }

        return try {
            resultFuture.get(waitTimeoutMs, TimeUnit.MILLISECONDS)
        } catch (_: Exception) {
            isDecided.set(true)
            activeCalls.forEach { tracked -> tracked.cancelByTimeout("probe_race_timeout") }
            null
        }
    }

    private fun probeMediaOutcome(
        urls: List<String>,
        session: BtrMediaSession,
        startOffset: Long,
        chunkSize: Long,
        userAgent: String,
        referer: String?,
        extraHeaders: Map<String, String> = emptyMap(),
        concurrencyController: DynamicConcurrencyController? = null,
        streamCallKey: String? = null
    ): ProbeOutcome {
        val sortedUrls = sortUrlsByHealthAndSpeed(urls, 0L, session)
        if (sortedUrls.isEmpty()) return ProbeOutcome(null, sawForbidden403 = false)
        val primaryHost = extractHostname(session.primaryUrl)
        val okClient = if (session.isAudio) BtrHttpClient.audioProbeClient else BtrHttpClient.probeClient
        val sawForbidden403 = AtomicBoolean(false)
        val saw416 = AtomicBoolean(false)
        val total416 = AtomicLong(-1L)
        val primaryFailReason = AtomicReference<String?>(null)

        fun finalizeOutcome(winner: ProbeResult?): ProbeOutcome {
            val failReason = primaryFailReason.get()
            if (failReason != null) {
                val target = winner?.servingHost?.takeIf { it.isNotBlank() } ?: "备用镜像/CDN"
                reportFailover(primaryHost, failReason, session.sessionId, targetHost = target)
            }
            return if (winner != null) {
                ProbeOutcome(winner, sawForbidden403.get())
            } else {
                ProbeOutcome(
                    result = null,
                    sawForbidden403 = sawForbidden403.get(),
                    sawRangeNotSatisfiable416 = saw416.get(),
                    totalLengthFrom416 = total416.get().takeIf { it > 0L }
                )
            }
        }

        val raceCandidates = sortedUrls.take(3)
        val fallbackCandidates = sortedUrls.drop(3)

        val primaryWinner = raceProbeBatch(
            batch = raceCandidates,
            delaysMs = longArrayOf(0L, 120L, 300L),
            waitTimeoutMs = 7_500L,
            session = session,
            startOffset = startOffset,
            chunkSize = chunkSize,
            userAgent = userAgent,
            referer = referer,
            extraHeaders = extraHeaders,
            okClient = okClient,
            primaryHost = primaryHost,
            concurrencyController = concurrencyController,
            streamCallKey = streamCallKey,
            sawForbidden403 = sawForbidden403,
            saw416 = saw416,
            total416 = total416,
            primaryFailReason = primaryFailReason
        )
        if (primaryWinner != null) {
            return finalizeOutcome(primaryWinner)
        }

        val nowSec = System.currentTimeMillis() / 1000L
        for (batch in fallbackCandidates.chunked(3)) {
            if (sawForbidden403.get() && isUrlExpired(session) &&
                batch.all { u ->
                    val d = BiliAcceleratorCore.extractUrlDeadlineSeconds(u)
                    d > 0L && d <= nowSec
                }
            ) {
                break
            }
            val knownTotal416 = total416.get()
            if (saw416.get() && !sawForbidden403.get() && knownTotal416 > 0L && startOffset >= knownTotal416) break
            val fallbackWinner = raceProbeBatch(
                batch = batch,
                delaysMs = longArrayOf(0L, 80L, 180L),
                waitTimeoutMs = 6_500L,
                session = session,
                startOffset = startOffset,
                chunkSize = chunkSize,
                userAgent = userAgent,
                referer = referer,
                extraHeaders = extraHeaders,
                okClient = okClient,
                primaryHost = primaryHost,
                concurrencyController = concurrencyController,
                streamCallKey = streamCallKey,
                sawForbidden403 = sawForbidden403,
                saw416 = saw416,
                total416 = total416,
                primaryFailReason = primaryFailReason
            )
            if (fallbackWinner != null) {
                return finalizeOutcome(fallbackWinner)
            }
        }
        return finalizeOutcome(null)
    }

    private fun fetchChunkWithFailover(
        task: ChunkTask,
        session: BtrMediaSession,
        userAgent: String,
        referer: String?,
        extraHeaders: Map<String, String> = emptyMap(),
        statsTracker: StreamStatsTracker,
        concurrencyController: DynamicConcurrencyController?,
        isClientAborted: AtomicBoolean,
        streamCallKey: String,
        totalTasks: Long,
        onTaskCompleted: (() -> Unit)? = null
    ) {
        if (isClientAborted.get()) return
        val lastHttpCode = AtomicInteger(0)
        val sawForbidden403 = AtomicBoolean(false)
        var didRefresh403 = false
        val triedUrlsInPiece = java.util.Collections.synchronizedSet(HashSet<String>())
        var round = 0

        while (round < PIECE_ROUNDS) {
            if (isClientAborted.get() || task.isSuccess) break
            if (round > 0 && !sawForbidden403.get() && lastHttpCode.get() != 403) {
                concurrencyController?.onSlow()
            }

            val roundStartMs = System.currentTimeMillis()
            val baselineBytes = task.receivedBytes.toLong()
            val activeUrls = session.currentCandidateUrls()
            var healthyMeasured = 0
            var healthyUnmeasured = 0
            for (u in activeUrls) {
                val h = extractHostname(u)
                if (!isPenalized(h) && !session.banList.isBanned(u, h, roundStartMs)) {
                    if (getRouteSpeed(u) > 0.0) healthyMeasured++ else healthyUnmeasured++
                }
            }
            val allowUnmeasuredTrial = (round == 0) && (
                shouldAllowUnmeasuredTrial(task.index, totalTasks, healthyMeasured, healthyUnmeasured) || (
                    healthyUnmeasured > 0 &&
                        totalTasks in 2L..3L &&
                        task.index == totalTasks - 1L &&
                        (session.unmeasuredTrialWaited.incrementAndGet() % 4 == 0)
                    )
                )
            val rotationIndex = session.globalChunkIndex.getAndIncrement()
            val sortedUrls = sortUrlsByHealthAndSpeed(activeUrls, rotationIndex, session, allowUnmeasuredTrial)
            if (sortedUrls.isEmpty()) break

            val now = System.currentTimeMillis()
            val unbannedUrls = sortedUrls.filter { url ->
                val host = extractHostname(url)
                !session.banList.isBanned(url, host, now) && !isPenalized(host)
            }
            val roundUrls = when {
                unbannedUrls.isNotEmpty() -> unbannedUrls
                else -> {
                    // 所有节点被临时封禁或惩罚时兜底尝试非永久封禁列表（对齐 BTR all-banned reset fallback）
                    sortedUrls.filter { url ->
                        !session.banList.isPermanentlyBanned(url, extractHostname(url))
                    }.ifEmpty { sortedUrls }
                }
            }

            val untriedRoundUrls = roundUrls.filter { it !in triedUrlsInPiece }
            val alreadyTriedRoundUrls = roundUrls.filter { it in triedUrlsInPiece }
            val orderedRoundUrls = when {
                untriedRoundUrls.isNotEmpty() -> untriedRoundUrls + alreadyTriedRoundUrls
                roundUrls.size > 1 -> {
                    val shift = round % roundUrls.size
                    roundUrls.subList(shift, roundUrls.size) + roundUrls.subList(0, shift)
                }
                else -> roundUrls
            }

            val primaryUrl = orderedRoundUrls.first()
            triedUrlsInPiece.add(primaryUrl)
            val primaryTrialKey = if (allowUnmeasuredTrial && getRouteSpeed(primaryUrl) <= 0.0) {
                routeKey(primaryUrl).takeIf { it.isNotBlank() }?.also { rk ->
                    sessionInFlightTrials.computeIfAbsent(session.sessionId) { ConcurrentHashMap.newKeySet() }.add(rk)
                }
            } else null
            val primaryEndpoint = extractEndpointKey(primaryUrl)
            val hedgeUrl = orderedRoundUrls.drop(1).firstOrNull { extractEndpointKey(it) != primaryEndpoint }
                ?: orderedRoundUrls.getOrNull(1)
            val (avgConnectionBps, ewmaPieceMs) = getTransferMeterSnapshot(session.isAudio)
            val hedgeDelayMs = computeDynamicHedgeDelayMs(
                ewmaPieceMs = ewmaPieceMs,
                configuredHedgeDelayMs = DEFAULT_HEDGE_DELAY_MS,
                isStartupChunk = task.index <= 1L
            )

            val hasWinner = AtomicBoolean(false)
            val primaryFinished = AtomicBoolean(false)
            val hedgeFired = AtomicBoolean(false)
            val hedgeLock = Object()
            registerHedgeLock(streamCallKey, hedgeLock)
            val primaryCallRef = AtomicReference<TrackedCall?>(null)
            val hedgeCallRef = AtomicReference<TrackedCall?>(null)
            val onRoundWon: () -> Unit = {
                synchronized(hedgeLock) { hedgeLock.notifyAll() }
                onTaskCompleted?.invoke()
            }

            val hedgeFuture = if (hedgeUrl != null && !isClientAborted.get()) {
                runCatching {
                    hedgeWorkerPool.submit {
                        try {
                            val expectedTotalBytes = (task.endByte - task.startByte + 1L).coerceAtLeast(1L)
                            val expectedRoundBytes = (expectedTotalBytes - baselineBytes).coerceAtLeast(1L)
                            val hedgeBps = getRouteSpeed(hedgeUrl)
                            var lastSeenBytes = baselineBytes
                            var lastProgressAtMs = roundStartMs
                            var shouldFireHedge = false
                            synchronized(hedgeLock) {
                                while (!hasWinner.get() && !primaryFinished.get() && !isClientAborted.get()) {
                                    val nowCheck = System.currentTimeMillis()
                                    val downloaded = task.receivedBytes.toLong()
                                    if (downloaded > lastSeenBytes) {
                                        lastSeenBytes = downloaded
                                        lastProgressAtMs = nowCheck
                                    }
                                    val liveBytes = maxOf(0L, downloaded - baselineBytes)
                                    val elapsedMs = maxOf(0L, nowCheck - roundStartMs)
                                    val msSinceProgress = maxOf(0L, nowCheck - lastProgressAtMs)
                                    if (shouldTriggerEarlyHedge(
                                            elapsedMs = elapsedMs,
                                            receivedBytes = liveBytes,
                                            expectedBytes = expectedRoundBytes,
                                            msSinceLastProgress = msSinceProgress,
                                            dynamicHedgeDelayMs = hedgeDelayMs,
                                            avgConnectionBps = avgConnectionBps,
                                            hedgeNodeBps = hedgeBps
                                        )
                                    ) {
                                        shouldFireHedge = true
                                        break
                                    }
                                    hedgeLock.wait(50L)
                                }
                            }
                            if (!hasWinner.get() && !isClientAborted.get() && !task.isSuccess && (shouldFireHedge || primaryFinished.get())) {
                                hedgeFired.set(true)
                                triedUrlsInPiece.add(hedgeUrl)
                                tryAttemptFetch(
                                    task = task,
                                    url = hedgeUrl,
                                    session = session,
                                    userAgent = userAgent,
                                    referer = referer,
                                    extraHeaders = extraHeaders,
                                    statsTracker = statsTracker,
                                    concurrencyController = concurrencyController,
                                    hasWinner = hasWinner,
                                    isHedge = true,
                                    callHolder = hedgeCallRef,
                                    counterpartCallHolder = primaryCallRef,
                                    streamCallKey = streamCallKey,
                                    lastHttpCode = lastHttpCode,
                                    sawForbidden403 = sawForbidden403,
                                    isClientAborted = isClientAborted,
                                    onWin = onRoundWon
                                )
                            }
                        } catch (_: InterruptedException) {}
                    }
                }.getOrNull()
            } else null

            try {
                tryAttemptFetch(
                    task = task,
                    url = primaryUrl,
                    session = session,
                    userAgent = userAgent,
                    referer = referer,
                    extraHeaders = extraHeaders,
                    statsTracker = statsTracker,
                    concurrencyController = concurrencyController,
                    hasWinner = hasWinner,
                    isHedge = false,
                    callHolder = primaryCallRef,
                    counterpartCallHolder = hedgeCallRef,
                    streamCallKey = streamCallKey,
                    lastHttpCode = lastHttpCode,
                    sawForbidden403 = sawForbidden403,
                    isClientAborted = isClientAborted,
                    onWin = onRoundWon
                )
            } finally {
                if (primaryTrialKey != null) {
                    sessionInFlightTrials[session.sessionId]?.remove(primaryTrialKey)
                }
                primaryFinished.set(true)
                synchronized(hedgeLock) { hedgeLock.notifyAll() }
                unregisterHedgeLock(streamCallKey, hedgeLock)
            }

            if (hedgeFuture != null) {
                if (task.isSuccess || isClientAborted.get()) {
                    hedgeFuture.cancel(true)
                    hedgeCallRef.get()?.cancelByCaller()
                } else {
                    val hedgeWaitTimedOut = try {
                        hedgeFuture.get(15, TimeUnit.SECONDS)
                        false
                    } catch (_: java.util.concurrent.TimeoutException) {
                        true
                    } catch (_: Throwable) {
                        false
                    }
                    if (!task.isSuccess) {
                        hedgeFuture.cancel(true)
                        hedgeCallRef.get()?.let { slowHedge ->
                            if (hedgeWaitTimedOut && !isClientAborted.get()) {
                                slowHedge.cancelByTimeout("hedge_future_timeout")
                            } else {
                                slowHedge.cancelByCaller()
                            }
                        }
                    }
                }
            }

            if (!task.isSuccess && !isClientAborted.get()) {
                val remainingUrls = orderedRoundUrls.filter { candidate ->
                    candidate != primaryUrl && (!hedgeFired.get() || candidate != hedgeUrl)
                }
                for (url in remainingUrls) {
                    if (isClientAborted.get() || task.isSuccess) break
                    if ((sawForbidden403.get() || lastHttpCode.get() == 403) && isUrlExpired(session) && didRefresh403) break
                    triedUrlsInPiece.add(url)
                    val singleCallRef = AtomicReference<TrackedCall?>(null)
                    tryAttemptFetch(
                        task = task,
                        url = url,
                        session = session,
                        userAgent = userAgent,
                        referer = referer,
                        extraHeaders = extraHeaders,
                        statsTracker = statsTracker,
                        concurrencyController = concurrencyController,
                        hasWinner = hasWinner,
                        isHedge = false,
                        callHolder = singleCallRef,
                        counterpartCallHolder = null,
                        streamCallKey = streamCallKey,
                        lastHttpCode = lastHttpCode,
                        sawForbidden403 = sawForbidden403,
                        isClientAborted = isClientAborted,
                        onWin = onTaskCompleted
                    )
                    if (task.isSuccess) break
                }
            }

            if (task.isSuccess || isClientAborted.get()) break

            if ((sawForbidden403.get() || lastHttpCode.get() == 403) && !didRefresh403 &&
                BtrSessionManager.maybeTriggerProactiveRefresh(session, forceOnAuthFailure = true)
            ) {
                didRefresh403 = true
                sawForbidden403.set(false)
                lastHttpCode.set(0)
                triedUrlsInPiece.clear()
                continue
            }

            if (round + 1 < PIECE_ROUNDS) {
                if ((sawForbidden403.get() || lastHttpCode.get() == 403) && isUrlExpired(session)) {
                    task.lastHttpCode = 403
                    break
                }
                val backoffMs = minOf(2000L, 500L * (1L shl round))
                var slept = 0L
                while (slept < backoffMs && !isClientAborted.get()) {
                    try {
                        Thread.sleep(minOf(50L, backoffMs - slept))
                    } catch (_: InterruptedException) {
                        break
                    }
                    slept += 50L
                }
            }
            round++
        }

        // 最终直连兜底尝试
        val latestUrls = session.currentCandidateUrls()
        if (!task.isSuccess && !isClientAborted.get() && latestUrls.isNotEmpty() && !((sawForbidden403.get() || lastHttpCode.get() == 403) && isUrlExpired(session))) {
            val sortedFallback = sortUrlsByHealthAndSpeed(latestUrls, task.index, session)
            val fallbackCandidate = sortedFallback.firstOrNull { u ->
                val h = extractHostname(u)
                !session.banList.isBanned(u, h)
            } ?: latestUrls.first()
            tryDirectFetch(
                task = task,
                url = fallbackCandidate,
                session = session,
                userAgent = userAgent,
                referer = referer,
                extraHeaders = extraHeaders,
                statsTracker = statsTracker,
                concurrencyController = concurrencyController,
                streamCallKey = streamCallKey,
                lastHttpCode = lastHttpCode,
                onTaskCompleted = onTaskCompleted
            )
        }

        if (!task.isSuccess && !isClientAborted.get()) {
            task.lastHttpCode = if (sawForbidden403.get()) 403 else lastHttpCode.get()
            task.isFailed = true
            task.errorMessage = "All candidate CDNs failed for range ${task.startByte}-${task.endByte}"
        }
    }

    private fun tryAttemptFetch(
        task: ChunkTask,
        url: String,
        session: BtrMediaSession,
        userAgent: String,
        referer: String?,
        extraHeaders: Map<String, String> = emptyMap(),
        statsTracker: StreamStatsTracker,
        concurrencyController: DynamicConcurrencyController?,
        hasWinner: AtomicBoolean,
        isHedge: Boolean,
        callHolder: AtomicReference<TrackedCall?>,
        counterpartCallHolder: AtomicReference<TrackedCall?>?,
        streamCallKey: String,
        lastHttpCode: AtomicInteger? = null,
        sawForbidden403: AtomicBoolean? = null,
        isClientAborted: AtomicBoolean? = null,
        onWin: (() -> Unit)? = null
    ) {
        if (hasWinner.get() || task.isSuccess || isClientAborted?.get() == true) return
        val host = extractHostname(url)
        val attemptId = attemptSeq.incrementAndGet()
        val startTime = System.currentTimeMillis()
        val expectedTotal = (task.endByte - task.startByte + 1).toInt()
        if (expectedTotal <= 0) return

        // P1: 在途实时前缀共享 + 断点续传（含 Round 1+ 续传主路：以 base + live 合并共享给同轮对冲）
        val existingPrefix = task.takePrefixSnapshot(RESUME_MIN_BYTES, expectedTotal)
        val resumeOffset = existingPrefix?.size ?: 0
        val requestStart = task.startByte + resumeOffset
        val needBytes = (task.endByte - requestStart + 1).toInt()
        if (needBytes <= 0) return

        val request = buildUpstreamRequest(
            url = url,
            userAgent = userAgent,
            referer = referer,
            extraHeaders = extraHeaders,
            rangeHeader = "bytes=$requestStart-${task.endByte}"
        )

        val okClient = if (session.isAudio) BtrHttpClient.chunkAudioClient else BtrHttpClient.chunkClient
        val call = okClient.newCall(request)
        val trackedCall = TrackedCall(call)
        callHolder.set(trackedCall)
        registerStreamCall(streamCallKey, session.sessionId, trackedCall)
        if (hasWinner.get() || task.isSuccess || isClientAborted?.get() == true) {
            trackedCall.cancelByCaller()
            unregisterStreamCall(streamCallKey, trackedCall)
            return
        }

        var localBytesRead = 0L
        var completedNormally = false
        var partialResultBuf: ByteArray? = null
        var partialWritePos = 0
        val isPrefixProducer = !isHedge && task.tryClaimPrefixProducer(attemptId, existingPrefix, expectedTotal)

        try {
            AttemptTimeoutWatchdog(trackedCall).use { watchdog ->
                call.execute().use { response ->
                    watchdog.onResponseHeaders()
                    val code = response.code
                    val cr = response.header("Content-Range")
                    val cl = response.body?.contentLength() ?: -1L

                    // 严格拒绝非 0 起始偏移的 HTTP 200 响应（否则从 0 字节返回导致音频/画面错位损坏），以及超大/未知长度/伪造 Content-Range 的 200 响应
                    if (code == 200) {
                        if (!cr.isNullOrBlank() || requestStart > 0L || cl != expectedTotal.toLong()) {
                            session.banList.recordHttpFailure(url, host, 410, receivedBytes = 0L)
                            penalizeHost(host, 20_000L)
                            reportFailover(host, "HTTP 200 非 Range 错位/异常 Content-Range 拒绝 (reqStart=$requestStart, cl=$cl, cr=$cr)", session.sessionId)
                            return
                        }
                    }

                    if (code == 200 || code == 206) {
                        if (code == 206 && !isValidContentRange(code, cr, requestStart, session, expectedEnd = task.endByte)) {
                            session.banList.recordHttpFailure(url, host, 410, receivedBytes = 0L)
                            reportFailover(host, "分片 Content-Range 异常 ($cr vs reqStart=$requestStart-${task.endByte})", session.sessionId)
                            return
                        }

                        val body = response.body ?: return
                        val stream = body.byteStream()

                        fun commitWin(data: ByteArray) {
                            task.data = data
                            task.isSuccess = true
                            task.clearPrefixBuffersOnWin()
                            onWin?.invoke()
                            session.banList.recordSuccess(url, host)
                            val latency = maxOf(1L, System.currentTimeMillis() - startTime)
                            // Issue 8: 仅按本连接实际读取的 localBytesRead 测速（>= 48 KiB），严禁用拼接后的整片长度虚高测速
                            recordRouteSpeed(url, localBytesRead, latency)
                            recordTransferMeter(session.isAudio, localBytesRead, latency)
                            statsTracker.recordChunk(host, data.size.toLong(), latency)
                            concurrencyController?.recordProgress(data.size.toLong())
                            counterpartCallHolder?.get()?.cancelByCaller()
                        }

                        if (isPrefixProducer) {
                            val tempBuf = ByteArray(16 * 1024)
                            while (localBytesRead < needBytes) {
                                val toRead = minOf(tempBuf.size, needBytes - localBytesRead.toInt())
                                val read = stream.read(tempBuf, 0, toRead)
                                if (read == -1) break
                                watchdog.onBytesRead()
                                if (hasWinner.get() && task.isSuccess) return
                                task.appendLivePrefix(attemptId, tempBuf, read, expectedTotal)
                                localBytesRead += read
                            }
                            if (localBytesRead == needBytes.toLong() && stream.read() != -1) {
                                session.banList.recordHttpFailure(url, host, 410, receivedBytes = localBytesRead)
                                reportFailover(host, "分片响应体超出预期字节数", session.sessionId)
                                return
                            }
                            if (localBytesRead == 0L) {
                                session.banList.recordTransportFailure(host, 20_000L, url = url)
                                penalizeHost(host, 20_000L)
                                reportFailover(host, "分片拉取返回 0KB 空响应熔断", session.sessionId)
                                return
                            }
                            if (localBytesRead < needBytes.toLong()) {
                                session.banList.recordTransportFailure(host, 15_000L, url = url, receivedBytes = localBytesRead)
                                penalizeHost(host, 15_000L)
                                reportFailover(host, "分片数据截断 ($localBytesRead/$needBytes 字节)", session.sessionId)
                                return
                            }
                            completedNormally = true
                            // Issue 10: 先取出并校验 ownedData 再执行 hasWinner CAS，杜绝 hasWinner=true 且 task.isSuccess=false 死锁
                            val ownedData = task.consumeOwnedLiveBuffer(attemptId)
                            if (ownedData != null && ownedData.size == expectedTotal) {
                                if (hasWinner.compareAndSet(false, true)) {
                                    commitWin(ownedData)
                                }
                            } else if (!hasWinner.get() && !task.isSuccess) {
                                session.banList.recordTransportFailure(host, 15_000L, url = url, receivedBytes = localBytesRead)
                            }
                            return
                        } else {
                            val resultBuf = ByteArray(expectedTotal)
                            if (resumeOffset > 0 && existingPrefix != null) {
                                System.arraycopy(existingPrefix, 0, resultBuf, 0, resumeOffset)
                            }
                            var writePos = resumeOffset
                            partialResultBuf = resultBuf
                            partialWritePos = writePos
                            while (writePos < expectedTotal) {
                                val toRead = minOf(16 * 1024, expectedTotal - writePos)
                                val read = stream.read(resultBuf, writePos, toRead)
                                if (read == -1) break
                                watchdog.onBytesRead()
                                if (hasWinner.get() && task.isSuccess) return
                                writePos += read
                                partialWritePos = writePos
                                localBytesRead += read
                                if (writePos > task.receivedBytes) {
                                    task.receivedBytes = writePos
                                }
                            }
                            if (localBytesRead == 0L) {
                                session.banList.recordTransportFailure(host, 20_000L, url = url)
                                penalizeHost(host, 20_000L)
                                reportFailover(host, "分片拉取返回 0KB 空响应熔断", session.sessionId)
                                return
                            }
                            if (writePos == expectedTotal && stream.read() != -1) {
                                session.banList.recordHttpFailure(url, host, 410, receivedBytes = localBytesRead)
                                reportFailover(host, "分片响应体超出预期字节数", session.sessionId)
                                return
                            }
                            if (writePos < expectedTotal) {
                                if (writePos >= RESUME_MIN_BYTES && !hasWinner.get()) {
                                    task.saveFallbackPrefixIfBetter(resultBuf, writePos)
                                }
                                session.banList.recordTransportFailure(host, 15_000L, url = url, receivedBytes = localBytesRead)
                                penalizeHost(host, 15_000L)
                                reportFailover(host, "分片数据截断 (${localBytesRead}/$needBytes 字节)", session.sessionId)
                                return
                            }
                            completedNormally = true
                            if (hasWinner.compareAndSet(false, true)) {
                                commitWin(resultBuf)
                            }
                            return
                        }
                    } else if (code >= 400) {
                        lastHttpCode?.set(code)
                        if (code == 403) {
                            sawForbidden403?.set(true)
                        }
                        val isNodeFault = session.banList.recordHttpFailure(url, host, code)
                        if (code == 412 || code == 429) {
                            concurrencyController?.onPushback()
                        }
                        if (isNodeFault) {
                            val penaltyTime = if (code == 429) 45_000L else if (code == 412) 30_000L else 20_000L
                            penalizeHost(host, penaltyTime)
                        }
                        if (!isHedge || session.banList.isHostKnownGood(host)) {
                            reportFailover(host, "HTTP $code 熔断", session.sessionId)
                        }
                    }
                }
            }
        } catch (e: Exception) {
            val callerCanceled = trackedCall.isCallerCanceled() || isClientAborted?.get() == true
            val timedOut = isTimeoutError(
                e = e,
                callCanceled = call.isCanceled(),
                callerCanceled = callerCanceled,
                timedOutFlag = trackedCall.isTimedOut()
            )
            if (!callerCanceled || timedOut) {
                if (timedOut && localBytesRead == 0L) {
                    concurrencyController?.onSlow()
                }
                session.banList.recordTransportFailure(host, 15_000L, url = url, receivedBytes = localBytesRead)
                penalizeHost(host, 12_000L)
                val detail = trackedCall.timeoutReason.get() ?: e.message ?: if (timedOut) "timeout" else "IO异常"
                reportFailover(host, "分片拉取异常: $detail", session.sessionId)
            }
        } finally {
            if (!completedNormally && partialResultBuf != null && partialWritePos >= RESUME_MIN_BYTES && partialWritePos < expectedTotal && !hasWinner.get() && !task.isSuccess) {
                task.saveFallbackPrefixIfBetter(partialResultBuf!!, partialWritePos)
            }
            if (!completedNormally && localBytesRead >= CANCEL_SPEED_MIN_BYTES) {
                val partialElapsed = maxOf(1L, System.currentTimeMillis() - startTime)
                recordRouteSpeed(url, localBytesRead, partialElapsed)
            }
            if (isPrefixProducer && !task.isSuccess) {
                task.releasePrefixProducer(attemptId, keepPartial = !hasWinner.get())
            }
            unregisterStreamCall(streamCallKey, trackedCall)
        }
    }

    private val RANGE_START_REGEX = Regex("""bytes\s+(\d+)-""", RegexOption.IGNORE_CASE)

    internal fun parseRangeStart(cr: String?): Long? {
        if (cr.isNullOrBlank()) return null
        val match = RANGE_START_REGEX.find(cr)
        return match?.groupValues?.get(1)?.toLongOrNull()
    }

    /**
     * 平滑加权轮询 (Nginx SWRR) 选择下一主路由：
     * 避免慢节点获得与极速节点同等比例的分片（木桶效应），且避免连续突发打满单一快节点。
     */
    private fun pickSmoothWeighted(
        sessionId: String,
        candidates: List<String>,
        speeds: Map<String, Double>
    ): String {
        if (candidates.size <= 1) return candidates.first()
        val stateMap = sessionSwrrWeights.computeIfAbsent(sessionId) { HashMap() }
        val totalSpeed = candidates.sumOf { speeds[it] ?: 0.0 }.coerceAtLeast(1.0)
        synchronized(stateMap) {
            var totalWeight = 0.0
            var bestUrl = candidates.first()
            var bestWeight = Double.NEGATIVE_INFINITY
            for (url in candidates) {
                val staticWeight = ((speeds[url] ?: 0.0) / totalSpeed).coerceAtLeast(0.05)
                val nextCur = (stateMap[url] ?: 0.0) + staticWeight
                stateMap[url] = nextCur
                totalWeight += staticWeight
                if (nextCur > bestWeight) {
                    bestWeight = nextCur
                    bestUrl = url
                }
            }
            stateMap[bestUrl] = (stateMap[bestUrl] ?: 0.0) - totalWeight
            return bestUrl
        }
    }

    internal fun sortUrlsByHealthAndSpeed(
        urls: List<String>,
        taskIndex: Long,
        session: BtrMediaSession? = null,
        allowUnmeasuredTrial: Boolean = false
    ): List<String> {
        if (urls.isEmpty()) return emptyList()
        val now = System.currentTimeMillis()

        val sortedAll = urls.sortedBy { url ->
            val host = extractHostname(url)
            val isPermBanned = session?.banList?.isPermanentlyBanned(url, host) == true
            val isUrlBanned = session?.banList?.isBanned(url, host, now) == true
            val penaltyUntil = cdnPenalties[host] ?: 0L
            val globalPenalty = if (penaltyUntil > now) penaltyUntil - now else 0L
            (if (isPermBanned) 300_000L else 0L) + (if (isUrlBanned) 60_000L else 0L) + globalPenalty
        }

        val healthyUrls = sortedAll.filter { url ->
            val host = extractHostname(url)
            !isPenalized(host) && session?.banList?.isBanned(url, host, now) != true
        }

        if (healthyUrls.isEmpty()) {
            val offset = ((taskIndex % sortedAll.size).toInt() + sortedAll.size) % sortedAll.size
            return sortedAll.subList(offset, sortedAll.size) + sortedAll.subList(0, offset)
        }

        val speeds = healthyUrls.associateWith { getRouteSpeed(it) }
        val measured = healthyUrls.filter { (speeds[it] ?: 0.0) > 0.0 }
        val unmeasured = healthyUrls.filter { (speeds[it] ?: 0.0) <= 0.0 }

        val healthyOrdered = if (measured.isNotEmpty()) {
            val topSpeed = measured.maxOf { speeds[it] ?: 0.0 }
            val fastMeasured = measured.filter { (speeds[it] ?: 0.0) >= topSpeed / SPEED_CUTOFF_RATIO }
                .ifEmpty { measured }
                .sortedByDescending { speeds[it] ?: 0.0 }

            val swrrPrimary = if (session != null) {
                pickSmoothWeighted(session.sessionId, fastMeasured, speeds)
            } else {
                fastMeasured[(taskIndex % fastMeasured.size).toInt()]
            }

            // 未测速节点在暖机期与稳态探索位按轮询游标分散试探，并跳过正在进行中的同节点在途试探
            val rotatedUnmeasured = if (unmeasured.size > 1) {
                val uOffset = ((taskIndex % unmeasured.size).toInt() + unmeasured.size) % unmeasured.size
                unmeasured.subList(uOffset, unmeasured.size) + unmeasured.subList(0, uOffset)
            } else {
                unmeasured
            }
            val inFlightSet = session?.let { sessionInFlightTrials[it.sessionId] }
            val availableForTrial = if (!inFlightSet.isNullOrEmpty()) {
                unmeasured.filter { routeKey(it) !in inFlightSet }
            } else {
                unmeasured
            }
            val trialPrimary = if (allowUnmeasuredTrial && availableForTrial.isNotEmpty()) {
                val cursor = session?.let {
                    sessionTrialCursors.computeIfAbsent(it.sessionId) { AtomicInteger(0) }.getAndIncrement()
                } ?: taskIndex.toInt()
                val idx = ((cursor % availableForTrial.size) + availableForTrial.size) % availableForTrial.size
                availableForTrial[idx]
            } else null

            val headList = if (trialPrimary != null) {
                listOf(trialPrimary, swrrPrimary)
            } else {
                listOf(swrrPrimary)
            }

            val slowMeasured = (measured - fastMeasured.toSet()).sortedByDescending { speeds[it] ?: 0.0 }
            (headList + fastMeasured + rotatedUnmeasured + slowMeasured).distinct()
        } else {
            val offset = ((taskIndex % healthyUrls.size).toInt() + healthyUrls.size) % healthyUrls.size
            healthyUrls.subList(offset, healthyUrls.size) + healthyUrls.subList(0, offset)
        }

        return (healthyOrdered + sortedAll).distinct()
    }

    private fun tryDirectFetch(
        task: ChunkTask,
        url: String,
        session: BtrMediaSession,
        userAgent: String,
        referer: String?,
        extraHeaders: Map<String, String> = emptyMap(),
        statsTracker: StreamStatsTracker,
        concurrencyController: DynamicConcurrencyController? = null,
        streamCallKey: String? = null,
        lastHttpCode: AtomicInteger? = null,
        onTaskCompleted: (() -> Unit)? = null
    ) {
        val host = extractHostname(url)
        val startTime = System.currentTimeMillis()
        val request = buildUpstreamRequest(
            url = url,
            userAgent = userAgent,
            referer = referer,
            extraHeaders = extraHeaders,
            rangeHeader = "bytes=${task.startByte}-${task.endByte}"
        )

        val okClient = if (session.isAudio) BtrHttpClient.audioClient else BtrHttpClient.fallbackClient
        val call = okClient.newCall(request)
        val trackedCall = TrackedCall(call)
        if (streamCallKey != null) {
            registerStreamCall(streamCallKey, session.sessionId, trackedCall)
        }
        var localBytesRead = 0L
        try {
            AttemptTimeoutWatchdog(trackedCall).use { watchdog ->
                call.execute().use { response ->
                    watchdog.onResponseHeaders()
                    val code = response.code
                    val expectedBytes = (task.endByte - task.startByte + 1).toInt()
                    val cr = response.header("Content-Range")
                    val cl = response.body?.contentLength() ?: -1L

                    if (code == 200) {
                        if (!cr.isNullOrBlank() || task.startByte > 0L || cl != expectedBytes.toLong()) {
                            session.banList.recordHttpFailure(url, host, 410, receivedBytes = 0L)
                            return@use
                        }
                    }
                    if (code == 206 && !isValidContentRange(code, cr, task.startByte, session, expectedEnd = task.endByte)) {
                        session.banList.recordHttpFailure(url, host, 410)
                        return@use
                    }

                    if (code == 200 || code == 206) {
                        val stream = response.body?.byteStream() ?: return@use
                        val resultBuf = ByteArray(expectedBytes.coerceAtLeast(0))
                        var writePos = 0
                        while (writePos < expectedBytes) {
                            val read = stream.read(resultBuf, writePos, minOf(16 * 1024, expectedBytes - writePos))
                            if (read == -1) break
                            watchdog.onBytesRead()
                            writePos += read
                            localBytesRead += read
                        }
                        if (writePos == expectedBytes && expectedBytes > 0 && stream.read() == -1) {
                            task.data = resultBuf
                            task.isSuccess = true
                            onTaskCompleted?.invoke()
                            session.banList.recordSuccess(url, host)
                            val latency = maxOf(1L, System.currentTimeMillis() - startTime)
                            recordRouteSpeed(url, localBytesRead, latency)
                            recordTransferMeter(session.isAudio, localBytesRead, latency)
                            statsTracker.recordChunk(host, resultBuf.size.toLong(), latency)
                            concurrencyController?.recordProgress(resultBuf.size.toLong())
                        }
                    } else {
                        if (code >= 400) lastHttpCode?.set(code)
                        session.banList.recordHttpFailure(url, host, code)
                        if (code == 412 || code == 429) {
                            concurrencyController?.onPushback()
                        }
                    }
                }
            }
        } catch (t: Throwable) {
            val callerCanceled = trackedCall.isCallerCanceled()
            val timedOut = isTimeoutError(t, call.isCanceled(), callerCanceled, trackedCall.isTimedOut())
            if (!callerCanceled || timedOut) {
                if (timedOut && localBytesRead == 0L) {
                    concurrencyController?.onSlow()
                }
                session.banList.recordTransportFailure(host, 15_000L, url = url, receivedBytes = localBytesRead)
                penalizeHost(host, 12_000L)
            }
        } finally {
            if (streamCallKey != null) {
                unregisterStreamCall(streamCallKey, trackedCall)
            }
        }
    }

    private fun extractHostname(url: String): String {
        val raw = BiliAcceleratorCore.parseUri(url)?.host
            ?: runCatching { java.net.URI(url).host }.getOrNull()
            ?: ""
        return BiliAcceleratorCore.normalizeHost(raw)
    }

    private fun extractEndpointKey(url: String): String {
        val parsed = BiliAcceleratorCore.parseUri(url) ?: return extractHostname(url).lowercase()
        val host = BiliAcceleratorCore.normalizeHost(parsed.host)
        if (host.isBlank()) return ""
        val port = parsed.port
        return if (port > 0 && port != 80 && port != 443) {
            if (host.contains(':')) "[$host]:$port" else "$host:$port"
        } else host
    }

    private fun isPenalized(host: String): Boolean {
        val penalty = cdnPenalties[host] ?: return false
        return penalty > System.currentTimeMillis()
    }

    private fun reportFailover(
        badHost: String,
        reason: String,
        sessionId: String,
        targetHost: String = "备用镜像/CDN"
    ) {
        val result = RewriteResult(
            changed = true,
            originalUrl = "",
            finalUrl = "",
            originalHost = badHost,
            targetHost = targetHost,
            reason = "BTR节点熔断: $reason",
            isPcdn = false,
            isMcdn = false,
            segmentKey = "btr_failover_${sessionId}_${System.currentTimeMillis()}",
            isBtr = true
        )
        RemoteClient.notifyRewrite(result)
    }

    private fun sendHttpError(out: OutputStream, code: Int, message: String) {
        try {
            val resp = "HTTP/1.1 $code $message\r\nContent-Length: 0\r\nConnection: close\r\n\r\n"
            out.write(resp.toByteArray(Charsets.UTF_8))
            out.flush()
        } catch (_: Throwable) {}
    }

    private fun sendHttp416(out: OutputStream, totalLength: Long?) {
        try {
            val totalStr = totalLength?.toString() ?: "*"
            val resp = "HTTP/1.1 416 Range Not Satisfiable\r\nContent-Range: bytes */$totalStr\r\nContent-Length: 0\r\nConnection: close\r\n\r\n"
            out.write(resp.toByteArray(Charsets.UTF_8))
            out.flush()
        } catch (_: Throwable) {}
    }
}
