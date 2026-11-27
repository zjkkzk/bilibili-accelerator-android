package io.github.jh_mmm.biliaccelerator.btr

import android.util.Log
import io.github.jh_mmm.biliaccelerator.core.BiliAcceleratorCore
import io.github.jh_mmm.biliaccelerator.core.RewriteResult
import io.github.jh_mmm.biliaccelerator.hook.RemoteClient
import okhttp3.Call
import okhttp3.Request
import okhttp3.Response
import java.io.ByteArrayOutputStream
import java.io.OutputStream
import java.net.URLDecoder
import java.net.URLEncoder
import java.util.concurrent.CancellationException
import java.util.concurrent.CompletableFuture
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.ConcurrentLinkedDeque
import java.util.concurrent.LinkedBlockingQueue
import java.util.concurrent.RejectedExecutionException
import java.util.concurrent.ScheduledFuture
import java.util.concurrent.ScheduledThreadPoolExecutor
import java.util.concurrent.ThreadPoolExecutor
import java.util.concurrent.TimeUnit
import java.util.concurrent.TimeoutException
import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.atomic.AtomicInteger
import java.util.concurrent.atomic.AtomicLong

data class CachedLiveSegment(
    val bytes: ByteArray,
    val contentType: String,
    val host: String,
    val cachedAt: Long = System.currentTimeMillis(),
    val targetUrl: String = "",
    val firstByteMs: Long = 0L,
    val candidateCount: Int = 1
) {
    internal val reported: AtomicBoolean = AtomicBoolean(false)
}

object BtrLiveEngine {

    private const val TAG = "BiliAccelerator-Live"

    private const val HEDGE_MS = 400L
    private const val URGENT_HEDGE_MS = 150L
    private const val FIRST_BYTE_TIMEOUT_MS = 2500L
    private const val SEGMENT_TIMEOUT_MS = 8000L
    private const val CACHE_LIMIT = 32
    private const val CACHE_TTL_MS = 45_000L
    private const val MAX_PROTECTED_MAP_URLS = 4
    private const val MAX_PREFETCH_QUEUE_DEPTH = 12
    private const val MAX_SPECULATIVE_MISSES = 6
    private const val DEFAULT_TEST_SESSION_ID = "__default_test__"

    @Volatile
    internal var notifyRewriteSink: (RewriteResult) -> Unit = { RemoteClient.notifyRewrite(it) }

    private data class PrefetchItem(
        val session: BtrMediaSession,
        val url: String,
        val isSpeculative: Boolean = false,
        val cacheKey: String = BtrLiveCore.normalizeCacheKey(url)
    )

    private class InFlightEntry(
        val future: CompletableFuture<CachedLiveSegment>,
        val isSpeculative: Boolean,
        val isUrgent: Boolean,
        val accelerateSignal: AtomicBoolean = AtomicBoolean(false),
        val hedgeWakeLock: Object = Object()
    )

    private class ActiveSegmentAttempt(
        val host: String,
        val call: Call,
        val receivedBytes: AtomicLong,
        val hostPenalized: AtomicBoolean
    )

    private class LiveSessionState(val sessionId: String) {
        val segmentCache = ConcurrentHashMap<String, CachedLiveSegment>()
        val protectedMapUrls = object : java.util.LinkedHashMap<String, Boolean>(MAX_PROTECTED_MAP_URLS, 0.75f, true) {
            override fun removeEldestEntry(eldest: MutableMap.MutableEntry<String, Boolean>?): Boolean {
                return size > MAX_PROTECTED_MAP_URLS
            }
        }
        val inFlightSegments = ConcurrentHashMap<String, InFlightEntry>()
        val prefetchQueue = ConcurrentLinkedDeque<PrefetchItem>()
        val queuedPrefetchUrls = ConcurrentHashMap.newKeySet<String>()
        val inflightPrefetches = AtomicInteger(0)
        val urgentInflight = AtomicInteger(0)
        val probedHosts = ConcurrentHashMap.newKeySet<String>()
        val activeCalls = ConcurrentHashMap.newKeySet<Call>()
        val evicted = AtomicBoolean(false)

        fun isProtectedMapKey(cacheKey: String): Boolean {
            if (cacheKey.isBlank()) return false
            return synchronized(protectedMapUrls) { protectedMapUrls.containsKey(cacheKey) }
        }
    }

    private class SpeculativeNotReadyException(code: Int) : Exception("HTTP $code speculative miss")
    private class NonPenaltyHttpException(code: Int) : Exception("HTTP $code non-penalty")

    // 按 sessionId 物理隔离直播缓存、在途表、预加载队列及并发计数，避免跨直播间串台与旧房间残留下载
    private val sessionStates = ConcurrentHashMap<String, LiveSessionState>()

    private fun stateFor(sessionId: String): LiveSessionState {
        return sessionStates.computeIfAbsent(sessionId) { LiveSessionState(it) }
    }

    private val liveWorkerPool = ThreadPoolExecutor(
        16,
        16,
        45L,
        TimeUnit.SECONDS,
        LinkedBlockingQueue(64)
    ) { r ->
        Thread(r, "BTR-LiveWorker").apply { isDaemon = true }
    }.apply { allowCoreThreadTimeOut(true) }

    private val livePrefetchPool = ThreadPoolExecutor(
        4,
        4,
        45L,
        TimeUnit.SECONDS,
        LinkedBlockingQueue(32)
    ) { r ->
        Thread(r, "BTR-LivePrefetch").apply { isDaemon = true }
    }.apply { allowCoreThreadTimeOut(true) }

    private val probingPool = ThreadPoolExecutor(
        6,
        6,
        30L,
        TimeUnit.SECONDS,
        LinkedBlockingQueue(32)
    ) { r ->
        Thread(r, "BTR-LiveProber").apply { isDaemon = true }
    }.apply { allowCoreThreadTimeOut(true) }

    private val firstByteWatchdog = ScheduledThreadPoolExecutor(1) { r ->
        Thread(r, "BTR-LiveFBWatchdog").apply { isDaemon = true }
    }.apply {
        setKeepAliveTime(45L, TimeUnit.SECONDS)
        allowCoreThreadTimeOut(true)
        removeOnCancelPolicy = true
    }

    private fun executeLiveWorker(task: Runnable) {
        try {
            liveWorkerPool.execute(task)
        } catch (_: RejectedExecutionException) {
            Thread(task, "BTR-LiveWorker-Overflow").apply { isDaemon = true }.start()
        }
    }

    fun clearCache() {
        val all = sessionStates.values.toList()
        sessionStates.clear()
        for (st in all) {
            evictState(st)
        }
    }

    fun onSessionEvicted(session: BtrMediaSession) {
        val state = sessionStates.remove(session.sessionId) ?: return
        evictState(state)
    }

    private fun evictState(state: LiveSessionState) {
        state.evicted.set(true)
        state.prefetchQueue.clear()
        state.queuedPrefetchUrls.clear()
        state.probedHosts.clear()
        state.activeCalls.forEach { call -> runCatching { call.cancel() } }
        state.activeCalls.clear()
        val cancelErr = CancellationException("Live session evicted")
        for ((_, entry) in state.inFlightSegments) {
            synchronized(entry.hedgeWakeLock) { entry.hedgeWakeLock.notifyAll() }
            entry.future.completeExceptionally(cancelErr)
        }
        state.inFlightSegments.clear()
        state.segmentCache.clear()
        synchronized(state.protectedMapUrls) { state.protectedMapUrls.clear() }
    }

    internal fun protectMapUrl(url: String, sessionId: String = DEFAULT_TEST_SESSION_ID) {
        val key = BtrLiveCore.normalizeCacheKey(url)
        if (key.isNotBlank()) {
            val state = stateFor(sessionId)
            synchronized(state.protectedMapUrls) {
                state.protectedMapUrls[key] = true
            }
        }
    }

    internal fun putCacheForTest(url: String, segment: CachedLiveSegment, sessionId: String = DEFAULT_TEST_SESSION_ID) {
        val key = BtrLiveCore.normalizeCacheKey(url)
        if (key.isBlank()) return
        val state = stateFor(sessionId)
        state.segmentCache[key] = segment
        pruneCache(state)
    }

    internal fun getCachedForTest(url: String, sessionId: String = DEFAULT_TEST_SESSION_ID): CachedLiveSegment? {
        val key = BtrLiveCore.normalizeCacheKey(url)
        return sessionStates[sessionId]?.segmentCache?.get(key)
    }

    private fun isProtectedMap(state: LiveSessionState, session: BtrMediaSession, cacheKey: String): Boolean {
        if (cacheKey.isBlank()) return false
        if (state.isProtectedMapKey(cacheKey)) return true
        val mapKey = session.liveMapUrl?.let { BtrLiveCore.normalizeCacheKey(it) }
        return !mapKey.isNullOrBlank() && mapKey == cacheKey
    }

    private fun pruneCache(state: LiveSessionState, protectedUrl: String? = null) {
        val protectedKey = protectedUrl?.let { BtrLiveCore.normalizeCacheKey(it) }?.takeIf { it.isNotBlank() }
        val now = System.currentTimeMillis()
        val iterator = state.segmentCache.entries.iterator()
        while (iterator.hasNext()) {
            val entry = iterator.next()
            val isProtected = (protectedKey != null && entry.key == protectedKey) || state.isProtectedMapKey(entry.key)
            if (!isProtected && now - entry.value.cachedAt > CACHE_TTL_MS) {
                iterator.remove()
            }
        }
        while (state.segmentCache.size > CACHE_LIMIT) {
            val oldest = state.segmentCache.entries
                .filter { entry ->
                    val isProtected = (protectedKey != null && entry.key == protectedKey) || state.isProtectedMapKey(entry.key)
                    !isProtected
                }
                .minByOrNull { it.value.cachedAt }
                ?: break
            state.segmentCache.remove(oldest.key)
        }
    }

    internal fun resolveTargetUrlForHost(
        session: BtrMediaSession,
        targetUrl: String,
        host: String
    ): String? {
        val effectiveHostUrls = if (session.liveHostUrls.isNotEmpty() || session.backupUrls.isEmpty()) {
            session.liveHostUrls
        } else {
            val map = LinkedHashMap<String, String>()
            for (u in (listOf(session.primaryUrl) + session.backupUrls)) {
                val unwrapped = BtrLiveCore.unwrapProxyUrl(u).ifBlank { u }
                val h = BiliAcceleratorCore.cleanHost(BiliAcceleratorCore.parseUri(unwrapped)?.host).lowercase()
                if (h.isNotBlank() && !BtrLiveCore.isP2pUrl(unwrapped)) {
                    map.putIfAbsent(h, unwrapped)
                }
            }
            map
        }
        return BtrLiveCore.resolveTargetUrlForHost(
            segmentUrl = targetUrl,
            targetHost = host,
            liveHostUrls = effectiveHostUrls,
            primaryUrl = session.primaryUrl,
            customPoolOnly = session.liveCustomPoolOnly
        )
    }

    private fun buildLiveRequest(
        url: String,
        userAgent: String,
        referer: String?,
        extraHeaders: Map<String, String> = emptyMap(),
        rangeHeader: String? = null
    ): Request {
        val builder = Request.Builder().url(url)
        for ((k, v) in extraHeaders) {
            if (k.isNotBlank() && v.isNotBlank()) {
                runCatching { builder.header(k, v) }
            }
        }
        if (!rangeHeader.isNullOrBlank()) {
            builder.header("Range", rangeHeader)
        }
        builder.header("User-Agent", userAgent)
        if (!referer.isNullOrBlank()) {
            builder.header("Referer", referer)
        }
        return builder.build()
    }

    /**
     * 异步并发探测候选节点有效性（对齐 BTR probeCandidates：Promise.allSettled 并发发送 Range: 0-2047 探测包）
     */
    fun probeCandidates(session: BtrMediaSession, sampleUrl: String) {
        val state = stateFor(session.sessionId)
        if (state.evicted.get()) return
        val pool = session.liveHostPool ?: return
        val unprovenHosts = pool.unproven()
        if (unprovenHosts.isEmpty()) return
        val probeTargets = unprovenHosts.mapNotNull { host ->
            val probeUrl = resolveTargetUrlForHost(session, sampleUrl, host) ?: return@mapNotNull null
            if (state.probedHosts.add(host)) host to probeUrl else null
        }
        if (probeTargets.isEmpty()) return

        val ua = session.userAgent?.takeIf { BtrLocalProxyServer.isUsableClientUserAgent(it) }
            ?: BtrChunkEngine.resolveDefaultUserAgent()
        val ref = session.referer
        val extra = session.extraHeaders
        for ((host, probeUrl) in probeTargets) {
            try {
                probingPool.execute {
                    if (state.evicted.get()) {
                        state.probedHosts.remove(host)
                        return@execute
                    }
                    var call: Call? = null
                    try {
                        val req = buildLiveRequest(
                            url = probeUrl,
                            userAgent = ua,
                            referer = ref,
                            extraHeaders = extra,
                            rangeHeader = "bytes=0-2047"
                        )

                        val startTime = System.currentTimeMillis()
                        call = BtrHttpClient.probeClient.newCall(req)
                        state.activeCalls.add(call)
                        call.execute().use { resp ->
                            val duration = (System.currentTimeMillis() - startTime).toDouble()
                            val bytes = resp.body?.bytes()
                            val byteLen = bytes?.size ?: 0
                            if ((resp.code == 200 || resp.code == 206) && byteLen > 0) {
                                pool.success(host, duration, 0.0)
                                Log.d(TAG, "Live host $host verified OK (${duration.toLong()}ms)")
                            } else if (resp.code != 404) {
                                pool.failure(host, byteLen.toLong())
                            }
                        }
                    } catch (_: Throwable) {
                        if (!state.evicted.get()) {
                            pool.failure(host, 0L)
                        }
                    } finally {
                        call?.let { state.activeCalls.remove(it) }
                        // 移除 in-flight 占位，允许节点在 blockedUntil 惩罚期满后通过 unproven() 自动重新探测恢复
                        state.probedHosts.remove(host)
                    }
                }
            } catch (_: RejectedExecutionException) {
                state.probedHosts.remove(host)
            }
        }
    }

    private val LIVE_CONTENT_RANGE_REGEX = Regex("""^bytes\s+(\d+)-(\d+)/(\d+|\*)$""", RegexOption.IGNORE_CASE)

    /**
     * 单分片拉取：多节点对冲竞速（对齐 BTR attemptSegment / downloadSegment）
     */
    fun downloadLiveSegment(
        session: BtrMediaSession,
        url: String,
        urgent: Boolean = true,
        allow404: Boolean = false,
        isSpeculative: Boolean = false,
        requestUserAgent: String? = null,
        requestReferer: String? = null,
        firstByteTimeoutMs: Long = FIRST_BYTE_TIMEOUT_MS,
        segmentTimeoutMs: Long = SEGMENT_TIMEOUT_MS,
        extraHeaders: Map<String, String> = emptyMap()
    ): CachedLiveSegment {
        val state = stateFor(session.sessionId)
        if (state.evicted.get()) {
            throw CancellationException("Live session evicted")
        }
        val normalizedUrl = BtrLiveCore.unwrapProxyUrl(url).ifBlank { url }
        val cacheKey = BtrLiveCore.normalizeCacheKey(normalizedUrl)
        val isMap = isProtectedMap(state, session, cacheKey)

        // 1. 优先命中内存缓存（按规范化路径键跨节点/跨签名复用，#EXT-X-MAP 初始化分片受免过期保护）
        state.segmentCache[cacheKey]?.let {
            if (isMap || System.currentTimeMillis() - it.cachedAt < CACHE_TTL_MS) {
                return it
            }
        }

        // 2. 避免预取与即时请求重复下载：共享 in-flight CompletableFuture
        var isOwner = false
        val entry = state.inFlightSegments.computeIfAbsent(cacheKey) {
            isOwner = true
            InFlightEntry(CompletableFuture(), isSpeculative = isSpeculative, isUrgent = urgent)
        }
        val future = entry.future
        if (!isOwner) {
            // 无论在途任务是预测性预取还是普通后台预取，只要播放器即时命中即刻唤醒并将对冲延迟缩短至 URGENT_HEDGE_MS
            if (urgent && !entry.isUrgent) {
                entry.accelerateSignal.set(true)
                synchronized(entry.hedgeWakeLock) { entry.hedgeWakeLock.notifyAll() }
            }
            return try {
                future.get(segmentTimeoutMs, TimeUnit.MILLISECONDS)
            } catch (e: Exception) {
                if (state.evicted.get()) throw e
                state.segmentCache[cacheKey]?.let { return it }
                if (allow404) throw e
                downloadFallbackSegment(session, normalizedUrl, requestUserAgent, requestReferer, state, extraHeaders)
            }
        }

        val pool = session.liveHostPool ?: BtrLiveHostPool().also { session.liveHostPool = it }
        val originHost = BiliAcceleratorCore.cleanHost(BiliAcceleratorCore.parseUri(normalizedUrl)?.host)
        val compatibleRanked = pool.pick(8)
            .filter { resolveTargetUrlForHost(session, normalizedUrl, it) != null }
            .distinct()
            .take(2)
        val hosts = compatibleRanked.ifEmpty {
            if (originHost.isNotBlank()) listOf(originHost) else listOf("d1--cn-gotcha204.bilivideo.com")
        }

        val hedgeDelay = when {
            isSpeculative -> HEDGE_MS * 3
            urgent -> URGENT_HEDGE_MS
            else -> HEDGE_MS
        }
        val effectiveUa = requestUserAgent?.takeIf { BtrLocalProxyServer.isUsableClientUserAgent(it) }
            ?: session.userAgent?.takeIf { BtrLocalProxyServer.isUsableClientUserAgent(it) }
            ?: BtrChunkEngine.resolveDefaultUserAgent()
        val effectiveRef = requestReferer ?: session.referer
        val effectiveExtra = if (extraHeaders.isNotEmpty()) extraHeaders else session.extraHeaders
        val activeAttempts = mutableListOf<ActiveSegmentAttempt>()
        val isCompleted = AtomicBoolean(false)
        val totalTimedOut = AtomicBoolean(false)
        val primaryFailedSignal = AtomicBoolean(false)
        val hedgeWakeLock = entry.hedgeWakeLock
        val failureCount = AtomicInteger(0)
        val hit404Count = AtomicInteger(0)

        fun attemptOnHost(host: String, isPrimary: Boolean) {
            if (state.evicted.get() || isCompleted.get() || totalTimedOut.get()) return
            val targetUrl = resolveTargetUrlForHost(session, normalizedUrl, host)
            if (targetUrl == null) {
                if (isPrimary) {
                    primaryFailedSignal.set(true)
                    synchronized(hedgeWakeLock) { hedgeWakeLock.notifyAll() }
                }
                if (failureCount.incrementAndGet() >= hosts.size) {
                    future.completeExceptionally(IllegalStateException("Incompatible live host without signed URL: $host"))
                }
                return
            }
            val req = buildLiveRequest(
                url = targetUrl,
                userAgent = effectiveUa,
                referer = effectiveRef,
                extraHeaders = effectiveExtra
            )

            val call = BtrHttpClient.client.newCall(req)
            val receivedBytes = AtomicLong(0L)
            val hostPenalized = AtomicBoolean(false)
            val attemptRecord = ActiveSegmentAttempt(host, call, receivedBytes, hostPenalized)
            synchronized(activeAttempts) {
                if (isCompleted.get() || state.evicted.get() || totalTimedOut.get()) return
                activeAttempts.add(attemptRecord)
            }
            state.activeCalls.add(call)

            val startTime = System.currentTimeMillis()
            val hasFirstByte = AtomicBoolean(false)
            val fbTimedOut = AtomicBoolean(false)
            val fbWatchdog: ScheduledFuture<*> = firstByteWatchdog.schedule(
                {
                    if (!hasFirstByte.get() && !isCompleted.get() && !state.evicted.get()) {
                        fbTimedOut.set(true)
                        runCatching { call.cancel() }
                    }
                },
                firstByteTimeoutMs,
                TimeUnit.MILLISECONDS
            )

            try {
                call.execute().use { resp ->
                    if (resp.code != 200 && resp.code != 206) {
                        if (resp.code == 404) {
                            hit404Count.incrementAndGet()
                            // 对齐 BTR live-hook.js:231: 404 仅表示边缘节点尚未同步最新切片，绝不触发节点惩罚或封禁
                            if (allow404) {
                                throw SpeculativeNotReadyException(404)
                            }
                            throw NonPenaltyHttpException(404)
                        }
                        throw Exception("HTTP ${resp.code}")
                    }
                    // 对齐 BTR live-hook.js:191-194: 直播请求不带 Range，若上游返回 206 必须校验 Content-Range 覆盖完整分片 (0..total-1)
                    var declaredTotalFromRange: Long? = null
                    if (resp.code == 206) {
                        val cr = resp.header("Content-Range")?.trim().orEmpty()
                        val crMatch = LIVE_CONTENT_RANGE_REGEX.matchEntire(cr)
                        val start = crMatch?.groupValues?.get(1)?.toLongOrNull()
                        val end = crMatch?.groupValues?.get(2)?.toLongOrNull()
                        val total = crMatch?.groupValues?.get(3)?.toLongOrNull()
                        if (start != 0L || total == null || end != total - 1L) {
                            throw Exception("Partial 206 live segment ($cr)")
                        }
                        declaredTotalFromRange = total
                    }

                    val body = resp.body ?: throw Exception("Empty body")
                    val expectedLength = declaredTotalFromRange ?: body.contentLength()
                    val stream = body.byteStream()
                    val out = ByteArrayOutputStream(expectedLength.toInt().coerceIn(8192, 2 * 1024 * 1024))
                    val buffer = ByteArray(16 * 1024)
                    var firstByteMs = 0.0
                    var read: Int

                    while (stream.read(buffer).also { read = it } != -1) {
                        if (read > 0 && hasFirstByte.compareAndSet(false, true)) {
                            firstByteMs = maxOf(1.0, (System.currentTimeMillis() - startTime).toDouble())
                            fbWatchdog.cancel(false)
                        }
                        if (isCompleted.get() || state.evicted.get() || totalTimedOut.get()) return
                        out.write(buffer, 0, read)
                        receivedBytes.addAndGet(read.toLong())
                    }

                    if (fbTimedOut.get() || totalTimedOut.get()) {
                        throw TimeoutException("Live segment timed out on $host")
                    }

                    val data = out.toByteArray()
                    if (data.isEmpty()) {
                        throw Exception("0KB empty response from $host")
                    }
                    if (expectedLength > 0 && data.size.toLong() != expectedLength) {
                        throw Exception("Incomplete body from $host (${data.size}/$expectedLength)")
                    }

                    val totalDurationMs = maxOf(1.0, (System.currentTimeMillis() - startTime).toDouble())
                    val bps = (data.size * 1000.0) / totalDurationMs
                    val effectiveFbMs = if (firstByteMs > 0.0) firstByteMs else totalDurationMs

                    pool.success(host, effectiveFbMs, bps)
                    val segment = CachedLiveSegment(
                        bytes = data,
                        contentType = resp.header("Content-Type") ?: "video/iso.segment",
                        host = host,
                        targetUrl = targetUrl,
                        firstByteMs = effectiveFbMs.toLong(),
                        candidateCount = hosts.size
                    )

                    if (!state.evicted.get() && isCompleted.compareAndSet(false, true)) {
                        state.segmentCache[cacheKey] = segment
                        pruneCache(state, session.liveMapUrl)
                        synchronized(hedgeWakeLock) { hedgeWakeLock.notifyAll() }
                        synchronized(activeAttempts) {
                            activeAttempts.forEach { if (it.call != call && !it.call.isCanceled()) it.call.cancel() }
                        }
                        future.complete(segment)
                    }
                }
            } catch (t: Throwable) {
                // 对齐上游 live-hook.js：首字节看门狗超时与总超时属于节点故障（TimeoutError），必须上报 pool.failure，
                // 仅在另一竞速副本胜出、会话淘汰或非超时主动取消（AbortError）时豁免惩罚。
                val isTimeoutCancel = fbTimedOut.get() || totalTimedOut.get()
                val isNonTimeoutAbort = isCompleted.get() ||
                    state.evicted.get() ||
                    (call.isCanceled() && !isTimeoutCancel)
                if (!isNonTimeoutAbort &&
                    t !is SpeculativeNotReadyException &&
                    t !is NonPenaltyHttpException &&
                    hostPenalized.compareAndSet(false, true)
                ) {
                    pool.failure(host, receivedBytes.get())
                }
                if (isPrimary) {
                    primaryFailedSignal.set(true)
                    synchronized(hedgeWakeLock) { hedgeWakeLock.notifyAll() }
                }
                if (failureCount.incrementAndGet() >= hosts.size) {
                    future.completeExceptionally(t)
                }
            } finally {
                state.activeCalls.remove(call)
                fbWatchdog.cancel(false)
            }
        }

        // 启动主请求
        executeLiveWorker {
            attemptOnHost(hosts[0], isPrimary = true)
        }

        // 若有备选节点，监听对冲超时、主路提前失败或即时请求升级唤醒（对预测性与普通预取均生效）
        if (hosts.size > 1) {
            executeLiveWorker {
                try {
                    val hedgeStartMs = System.currentTimeMillis()
                    val deadline = hedgeStartMs + hedgeDelay
                    synchronized(hedgeWakeLock) {
                        while (!isCompleted.get() &&
                            !primaryFailedSignal.get() &&
                            !state.evicted.get() &&
                            !totalTimedOut.get()
                        ) {
                            val effectiveDeadline = if (entry.accelerateSignal.get()) {
                                minOf(deadline, hedgeStartMs + URGENT_HEDGE_MS)
                            } else {
                                deadline
                            }
                            val rem = effectiveDeadline - System.currentTimeMillis()
                            if (rem <= 0L) break
                            hedgeWakeLock.wait(rem)
                        }
                    }
                    if (!isCompleted.get() && !state.evicted.get() && !totalTimedOut.get()) {
                        attemptOnHost(hosts[1], isPrimary = false)
                    }
                } catch (_: InterruptedException) {}
            }
        }

        return try {
            future.get(segmentTimeoutMs, TimeUnit.MILLISECONDS)
        } catch (e: Exception) {
            val isTimeout = e is TimeoutException
            if (isTimeout) {
                totalTimedOut.set(true)
            }
            synchronized(hedgeWakeLock) { hedgeWakeLock.notifyAll() }
            synchronized(activeAttempts) {
                for (attempt in activeAttempts) {
                    if (isTimeout &&
                        !isCompleted.get() &&
                        !state.evicted.get() &&
                        attempt.hostPenalized.compareAndSet(false, true)
                    ) {
                        pool.failure(attempt.host, attempt.receivedBytes.get())
                    }
                    if (!attempt.call.isCanceled()) {
                        runCatching { attempt.call.cancel() }
                    }
                }
            }
            if (state.evicted.get()) {
                val err = CancellationException("Live session evicted")
                future.completeExceptionally(err)
                throw err
            }
            if (allow404 && hit404Count.get() > 0) {
                val err = SpeculativeNotReadyException(404)
                future.completeExceptionally(err)
                throw err
            }
            try {
                val fb = downloadFallbackSegment(session, normalizedUrl, effectiveUa, effectiveRef, state, effectiveExtra)
                future.complete(fb)
                fb
            } catch (fbErr: Throwable) {
                future.completeExceptionally(fbErr)
                throw fbErr
            }
        } finally {
            state.inFlightSegments.remove(cacheKey, entry)
        }
    }

    private fun downloadFallbackSegment(
        session: BtrMediaSession,
        url: String,
        requestUserAgent: String? = null,
        requestReferer: String? = null,
        state: LiveSessionState = stateFor(session.sessionId),
        extraHeaders: Map<String, String> = emptyMap()
    ): CachedLiveSegment {
        val normalizedUrl = BtrLiveCore.unwrapProxyUrl(url).ifBlank { url }
        val ua = requestUserAgent?.takeIf { BtrLocalProxyServer.isUsableClientUserAgent(it) }
            ?: session.userAgent?.takeIf { BtrLocalProxyServer.isUsableClientUserAgent(it) }
            ?: BtrChunkEngine.resolveDefaultUserAgent()
        val ref = requestReferer ?: session.referer
        val fallbackReq = buildLiveRequest(
            url = normalizedUrl,
            userAgent = ua,
            referer = ref,
            extraHeaders = if (extraHeaders.isNotEmpty()) extraHeaders else session.extraHeaders
        )
        val call = BtrHttpClient.client.newCall(fallbackReq)
        state.activeCalls.add(call)
        return try {
            call.execute().use { resp ->
                if (!resp.isSuccessful) {
                    throw Exception("Fallback request failed: HTTP ${resp.code}")
                }
                if (resp.code == 206) {
                    val cr = resp.header("Content-Range")?.trim().orEmpty()
                    val crMatch = LIVE_CONTENT_RANGE_REGEX.matchEntire(cr)
                    val start = crMatch?.groupValues?.get(1)?.toLongOrNull()
                    val end = crMatch?.groupValues?.get(2)?.toLongOrNull()
                    val total = crMatch?.groupValues?.get(3)?.toLongOrNull()
                    if (start != 0L || total == null || end != total - 1L) {
                        throw Exception("Fallback partial 206 live segment ($cr)")
                    }
                }
                val body = resp.body ?: throw Exception("Empty fallback body")
                val expectedLen = body.contentLength()
                val bytes = body.bytes()
                if (bytes.isEmpty()) {
                    throw Exception("Fallback request returned 0 bytes")
                }
                if (expectedLen > 0 && bytes.size.toLong() != expectedLen) {
                    throw Exception("Fallback request incomplete (${bytes.size}/$expectedLen)")
                }
                val segment = CachedLiveSegment(
                    bytes = bytes,
                    contentType = resp.header("Content-Type") ?: "video/iso.segment",
                    host = "fallback"
                )
                if (!state.evicted.get()) {
                    state.segmentCache[BtrLiveCore.normalizeCacheKey(normalizedUrl)] = segment
                    pruneCache(state, session.liveMapUrl)
                }
                segment
            }
        } finally {
            state.activeCalls.remove(call)
        }
    }

    /**
     * 将 m3u8 播放列表中的分片地址改写为本地代理地址，并后台触发候选节点探测与分片预拉取
     */
    fun pipePlaylist(
        session: BtrMediaSession,
        requestPath: String,
        headers: Map<String, String>,
        clientOut: OutputStream,
        proxyPort: Int,
        keepAlive: Boolean = false,
        isHeadOnly: Boolean = false,
        requestUserAgent: String? = null,
        requestReferer: String? = null,
        extraHeaders: Map<String, String> = emptyMap()
    ): Boolean {
        val state = stateFor(session.sessionId)
        val requestedSubUrl = extractUrlFromPath(requestPath)
        val rawTarget = requestedSubUrl ?: session.primaryUrl
        val originalUrl = BtrLiveCore.unwrapProxyUrl(rawTarget).ifBlank { rawTarget }
        val pool = session.liveHostPool ?: BtrLiveHostPool().also { session.liveHostPool = it }
        val origHost = BiliAcceleratorCore.cleanHost(BiliAcceleratorCore.parseUri(originalUrl)?.host)
        val isConfirmedFmp4Session = session.liveMapUrl != null || BtrLiveCore.isLiveSegmentUrl(session.primaryUrl)
        // 对齐 BTR live-hook.js:440：优先从原始非 P2P 宿主拉取 .m3u8，避免在确认 fMP4 前盲切到 KNOWN_FMP4_HOSTS 导致 HLS-TS 403
        val candidateHosts = if (origHost.isNotBlank() && !BtrLiveCore.isP2pUrl(originalUrl)) {
            val fallbackHosts = pool.pick(3).filter { h ->
                h != origHost && (isConfirmedFmp4Session || pool.status().any { it.host == h && it.state == "healthy" })
            }
            (listOf(origHost) + fallbackHosts).distinct()
        } else {
            pool.pick(3).ifEmpty { listOfNotNull(origHost.takeIf { it.isNotBlank() }) }.distinct()
        }
        val candidateUrls = if (candidateHosts.isNotEmpty()) {
            candidateHosts.mapNotNull { host ->
                val targetUrl = if (requestedSubUrl == null) {
                    session.liveHostUrls[host.lowercase()]
                        ?: resolveTargetUrlForHost(session, originalUrl, host)
                } else {
                    resolveTargetUrlForHost(session, originalUrl, host)
                }
                targetUrl?.let { it to host }
            }.ifEmpty { listOf(originalUrl to origHost) }
        } else {
            listOf(originalUrl to origHost)
        }

        val ua = requestUserAgent?.takeIf { BtrLocalProxyServer.isUsableClientUserAgent(it) }
            ?: session.userAgent?.takeIf { BtrLocalProxyServer.isUsableClientUserAgent(it) }
            ?: BtrChunkEngine.resolveDefaultUserAgent()
        val ref = requestReferer ?: session.referer
        val effectiveExtra = if (extraHeaders.isNotEmpty()) extraHeaders else session.extraHeaders
        var lastErrorCode = 502
        var lastErrorMessage = "Upstream error"

        for ((playlistFetchUrl, candHost) in candidateUrls) {
            val req = buildLiveRequest(
                url = playlistFetchUrl,
                userAgent = ua,
                referer = ref,
                extraHeaders = effectiveExtra
            )
            val startTime = System.currentTimeMillis()
            val call = BtrHttpClient.client.newCall(req)
            state.activeCalls.add(call)
            var headersSent = false
            try {
                call.execute().use { resp ->
                    if (!resp.isSuccessful) {
                        lastErrorCode = resp.code
                        lastErrorMessage = "Upstream error"
                        if (resp.code != 404 && candHost.isNotBlank()) pool.failure(candHost, 0L)
                        return@use
                    }
                    val effectivePlaylistUrl = resp.request.url.toString().ifBlank { playlistFetchUrl }
                    val text = resp.body?.string().orEmpty()
                    if (text.isBlank()) {
                        if (candHost.isNotBlank()) pool.failure(candHost, 0L)
                        return@use
                    }
                    val elapsedMs = maxOf(1L, System.currentTimeMillis() - startTime).toDouble()
                    if (candHost.isNotBlank()) {
                        // 播放列表仅作延迟/可用性健康上报，bps 传 0.0 以免几十字节文本拉低分片吞吐 EWMA
                        pool.success(candHost, elapsedMs, 0.0)
                    }
                    val connHeader = if (keepAlive) "Connection: keep-alive\r\nKeep-Alive: timeout=15, max=100" else "Connection: close"

                    // Master Playlist (#EXT-X-STREAM-INF / #EXT-X-I-FRAME-STREAM-INF): 将子播放列表相对路径改写为本地代理或绝对路径
                    if (text.contains("#EXT-X-STREAM-INF", ignoreCase = true) ||
                        text.contains("#EXT-X-I-FRAME-STREAM-INF", ignoreCase = true)
                    ) {
                        val rewrittenMaster = rewriteMasterPlaylistContent(text, effectivePlaylistUrl, session.sessionId, proxyPort)
                        val bodyBytes = rewrittenMaster.toByteArray(Charsets.UTF_8)
                        val headerBuilder = StringBuilder()
                        headerBuilder.append("HTTP/1.1 200 OK\r\n")
                        headerBuilder.append("Content-Type: application/vnd.apple.mpegurl\r\n")
                        headerBuilder.append("Content-Length: ${bodyBytes.size}\r\n")
                        headerBuilder.append("$connHeader\r\n\r\n")

                        headersSent = true
                        clientOut.write(headerBuilder.toString().toByteArray(Charsets.US_ASCII))
                        if (!isHeadOnly) {
                            clientOut.write(bodyBytes)
                        }
                        clientOut.flush()
                        return keepAlive
                    }

                    val parsed = BtrLiveCore.parseM3u8(text, effectivePlaylistUrl)
                    val isFmp4 = BtrLiveCore.isFmp4Playlist(text, parsed)

                    val outputPlaylist = if (isFmp4) {
                        // 仅在确认 fMP4 直播且非自定义池模式下才注入 KNOWN_FMP4_HOSTS
                        if (!session.liveCustomPoolOnly) {
                            for (knownHost in BtrLiveCore.KNOWN_FMP4_HOSTS) {
                                pool.add(knownHost, proven = false)
                            }
                        }
                        // 保护 #EXT-X-MAP 初始化分片防止被缓存淘汰
                        parsed.mapUrl?.let { mapU ->
                            val normalizedMap = BtrLiveCore.unwrapProxyUrl(mapU).ifBlank { mapU }
                            session.liveMapUrl = normalizedMap
                            protectMapUrl(normalizedMap, session.sessionId)
                        }
                        // 对齐上游 live-hook.js：仅在预测性预取实际命中成功时才清零 liveSpeculativeMisses，
                        // 绝不在播放列表推进 lastNum 时重置，避免非 +1（时间戳命名）直播流无限刷 404
                        if (parsed.lastNum > session.liveLastNum.get()) {
                            session.liveLastNum.set(parsed.lastNum)
                        }

                        // 探测候选节点
                        if (parsed.segments.isNotEmpty()) {
                            probeCandidates(session, parsed.segments.first().url)
                        }

                        // 预加载即将消费的分片（最旧优先）+ 预测性预拉取 lastNum + 1
                        pumpPrefetches(session, parsed.segments, session.liveMapUrl)

                        rewritePlaylistContent(text, effectivePlaylistUrl, session.sessionId, proxyPort)
                    } else {
                        // 非 fMP4（如 HLS-TS 或含 ?wsSecret 的 .ts 切片）：将相对路径解析为绝对上游 URL，不送入 fMP4 节点池（避免 403/404）
                        resolvePlaylistUrlsToAbsolute(text, effectivePlaylistUrl)
                    }

                    val bodyBytes = outputPlaylist.toByteArray(Charsets.UTF_8)

                    val headerBuilder = StringBuilder()
                    headerBuilder.append("HTTP/1.1 200 OK\r\n")
                    headerBuilder.append("Content-Type: application/vnd.apple.mpegurl\r\n")
                    headerBuilder.append("Content-Length: ${bodyBytes.size}\r\n")
                    headerBuilder.append("$connHeader\r\n\r\n")

                    headersSent = true
                    clientOut.write(headerBuilder.toString().toByteArray(Charsets.US_ASCII))
                    if (!isHeadOnly) {
                        clientOut.write(bodyBytes)
                    }
                    clientOut.flush()
                    return keepAlive
                }
            } catch (t: Throwable) {
                if (headersSent) {
                    Log.i(TAG, "Live playlist client write interrupted on $candHost: ${t.message}")
                    return false
                }
                lastErrorCode = 502
                lastErrorMessage = "Bad Gateway - Live Playlist Unreachable"
                if (candHost.isNotBlank()) pool.failure(candHost, 0L)
                Log.w(TAG, "Live playlist host $candHost failed: ${t.message}")
            } finally {
                state.activeCalls.remove(call)
            }
        }

        sendError(clientOut, lastErrorCode, lastErrorMessage)
        return false
    }

    private val EXT_URI_ATTR_REGEX = Regex("""URI="([^"]+)"""", RegexOption.IGNORE_CASE)

    private fun resolveUriAttrLine(line: String, baseUrl: String, transform: (String) -> String): String {
        val uriMatch = EXT_URI_ATTR_REGEX.find(line) ?: return line
        val uriStr = uriMatch.groupValues[1]
        if (uriStr.isBlank()) return line
        val rawResolved = try { java.net.URI(baseUrl).resolve(uriStr).toASCIIString() } catch (_: Throwable) { uriStr }
        val resolved = BtrLiveCore.unwrapProxyUrl(rawResolved).ifBlank { rawResolved }
        return line.replaceRange(uriMatch.groups[1]!!.range, transform(resolved))
    }

    internal fun rewriteMasterPlaylistContent(
        m3u8Text: String,
        playlistUrl: String,
        sessionId: String,
        proxyPort: Int
    ): String {
        val sb = StringBuilder()
        for (rawLine in m3u8Text.lines()) {
            val line = rawLine.trim()
            if ((line.startsWith("#EXT-X-MEDIA:", ignoreCase = true) ||
                    line.startsWith("#EXT-X-I-FRAME-STREAM-INF:", ignoreCase = true)) &&
                line.contains("URI=", ignoreCase = true)
            ) {
                sb.append(
                    resolveUriAttrLine(line, playlistUrl) { resolved ->
                        val encoded = URLEncoder.encode(resolved, "UTF-8")
                        "http://127.0.0.1:$proxyPort/stream/$sessionId/playlist.m3u8?url=$encoded"
                    }
                ).append("\n")
            } else if (line.isNotBlank() && !line.startsWith("#")) {
                val rawResolved = try { java.net.URI(playlistUrl).resolve(line).toASCIIString() } catch (_: Throwable) { line }
                val resolved = BtrLiveCore.unwrapProxyUrl(rawResolved).ifBlank { rawResolved }
                val encoded = URLEncoder.encode(resolved, "UTF-8")
                sb.append("http://127.0.0.1:$proxyPort/stream/$sessionId/playlist.m3u8?url=$encoded").append("\n")
            } else {
                sb.append(rawLine).append("\n")
            }
        }
        return sb.toString()
    }

    private fun resolvePlaylistUrlsToAbsolute(m3u8Text: String, playlistUrl: String): String {
        val sb = StringBuilder()
        for (rawLine in m3u8Text.lines()) {
            val line = rawLine.trim()
            if ((line.startsWith("#EXT-X-MAP:", ignoreCase = true) || line.startsWith("#EXT-X-KEY:", ignoreCase = true)) &&
                line.contains("URI=", ignoreCase = true)
            ) {
                sb.append(resolveUriAttrLine(line, playlistUrl) { it }).append("\n")
            } else if (line.isNotBlank() && !line.startsWith("#")) {
                val rawResolved = try { java.net.URI(playlistUrl).resolve(line).toASCIIString() } catch (_: Throwable) { line }
                val resolved = BtrLiveCore.unwrapProxyUrl(rawResolved).ifBlank { rawResolved }
                sb.append(resolved).append("\n")
            } else {
                sb.append(rawLine).append("\n")
            }
        }
        return sb.toString()
    }

    internal fun rewritePlaylistContent(
        m3u8Text: String,
        playlistUrl: String,
        sessionId: String,
        proxyPort: Int
    ): String {
        val sb = StringBuilder()
        for (rawLine in m3u8Text.lines()) {
            val line = rawLine.trim()
            if (line.startsWith("#EXT-X-MAP:", ignoreCase = true)) {
                sb.append(
                    resolveUriAttrLine(line, playlistUrl) { resolved ->
                        val encoded = URLEncoder.encode(resolved, "UTF-8")
                        "http://127.0.0.1:$proxyPort/stream/$sessionId/segment.m4s?url=$encoded"
                    }
                ).append("\n")
            } else if (line.startsWith("#EXT-X-KEY:", ignoreCase = true) && line.contains("URI=", ignoreCase = true)) {
                sb.append(resolveUriAttrLine(line, playlistUrl) { it }).append("\n")
            } else if (line.isNotBlank() && !line.startsWith("#")) {
                val pathOnly = line.substringBefore('?').substringBefore('#')
                val rawResolved = try { java.net.URI(playlistUrl).resolve(line).toASCIIString() } catch (_: Throwable) { line }
                val resolved = BtrLiveCore.unwrapProxyUrl(rawResolved).ifBlank { rawResolved }
                if (pathOnly.endsWith(".ts", ignoreCase = true)) {
                    sb.append(resolved).append("\n")
                } else {
                    val encoded = URLEncoder.encode(resolved, "UTF-8")
                    val proxied = "http://127.0.0.1:$proxyPort/stream/$sessionId/segment.m4s?url=$encoded"
                    sb.append(proxied).append("\n")
                }
            } else {
                sb.append(rawLine).append("\n")
            }
        }
        return sb.toString()
    }

    private fun enqueuePrefetch(state: LiveSessionState, session: BtrMediaSession, url: String, isSpeculative: Boolean) {
        if (state.evicted.get()) return
        val cleanUrl = BtrLiveCore.unwrapProxyUrl(url).ifBlank { url }
        val cacheKey = BtrLiveCore.normalizeCacheKey(cleanUrl)
        if (cleanUrl.isBlank() || cacheKey.isBlank() || state.segmentCache.containsKey(cacheKey) || state.inFlightSegments.containsKey(cacheKey)) return
        if (state.queuedPrefetchUrls.add(cacheKey)) {
            while (state.prefetchQueue.size >= MAX_PREFETCH_QUEUE_DEPTH) {
                val dropped = state.prefetchQueue.pollFirst() ?: break
                state.queuedPrefetchUrls.remove(dropped.cacheKey)
            }
            state.prefetchQueue.offerLast(PrefetchItem(session, cleanUrl, isSpeculative, cacheKey))
        }
    }

    private fun drainPrefetchQueue(state: LiveSessionState) {
        if (state.evicted.get()) return
        val maxConcurrent = if (state.urgentInflight.get() > 0) 1 else 3
        while (!state.evicted.get() && state.inflightPrefetches.get() < maxConcurrent) {
            val item = state.prefetchQueue.pollFirst() ?: break
            state.queuedPrefetchUrls.remove(item.cacheKey)
            if (state.segmentCache.containsKey(item.cacheKey)) continue

            if (state.inflightPrefetches.incrementAndGet() > maxConcurrent) {
                state.inflightPrefetches.decrementAndGet()
                if (state.queuedPrefetchUrls.add(item.cacheKey)) {
                    state.prefetchQueue.offerFirst(item)
                }
                break
            }

            try {
                livePrefetchPool.execute {
                    try {
                        if (!state.evicted.get()) {
                            downloadLiveSegment(
                                session = item.session,
                                url = item.url,
                                urgent = false,
                                allow404 = item.isSpeculative,
                                isSpeculative = item.isSpeculative
                            )
                            if (item.isSpeculative) {
                                item.session.liveSpeculativeMisses.set(0)
                            }
                        }
                    } catch (e: Throwable) {
                        if (item.isSpeculative && !state.evicted.get()) {
                            item.session.liveSpeculativeMisses.incrementAndGet()
                        }
                    } finally {
                        state.inflightPrefetches.decrementAndGet()
                        drainPrefetchQueue(state)
                    }
                }
            } catch (_: RejectedExecutionException) {
                state.inflightPrefetches.decrementAndGet()
                break
            }
        }
    }

    private fun pumpPrefetches(session: BtrMediaSession, segments: List<LiveSegment>, mapUrl: String? = null) {
        if (segments.isEmpty() && mapUrl.isNullOrBlank()) return
        val state = stateFor(session.sessionId)
        if (state.evicted.get()) return

        // 1. 最高优先级立即异步预取 #EXT-X-MAP 初始化分片，避免排在普通分片预取队列尾部造成首帧等待
        val cleanMapUrl = mapUrl?.let { BtrLiveCore.unwrapProxyUrl(it).ifBlank { it } }
        val mapCacheKey = cleanMapUrl?.let { BtrLiveCore.normalizeCacheKey(it) }.orEmpty()
        if (!cleanMapUrl.isNullOrBlank() &&
            mapCacheKey.isNotBlank() &&
            !state.segmentCache.containsKey(mapCacheKey) &&
            !state.inFlightSegments.containsKey(mapCacheKey) &&
            state.queuedPrefetchUrls.add(mapCacheKey)
        ) {
            try {
                livePrefetchPool.execute {
                    try {
                        if (!state.evicted.get()) {
                            downloadLiveSegment(
                                session = session,
                                url = cleanMapUrl,
                                urgent = false,
                                allow404 = false,
                                isSpeculative = false
                            )
                        }
                    } catch (_: Throwable) {
                    } finally {
                        state.queuedPrefetchUrls.remove(mapCacheKey)
                    }
                }
            } catch (_: RejectedExecutionException) {
                state.queuedPrefetchUrls.remove(mapCacheKey)
            }
        }

        // 2. 正序（最旧分片优先，对齐 BTR live-hook.js:348 与播放器消费顺序一致）
        var enqueuedReal = 0
        for (seg in segments) {
            val cleanSegUrl = BtrLiveCore.unwrapProxyUrl(seg.url).ifBlank { seg.url }
            val segKey = BtrLiveCore.normalizeCacheKey(cleanSegUrl)
            if (segKey.isNotBlank() && !state.segmentCache.containsKey(segKey)) {
                enqueuePrefetch(state, session, cleanSegUrl, isSpeculative = false)
                enqueuedReal++
            }
        }

        // 3. 当列表中已有分片全部命中有序缓存时，预测性预取下一序号分片 lastNum + 1（404 不计节点故障，对冲延迟 3*HEDGE_MS）
        if (enqueuedReal == 0 && segments.isNotEmpty() && session.liveSpeculativeMisses.get() < MAX_SPECULATIVE_MISSES) {
            val nextUrl = BtrLiveCore.guessNextSegmentUrl(segments.last())
            if (!nextUrl.isNullOrBlank()) {
                enqueuePrefetch(state, session, nextUrl, isSpeculative = true)
            }
        }

        drainPrefetchQueue(state)
    }

    private fun notifySegmentConsumed(originalUrl: String, segment: CachedLiveSegment) {
        if (segment.host == "fallback") return
        if (!segment.reported.compareAndSet(false, true)) return
        val finalUrl = segment.targetUrl.ifBlank { originalUrl }
        val fbMs = segment.firstByteMs
        notifyRewriteSink(
            RewriteResult(
                changed = true,
                originalUrl = originalUrl,
                finalUrl = finalUrl,
                originalHost = BiliAcceleratorCore.cleanHost(BiliAcceleratorCore.parseUri(originalUrl)?.host),
                targetHost = segment.host,
                reason = "BTR直播分片对冲加速 · 首包${fbMs}ms",
                isPcdn = false,
                isMcdn = false,
                segmentKey = originalUrl.substringAfterLast('/'),
                isBtr = true,
                btrChunks = 1,
                btrBytes = segment.bytes.size.toLong(),
                btrConcurrency = segment.candidateCount,
                btrCdnCount = segment.candidateCount,
                btrAvgLatencyMs = fbMs
            )
        )
    }

    private fun httpStatusReason(code: Int): String = when (code) {
        200 -> "OK"
        206 -> "Partial Content"
        404 -> "Not Found"
        416 -> "Range Not Satisfiable"
        else -> "Error"
    }

    private fun serveCachedRangeSegment(
        targetUrl: String,
        segment: CachedLiveSegment,
        parsedRange: ParsedRange,
        clientOut: OutputStream,
        keepAlive: Boolean,
        isHeadOnly: Boolean
    ): Boolean {
        val total = segment.bytes.size.toLong()
        val connHeader = if (keepAlive) "Connection: keep-alive\r\nKeep-Alive: timeout=15, max=100\r\n\r\n" else "Connection: close\r\n\r\n"
        if (parsedRange.isInvalid || total <= 0L) {
            val resp = "HTTP/1.1 416 Range Not Satisfiable\r\nContent-Range: bytes */$total\r\nContent-Length: 0\r\n$connHeader"
            clientOut.write(resp.toByteArray(Charsets.US_ASCII))
            clientOut.flush()
            return keepAlive
        }
        val sliceStart: Long
        val sliceEnd: Long
        if (parsedRange.isSuffix) {
            if (parsedRange.suffixLength <= 0L) {
                val resp = "HTTP/1.1 416 Range Not Satisfiable\r\nContent-Range: bytes */$total\r\nContent-Length: 0\r\n$connHeader"
                clientOut.write(resp.toByteArray(Charsets.US_ASCII))
                clientOut.flush()
                return keepAlive
            }
            sliceStart = maxOf(0L, total - parsedRange.suffixLength)
            sliceEnd = total - 1L
        } else {
            if (parsedRange.start >= total) {
                val resp = "HTTP/1.1 416 Range Not Satisfiable\r\nContent-Range: bytes */$total\r\nContent-Length: 0\r\n$connHeader"
                clientOut.write(resp.toByteArray(Charsets.US_ASCII))
                clientOut.flush()
                return keepAlive
            }
            sliceStart = parsedRange.start
            sliceEnd = minOf(parsedRange.end ?: (total - 1L), total - 1L)
            if (sliceEnd < sliceStart) {
                val resp = "HTTP/1.1 416 Range Not Satisfiable\r\nContent-Range: bytes */$total\r\nContent-Length: 0\r\n$connHeader"
                clientOut.write(resp.toByteArray(Charsets.US_ASCII))
                clientOut.flush()
                return keepAlive
            }
        }
        val sliceLen = (sliceEnd - sliceStart + 1L).toInt()
        val headerBuilder = StringBuilder()
        headerBuilder.append("HTTP/1.1 206 Partial Content\r\n")
        headerBuilder.append("Content-Type: ${segment.contentType}\r\n")
        headerBuilder.append("Accept-Ranges: bytes\r\n")
        headerBuilder.append("Content-Range: bytes $sliceStart-$sliceEnd/$total\r\n")
        headerBuilder.append("Content-Length: $sliceLen\r\n")
        headerBuilder.append(connHeader)

        clientOut.write(headerBuilder.toString().toByteArray(Charsets.US_ASCII))
        if (!isHeadOnly) {
            clientOut.write(segment.bytes, sliceStart.toInt(), sliceLen)
            notifySegmentConsumed(targetUrl, segment)
        }
        clientOut.flush()
        return keepAlive
    }

    private fun pipeRangeSegmentPassthrough(
        session: BtrMediaSession,
        state: LiveSessionState,
        targetUrl: String,
        rangeHeader: String,
        clientOut: OutputStream,
        keepAlive: Boolean,
        isHeadOnly: Boolean,
        requestUserAgent: String?,
        requestReferer: String?,
        extraHeaders: Map<String, String> = emptyMap()
    ): Boolean {
        val pool = session.liveHostPool
        val candidateUrls = buildList {
            if (pool != null) {
                for (host in pool.pick(3)) {
                    val resolved = resolveTargetUrlForHost(session, targetUrl, host)
                    if (!resolved.isNullOrBlank()) add(resolved)
                }
            }
            add(targetUrl)
        }.distinct()

        val effectiveUa = requestUserAgent?.takeIf { BtrLocalProxyServer.isUsableClientUserAgent(it) }
            ?: session.userAgent?.takeIf { BtrLocalProxyServer.isUsableClientUserAgent(it) }
            ?: BtrChunkEngine.resolveDefaultUserAgent()
        val effectiveRef = requestReferer ?: session.referer
        val effectiveExtra = if (extraHeaders.isNotEmpty()) extraHeaders else session.extraHeaders
        var lastErrorCode = 502

        for (candUrl in candidateUrls) {
            if (state.evicted.get()) return false
            val req = buildLiveRequest(
                url = candUrl,
                userAgent = effectiveUa,
                referer = effectiveRef,
                extraHeaders = effectiveExtra,
                rangeHeader = rangeHeader
            )
            val call = BtrHttpClient.client.newCall(req)
            state.activeCalls.add(call)
            var headersSent = false
            try {
                call.execute().use { resp ->
                    val code = resp.code
                    if (code != 206 && code != 200 && code != 416) {
                        lastErrorCode = code
                        return@use
                    }
                    val contentRange = resp.header("Content-Range")
                    val contentType = resp.header("Content-Type") ?: "video/iso.segment"
                    val body = resp.body
                    val contentLength = if (code == 416) 0L else (body?.contentLength() ?: -1L)
                    val canKeepAlive = keepAlive && contentLength >= 0L
                    val connHeader = if (canKeepAlive) {
                        "Connection: keep-alive\r\nKeep-Alive: timeout=15, max=100\r\n\r\n"
                    } else {
                        "Connection: close\r\n\r\n"
                    }

                    val headerBuilder = StringBuilder()
                    headerBuilder.append("HTTP/1.1 $code ${httpStatusReason(code)}\r\n")
                    if (code != 416) {
                        headerBuilder.append("Content-Type: $contentType\r\n")
                        headerBuilder.append("Accept-Ranges: bytes\r\n")
                    }
                    if (!contentRange.isNullOrBlank()) {
                        headerBuilder.append("Content-Range: $contentRange\r\n")
                    }
                    if (contentLength >= 0L) {
                        headerBuilder.append("Content-Length: $contentLength\r\n")
                    }
                    headerBuilder.append(connHeader)

                    headersSent = true
                    clientOut.write(headerBuilder.toString().toByteArray(Charsets.US_ASCII))
                    if (!isHeadOnly && code != 416 && body != null) {
                        val inStream = body.byteStream()
                        val buf = ByteArray(16 * 1024)
                        var read: Int
                        while (inStream.read(buf).also { read = it } != -1) {
                            clientOut.write(buf, 0, read)
                        }
                    }
                    clientOut.flush()
                    return canKeepAlive
                }
            } catch (t: Throwable) {
                if (headersSent) {
                    Log.i(TAG, "Live ranged segment client write interrupted: ${t.message}")
                    return false
                }
                lastErrorCode = 502
            } finally {
                state.activeCalls.remove(call)
            }
        }
        sendError(clientOut, lastErrorCode, "Bad Gateway - Live Segment Range Unreachable")
        return false
    }

    /**
     * 处理单个直播分片（.m4s）本地代理请求
     */
    fun pipeSegment(
        session: BtrMediaSession,
        requestPath: String,
        headers: Map<String, String>,
        clientOut: OutputStream,
        keepAlive: Boolean = false,
        isHeadOnly: Boolean = false,
        requestUserAgent: String? = null,
        requestReferer: String? = null,
        extraHeaders: Map<String, String> = emptyMap()
    ): Boolean {
        val state = stateFor(session.sessionId)
        val rawTargetUrl = extractUrlFromPath(requestPath) ?: run {
            val fileName = requestPath.substringAfterLast('/')
            val base = session.primaryUrl.substringBeforeLast('/')
            "$base/$fileName"
        }
        val targetUrl = BtrLiveCore.unwrapProxyUrl(rawTargetUrl).ifBlank { rawTargetUrl }
        val cacheKey = BtrLiveCore.normalizeCacheKey(targetUrl)
        val rangeHeader = headers.entries
            .firstOrNull { it.key.equals("range", ignoreCase = true) }
            ?.value
            ?.trim()
            ?.takeIf { it.isNotEmpty() }

        state.urgentInflight.incrementAndGet()
        var headersSent = false
        return try {
            if (rangeHeader != null) {
                // 对齐上游 live-hook.js：客户端携带 Range 时绝不忽略 Range 返回 200 OK 全量字节
                val cached = state.segmentCache[cacheKey]?.takeIf {
                    isProtectedMap(state, session, cacheKey) || System.currentTimeMillis() - it.cachedAt < CACHE_TTL_MS
                }
                if (cached != null && !rangeHeader.contains(',')) {
                    val parsedRange = BtrLocalProxyServer.parseRangeHeader(rangeHeader)
                    if (parsedRange.hasRange) {
                        headersSent = true
                        return serveCachedRangeSegment(
                            targetUrl = targetUrl,
                            segment = cached,
                            parsedRange = parsedRange,
                            clientOut = clientOut,
                            keepAlive = keepAlive,
                            isHeadOnly = isHeadOnly
                        )
                    }
                }
                return pipeRangeSegmentPassthrough(
                    session = session,
                    state = state,
                    targetUrl = targetUrl,
                    rangeHeader = rangeHeader,
                    clientOut = clientOut,
                    keepAlive = keepAlive,
                    isHeadOnly = isHeadOnly,
                    requestUserAgent = requestUserAgent,
                    requestReferer = requestReferer,
                    extraHeaders = extraHeaders
                )
            }

            val segment = downloadLiveSegment(
                session = session,
                url = targetUrl,
                urgent = true,
                allow404 = false,
                isSpeculative = false,
                requestUserAgent = requestUserAgent,
                requestReferer = requestReferer,
                extraHeaders = extraHeaders
            )
            val headerBuilder = StringBuilder()
            headerBuilder.append("HTTP/1.1 200 OK\r\n")
            headerBuilder.append("Content-Type: ${segment.contentType}\r\n")
            headerBuilder.append("Accept-Ranges: bytes\r\n")
            headerBuilder.append("Content-Length: ${segment.bytes.size}\r\n")
            if (keepAlive) {
                headerBuilder.append("Connection: keep-alive\r\nKeep-Alive: timeout=15, max=100\r\n\r\n")
            } else {
                headerBuilder.append("Connection: close\r\n\r\n")
            }

            headersSent = true
            clientOut.write(headerBuilder.toString().toByteArray(Charsets.US_ASCII))
            if (!isHeadOnly) {
                clientOut.write(segment.bytes)
                notifySegmentConsumed(targetUrl, segment)
            }
            clientOut.flush()
            keepAlive
        } catch (t: Throwable) {
            if (headersSent) {
                Log.i(TAG, "Live segment client write interrupted: ${t.message}")
            } else {
                Log.e(TAG, "Failed to serve live segment: ${t.message}")
                sendError(clientOut, 502, "Bad Gateway - Live Segment Unreachable")
            }
            false
        } finally {
            if (state.urgentInflight.decrementAndGet() == 0) {
                drainPrefetchQueue(state)
            }
        }
    }

    private data class FlvRaceWinner(
        val host: String,
        val targetUrl: String,
        val response: Response,
        val call: Call,
        val connectLatencyMs: Double
    )

    private class ActiveFlvAttempt(
        val host: String,
        val call: Call,
        val hostPenalized: AtomicBoolean
    )

    /**
     * 处理持续长连接直播流（如 .flv）：多源对冲竞速响应最快上游并无缝透传
     */
    fun pipeLiveFlvStream(
        session: BtrMediaSession,
        headers: Map<String, String>,
        clientOut: OutputStream,
        isHeadOnly: Boolean = false,
        requestUserAgent: String? = null,
        requestReferer: String? = null,
        timeoutMs: Long = SEGMENT_TIMEOUT_MS,
        extraHeaders: Map<String, String> = emptyMap()
    ): Boolean {
        val state = stateFor(session.sessionId)
        val originalUrl = BtrLiveCore.unwrapProxyUrl(session.primaryUrl).ifBlank { session.primaryUrl }
        val pool = session.liveHostPool ?: BtrLiveHostPool().also { session.liveHostPool = it }
        val originHost = BiliAcceleratorCore.cleanHost(BiliAcceleratorCore.parseUri(originalUrl)?.host)
        // 使用 resolveTargetUrlForHost 校验跨厂商 FLV 鉴权签名兼容性，严禁盲目 swapHost 触发 HTTP 403 误封
        val compatibleCandidates = pool.pick(8)
            .mapNotNull { host ->
                val resolved = resolveTargetUrlForHost(session, originalUrl, host) ?: return@mapNotNull null
                host to resolved
            }
            .distinctBy { it.first.lowercase() }
            .take(2)
        val raceCandidates = compatibleCandidates.ifEmpty {
            val fallbackHost = originHost.ifBlank { "d1--cn-gotcha204.bilivideo.com" }
            listOf(fallbackHost to originalUrl)
        }

        val effectiveUa = requestUserAgent?.takeIf { BtrLocalProxyServer.isUsableClientUserAgent(it) }
            ?: session.userAgent?.takeIf { BtrLocalProxyServer.isUsableClientUserAgent(it) }
            ?: BtrChunkEngine.resolveDefaultUserAgent()
        val effectiveRef = requestReferer ?: session.referer
        val effectiveExtra = if (extraHeaders.isNotEmpty()) extraHeaders else session.extraHeaders
        val winnerFuture = CompletableFuture<FlvRaceWinner?>()
        val winnerClaimed = AtomicBoolean(false)
        val timedOut = AtomicBoolean(false)
        val raceFinished = AtomicBoolean(false)
        val primaryFailed = AtomicBoolean(false)
        val wakeLock = Object()
        val activeAttempts = ConcurrentHashMap.newKeySet<ActiveFlvAttempt>()
        val failedCount = AtomicInteger(0)

        fun connectCandidate(host: String, targetUrl: String, isPrimary: Boolean) {
            if (state.evicted.get() || raceFinished.get() || winnerClaimed.get()) return
            val req = buildLiveRequest(
                url = targetUrl,
                userAgent = effectiveUa,
                referer = effectiveRef,
                extraHeaders = effectiveExtra
            )

            val call = BtrHttpClient.liveStreamClient.newCall(req)
            val hostPenalized = AtomicBoolean(false)
            val attemptRecord = ActiveFlvAttempt(host, call, hostPenalized)
            activeAttempts.add(attemptRecord)
            state.activeCalls.add(call)
            val startTime = System.currentTimeMillis()

            try {
                val resp = call.execute()
                if (!resp.isSuccessful || resp.body == null) {
                    val code = resp.code
                    runCatching { resp.close() }
                    if (code == 404) {
                        throw NonPenaltyHttpException(404)
                    }
                    throw Exception("FLV HTTP $code")
                }
                val connectLatency = maxOf(1.0, (System.currentTimeMillis() - startTime).toDouble())
                if (!raceFinished.get() && !state.evicted.get() && winnerClaimed.compareAndSet(false, true)) {
                    val candidateWinner = FlvRaceWinner(host, targetUrl, resp, call, connectLatency)
                    if (raceFinished.get() || state.evicted.get() || !winnerFuture.complete(candidateWinner)) {
                        runCatching { resp.close() }
                    } else {
                        synchronized(wakeLock) { wakeLock.notifyAll() }
                        activeAttempts.forEach { other ->
                            if (other.call != call && !other.call.isCanceled()) runCatching { other.call.cancel() }
                        }
                    }
                } else {
                    runCatching { resp.close() }
                }
            } catch (t: Throwable) {
                val isTimeoutCancel = timedOut.get()
                val isNonTimeoutAbort = state.evicted.get() ||
                    (winnerClaimed.get() && !isTimeoutCancel) ||
                    (call.isCanceled() && !isTimeoutCancel)
                if (t !is NonPenaltyHttpException &&
                    !isNonTimeoutAbort &&
                    hostPenalized.compareAndSet(false, true)
                ) {
                    pool.failure(host, 0L)
                }
                if (isPrimary) {
                    primaryFailed.set(true)
                    synchronized(wakeLock) { wakeLock.notifyAll() }
                }
                if (failedCount.incrementAndGet() >= raceCandidates.size && winnerClaimed.compareAndSet(false, true)) {
                    winnerFuture.complete(null)
                }
            } finally {
                activeAttempts.remove(attemptRecord)
                state.activeCalls.remove(call)
            }
        }

        executeLiveWorker { connectCandidate(raceCandidates[0].first, raceCandidates[0].second, isPrimary = true) }
        if (raceCandidates.size > 1) {
            executeLiveWorker {
                val deadline = System.currentTimeMillis() + HEDGE_MS
                synchronized(wakeLock) {
                    while (!winnerClaimed.get() && !primaryFailed.get() && !raceFinished.get() && !state.evicted.get()) {
                        val rem = deadline - System.currentTimeMillis()
                        if (rem <= 0L) break
                        try {
                            wakeLock.wait(rem)
                        } catch (_: InterruptedException) {
                            break
                        }
                    }
                }
                if (!winnerClaimed.get() && !raceFinished.get() && !state.evicted.get()) {
                    connectCandidate(raceCandidates[1].first, raceCandidates[1].second, isPrimary = false)
                }
            }
        }

        val winner = try {
            winnerFuture.get(timeoutMs, TimeUnit.MILLISECONDS)
        } catch (e: Throwable) {
            val isTimeout = e is TimeoutException
            if (isTimeout) {
                timedOut.set(true)
            }
            raceFinished.set(true)
            winnerClaimed.compareAndSet(false, true)
            winnerFuture.complete(null)
            runCatching { winnerFuture.getNow(null)?.response?.close() }
            if (isTimeout && !state.evicted.get()) {
                activeAttempts.forEach { attempt ->
                    if (attempt.hostPenalized.compareAndSet(false, true)) {
                        pool.failure(attempt.host, 0L)
                    }
                }
            }
            null
        }

        if (winner == null) {
            raceFinished.set(true)
            synchronized(wakeLock) { wakeLock.notifyAll() }
            activeAttempts.forEach { runCatching { it.call.cancel() } }
            sendError(clientOut, 502, "FLV upstream all hosts failed")
            return false
        }

        state.activeCalls.add(winner.call)
        return try {
            winner.response.use { resp ->
                val headerBuilder = StringBuilder()
                headerBuilder.append("HTTP/1.1 200 OK\r\n")
                headerBuilder.append("Content-Type: ${resp.header("Content-Type") ?: "video/x-flv"}\r\n")
                headerBuilder.append("Connection: close\r\n\r\n")
                clientOut.write(headerBuilder.toString().toByteArray(Charsets.US_ASCII))
                clientOut.flush()

                if (isHeadOnly) {
                    runCatching { winner.call.cancel() }
                    return false
                }

                notifyRewriteSink(
                    RewriteResult(
                        changed = true,
                        originalUrl = originalUrl,
                        finalUrl = winner.targetUrl,
                        originalHost = BiliAcceleratorCore.cleanHost(BiliAcceleratorCore.parseUri(originalUrl)?.host),
                        targetHost = winner.host,
                        reason = "BTR直播FLV加速 · 首包${winner.connectLatencyMs.toLong()}ms",
                        isPcdn = false,
                        isMcdn = false,
                        segmentKey = "live_flv_${session.sessionId}",
                        isBtr = true,
                        btrAvgLatencyMs = winner.connectLatencyMs.toLong()
                    )
                )

                val inStream = resp.body?.byteStream()
                val buf = ByteArray(32 * 1024)
                var totalBytes = 0L
                val streamStart = System.currentTimeMillis()
                var read = 0
                while (inStream != null && inStream.read(buf).also { read = it } != -1) {
                    clientOut.write(buf, 0, read)
                    clientOut.flush()
                    totalBytes += read
                }
                if (totalBytes > 0L) {
                    val streamElapsed = maxOf(1.0, (System.currentTimeMillis() - streamStart).toDouble())
                    pool.success(winner.host, winner.connectLatencyMs, (totalBytes * 1000.0) / streamElapsed)
                } else {
                    pool.failure(winner.host, 0L)
                }
                // 响应头写明 Connection: close，必须返回 false 让 BtrLocalProxyServer 立即关闭 Socket
                false
            }
        } catch (t: Throwable) {
            Log.i(TAG, "Live FLV stream on ${winner.host} interrupted: ${t.message}")
            false
        } finally {
            state.activeCalls.remove(winner.call)
        }
    }

    private fun extractUrlFromPath(path: String): String? {
        val query = path.substringAfter('?', "")
        for (param in query.split('&')) {
            if (param.startsWith("url=")) {
                val encoded = param.substringAfter("url=")
                return try { URLDecoder.decode(encoded, "UTF-8") } catch (_: Throwable) { encoded }
            }
        }
        return null
    }

    private fun sendError(out: OutputStream, code: Int, message: String) {
        try {
            val resp = "HTTP/1.1 $code $message\r\nContent-Length: 0\r\nConnection: close\r\n\r\n"
            out.write(resp.toByteArray(Charsets.US_ASCII))
            out.flush()
        } catch (_: Throwable) {}
    }
}
