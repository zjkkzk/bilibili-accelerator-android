package io.github.jh_mmm.biliaccelerator.btr

import io.github.jh_mmm.biliaccelerator.core.AcceleratorConfig
import org.junit.Assert.assertEquals
import org.junit.Assert.assertFalse
import org.junit.Assert.assertNotNull
import org.junit.Assert.assertTrue
import org.junit.Assert.fail
import org.junit.Test
import java.io.ByteArrayOutputStream
import java.io.IOException
import java.io.InterruptedIOException
import java.net.HttpURLConnection
import java.net.ServerSocket
import java.net.Socket
import java.net.URL
import java.util.concurrent.atomic.AtomicInteger

class BtrChunkEngineFixTest {

    // ==================== Issue I-5: Early Hedge / Straggler Rescue ====================

    @Test
    fun testDynamicHedgeDelayFormulaAndClamping() {
        // Unmeasured (ewmaPieceMs <= 0) falls back to configured cap (1200ms normal, 800ms startup)
        assertEquals(1200L, BtrChunkEngine.computeDynamicHedgeDelayMs(0.0, configuredHedgeDelayMs = 1200L, isStartupChunk = false))
        assertEquals(800L, BtrChunkEngine.computeDynamicHedgeDelayMs(0.0, configuredHedgeDelayMs = 1200L, isStartupChunk = true))

        // Fast network: round(100 * 1.5) = 150 -> clamped to MIN_HEDGE_DELAY_MS (250ms)
        assertEquals(250L, BtrChunkEngine.computeDynamicHedgeDelayMs(100.0, configuredHedgeDelayMs = 1200L, isStartupChunk = false))

        // Medium network: round(300 * 1.5) = 450ms
        assertEquals(450L, BtrChunkEngine.computeDynamicHedgeDelayMs(300.0, configuredHedgeDelayMs = 1200L, isStartupChunk = false))

        // Slow network: round(1000 * 1.5) = 1500 -> clamped to 1200ms (normal) / 800ms (startup)
        assertEquals(1200L, BtrChunkEngine.computeDynamicHedgeDelayMs(1000.0, configuredHedgeDelayMs = 1200L, isStartupChunk = false))
        assertEquals(800L, BtrChunkEngine.computeDynamicHedgeDelayMs(1000.0, configuredHedgeDelayMs = 1200L, isStartupChunk = true))
    }

    @Test
    fun testEarlyHedgeTriggerOnTricklingAndStallAndGracePeriod() {
        val expected = 512L * 1024L
        val avgBps = 4.0 * 1024.0 * 1024.0 // 4 MB/s moving average connection speed

        // 1. Within 250ms grace period: must NOT trigger even if slow
        assertFalse(
            "Must honor 250ms minimum grace period",
            BtrChunkEngine.shouldTriggerEarlyHedge(
                elapsedMs = 200L,
                receivedBytes = 4 * 1024L,
                expectedBytes = expected,
                msSinceLastProgress = 180L,
                dynamicHedgeDelayMs = 1200L,
                avgConnectionBps = avgBps,
                hedgeNodeBps = avgBps
            )
        )

        // 2. Healthy primary on pace (>= 55% of avgBps) before dynamic delay: must NOT hedge
        // 900KB in 300ms = 3 MB/s (75% of 4 MB/s avgBps)
        assertFalse(
            "Healthy on-pace primary must not trigger early hedge",
            BtrChunkEngine.shouldTriggerEarlyHedge(
                elapsedMs = 300L,
                receivedBytes = 900 * 1024L,
                expectedBytes = 2L * 1024 * 1024,
                msSinceLastProgress = 20L,
                dynamicHedgeDelayMs = 1200L,
                avgConnectionBps = avgBps,
                hedgeNodeBps = 0.0
            )
        )

        // 3. Trickling primary (< 55% of avgBps with ETA >= 400ms) right after 250ms grace: MUST trigger early!
        assertTrue(
            "Trickling primary well below moving average speed must trigger early hedge before 1200ms",
            BtrChunkEngine.shouldTriggerEarlyHedge(
                elapsedMs = 300L,
                receivedBytes = 32 * 1024L, // ~106 KB/s << 55% of 4 MB/s, ETA ~4.5s
                expectedBytes = expected,
                msSinceLastProgress = 50L,
                dynamicHedgeDelayMs = 1200L,
                avgConnectionBps = avgBps,
                hedgeNodeBps = 0.0
            )
        )

        // 4. Mid-chunk stall (received some bytes, then no new bytes for >= 350ms): MUST trigger early!
        assertTrue(
            "Mid-chunk stall >= 350ms must trigger early hedge",
            BtrChunkEngine.shouldTriggerEarlyHedge(
                elapsedMs = 450L,
                receivedBytes = 256 * 1024L,
                expectedBytes = expected,
                msSinceLastProgress = 360L,
                dynamicHedgeDelayMs = 1200L,
                avgConnectionBps = 0.0,
                hedgeNodeBps = 0.0
            )
        )

        // 5. At dynamic deadline, fast almost-done primary (> 75% done, healthy rate) skips unnecessary hedge
        assertFalse(
            "Fast almost-done primary at deadline should not waste hedge bandwidth",
            BtrChunkEngine.shouldTriggerEarlyHedge(
                elapsedMs = 1200L,
                receivedBytes = 420 * 1024L, // > 75% of 512KB
                expectedBytes = expected,
                msSinceLastProgress = 15L,
                dynamicHedgeDelayMs = 1200L,
                avgConnectionBps = 400 * 1024.0,
                hedgeNodeBps = 400 * 1024.0
            )
        )
    }

    @Test
    fun testEndToEndEarlyHedgeRescuesTricklingPrimaryWellBefore1200ms() {
        val totalLen = 64 * 1024 + 256 * 1024 // 64KB probe + 1x256KB chunk
        val fullPayload = ByteArray(totalLen) { idx -> (idx % 247).toByte() }
        val backupHits = AtomicInteger(0)

        // Primary CDN: serves probe fast, but trickles chunk >= 64KB very slowly (16KB then sleeps 2500ms)
        val slowPrimaryServer = ServerSocket(0)
        val fastBackupServer = ServerSocket(0)

        val tSlow = Thread {
            while (!slowPrimaryServer.isClosed) {
                val sock = try { slowPrimaryServer.accept() } catch (_: Throwable) { break }
                Thread {
                    sock.use { s ->
                        val reader = s.getInputStream().bufferedReader()
                        var rangeHeader = ""
                        reader.readLine() ?: return@use
                        while (true) {
                            val line = reader.readLine() ?: break
                            if (line.isEmpty()) break
                            if (line.startsWith("Range:", ignoreCase = true)) rangeHeader = line.substringAfter(":").trim()
                        }
                        val (start, end) = BtrLocalProxyServer.parseRangeHeaderPair(rangeHeader)
                        val actualEnd = minOf(end ?: (totalLen - 1L), totalLen - 1L).toInt()
                        val startInt = start.toInt()
                        val len = (actualEnd - startInt + 1).coerceAtLeast(0)
                        val out = s.getOutputStream()
                        out.write(
                            ("HTTP/1.1 206 Partial Content\r\n" +
                                "Content-Type: video/mp4\r\n" +
                                "Content-Range: bytes $startInt-$actualEnd/$totalLen\r\n" +
                                "Content-Length: $len\r\n" +
                                "Connection: close\r\n\r\n").toByteArray()
                        )
                        if (startInt == 0) {
                            out.write(fullPayload, startInt, len)
                            out.flush()
                        } else {
                            // Trickle 16KB then stall 2.5s (well beyond 1200ms default delay)
                            val trickle = minOf(len, 16 * 1024)
                            out.write(fullPayload, startInt, trickle)
                            out.flush()
                            runCatching { Thread.sleep(2_500L) }
                            runCatching {
                                out.write(fullPayload, startInt + trickle, len - trickle)
                                out.flush()
                            }
                        }
                    }
                }.apply { isDaemon = true; start() }
            }
        }.apply { isDaemon = true; start() }

        val tFast = Thread {
            while (!fastBackupServer.isClosed) {
                val sock = try { fastBackupServer.accept() } catch (_: Throwable) { break }
                Thread {
                    sock.use { s ->
                        val reader = s.getInputStream().bufferedReader()
                        var rangeHeader = ""
                        reader.readLine() ?: return@use
                        while (true) {
                            val line = reader.readLine() ?: break
                            if (line.isEmpty()) break
                            if (line.startsWith("Range:", ignoreCase = true)) rangeHeader = line.substringAfter(":").trim()
                        }
                        val (start, end) = BtrLocalProxyServer.parseRangeHeaderPair(rangeHeader)
                        val actualEnd = minOf(end ?: (totalLen - 1L), totalLen - 1L).toInt()
                        val startInt = start.toInt()
                        val len = (actualEnd - startInt + 1).coerceAtLeast(0)
                        if (startInt > 0) backupHits.incrementAndGet()
                        val out = s.getOutputStream()
                        out.write(
                            ("HTTP/1.1 206 Partial Content\r\n" +
                                "Content-Type: video/mp4\r\n" +
                                "Content-Range: bytes $startInt-$actualEnd/$totalLen\r\n" +
                                "Content-Length: $len\r\n" +
                                "Connection: close\r\n\r\n").toByteArray()
                        )
                        out.write(fullPayload, startInt, len)
                        out.flush()
                    }
                }.apply { isDaemon = true; start() }
            }
        }.apply { isDaemon = true; start() }

        var sid = ""
        try {
            BtrChunkEngine.resetTransferMetersForTesting()
            // Pre-seed EWMA meter: 512KB in 600ms -> avgConnectionBps ~853KB/s, dynamicDelay = 800ms (startup)
            // Primary trickles 16KB in 250ms (~64KB/s << 55% of 853KB/s) -> early hedge wakes right around ~250-300ms!
            BtrChunkEngine.recordTransferMeter(isAudio = false, bytes = 512 * 1024L, elapsedMs = 600L)

            val primaryUrl = "http://127.0.0.1:${slowPrimaryServer.localPort}/upgcxcode/10/20/hedge_early.m4s"
            val backupUrl = "http://localhost:${fastBackupServer.localPort}/upgcxcode/10/20/hedge_early.m4s"
            // Seed primary as faster so SWRR definitely picks slowPrimary first
            BtrChunkEngine.recordRouteSpeed(primaryUrl, 10 * 1024 * 1024L, 1000L)
            BtrChunkEngine.recordRouteSpeed(backupUrl, 2 * 1024 * 1024L, 1000L)

            sid = BtrSessionManager.register(primaryUrl, listOf(backupUrl))
            val session = BtrSessionManager.get(sid)!!
            val outBuf = ByteArrayOutputStream()
            val t0 = System.currentTimeMillis()
            BtrChunkEngine.pipeStream(
                session = session,
                requestedStart = 0L,
                requestedEnd = null,
                clientOut = outBuf,
                concurrency = 2,
                chunkSizeKb = 256,
                autoConcurrency = false,
                autoChunkSize = false,
                keepAlive = false
            )
            val elapsed = System.currentTimeMillis() - t0

            val raw = outBuf.toByteArray()
            val bodyStart = findHeaderEnd(raw)
            assertTrue("Must emit valid HTTP headers", bodyStart > 0)
            val body = raw.copyOfRange(bodyStart, raw.size)
            assertEquals("Full payload must be delivered", fullPayload.size, body.size)
            assertTrue("Payload bytes must match", fullPayload.contentEquals(body))
            assertTrue("Fast backup hedge must have rescued the chunk", backupHits.get() >= 1)
            assertTrue("Early hedge must rescue trickling primary well before 1200ms (elapsed=${elapsed}ms)", elapsed < 1050L)
        } finally {
            if (sid.isNotBlank()) BtrSessionManager.remove(sid)
            BtrChunkEngine.resetTransferMetersForTesting()
            runCatching { slowPrimaryServer.close() }
            runCatching { fastBackupServer.close() }
            tSlow.interrupt()
            tFast.interrupt()
        }
    }

    // ==================== Issue I-6 & III-3: Concurrency Pressure, >=8% Rollback, Startup Sprint ====================

    @Test
    fun testNoBlindStepUpWithoutGenuinePressure() {
        var now = 10_000_000L
        val controller = BtrChunkEngine.DynamicConcurrencyController(
            initialConcurrency = 6,
            autoScale = true,
            minConcurrency = 2,
            maxConcurrency = 16,
            initialNowMs = now
        )

        // 1. Repeated recordProgress with NO demand metrics must NEVER blindly step up
        for (i in 1..10) {
            now += 1_500L
            controller.recordProgress(2L * 1024 * 1024, nowMs = now)
        }
        assertEquals("Must not blindly step up with zero demand metrics", 6, controller.currentConcurrency)

        // 2. onSlow() without demand metrics / saturation must NOT step up
        now += 3_000L
        controller.onSlow(nowMs = now)
        assertEquals("onSlow without saturation must not step up", 6, controller.currentConcurrency)

        // 3. Demand metrics present but NOT saturated (queued == 0): must NOT step up
        controller.recordDemand(active = 4, limit = 6, queued = 0, nowMs = now)
        now += 1_500L
        controller.recordProgress(4L * 1024 * 1024, nowMs = now)
        assertEquals("Unsaturated queue must not step up", 6, controller.currentConcurrency)

        // 4. Momentary saturation (< 1000ms) must NOT step up
        controller.recordDemand(active = 6, limit = 6, queued = 2, nowMs = now)
        now += 500L
        controller.recordDemand(active = 3, limit = 6, queued = 0, nowMs = now)
        now += 1_000L
        controller.recordProgress(4L * 1024 * 1024, nowMs = now)
        assertEquals("Brief transient saturation must not step up", 6, controller.currentConcurrency)

        // 5. Sustained saturation >= 1s + onSlow DOES step up
        controller.recordDemand(active = 6, limit = 6, queued = 4, nowMs = now)
        now += 3_200L
        controller.onSlow(nowMs = now)
        assertEquals("onSlow under sustained high saturation must step up", 8, controller.currentConcurrency)
    }

    @Test
    fun testTrialRollbackEnforcesEightPercentGainThresholdWithSlidingWindow() {
        var now = 20_000_000L
        val controller = BtrChunkEngine.DynamicConcurrencyController(
            initialConcurrency = 6,
            autoScale = true,
            minConcurrency = 2,
            maxConcurrency = 16,
            initialNowMs = now
        )

        // Establish ~10 MB/s baseline across multiple 250ms buckets under sustained saturation
        controller.recordDemand(active = 6, limit = 6, queued = 6, nowMs = now)
        repeat(6) {
            now += 250L
            // 2.5 MB per 250ms = 10 MB/s
            controller.recordProgress((2.5 * 1024 * 1024).toLong(), nowMs = now)
        }
        assertEquals("Sustained saturation should trigger trial step-up 6 -> 8", 8, controller.currentConcurrency)

        // Run 5.25s trial at ~10.4 MB/s (+4% gain, strictly < 8% threshold!)
        controller.recordDemand(active = 8, limit = 8, queued = 6, nowMs = now)
        repeat(21) {
            now += 250L
            // 2.6 MB per 250ms = 10.4 MB/s (+4% vs 10 MB/s)
            controller.recordProgress((2.6 * 1024 * 1024).toLong(), nowMs = now)
        }
        assertEquals("Trial with only +4% gain (< 8%) must roll back from 8 to 6", 6, controller.currentConcurrency)

        // Immediately trying again during 90s soft rest must stay at 6
        now += 10_000L
        controller.recordDemand(active = 6, limit = 6, queued = 6, nowMs = now)
        repeat(6) {
            now += 250L
            controller.recordProgress((2.5 * 1024 * 1024).toLong(), nowMs = now)
        }
        assertEquals("Soft-rested rung 8 must not be retried before 90s", 6, controller.currentConcurrency)

        // Wait past 90s soft rest, re-establish ~10 MB/s baseline, step up 6 -> 8
        now += 90_000L
        controller.recordDemand(active = 6, limit = 6, queued = 6, nowMs = now)
        repeat(6) {
            now += 250L
            controller.recordProgress((2.5 * 1024 * 1024).toLong(), nowMs = now)
        }
        assertEquals("After 90s rest, rung 8 should be retried", 8, controller.currentConcurrency)

        // Run 5.25s trial at ~11.2 MB/s (+12% gain, >= 8% threshold!) -> MUST commit 8
        controller.recordDemand(active = 8, limit = 8, queued = 6, nowMs = now)
        repeat(21) {
            now += 250L
            // 2.8 MB per 250ms = 11.2 MB/s (+12% vs 10 MB/s)
            controller.recordProgress((2.8 * 1024 * 1024).toLong(), nowMs = now)
        }
        assertEquals("Trial with +12% gain (>= 8%) must commit rung 8", 8, controller.currentConcurrency)
    }

    @Test
    fun testStartupSprintAndAutomaticDecayTowardBase() {
        var now = 30_000_000L
        val controller = BtrChunkEngine.DynamicConcurrencyController(
            initialConcurrency = 6,
            autoScale = true,
            minConcurrency = 2,
            maxConcurrency = 16,
            initialNowMs = now
        )
        assertEquals(6, controller.currentConcurrency)

        // Initial playback startup sprint to 12 threads
        controller.startSprint(12, nowMs = now)
        assertEquals("Startup sprint with maxCon=16 must jump to 12", 12, controller.currentConcurrency)
        assertTrue("Startup sprint flag must be active", controller.isStartupSprintActive)

        // Deliver >= 4MB initial burst while queue is not saturated -> after 2.5s cooldown decays 12 -> 8
        controller.recordDemand(active = 4, limit = 12, queued = 0, nowMs = now)
        now += 1_500L
        controller.recordProgress(4L * 1024 * 1024, nowMs = now)
        assertEquals("Must not decay before 2.5s step cooldown", 12, controller.currentConcurrency)

        now += 1_500L // total 3s since sprint start
        controller.recordProgress(512 * 1024L, nowMs = now)
        assertEquals("Startup sprint must decay first step 12 -> 8", 8, controller.currentConcurrency)
        assertTrue("Sprint still active above base 6", controller.isStartupSprintActive)

        // After another 2.5s cooldown unsaturated -> decays 8 -> 6 and exits sprint
        now += 2_600L
        controller.recordDemand(active = 2, limit = 8, queued = 0, nowMs = now)
        assertEquals("Startup sprint must decay back to base 6", 6, controller.currentConcurrency)
        assertFalse("Sprint must complete once back at base", controller.isStartupSprintActive)

        // Clamping when userMaxCon == 8: sprint target 12 clamps to 8 and decays back to 6
        val cappedController = BtrChunkEngine.DynamicConcurrencyController(
            initialConcurrency = 6,
            autoScale = true,
            minConcurrency = 2,
            maxConcurrency = 8,
            initialNowMs = now
        )
        cappedController.startSprint(12, nowMs = now)
        assertEquals("Sprint must clamp to userMaxCon=8", 8, cappedController.currentConcurrency)

        // Hard pushback cap constrains seek sprint
        now += 1_000L
        controller.onPushback(nowMs = now, isHard = true) // drops 6 -> 4, hardCap = 4
        assertEquals(4, controller.currentConcurrency)
        now += 1_000L
        controller.onSeekSprint(12, nowMs = now)
        assertEquals("Hard pushback cap must block seek sprint above cap", 4, controller.currentConcurrency)
    }

    // ==================== Issue III-2: Unknown totalLength Chunked Framing & Truncation Detection ====================

    @Test
    fun testUnknownLengthStreamUsesChunkedEncodingAndCompletesWithTerminator() {
        val totalPayload = ByteArray(140_000) { idx -> (idx % 233).toByte() }
        val mockCdn = ServerSocket(0)
        val cdnThread = Thread {
            while (!mockCdn.isClosed) {
                val sock = try { mockCdn.accept() } catch (_: Throwable) { break }
                Thread {
                    sock.use { s ->
                        val reader = s.getInputStream().bufferedReader()
                        var rangeHeader = ""
                        reader.readLine() ?: return@use
                        while (true) {
                            val line = reader.readLine() ?: break
                            if (line.isEmpty()) break
                            if (line.startsWith("Range:", ignoreCase = true)) rangeHeader = line.substringAfter(":").trim()
                        }
                        val (start, end) = BtrLocalProxyServer.parseRangeHeaderPair(rangeHeader)
                        val startInt = start.toInt()
                        val out = s.getOutputStream()
                        if (startInt >= totalPayload.size) {
                            out.write("HTTP/1.1 416 Range Not Satisfiable\r\nContent-Range: bytes */*\r\nContent-Length: 0\r\nConnection: close\r\n\r\n".toByteArray())
                            out.flush()
                            return@use
                        }
                        val actualEnd = minOf(end ?: (totalPayload.size - 1L), totalPayload.size - 1L).toInt()
                        val len = actualEnd - startInt + 1
                        out.write(
                            ("HTTP/1.1 206 Partial Content\r\n" +
                                "Content-Type: video/mp4\r\n" +
                                "Content-Range: bytes $startInt-$actualEnd/*\r\n" +
                                "Content-Length: $len\r\n" +
                                "Connection: close\r\n\r\n").toByteArray()
                        )
                        out.write(totalPayload, startInt, len)
                        out.flush()
                    }
                }.apply { isDaemon = true; start() }
            }
        }.apply { isDaemon = true; start() }

        var sid = ""
        try {
            BtrLocalProxyServer.start()
            val port = BtrLocalProxyServer.port
            val url = "http://127.0.0.1:${mockCdn.localPort}/upgcxcode/55/66/unknown_chunked_ok.m4s"
            sid = BtrSessionManager.register(url)

            // Inspect raw HTTP/1.1 wire bytes over raw socket
            val rawResponse = Socket("127.0.0.1", port).use { sock ->
                sock.soTimeout = 5_000
                val out = sock.getOutputStream()
                out.write("GET /stream/$sid/v.m4s HTTP/1.1\r\nHost: 127.0.0.1:$port\r\nRange: bytes=0-\r\nConnection: keep-alive\r\n\r\n".toByteArray())
                out.flush()
                sock.getInputStream().readBytes()
            }

            val headerEnd = findHeaderEnd(rawResponse)
            assertTrue("Must contain valid HTTP response headers", headerEnd > 0)
            val headersText = String(rawResponse, 0, headerEnd, Charsets.US_ASCII)
            assertTrue("Status line must be 200 OK", headersText.startsWith("HTTP/1.1 200 OK\r\n"))
            assertTrue("Unknown-length response must declare Transfer-Encoding: chunked", headersText.contains("Transfer-Encoding: chunked\r\n", ignoreCase = true))
            assertTrue("Unknown-length response must declare Connection: close", headersText.contains("Connection: close\r\n", ignoreCase = true))
            assertFalse("Unknown-length chunked response must NOT declare Content-Length", headersText.contains("Content-Length:", ignoreCase = true))

            val rawBody = rawResponse.copyOfRange(headerEnd, rawResponse.size)
            val tail = String(rawBody, maxOf(0, rawBody.size - 5), minOf(5, rawBody.size), Charsets.US_ASCII)
            assertEquals("Complete chunked stream must end with terminating 0\\r\\n\\r\\n", "0\r\n\r\n", tail)

            val decoded = decodeChunkedBody(rawBody)
            assertEquals("Decoded payload size must match source", totalPayload.size, decoded.size)
            assertTrue("Decoded payload bytes must match source", totalPayload.contentEquals(decoded))
        } finally {
            if (sid.isNotBlank()) BtrSessionManager.remove(sid)
            runCatching { mockCdn.close() }
            cdnThread.interrupt()
        }
    }

    @Test
    fun testUnknownLengthMidStreamFailureOmitsChunkTerminatorAndTriggersClientError() {
        val probePayload = ByteArray(64 * 1024) { idx -> (idx % 211).toByte() }
        val mockCdn = ServerSocket(0)
        val cdnThread = Thread {
            while (!mockCdn.isClosed) {
                val sock = try { mockCdn.accept() } catch (_: Throwable) { break }
                Thread {
                    sock.use { s ->
                        val reader = s.getInputStream().bufferedReader()
                        var rangeHeader = ""
                        reader.readLine() ?: return@use
                        while (true) {
                            val line = reader.readLine() ?: break
                            if (line.isEmpty()) break
                            if (line.startsWith("Range:", ignoreCase = true)) rangeHeader = line.substringAfter(":").trim()
                        }
                        val (start, _) = BtrLocalProxyServer.parseRangeHeaderPair(rangeHeader)
                        val out = s.getOutputStream()
                        if (start == 0L) {
                            // First 64KB probe succeeds with unknown total (*)
                            out.write(
                                ("HTTP/1.1 206 Partial Content\r\n" +
                                    "Content-Type: video/mp4\r\n" +
                                    "Content-Range: bytes 0-${probePayload.size - 1}/*\r\n" +
                                    "Content-Length: ${probePayload.size}\r\n" +
                                    "Connection: close\r\n\r\n").toByteArray()
                            )
                            out.write(probePayload)
                            out.flush()
                        } else {
                            // Subsequent sequential chunk fails with 500
                            out.write("HTTP/1.1 500 Internal Server Error\r\nContent-Length: 0\r\nConnection: close\r\n\r\n".toByteArray())
                            out.flush()
                        }
                    }
                }.apply { isDaemon = true; start() }
            }
        }.apply { isDaemon = true; start() }

        var sid1 = ""
        var sid2 = ""
        try {
            BtrLocalProxyServer.start()
            val port = BtrLocalProxyServer.port
            val url = "http://127.0.0.1:${mockCdn.localPort}/upgcxcode/55/77/unknown_truncated.m4s"

            // 1. Raw socket check: must emit Transfer-Encoding: chunked and omit terminating 0\r\n\r\n on mid-stream failure
            sid1 = BtrSessionManager.register(url)
            val rawResponse = Socket("127.0.0.1", port).use { sock ->
                sock.soTimeout = 5_000
                val out = sock.getOutputStream()
                out.write("GET /stream/$sid1/v.m4s HTTP/1.1\r\nHost: 127.0.0.1:$port\r\nRange: bytes=0-\r\nConnection: close\r\n\r\n".toByteArray())
                out.flush()
                sock.getInputStream().readBytes()
            }
            val headerEnd = findHeaderEnd(rawResponse)
            assertTrue("Must contain HTTP headers", headerEnd > 0)
            val headersText = String(rawResponse, 0, headerEnd, Charsets.US_ASCII)
            assertTrue("Must declare Transfer-Encoding: chunked", headersText.contains("Transfer-Encoding: chunked\r\n", ignoreCase = true))
            val rawBody = rawResponse.copyOfRange(headerEnd, rawResponse.size)
            val tail = String(rawBody, maxOf(0, rawBody.size - 5), minOf(5, rawBody.size), Charsets.US_ASCII)
            assertFalse("Aborted mid-stream response must NOT write terminating 0\\r\\n\\r\\n chunk!", tail == "0\r\n\r\n")

            // 2. Standard HTTP/1.1 client (HttpURLConnection) check: MUST throw IOException instead of silent normal EOF
            sid2 = BtrSessionManager.register(url)
            val conn = (URL("http://127.0.0.1:$port/stream/$sid2/v.m4s").openConnection() as HttpURLConnection).apply {
                connectTimeout = 5_000
                readTimeout = 5_000
                setRequestProperty("Range", "bytes=0-")
            }
            assertEquals(200, conn.responseCode)
            try {
                conn.inputStream.use { it.readBytes() }
                fail("HTTP/1.1 client MUST detect truncated chunked transfer with IOException, not silent EOF!")
            } catch (_: IOException) {
                // Expected: premature EOF detected by HTTP/1.1 chunked decoder
            }
        } finally {
            if (sid1.isNotBlank()) BtrSessionManager.remove(sid1)
            if (sid2.isNotBlank()) BtrSessionManager.remove(sid2)
            runCatching { mockCdn.close() }
            cdnThread.interrupt()
        }
    }

    private fun findHeaderEnd(raw: ByteArray): Int {
        for (i in 0 until raw.size - 3) {
            if (raw[i] == '\r'.code.toByte() &&
                raw[i + 1] == '\n'.code.toByte() &&
                raw[i + 2] == '\r'.code.toByte() &&
                raw[i + 3] == '\n'.code.toByte()
            ) {
                return i + 4
            }
        }
        return -1
    }

    private fun decodeChunkedBody(rawBody: ByteArray): ByteArray {
        val out = ByteArrayOutputStream()
        var pos = 0
        while (pos < rawBody.size) {
            var lineEnd = -1
            for (i in pos until rawBody.size - 1) {
                if (rawBody[i] == '\r'.code.toByte() && rawBody[i + 1] == '\n'.code.toByte()) {
                    lineEnd = i
                    break
                }
            }
            assertTrue("Malformed chunked size line at pos=$pos", lineEnd >= pos)
            val sizeHex = String(rawBody, pos, lineEnd - pos, Charsets.US_ASCII).trim()
            val chunkLen = sizeHex.toInt(16)
            pos = lineEnd + 2
            if (chunkLen == 0) {
                assertTrue("Missing trailing CRLF after 0 chunk", pos + 2 <= rawBody.size)
                assertEquals('\r'.code.toByte(), rawBody[pos])
                assertEquals('\n'.code.toByte(), rawBody[pos + 1])
                break
            }
            assertTrue("Truncated chunk data", pos + chunkLen + 2 <= rawBody.size)
            out.write(rawBody, pos, chunkLen)
            pos += chunkLen
            assertEquals('\r'.code.toByte(), rawBody[pos])
            assertEquals('\n'.code.toByte(), rawBody[pos + 1])
            pos += 2
        }
        return out.toByteArray()
    }

    // ==================== Issue 4: BtrSessionManager IPv6 Loopback Blocking ====================

    @Test
    fun testSessionManagerIpv6LoopbackBlocking() {
        assertTrue(BtrSessionManager.isLoopbackHost("[::1]"))
        assertTrue(BtrSessionManager.isLoopbackHost("::1"))
        assertTrue(BtrSessionManager.isLoopbackHost("[::1]:8080"))
        assertTrue(BtrSessionManager.isLoopbackHost("0:0:0:0:0:0:0:1"))
        assertTrue(BtrSessionManager.isLoopbackHost("::ffff:127.0.0.1"))
        assertTrue(BtrSessionManager.isLoopbackHost("[::ffff:7f00:1]"))
        assertTrue(BtrSessionManager.isLoopbackHost("127.0.0.1"))
        assertTrue(BtrSessionManager.isLoopbackHost("localhost"))
        assertFalse(BtrSessionManager.isLoopbackHost("upos-sz-mirrorali.bilivideo.com"))
        assertFalse(BtrSessionManager.isLoopbackHost("240e:1234::1"))

        assertTrue(
            "IPv6 loopback URL must never synthesize UPOS mirrors",
            BtrSessionManager.synthesizeCompatibleBackups("https://[::1]/upgcxcode/12/34/56.m4s?deadline=1900000000").isEmpty()
        )
        assertTrue(
            "IPv6 loopback with port must never synthesize UPOS mirrors",
            BtrSessionManager.synthesizeCompatibleBackups("http://[::1]:18080/upgcxcode/12/34/56.m4s?deadline=1900000000").isEmpty()
        )
    }

    // ==================== Issue 5: OkHttp 4.12 callTimeout vs Explicit Cancel ====================

    @Test
    fun testOkHttp412CallTimeoutVsExplicitCancelAndFirstByteWatchdogFailover() {
        // 1. Unit check: OkHttp 4.12 callTimeout sets call.isCanceled() == true and throws InterruptedIOException("timeout")
        assertTrue(
            "OkHttp callTimeout (callCanceled=true, callerCanceled=false) MUST be treated as timeout",
            BtrChunkEngine.isTimeoutError(
                e = InterruptedIOException("timeout"),
                callCanceled = true,
                callerCanceled = false,
                timedOutFlag = false
            )
        )
        assertFalse(
            "Explicit caller cancellation (e.g. hedge won) must NOT be treated as timeout",
            BtrChunkEngine.isTimeoutError(
                e = IOException("Canceled"),
                callCanceled = true,
                callerCanceled = true,
                timedOutFlag = false
            )
        )
        assertTrue(
            "Watchdog timedOutFlag=true MUST be treated as timeout",
            BtrChunkEngine.isTimeoutError(
                e = IOException("Canceled"),
                callCanceled = true,
                callerCanceled = false,
                timedOutFlag = true
            )
        )

        // 2. End-to-end: hanging primary triggers firstByte watchdog -> penalizes primary & fails over to fast backup
        val totalLen = 64 * 1024 + 128 * 1024
        val payload = ByteArray(totalLen) { (it % 239).toByte() }
        val hangingServer = ServerSocket(0)
        val fastServer = ServerSocket(0)

        val tHang = Thread {
            while (!hangingServer.isClosed) {
                val sock = try { hangingServer.accept() } catch (_: Throwable) { break }
                Thread {
                    sock.use { s ->
                        val r = s.getInputStream().bufferedReader()
                        var rangeHdr = ""
                        r.readLine() ?: return@use
                        while (true) {
                            val line = r.readLine() ?: break
                            if (line.isEmpty()) break
                            if (line.startsWith("Range:", ignoreCase = true)) rangeHdr = line.substringAfter(':').trim()
                        }
                        val (start, end) = BtrLocalProxyServer.parseRangeHeaderPair(rangeHdr)
                        val startInt = start.toInt()
                        val actualEnd = minOf(end ?: (totalLen - 1L), totalLen - 1L).toInt()
                        val len = actualEnd - startInt + 1
                        if (startInt == 0) {
                            val out = s.getOutputStream()
                            out.write(
                                ("HTTP/1.1 206 Partial Content\r\nContent-Type: video/mp4\r\n" +
                                    "Content-Range: bytes $startInt-$actualEnd/$totalLen\r\nContent-Length: $len\r\nConnection: close\r\n\r\n").toByteArray()
                            )
                            out.write(payload, startInt, len)
                            out.flush()
                        } else {
                            // Blackhole: never send response headers for subsequent chunks
                            runCatching { Thread.sleep(5_000L) }
                        }
                    }
                }.apply { isDaemon = true; start() }
            }
        }.apply { isDaemon = true; start() }

        val tFast = Thread {
            while (!fastServer.isClosed) {
                val sock = try { fastServer.accept() } catch (_: Throwable) { break }
                Thread {
                    sock.use { s ->
                        val r = s.getInputStream().bufferedReader()
                        var rangeHdr = ""
                        r.readLine() ?: return@use
                        while (true) {
                            val line = r.readLine() ?: break
                            if (line.isEmpty()) break
                            if (line.startsWith("Range:", ignoreCase = true)) rangeHdr = line.substringAfter(':').trim()
                        }
                        val (start, end) = BtrLocalProxyServer.parseRangeHeaderPair(rangeHdr)
                        val startInt = start.toInt()
                        val actualEnd = minOf(end ?: (totalLen - 1L), totalLen - 1L).toInt()
                        val len = actualEnd - startInt + 1
                        val out = s.getOutputStream()
                        out.write(
                            ("HTTP/1.1 206 Partial Content\r\nContent-Type: video/mp4\r\n" +
                                "Content-Range: bytes $startInt-$actualEnd/$totalLen\r\nContent-Length: $len\r\nConnection: close\r\n\r\n").toByteArray()
                        )
                        out.write(payload, startInt, len)
                        out.flush()
                    }
                }.apply { isDaemon = true; start() }
            }
        }.apply { isDaemon = true; start() }

        val prevFb = BtrChunkEngine.firstByteTimeoutMsForTesting
        val prevStall = BtrChunkEngine.stallTimeoutMsForTesting
        var sid = ""
        try {
            BtrChunkEngine.resetRouteSpeedsAndPenaltiesForTesting()
            BtrChunkEngine.firstByteTimeoutMsForTesting = 250L
            BtrChunkEngine.stallTimeoutMsForTesting = 250L
            val hangUrl = "http://127.0.0.1:${hangingServer.localPort}/upgcxcode/5/1/fb_hang.m4s"
            val fastUrl = "http://localhost:${fastServer.localPort}/upgcxcode/5/1/fb_hang.m4s"
            BtrChunkEngine.recordRouteSpeed(hangUrl, 10 * 1024 * 1024L, 1000L)
            BtrChunkEngine.recordRouteSpeed(fastUrl, 2 * 1024 * 1024L, 1000L)

            sid = BtrSessionManager.register(hangUrl, listOf(fastUrl))
            val session = BtrSessionManager.get(sid)!!
            val outBuf = ByteArrayOutputStream()
            BtrChunkEngine.pipeStream(
                session = session,
                requestedStart = 0L,
                requestedEnd = null,
                clientOut = outBuf,
                concurrency = 2,
                chunkSizeKb = 128,
                autoConcurrency = false,
                autoChunkSize = false
            )
            val raw = outBuf.toByteArray()
            val bodyStart = findHeaderEnd(raw)
            assertTrue("Must emit HTTP headers", bodyStart > 0)
            val body = raw.copyOfRange(bodyStart, raw.size)
            assertTrue("Must deliver exact payload via backup after first-byte timeout", payload.contentEquals(body))
            assertTrue(
                "Blackhole primary host must be banned/penalized after watchdog timeout",
                session.banList.isBanned(hangUrl, "127.0.0.1")
            )
        } finally {
            BtrChunkEngine.firstByteTimeoutMsForTesting = prevFb
            BtrChunkEngine.stallTimeoutMsForTesting = prevStall
            if (sid.isNotBlank()) BtrSessionManager.remove(sid)
            BtrChunkEngine.resetRouteSpeedsAndPenaltiesForTesting()
            runCatching { hangingServer.close() }
            runCatching { fastServer.close() }
            tHang.interrupt()
            tFast.interrupt()
        }
    }

    // ==================== Issue 6: Early Warmup Probing Across Unmeasured Nodes ====================

    @Test
    fun testEarlyWarmupProbingSpreadsAcrossDistinctUnmeasuredNodes() {
        BtrChunkEngine.resetRouteSpeedsAndPenaltiesForTesting()
        try {
            // Warmup (only 1 node measured, 5 unmeasured): chunk 0 protected, chunks 1,2,4,5 allow trials
            assertFalse(BtrChunkEngine.shouldAllowUnmeasuredTrial(0L, 20L, measuredCount = 1, unmeasuredCount = 5))
            assertTrue(BtrChunkEngine.shouldAllowUnmeasuredTrial(1L, 20L, measuredCount = 1, unmeasuredCount = 5))
            assertTrue(BtrChunkEngine.shouldAllowUnmeasuredTrial(2L, 20L, measuredCount = 1, unmeasuredCount = 5))
            assertFalse(BtrChunkEngine.shouldAllowUnmeasuredTrial(3L, 20L, measuredCount = 1, unmeasuredCount = 5))
            assertTrue(BtrChunkEngine.shouldAllowUnmeasuredTrial(4L, 20L, measuredCount = 1, unmeasuredCount = 5))

            val measuredA = "https://upos-sz-mirrorali.bilivideo.com/upgcxcode/6/1/warm.m4s"
            val unmeasuredList = listOf(
                "https://upos-sz-mirrorcos.bilivideo.com/upgcxcode/6/1/warm.m4s",
                "https://upos-sz-mirrorhw.bilivideo.com/upgcxcode/6/1/warm.m4s",
                "https://upos-sz-mirrorbos.bilivideo.com/upgcxcode/6/1/warm.m4s",
                "https://upos-sz-mirrorbd.bilivideo.com/upgcxcode/6/1/warm.m4s"
            )
            BtrChunkEngine.recordRouteSpeed(measuredA, 10 * 1024 * 1024L, 1000L)
            val all = listOf(measuredA) + unmeasuredList
            val sid = BtrSessionManager.register(measuredA, unmeasuredList)
            try {
                val session = BtrSessionManager.get(sid)!!
                val pickedTrials = (1L..4L).map { idx ->
                    BtrChunkEngine.sortUrlsByHealthAndSpeed(all, idx, session, allowUnmeasuredTrial = true).first()
                }.toSet()
                assertEquals(
                    "Sequential warmup trials must spread across all 4 distinct unmeasured nodes",
                    unmeasuredList.toSet(),
                    pickedTrials
                )
            } finally {
                BtrSessionManager.remove(sid)
            }
        } finally {
            BtrChunkEngine.resetRouteSpeedsAndPenaltiesForTesting()
        }
    }

    // ==================== Issue 7: 412/429 BanList Cooldown, Pushback Debounce & HardCap Expiry ====================

    @Test
    fun testBanList412429TemporaryCooldownNeverPermanent() {
        val banList = SessionBanList()
        val host = "upos-sz-mirrorcos.bilivideo.com"
        val url = "https://$host/upgcxcode/7/1/pushback.m4s?deadline=1900000000"
        val t0 = 1_000_000L

        // Even after repeated 429 & 412 bursts, host is ONLY temporarily cooled down — NEVER permanently banned
        repeat(5) {
            assertTrue(banList.recordHttpFailure(url, host, 429, now = t0))
            assertTrue(banList.recordHttpFailure(url, host, 412, now = t0))
        }
        assertFalse("412/429 must NEVER cause permanent ban", banList.isPermanentlyBanned(url, host))
        assertTrue("Host must be temporarily cooled down during 45s TTL", banList.isBanned(url, host, now = t0 + 10_000L))
        assertFalse("Host must recover cleanly after 45s TTL expires", banList.isBanned(url, host, now = t0 + 46_000L))
    }

    @Test
    fun testDynamicConcurrencyPushbackDebounceAndHardCapExpiry() {
        var now = 50_000_000L
        val controller = BtrChunkEngine.DynamicConcurrencyController(
            initialConcurrency = 12,
            autoScale = true,
            minConcurrency = 2,
            maxConcurrency = 16,
            initialNowMs = now
        )
        assertEquals(12, controller.currentConcurrency)

        // 1. Burst of 6 simultaneous 429s within 500ms must debounce to a SINGLE step-down (12 -> 8, NOT 2!)
        for (i in 0 until 6) {
            controller.onPushback(nowMs = now + i * 80L, isHard = true)
        }
        assertEquals("Simultaneous 429 burst must debounce to single rung drop 12 -> 8", 8, controller.currentConcurrency)
        assertEquals("Hard cap after debounced burst should be 8", 8, controller.activeHardCapForTesting(now + 500L))

        // 2. Test hardCap expiration: a past 429 at 6 threads (cap 4) must NOT pin a later 429 at 12 threads down to 4!
        var t = 60_000_000L
        val c2 = BtrChunkEngine.DynamicConcurrencyController(
            initialConcurrency = 6,
            autoScale = true,
            minConcurrency = 2,
            maxConcurrency = 16,
            initialNowMs = t
        )
        c2.onPushback(nowMs = t, isHard = true) // 6 -> 4, hardCap = 4 for 45s
        assertEquals(4, c2.currentConcurrency)
        assertEquals(4, c2.activeHardCapForTesting(t + 10_000L))

        // Advance well past 45s hardCap expiry -> hardCap must expire to 0!
        t += 60_000L
        assertEquals("Expired hardCap must clear completely", 0, c2.activeHardCapForTesting(t))

        // Sprint up to 12 threads, then hit a fresh 429 -> drops 12 -> 8, hardCap MUST be 8 (NOT stale 4!)
        c2.startSprint(12, nowMs = t)
        assertEquals(12, c2.currentConcurrency)
        t += 5_000L
        c2.onPushback(nowMs = t, isHard = true)
        assertEquals(8, c2.currentConcurrency)
        assertEquals("Fresh 429 at 12 threads must cap at 8, not stale expired cap 4", 8, c2.activeHardCapForTesting(t + 1_000L))
    }

    // ==================== Issue 8: Resume/Hedge Speed Uses localBytesRead & >= 48 KiB ====================

    @Test
    fun testResumedChunkSpeedUsesLocalBytesReadAnd48KiBThreshold() {
        BtrChunkEngine.resetRouteSpeedsAndPenaltiesForTesting()
        try {
            val tinyUrl = "https://upos-sz-mirrorali.bilivideo.com/upgcxcode/8/1/tiny.m4s"
            BtrChunkEngine.recordRouteSpeed(tinyUrl, 47 * 1024L, 10L)
            assertEquals("Transfer < 48 KiB must NOT record speed", 0.0, BtrChunkEngine.getRouteSpeed(tinyUrl), 0.01)
            BtrChunkEngine.recordRouteSpeed(tinyUrl, 48 * 1024L, 100L)
            assertTrue("Transfer >= 48 KiB must record speed", BtrChunkEngine.getRouteSpeed(tinyUrl) > 0.0)

            // End-to-end: primary delivers 224KB of 256KB chunk then truncates; backup resumes ONLY remaining 32KB (< 48 KiB)
            val totalLen = 64 * 1024 + 256 * 1024
            val payload = ByteArray(totalLen) { (it % 227).toByte() }
            val truncPrimary = ServerSocket(0)
            val tailBackup = ServerSocket(0)

            val t1 = Thread {
                while (!truncPrimary.isClosed) {
                    val sock = try { truncPrimary.accept() } catch (_: Throwable) { break }
                    Thread {
                        sock.use { s ->
                            val r = s.getInputStream().bufferedReader()
                            var rangeHdr = ""
                            r.readLine() ?: return@use
                            while (true) {
                                val line = r.readLine() ?: break
                                if (line.isEmpty()) break
                                if (line.startsWith("Range:", ignoreCase = true)) rangeHdr = line.substringAfter(':').trim()
                            }
                            val (start, end) = BtrLocalProxyServer.parseRangeHeaderPair(rangeHdr)
                            val startInt = start.toInt()
                            val actualEnd = minOf(end ?: (totalLen - 1L), totalLen - 1L).toInt()
                            val len = actualEnd - startInt + 1
                            val out = s.getOutputStream()
                            out.write(
                                ("HTTP/1.1 206 Partial Content\r\nContent-Type: video/mp4\r\n" +
                                    "Content-Range: bytes $startInt-$actualEnd/$totalLen\r\nContent-Length: $len\r\nConnection: close\r\n\r\n").toByteArray()
                            )
                            val toSend = if (startInt == 0) len else minOf(len, 224 * 1024)
                            out.write(payload, startInt, toSend)
                            out.flush()
                        }
                    }.apply { isDaemon = true; start() }
                }
            }.apply { isDaemon = true; start() }

            val t2 = Thread {
                while (!tailBackup.isClosed) {
                    val sock = try { tailBackup.accept() } catch (_: Throwable) { break }
                    Thread {
                        sock.use { s ->
                            val r = s.getInputStream().bufferedReader()
                            var rangeHdr = ""
                            r.readLine() ?: return@use
                            while (true) {
                                val line = r.readLine() ?: break
                                if (line.isEmpty()) break
                                if (line.startsWith("Range:", ignoreCase = true)) rangeHdr = line.substringAfter(':').trim()
                            }
                            val (start, end) = BtrLocalProxyServer.parseRangeHeaderPair(rangeHdr)
                            val startInt = start.toInt()
                            val actualEnd = minOf(end ?: (totalLen - 1L), totalLen - 1L).toInt()
                            val len = actualEnd - startInt + 1
                            val out = s.getOutputStream()
                            out.write(
                                ("HTTP/1.1 206 Partial Content\r\nContent-Type: video/mp4\r\n" +
                                    "Content-Range: bytes $startInt-$actualEnd/$totalLen\r\nContent-Length: $len\r\nConnection: close\r\n\r\n").toByteArray()
                            )
                            out.write(payload, startInt, len)
                            out.flush()
                        }
                    }.apply { isDaemon = true; start() }
                }
            }.apply { isDaemon = true; start() }

            var sid = ""
            try {
                val pUrl = "http://127.0.0.1:${truncPrimary.localPort}/upgcxcode/8/2/tail_speed.m4s"
                val bUrl = "http://localhost:${tailBackup.localPort}/upgcxcode/8/2/tail_speed.m4s"
                BtrChunkEngine.recordRouteSpeed(pUrl, 10 * 1024 * 1024L, 1000L)
                sid = BtrSessionManager.register(pUrl, listOf(bUrl))
                val session = BtrSessionManager.get(sid)!!
                val outBuf = ByteArrayOutputStream()
                BtrChunkEngine.pipeStream(
                    session = session,
                    requestedStart = 0L,
                    requestedEnd = null,
                    clientOut = outBuf,
                    concurrency = 2,
                    chunkSizeKb = 256,
                    autoConcurrency = false,
                    autoChunkSize = false
                )
                val raw = outBuf.toByteArray()
                val body = raw.copyOfRange(findHeaderEnd(raw), raw.size)
                assertTrue("Full resumed payload must match", payload.contentEquals(body))
                assertEquals(
                    "Backup that only fetched 32KB resumed tail (< 48 KiB) must NOT record inflated speed from 256KB concatenated chunk",
                    0.0,
                    BtrChunkEngine.getRouteSpeed(bUrl),
                    0.01
                )
            } finally {
                if (sid.isNotBlank()) BtrSessionManager.remove(sid)
                runCatching { truncPrimary.close() }
                runCatching { tailBackup.close() }
                t1.interrupt()
                t2.interrupt()
            }
        } finally {
            BtrChunkEngine.resetRouteSpeedsAndPenaltiesForTesting()
        }
    }

    // ==================== Issue 9: Auto Concurrency > 6, Saturation Continuity & No False Hunger ====================

    @Test
    fun testAutoConcurrencyBoundsAboveDefault6() {
        val autoDefault = BtrChunkEngine.resolveConcurrencyBounds(
            concurrency = 6,
            autoConcurrency = true,
            isAudio = false,
            isEpisode = false
        )
        assertTrue("Auto mode must enable auto-scale", autoDefault.useAuto)
        assertEquals(6, autoDefault.initialCon)
        assertEquals("Auto mode must scale above default slider 6 up to 16", 16, autoDefault.maxCon)
        assertEquals("Startup sprint must target 12 even when manual default is 6", 12, autoDefault.startupSprintTarget)

        val manual = BtrChunkEngine.resolveConcurrencyBounds(
            concurrency = 6,
            autoConcurrency = false,
            isAudio = false,
            isEpisode = false
        )
        assertFalse(manual.useAuto)
        assertEquals(6, manual.initialCon)
        assertEquals(6, manual.maxCon)

        val audio = BtrChunkEngine.resolveConcurrencyBounds(
            concurrency = 16,
            autoConcurrency = true,
            isAudio = true,
            isEpisode = false
        )
        assertFalse(audio.useAuto)
        assertEquals(BtrChunkEngine.AUDIO_MAX_CONCURRENCY, audio.maxCon)
    }

    // ==================== Issue 10: Adaptive Chunk Size, Timeouts & Round 1+ Live Prefix Sharing ====================

    @Test
    fun testAdaptiveChunkSizeRangeAndThroughputAndRound1PrefixSharingAndTimeouts() {
        // 1. 1.5MB bounded DASH Range on a 500MB video must NOT collapse to totalTasks = 1
        val dashRangeBytes = 1536 * 1024L
        val rangeChunk = BtrChunkEngine.calculateDynamicChunkSize(
            totalLength = 500L * 1024 * 1024,
            baseChunkSizeKb = 512,
            autoChunkSize = true,
            isAudio = false,
            requestedRangeBytes = dashRangeBytes,
            measuredConnectionBps = 0.0,
            concurrency = 6,
            candidateHostCount = 4
        )
        val totalTasks = (dashRangeBytes + rangeChunk - 1L) / rangeChunk
        assertTrue("1.5MB bounded DASH Range must split into >= 4 tasks (chunk=$rangeChunk, tasks=$totalTasks)", totalTasks >= 4L)

        // 2. Slow link (512 KB/s) on 500MB video must NOT pick 2MB chunks (avoids 4s HOL blocking)
        val slowChunk = BtrChunkEngine.calculateDynamicChunkSize(
            totalLength = 500L * 1024 * 1024,
            baseChunkSizeKb = 512,
            autoChunkSize = true,
            isAudio = false,
            requestedRangeBytes = 500L * 1024 * 1024,
            measuredConnectionBps = 512.0 * 1024.0,
            concurrency = 6,
            candidateHostCount = 4
        )
        assertTrue("Slow link must cap chunk size <= 512KB (actual=$slowChunk)", slowChunk <= 512 * 1024L)

        // 3. Round 1+ live prefix sharing: Round 0 saves 64KB -> Round 1 primary resumes from 64KB and appends 64KB -> hedge gets 128KB!
        val expectedTotal = 256 * 1024
        val task = BtrChunkEngine.ChunkTask(index = 0L, startByte = 0L, endByte = expectedTotal - 1L)
        val round0Bytes = ByteArray(64 * 1024) { 0x11.toByte() }
        assertTrue(task.tryClaimPrefixProducer(attemptId = 1L))
        task.appendLivePrefix(1L, round0Bytes, round0Bytes.size, expectedTotal)
        task.releasePrefixProducer(1L, keepPartial = true)

        val baseSnap = task.takePrefixSnapshot(32 * 1024, expectedTotal)
        assertEquals(64 * 1024, baseSnap?.size)

        // Round 1 primary claims producer WITH baseSnap (resumeOffset = 64KB > 0!)
        assertTrue("Round 1 primary must be allowed as live prefix producer even with resumeOffset > 0",
            task.tryClaimPrefixProducer(attemptId = 2L, basePrefix = baseSnap, expectedTotal = expectedTotal))
        val round1Bytes = ByteArray(64 * 1024) { 0x22.toByte() }
        task.appendLivePrefix(2L, round1Bytes, round1Bytes.size, expectedTotal)

        // Round 1 hedge takes live snapshot and gets BOTH base + live (128KB)!
        val combinedSnap = task.takePrefixSnapshot(32 * 1024, expectedTotal)
        assertNotNull(combinedSnap)
        assertEquals("Round 1 hedge must receive combined base + live prefix (128KB)", 128 * 1024, combinedSnap!!.size)
        assertEquals(0x11.toByte(), combinedSnap[0])
        assertEquals(0x11.toByte(), combinedSnap[64 * 1024 - 1])
        assertEquals(0x22.toByte(), combinedSnap[64 * 1024])
        assertEquals(0x22.toByte(), combinedSnap[128 * 1024 - 1])

        // 4. BtrHttpClient call timeouts configured on chunk/probe/fallback/audio clients; 0 on long stream client
        assertEquals(15_000, BtrHttpClient.chunkClient.callTimeoutMillis)
        assertEquals(15_000, BtrHttpClient.chunkAudioClient.callTimeoutMillis)
        assertEquals(7_000, BtrHttpClient.probeClient.callTimeoutMillis)
        assertEquals(7_000, BtrHttpClient.audioProbeClient.callTimeoutMillis)
        assertTrue(BtrHttpClient.fallbackClient.callTimeoutMillis in 15_000..25_000)
        assertTrue(BtrHttpClient.audioClient.callTimeoutMillis in 15_000..25_000)
        assertEquals(0, BtrHttpClient.audioStreamClient.callTimeoutMillis)
    }

    // ==================== Issue 11: Akamai Donor Exclusion, Multi-Donor & Custom Pool Isolation ====================

    @Test
    fun testAkamaiPassthroughMultiDonorSynthesisAndCustomPoolIsolation() {
        // 1. Akamai URL without hdnts= (os=akam) must NOT be mirrored when rewriteAkamai=false
        val akamNoHdnts = "https://upos-hz-mirrorakam.akamaized.net/upgcxcode/11/1/v.m4s?os=akam&deadline=1900000000"
        val cfgNoAkam = AcceleratorConfig(enableBtr = true, rewriteAkamai = false)
        assertTrue(
            "Akamai host without hdnts= must not synthesize UPOS when rewriteAkamai=false",
            BtrSessionManager.synthesizeCompatibleBackups(akamNoHdnts, cfgNoAkam).isEmpty()
        )
        // P2P / raw IP / non-standard port URLs must NOT be used as donors
        assertTrue(BtrSessionManager.synthesizeCompatibleBackups("https://1.2.3.4/upgcxcode/11/1/v.m4s?deadline=1900000000").isEmpty())
        assertTrue(BtrSessionManager.synthesizeCompatibleBackups("https://upos-sz-mirrorali.bilivideo.com:8443/upgcxcode/11/1/v.m4s?deadline=1900000000").isEmpty())
        assertTrue(BtrSessionManager.synthesizeCompatibleBackups("https://p2p.szbdyd.com/upgcxcode/11/1/v.m4s?deadline=1900000000").isEmpty())

        // 2. All-Akamai multi-donor with rewriteAkamai=true expands ALL donors and preserves distinct signatures
        val akam1 = "https://upos-hz-mirrorakam.akamaized.net/upgcxcode/11/2/v.m4s?hdnts=sig1&deadline=1900000000"
        val akam2 = "https://upos-hz-mirrorakam.akamaized.net/upgcxcode/11/2/v.m4s?hdnts=sig2&deadline=1900000000"
        val cfgAkam = AcceleratorConfig(enableBtr = true, rewriteAkamai = true, btrCdnPoolSize = 4)
        val akamSid = BtrSessionManager.register(akam1, listOf(akam2), config = cfgAkam)
        try {
            val session = BtrSessionManager.get(akamSid)!!
            val allUrls = session.currentCandidateUrls()
            assertTrue("Must preserve sig1 variants on synthesized UPOS mirrors", allUrls.any { it.contains("bilivideo.com") && it.contains("sig1") })
            assertTrue("Must preserve sig2 variants on synthesized UPOS mirrors (not dropped by host-only dedup)", allUrls.any { it.contains("bilivideo.com") && it.contains("sig2") })
        } finally {
            BtrSessionManager.remove(akamSid)
        }

        // 3. Custom CDN pool mode isolates custom nodes (non-custom primaryUrl replaced, not kept ahead!)
        val nonCustomPrimary = "https://upos-sz-mirrorali.bilivideo.com/upgcxcode/11/3/v.m4s?deadline=1900000000"
        val nonCustomBackup = "https://upos-sz-mirrorbos.bilivideo.com/upgcxcode/11/3/v.m4s?deadline=1900000000"
        val customPool = listOf("upos-tf-all-tx.bilivideo.com", "upos-tf-all-hw.bilivideo.com")
        val customSid = BtrSessionManager.register(
            primaryUrl = nonCustomPrimary,
            backupUrls = listOf(nonCustomBackup),
            config = AcceleratorConfig(enableBtr = true, customCdnPool = customPool)
        )
        try {
            val session = BtrSessionManager.get(customSid)!!
            val allUrls = session.currentCandidateUrls()
            assertTrue("Primary URL must be promoted to custom pool node", session.primaryUrl.contains("upos-tf-all-tx.bilivideo.com"))
            assertTrue("All session URLs must belong strictly to custom pool", allUrls.all { u -> customPool.any { u.contains(it) } })
            assertFalse("Non-custom primary must not leak into custom pool session", allUrls.any { it.contains("mirrorali") || it.contains("mirrorbos") })
        } finally {
            BtrSessionManager.remove(customSid)
        }
    }

    // ==================== Issue 13: Live Session Isolates VOD customCdnPool ====================

    @Test
    fun testLiveSessionDoesNotInjectVodCustomCdnPool() {
        val liveSegUrl = "https://d1--cn-gotcha204.bilivideo.com/live-bvc/123456/live_100.m4s?sign=abc"
        val vodOnlyCustomConfig = AcceleratorConfig(
            enableBtr = true,
            enableLiveAcceleration = true,
            customCdnPool = listOf("upos-sz-mirrorali.bilivideo.com", "upos-sz-mirrorcos.bilivideo.com")
        )
        val liveSid = BtrSessionManager.register(
            primaryUrl = liveSegUrl,
            isLive = true,
            config = vodOnlyCustomConfig
        )
        try {
            val session = BtrSessionManager.get(liveSid)!!
            assertFalse("VOD customCdnPool must NOT set liveCustomPoolOnly=true", session.liveCustomPoolOnly)
            val hosts = session.liveHostPool!!.status().map { it.host }.toSet()
            assertFalse("VOD mirrorali must NOT be injected into liveHostPool", hosts.contains("upos-sz-mirrorali.bilivideo.com"))
            assertFalse("VOD mirrorcos must NOT be injected into liveHostPool", hosts.contains("upos-sz-mirrorcos.bilivideo.com"))
            assertTrue("Confirmed fMP4 live segment must still receive KNOWN_FMP4_HOSTS", hosts.contains("d1--cn-gotcha208.bilivideo.com"))
        } finally {
            BtrSessionManager.remove(liveSid)
        }
    }

    // ==================== Issue 15: defaultRefreshProvider Cross-Process Support ====================

    @Test
    fun testDefaultRefreshProviderInvokedWhenNoPerSessionRefresherAttached() {
        assertNotNull("defaultRefreshProvider must be wired by default", BtrSessionManager.defaultRefreshProvider)
        val nowMs = System.currentTimeMillis()
        val nowSec = nowMs / 1000L
        val expiringUrl = "https://upos-sz-mirrorcos.bilivideo.com/upgcxcode/15/1/cross_proc.m4s?deadline=${nowSec + 30}"
        val freshUrl = "https://upos-sz-mirrorcos.bilivideo.com/upgcxcode/15/1/cross_proc.m4s?deadline=${nowSec + 3600}&fresh=1"
        val invoked = AtomicInteger(0)
        // 注册临近过期会话前先暂停默认刷新器，避免后台定时任务抢先执行失败并写入冷却窗口导致偶发竞态
        BtrSessionManager.defaultRefreshProvider = null
        val sid = BtrSessionManager.register(expiringUrl)
        try {
            val session = BtrSessionManager.get(sid)!!
            session.proactiveRefreshTask?.cancel(false)
            BtrSessionManager.defaultRefreshProvider = { s ->
                invoked.incrementAndGet()
                BtrSessionManager.refreshSignatureIfSamePath(freshUrl, emptyList())
                s.urlDeadlineEpochSec > nowSec + 1000L
            }
            val ok = BtrSessionManager.maybeTriggerProactiveRefresh(session, nowMs = nowMs)
            assertTrue("defaultRefreshProvider must refresh session when no local callback attached", ok)
            assertEquals(1, invoked.get())
            assertEquals(freshUrl, session.primaryUrl)
        } finally {
            BtrSessionManager.resetDefaultRefreshProvider()
            BtrSessionManager.remove(sid)
        }
    }
}
