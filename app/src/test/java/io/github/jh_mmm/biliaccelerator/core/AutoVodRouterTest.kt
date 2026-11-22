package io.github.jh_mmm.biliaccelerator.core

import org.junit.After
import org.junit.Assert.assertEquals
import org.junit.Assert.assertFalse
import org.junit.Assert.assertNotNull
import org.junit.Assert.assertNull
import org.junit.Assert.assertTrue
import org.junit.Before
import org.junit.Test
import java.nio.file.Files
import kotlin.math.abs

class AutoVodRouterTest {

    companion object {
        private const val COSOV = "upos-sz-mirrorcosov.bilivideo.com"
        private const val AKAM = "upos-hz-mirrorakam.akamaized.net"
        private const val HW = "upos-sz-mirrorhw.bilivideo.com"
        private const val ALI = "upos-sz-mirrorali.bilivideo.com"
        private const val COS = "upos-sz-mirrorcos.bilivideo.com"
        private const val TFHW = "upos-tf-all-hw.bilivideo.com"
        private const val VIDEO_PATH = "/upgcxcode/05/16/42231991605/42231991605-1-30102.m4s"
        private const val AUDIO_PATH = "/upgcxcode/05/16/42231991605/42231991605-1-30280.m4s"
    }

    private val autoNonBtrConfig = AcceleratorConfig(
        enabled = true,
        enableBtr = false,
        selectionMode = "auto",
        stallRecovery = true,
        targetHost = COS,
        blockPcdn = true,
        proxyMcdn = true
    )

    private fun samplePlayurlJson(): String = """
        {
          "code": 0,
          "data": {
            "dash": {
              "video": [
                {
                  "id": 112,
                  "bandwidth": 3753786,
                  "codecs": "hvc1.1.6.L150.90",
                  "baseUrl": "https://$COSOV$VIDEO_PATH?os=cosovbv&upsig=a",
                  "backupUrl": [
                    "https://$AKAM$VIDEO_PATH?os=akam&hdnts=b",
                    "https://edge.mountaintoys.cn:8080$VIDEO_PATH?xy_usource=$ALI&os=upos&upsig=c"
                  ]
                }
              ],
              "audio": [
                {
                  "id": 30280,
                  "bandwidth": 319173,
                  "codecs": "mp4a.40.2",
                  "base_url": "https://$AKAM$AUDIO_PATH?os=akam&hdnts=c",
                  "backup_url": [
                    "https://$COSOV$AUDIO_PATH?os=cosovbv&upsig=d"
                  ]
                }
              ]
            }
          }
        }
    """.trimIndent()

    @Before
    fun setUp() {
        AutoVodRouter.resetForTesting()
        AutoVodRouter.setSynchronousRaceForTesting(true)
        AutoVodRouter.setRandomProviderForTesting { 0.99 } // disable random exploration in deterministic tests
    }

    @After
    fun tearDown() {
        AutoVodRouter.resetForTesting()
    }

    @Test
    fun testFragmentIdentityAndRangeParsing() {
        assertEquals("42231991605-1-30102.m4s", AutoVodRouter.fileKey("https://$COSOV$VIDEO_PATH?x=1"))
        assertEquals("42231991605-1-30102.m4s", AutoVodRouter.fileKey("https://$AKAM$VIDEO_PATH?hdnts=1"))
        assertNull(AutoVodRouter.fileKey("https://api.bilibili.com/x/player/playurl?x=1"))
        assertEquals("42242147914", AutoVodRouter.cidOf("42242147914_qe1-1-30032.m4s"))
        assertEquals("100027", AutoVodRouter.repIdOf("42231991605-1-100027.m4s"))

        val range = AutoVodRouter.parseRange("bytes=100-199")
        assertNotNull(range)
        assertEquals(100L, range!!.start)
        assertEquals(199L, range.end)
        assertEquals(100L, range.length)

        val openRange = AutoVodRouter.parseRange("bytes=100-")
        assertNotNull(openRange)
        assertEquals(100L, openRange!!.start)
        assertNull(openRange.end)
        assertNull(openRange.length)
        assertNull(AutoVodRouter.parseRange(null))
    }

    @Test
    fun testSessionTableUrlForAndCandidatesFor() {
        val table = AutoVodRouter.buildTableFromJson(samplePlayurlJson())
        assertNotNull(table)
        assertEquals("42231991605", table!!.cid)

        val videoRep = table.reps["42231991605-1-30102.m4s"]!!
        assertEquals("video", videoRep.kind)
        assertEquals(3753786L, videoRep.bandwidth)
        // mountaintoys.cn backup with xy_usource=ALI is recovered as ALI
        assertEquals(listOf(COSOV, AKAM, ALI), videoRep.issued)
        assertTrue(videoRep.urls[AKAM]!!.contains("hdnts=b"))

        // urlFor uses issued URL as-is (including Akamai) and swaps only onto UPOS mirrors using UPOS signature
        assertEquals(videoRep.urls[AKAM], AutoVodRouter.urlFor(videoRep, AKAM))
        val hwUrl = AutoVodRouter.urlFor(videoRep, HW)
        assertNotNull(hwUrl)
        assertEquals(HW, AutoVodRouter.hostOf(hwUrl))
        assertTrue("Host swap must borrow UPOS signature, never Akamai hdnts", hwUrl!!.contains("os=cosovbv"))
        assertNull(
            "Unissued Akamai host must never be synthesized via UPOS swap",
            AutoVodRouter.urlFor(videoRep, "upos-sz-mirrorakam.akamaized.net")
        )

        val candidates = AutoVodRouter.candidatesFor(videoRep, listOf(COSOV, HW, AKAM.uppercase(), TFHW))
        assertEquals(listOf(COSOV, AKAM, ALI, HW, TFHW), candidates)
    }

    @Test
    fun testShakaDualHalfLifeEwmaEstimatorAndRecentRate() {
        val est = AutoVodRouter.createEstimator()
        assertFalse("8 KB sample below 16 KB minimum must be ignored", est.sample(50L, 8000L))
        assertNull(est.estimate())

        assertTrue(est.sample(1000L, 100_000L))
        assertNull("100 KB total is below 128 KB minimum threshold", est.estimate())

        assertTrue(est.sample(1000L, 100_000L))
        val initialEst = est.estimate()
        assertNotNull(initialEst)
        assertTrue("Two 100 KB/s samples should estimate ~800 kbps", abs(initialEst!! - 800_000.0) < 10.0)

        // Fast drop (2s half-life) and slow recovery (5s half-life)
        val fastEst = AutoVodRouter.createEstimator()
        repeat(10) {
            fastEst.sample(250L, 1_250_000L) // 40 Mbps
        }
        val high = fastEst.estimate()!!
        fastEst.sample(4000L, 500_000L) // 4s slow fragment at 1 Mbps
        val afterDrop = fastEst.estimate()!!
        assertTrue("One slow 4s fragment must pull dual-EWMA estimate below 50% of high", afterDrop < high * 0.5)

        fastEst.sample(250L, 1_250_000L) // Single fast 250ms fragment
        val afterSingleFast = fastEst.estimate()!!
        assertTrue("Single fast fragment must not immediately undo slow EWMA drop", afterSingleFast < high * 0.6)

        // recentRate sliding window
        val samples = listOf(
            AutoVodRouter.ProgressSample(0L, 0L),
            AutoVodRouter.ProgressSample(500L, 500_000L),
            AutoVodRouter.ProgressSample(1500L, 525_000L),
            AutoVodRouter.ProgressSample(2000L, 537_500L)
        )
        assertEquals(200_000L, kotlin.math.round(AutoVodRouter.recentRate(samples, 2000L, 1000L)).toLong())
    }

    @Test
    fun testStuckAndShortfallDetection() {
        // 1. No first byte stuck verdict
        val noFirstByteReq = AutoVodRouter.InflightRequestSnapshot(
            total = 1_500_000L,
            loaded = 0L,
            startedAt = 0L,
            firstByteAt = null
        )
        assertNull("Not stuck before 1000ms", AutoVodRouter.stuckVerdict(noFirstByteReq, 900L, 3.75e6, 0.0))
        assertEquals(
            "no-first-byte",
            AutoVodRouter.stuckVerdict(noFirstByteReq, 1000L, 3.75e6, 0.0)?.reason
        )
        assertNull(
            "With 20s buffer ahead, 1s wait is not urgent",
            AutoVodRouter.stuckVerdict(noFirstByteReq, 1000L, 3.75e6, 20.0)
        )

        // 2. Slow fragment stuck verdict
        val slowReq = AutoVodRouter.InflightRequestSnapshot(
            total = 1_500_000L,
            loaded = 82_500L,
            startedAt = 0L,
            firstByteAt = 100L,
            rateBps = 1.1e6
        )
        val slowVerdict = AutoVodRouter.stuckVerdict(slowReq, 700L, 3.75e6, 0.0)
        assertNotNull(slowVerdict)
        assertEquals("slow-fragment", slowVerdict!!.reason)
        assertTrue(slowVerdict.remainingMs > 10_000.0)
        assertNull("60s buffer outlasts slow fragment", AutoVodRouter.stuckVerdict(slowReq, 700L, 3.75e6, 60.0))

        // 3. Evaluate priority: errors -> stuck -> shortfall
        val baseInput = AutoVodRouter.EvaluationInput(
            nowMs = 10_000L,
            requiredBps = 3.75e6,
            bufferAheadS = 10.0,
            inflight = emptyList(),
            estimateBps = 20e6,
            measuredBytes = 8_000_000L,
            measuredMs = 10_000L,
            recentErrors = 0
        )
        assertNull("Healthy 20 Mbps host on 3.75 Mbps stream needs no race", AutoVodRouter.evaluate(baseInput))
        assertEquals("errors", AutoVodRouter.evaluate(baseInput.copy(recentErrors = 2))?.trigger)
        assertEquals("shortfall", AutoVodRouter.evaluate(baseInput.copy(estimateBps = 4.0e6))?.trigger)
        assertNull(
            "Shortfall with 45s buffer does not race yet",
            AutoVodRouter.evaluate(baseInput.copy(estimateBps = 4.0e6, bufferAheadS = 45.0))
        )
        assertNull(
            "Shortfall requires >= 4MB or >= 8s of evidence",
            AutoVodRouter.evaluate(baseInput.copy(estimateBps = 4.0e6, measuredBytes = 1_000_000L, measuredMs = 3000L))
        )
    }

    @Test
    fun testPickChallengersAndSwitchGainVerdicts() {
        val history = AutoVodRouter.SpeedHistory(
            linkedMapOf(
                HW to AutoVodRouter.HostHistoryRecord(mbps = 20.0, n = 5.0, at = 0L),
                ALI to AutoVodRouter.HostHistoryRecord(mbps = 9.0, n = 5.0, at = 0L),
                TFHW to AutoVodRouter.HostHistoryRecord(mbps = 14.0, n = 5.0, at = 0L)
            )
        )
        // Unmeasured issued alternative (AKAM) comes first, then highest history (HW)
        val picks1 = AutoVodRouter.pickChallengers(
            AutoVodRouter.PickChallengersInput(
                candidates = listOf(COSOV, AKAM, HW, ALI, TFHW),
                current = COSOV,
                issued = listOf(COSOV, AKAM),
                measured = emptySet(),
                failed = emptyMap(),
                lost = emptyMap(),
                history = history,
                nowMs = 1000L,
                random = { 0.99 }
            )
        )
        assertEquals(listOf(AKAM, HW), picks1)

        // Once AKAM is measured and HW recently lost a race (within 60s), picks [TFHW, ALI]
        val picks2 = AutoVodRouter.pickChallengers(
            AutoVodRouter.PickChallengersInput(
                candidates = listOf(COSOV, AKAM, HW, ALI, TFHW),
                current = COSOV,
                issued = listOf(COSOV, AKAM),
                measured = setOf(AKAM),
                failed = emptyMap(),
                lost = mapOf(HW to 900L),
                history = history,
                nowMs = 1000L,
                random = { 0.99 }
            )
        )
        assertEquals(listOf(TFHW, ALI), picks2)

        // 1.5x SWITCH_GAIN verification in raceVerdict
        val raceResults = listOf(
            AutoVodRouter.RaceContenderResult(host = HW, ok = true, ms = 900L, bytes = AutoVodRouter.RACE_BYTES),
            AutoVodRouter.RaceContenderResult(host = AKAM, ok = true, ms = 2500L, bytes = AutoVodRouter.RACE_BYTES)
        )
        // Current host at 1.1 Mbps needs ~5719 ms for 768 KB; 900 ms * 1.5 = 1350 ms <= 5719 ms -> switches to HW
        val verdictSlowCurrent = AutoVodRouter.raceVerdict(raceResults, 1.1e6, AutoVodRouter.RACE_BYTES)
        assertEquals(HW, verdictSlowCurrent.winner)
        assertEquals(AKAM, verdictSlowCurrent.runnerUp)
        assertEquals(HW, verdictSlowCurrent.switchTo)

        // Current host at 5.5 Mbps needs ~1144 ms for 768 KB; 900 ms * 1.5 = 1350 ms > 1144 ms (< 1.5x gain) -> NO switch
        val verdictMarginal = AutoVodRouter.raceVerdict(raceResults, 5.5e6, AutoVodRouter.RACE_BYTES)
        assertEquals(HW, verdictMarginal.winner)
        assertNull("Must NOT switch when winner gain is below 1.5x", verdictMarginal.switchTo)

        // Stuck race verdict: single hang on a 15 Mbps host only detours the stuck fragment (retryOn)
        val singleHang = AutoVodRouter.stuckRaceVerdict(
            results = listOf(AutoVodRouter.RaceContenderResult(host = TFHW, ok = true, ms = 2200L)),
            stuckRateBps = 0.0,
            sustainedBps = 15e6,
            recentErrors = 0,
            raceBytes = AutoVodRouter.RACE_BYTES
        )
        assertNull("Single hang on fast host must not switch whole video", singleHang.switchTo)
        assertEquals(TFHW, singleHang.retryOn)

        // Second hang within error window (recentErrors = 1) gives no credit to past rate and switches whole video
        val secondHang = AutoVodRouter.stuckRaceVerdict(
            results = listOf(AutoVodRouter.RaceContenderResult(host = TFHW, ok = true, ms = 2200L)),
            stuckRateBps = 0.0,
            sustainedBps = 15e6,
            recentErrors = 1,
            raceBytes = AutoVodRouter.RACE_BYTES
        )
        assertEquals(TFHW, secondHang.switchTo)
        assertNull(secondHang.retryOn)
    }

    @Test
    fun testRegionalSpeedHistoryDecayAndFileStorePersistence() {
        val tempDir = Files.createTempDirectory("auto_vod_history_test").toFile()
        try {
            val dayMs = 24L * 60L * 60L * 1000L
            var h = AutoVodRouter.recordSample(null, HW, 40.0, 0L)
            h = AutoVodRouter.recordSample(h, HW, 40.0, 1000L)
            // 9 days (3 half-lives) later, a fresh 5 Mbps sample outweighs stale 40 Mbps samples
            h = AutoVodRouter.recordSample(h, HW, 5.0, 9L * dayMs)
            val decayedScore = AutoVodRouter.historyScore(h, HW)!!
            assertTrue("Fresh 5 Mbps sample after 3 half-lives must pull score below 15 Mbps (got $decayedScore)", decayedScore < 15.0)

            h = AutoVodRouter.recordFailure(h, HW, 9L * dayMs)
            assertTrue(AutoVodRouter.recentlyFailed(h, HW, 9L * dayMs + 1000L))
            assertFalse("Failure penalty expires after 1 hour", AutoVodRouter.recentlyFailed(h, HW, 9L * dayMs + 3_600_001L))

            // FileSpeedHistoryStore persistence round-trip
            val store = AutoVodRouter.FileSpeedHistoryStore(tempDir)
            val region = "America/Los_Angeles|en-US"
            store.save(region, h)

            val reloaded = store.load(region)
            assertNotNull(AutoVodRouter.historyScore(reloaded, HW))
            assertEquals(decayedScore, AutoVodRouter.historyScore(reloaded, HW)!!, 0.001)
            assertTrue(AutoVodRouter.recentlyFailed(reloaded, HW, 9L * dayMs + 1000L))
        } finally {
            tempDir.deleteRecursively()
        }
    }

    @Test
    fun testEndToEndAutoModePreservesHealthyCdnAndSwitchesAfter768KbRaceWin() {
        val racedUrls = java.util.Collections.synchronizedList(mutableListOf<Pair<String, String>>())
        AutoVodRouter.configureRangeFetcher { url, startByte, endByte, _ ->
            val host = AutoVodRouter.hostOf(url)
            racedUrls.add(host to "bytes=$startByte-$endByte")
            when (host) {
                ALI -> AutoVodRouter.RangeFetchResponse(status = 206, bytes = AutoVodRouter.RACE_BYTES, durationMs = 600L)
                AKAM -> AutoVodRouter.RangeFetchResponse(status = 206, bytes = AutoVodRouter.RACE_BYTES, durationMs = 1100L)
                else -> AutoVodRouter.RangeFetchResponse(status = 206, bytes = AutoVodRouter.RACE_BYTES, durationMs = 900L)
            }
        }

        AutoVodRouter.ingestPlayinfoJson(samplePlayurlJson(), autoNonBtrConfig)
        val initialCosovUrl = "https://$COSOV$VIDEO_PATH?os=cosovbv&upsig=a"

        // 1. In "auto" mode, healthy official CDN (COSOV) is NOT blindly rewritten to targetHost (COS)
        val initialSelect = AutoVodRouter.selectVodUrl(
            originalUrl = initialCosovUrl,
            config = autoNonBtrConfig,
            nowMs = 1000L
        )
        assertFalse("Healthy official CDN must not be blindly replaced in auto mode", initialSelect.changed)
        assertEquals(initialCosovUrl, initialSelect.finalUrl)

        // 2. Simulate sustained shortfall on COSOV (~2.4 Mbps on a 3.75 Mbps stream over 9s / 2.7 MB, buffer 8s)
        for (step in 1..3) {
            AutoVodRouter.reportTransferSample(
                urlOrHost = initialCosovUrl,
                durationMs = 3000L,
                bytes = 900_000L, // 2.4 Mbps
                requiredBps = 3_753_786.0,
                bufferAheadS = 8.0,
                config = autoNonBtrConfig,
                nowMs = 1000L + step * 3000L
            )
        }

        // Verify a 2-candidate 768KB HTTP range race was triggered and won by ALI (600ms vs ~2621ms current -> >4x gain >= 1.5x)
        assertEquals("Exactly 2 candidate hosts must be raced", 2, racedUrls.size)
        assertTrue(
            "Each race request must fetch 768KB (786432 bytes)",
            racedUrls.all { (_, range) ->
                val spec = AutoVodRouter.parseRange(range)
                spec?.length == AutoVodRouter.RACE_BYTES.toLong()
            }
        )
        val session = AutoVodRouter.getCurrentSessionForTesting()
        assertNotNull(session)
        assertEquals(ALI, session!!.activeHost)
        assertEquals(1, session.switches.size)
        assertEquals("shortfall", session.switches[0].trigger)

        // 3. Subsequent selectVodUrl calls automatically route to the race winner (ALI)
        val afterRaceSelect = AutoVodRouter.selectVodUrl(
            originalUrl = initialCosovUrl,
            config = autoNonBtrConfig,
            nowMs = 11_000L
        )
        assertTrue("After race win, selectVodUrl must switch to winning host", afterRaceSelect.changed)
        assertEquals(ALI, afterRaceSelect.targetHost)
        assertTrue(afterRaceSelect.finalUrl.startsWith("https://$ALI$VIDEO_PATH"))

        // 4. In "manual" mode, shouldUseAutoRouting is false and forceUpos replaces directly to static targetHost without racing
        val manualConfig = autoNonBtrConfig.copy(selectionMode = "manual", forceUpos = true, targetHost = HW)
        assertFalse(AutoVodRouter.shouldUseAutoRouting(manualConfig))
        val manualSelect = AutoVodRouter.selectVodUrl(
            originalUrl = initialCosovUrl,
            config = manualConfig,
            nowMs = 12_000L
        )
        assertTrue(manualSelect.changed)
        assertEquals(HW, manualSelect.targetHost)
        assertEquals("force-upos", manualSelect.reason)
    }

    @Test
    fun testConcurrentRaceEarlyStopAbortsSlowContenderAndUsesFreshTimestamp() {
        val fakeClock = java.util.concurrent.atomic.AtomicLong(10_000L)
        AutoVodRouter.setClockForTesting { fakeClock.get() }
        AutoVodRouter.setSynchronousRaceForTesting(true)
        AutoVodRouter.setRaceCutGraceMsForTesting(30L)

        val bothStarted = java.util.concurrent.CountDownLatch(2)
        val slowAborted = java.util.concurrent.CountDownLatch(1)
        val slowSettled = java.util.concurrent.CountDownLatch(1)
        val slowHostRef = java.util.concurrent.atomic.AtomicReference<String>()
        val listenerEvents = java.util.Collections.synchronizedList(mutableListOf<Triple<String, String, String?>>())
        AutoVodRouter.addActiveHostListener { cid, oldHost, newHost, newUrl, _ ->
            listenerEvents.add(Triple(cid, "$oldHost->$newHost", newUrl))
        }

        AutoVodRouter.configureRangeFetcher(object : AutoVodRouter.CancellableRangeRaceFetcher {
            override fun fetchRange(
                url: String,
                startByte: Long,
                endByte: Long,
                timeoutMs: Long,
                abortController: AutoVodRouter.RaceAbortController,
                onProgress: (Int) -> Unit
            ): AutoVodRouter.RangeFetchResponse {
                val host = AutoVodRouter.hostOf(url)
                bothStarted.countDown()
                assertTrue("Contenders must start concurrently", bothStarted.await(2, java.util.concurrent.TimeUnit.SECONDS))
                return if (host == AKAM) {
                    // Simulate 3.5s elapsing during the race on the fast contender (still faster than 0-rate stuck host)
                    fakeClock.set(13_500L)
                    onProgress(AutoVodRouter.RACE_BYTES)
                    AutoVodRouter.RangeFetchResponse(status = 206, bytes = AutoVodRouter.RACE_BYTES, durationMs = 2000L)
                } else {
                    slowHostRef.set(host)
                    onProgress(128 * 1024)
                    abortController.addOnAbort { slowAborted.countDown() }
                    assertTrue("Slow contender must be aborted after cut grace", slowAborted.await(2, java.util.concurrent.TimeUnit.SECONDS))
                    slowSettled.countDown()
                    AutoVodRouter.RangeFetchResponse(status = 206, bytes = 128 * 1024, durationMs = 2050L)
                }
            }
        })

        AutoVodRouter.ingestPlayinfoJson(samplePlayurlJson(), autoNonBtrConfig)
        val initialCosovUrl = "https://$COSOV$VIDEO_PATH?os=cosovbv&upsig=a"
        AutoVodRouter.selectVodUrl(initialCosovUrl, config = autoNonBtrConfig, nowMs = 10_000L)

        // Establish high sustained speed (15 Mbps) on COSOV so a single stuck fragment creates a detour (retryOn)
        for (i in 1..3) {
            AutoVodRouter.reportTransferSample(
                urlOrHost = initialCosovUrl,
                durationMs = 1000L,
                bytes = 1_875_000L, // 15 Mbps
                requiredBps = 3_753_786.0,
                bufferAheadS = 10.0,
                config = autoNonBtrConfig,
                nowMs = 10_000L
            )
        }

        // Trigger an immediate stuck stall at t=10_000L (clock advances to 13_500L during the race)
        val triggered = AutoVodRouter.reportPlayerStall(
            urlOrHost = initialCosovUrl,
            bufferAheadS = 0.5,
            config = autoNonBtrConfig,
            nowMs = 10_000L,
            immediate = true
        )
        assertTrue(triggered)

        val session = AutoVodRouter.getCurrentSessionForTesting()!!
        // Fresh post-race timestamp (13_500L) must be used so detourUntilMs = 16_500L (NOT 13_000L which would already be expired!)
        assertEquals(AKAM, session.detourHost)
        assertEquals(13_500L + AutoVodRouter.AVOID_MS, session.detourUntilMs)
        assertEquals(13_500L, session.races.last().atMs)
        assertTrue("ActiveHostListener must fire on race detour/switch", listenerEvents.isNotEmpty())

        // Wait for late cut contender to settle and verify it was NOT marked failed (since it was cut, not failed)
        assertTrue(slowSettled.await(2, java.util.concurrent.TimeUnit.SECONDS))
        Thread.sleep(60)
        val slowHost = slowHostRef.get()
        assertNotNull(slowHost)
        assertNull("Cut contender must not be penalized as failed", session.failed[slowHost])
    }

    @Test
    fun testStallGracePeriodCancelledByBufferingEndAndFiresWhenUnresolved() {
        AutoVodRouter.setStallGraceMsForTesting(120L)
        val raceCount = java.util.concurrent.atomic.AtomicInteger(0)
        val raceDone = java.util.concurrent.CountDownLatch(1)
        AutoVodRouter.configureRangeFetcher { _, _, _, _ ->
            raceCount.incrementAndGet()
            raceDone.countDown()
            AutoVodRouter.RangeFetchResponse(status = 206, bytes = AutoVodRouter.RACE_BYTES, durationMs = 400L)
        }

        AutoVodRouter.ingestPlayinfoJson(samplePlayurlJson(), autoNonBtrConfig)
        val initialCosovUrl = "https://$COSOV$VIDEO_PATH?os=cosovbv&upsig=a"
        AutoVodRouter.selectVodUrl(initialCosovUrl, config = autoNonBtrConfig, nowMs = 1000L)

        // 1. 701 followed quickly by 702 cancels the grace timer -> no stall counted, no race triggered
        assertTrue(AutoVodRouter.reportPlayerStall(initialCosovUrl, bufferAheadS = 0.0, config = autoNonBtrConfig, nowMs = 1000L))
        assertTrue(AutoVodRouter.reportPlayerBufferingEnd(initialCosovUrl, nowMs = 1050L))
        Thread.sleep(220)
        val session = AutoVodRouter.getCurrentSessionForTesting()!!
        assertEquals("Transient stall cancelled within grace must not increment stalls", 0, session.stalls)
        assertEquals("Transient stall cancelled within grace must not start a race", 0, raceCount.get())

        // 2. Unresolved 701 past grace period triggers stall and race
        assertTrue(AutoVodRouter.reportPlayerStall(initialCosovUrl, bufferAheadS = 0.0, config = autoNonBtrConfig, nowMs = 2000L))
        assertTrue("Unresolved stall must trigger race after grace period", raceDone.await(2, java.util.concurrent.TimeUnit.SECONDS))
        assertEquals(1, session.stalls)
        assertTrue(raceCount.get() > 0)
    }

    @Test
    fun testBufferingUpdateSemanticsAvoidFalseAlarmsOnLongVideos() {
        val raceCount = java.util.concurrent.atomic.AtomicInteger(0)
        AutoVodRouter.configureRangeFetcher { _, _, _, _ ->
            raceCount.incrementAndGet()
            AutoVodRouter.RangeFetchResponse(status = 206, bytes = AutoVodRouter.RACE_BYTES, durationMs = 400L)
        }

        AutoVodRouter.ingestPlayinfoJson(samplePlayurlJson(), autoNonBtrConfig)
        val initialCosovUrl = "https://$COSOV$VIDEO_PATH?os=cosovbv&upsig=a"
        AutoVodRouter.selectVodUrl(initialCosovUrl, config = autoNonBtrConfig, nowMs = 1000L)

        // 1. Raw percent <= 2 without duration/position must NOT fabricate 0..15s buffer or playerStalling=true
        assertFalse(AutoVodRouter.reportPlayerBufferHunger(percent = 1, urlOrHost = initialCosovUrl, config = autoNonBtrConfig, nowMs = 2000L))
        assertEquals(0, raceCount.get())

        // 2. 1-hour video at t=0 with 2% buffered => 72 seconds of buffer ahead, must NOT false-alarm!
        assertFalse(
            AutoVodRouter.reportPlayerBufferHunger(
                percent = 2,
                urlOrHost = initialCosovUrl,
                config = autoNonBtrConfig,
                nowMs = 3000L,
                currentPositionMs = 0L,
                durationMs = 3_600_000L
            )
        )
        val session = AutoVodRouter.getCurrentSessionForTesting()!!
        assertEquals(72.0, session.lastBufferAheadS, 0.01)
        assertEquals(0, raceCount.get())
    }

    @Test
    fun testBandwidthHintDoesNotInflateEndsAndHttp416DoesNotPoisonHost() {
        AutoVodRouter.ingestPlayinfoJson(samplePlayurlJson(), autoNonBtrConfig)
        val initialCosovUrl = "https://$COSOV$VIDEO_PATH?os=cosovbv&upsig=a"
        AutoVodRouter.selectVodUrl(initialCosovUrl, config = autoNonBtrConfig, nowMs = 1000L)
        val session = AutoVodRouter.getCurrentSessionForTesting()!!
        val key = "42231991605-1-30102.m4s"

        // Calling reportBandwidthHint repeatedly must NOT push session.ends past file size
        repeat(20) { i ->
            AutoVodRouter.reportBandwidthHint(
                kbps = 20_000,
                urlOrHost = initialCosovUrl,
                config = autoNonBtrConfig,
                nowMs = 2000L + i * 1000L
            )
        }
        assertNull("reportBandwidthHint must not blindly accumulate session.ends", session.ends[key])

        // Simulate a real byte offset that turns out to exceed file length -> HTTP 416 must reset ends and not fail host
        session.ends[key] = 500_000_000L
        AutoVodRouter.configureRangeFetcher { _, _, _, _ ->
            AutoVodRouter.RangeFetchResponse(status = 416, bytes = 0, durationMs = 50L)
        }
        assertTrue(AutoVodRouter.triggerManualRetest(autoNonBtrConfig, nowMs = 30_000L))
        assertEquals("HTTP 416 must reset probe offset to -1", -1L, session.ends[key])
        assertTrue("HTTP 416 must not mark challenger hosts as failed", session.failed.isEmpty())
    }

    @Test
    fun testNeverFabricatesUnsignedFakeUrlsInRace() {
        val fetchedUrls = java.util.Collections.synchronizedList(mutableListOf<String>())
        AutoVodRouter.configureRangeFetcher { url, _, _, _ ->
            fetchedUrls.add(url)
            AutoVodRouter.RangeFetchResponse(status = 403, bytes = 0, durationMs = 50L)
        }
        // Session with assignedHost but no real signed URLs in reps
        AutoVodRouter.reportTransferSample(
            urlOrHost = COSOV,
            durationMs = 10_000L,
            bytes = 1_000_000L,
            requiredBps = 3_750_000.0,
            bufferAheadS = 2.0,
            config = autoNonBtrConfig,
            nowMs = 10_000L
        )
        assertTrue("Router must NEVER fabricate unsigned fake /upgcxcode/00/00/ URLs", fetchedUrls.isEmpty())
    }

    @Test
    fun testMcdnPrimaryPromotesCleanOfficialBackupBeforeProxyAndDoesNotPolluteIssuedOrCurrentSession() {
        val mcdnUrl = "https://xy1x2x3x4xy.mcdn.bilivideo.cn$VIDEO_PATH?os=mcdn&deadline=1899999999"
        val cleanAli = "https://$ALI$VIDEO_PATH?os=upos&deadline=1899999999"

        // 1. MCDN primary + clean official backup promotes cleanAli BEFORE mcdn-proxy
        val promoted = AutoVodRouter.selectVodUrl(
            originalUrl = mcdnUrl,
            rawBackups = listOf(cleanAli),
            config = autoNonBtrConfig,
            nowMs = 1000L
        )
        assertTrue(promoted.changed)
        assertEquals("pcdn-promote", promoted.reason)
        assertEquals(cleanAli, promoted.finalUrl)
        assertFalse(promoted.isMcdn)

        // 2. PCDN primary without clean backup rewrites to targetHost (COS), but COS must NOT be registered in rep.issued
        val pcdnUrl = "http://112.34.56.78:8080/upgcxcode/11/22/77777777-1-30080.m4s?deadline=1899999999"
        val pcdnRes = AutoVodRouter.selectVodUrl(
            originalUrl = pcdnUrl,
            rawBackups = emptyList(),
            config = autoNonBtrConfig,
            nowMs = 2000L
        )
        assertTrue(pcdnRes.changed)
        assertEquals(COS, pcdnRes.targetHost)
        val pcdnSession = AutoVodRouter.getSessionForTesting("77777777")!!
        val pcdnRep = pcdnSession.table.reps["77777777-1-30080.m4s"]!!
        assertFalse("Rewritten targetHost must NOT be registered into rep.issued", pcdnRep.issued.contains(COS))

        // 3. Audio track of another CID must NOT overwrite currentSession
        val prevCurrentCid = AutoVodRouter.getCurrentSessionForTesting()?.cid
        assertEquals("77777777", prevCurrentCid)
        val otherAudioUrl = "https://$COSOV/upgcxcode/99/88/88888888-1-30280.m4s?deadline=1899999999"
        AutoVodRouter.selectVodUrl(
            originalUrl = otherAudioUrl,
            config = autoNonBtrConfig,
            nowMs = 3000L
        )
        assertEquals("Audio track must NOT overwrite currentSession", "77777777", AutoVodRouter.getCurrentSessionForTesting()?.cid)
    }

    @Test
    fun testIngestPlayinfoTracksSetsRealBandwidthLowerThanDefault() {
        val lowBitrateUrl = "https://$COSOV/upgcxcode/01/02/55555555-1-30032.m4s?deadline=1899999999"
        val entry = PlayinfoTrackEntry(
            pathKey = "/upgcxcode/01/02/55555555-1-30032.m4s",
            fileKey = "55555555-1-30032.m4s",
            primaryUrl = lowBitrateUrl,
            backupUrls = listOf("https://$ALI/upgcxcode/01/02/55555555-1-30032.m4s?deadline=1899999999"),
            deadlineEpochSec = 1899999999L,
            isAudio = false,
            hasDrm = false,
            isPreview = false,
            isEpisode = false,
            cid = 55555555L,
            bandwidth = 1_500_000L
        )
        AutoVodRouter.ingestPlayinfoTracks(listOf(entry), autoNonBtrConfig)
        AutoVodRouter.selectVodUrl(lowBitrateUrl, playinfoTrack = entry, config = autoNonBtrConfig, nowMs = 1000L)

        val session = AutoVodRouter.getSessionForTesting("55555555")!!
        assertEquals(1_500_000.0, session.requiredBps, 0.01)
    }
}
