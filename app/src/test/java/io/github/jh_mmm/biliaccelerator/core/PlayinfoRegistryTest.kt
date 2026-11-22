package io.github.jh_mmm.biliaccelerator.core

import io.github.jh_mmm.biliaccelerator.btr.BtrSessionManager
import io.github.jh_mmm.biliaccelerator.hook.RemoteClient
import org.junit.After
import org.junit.Assert.assertEquals
import org.junit.Assert.assertFalse
import org.junit.Assert.assertNotNull
import org.junit.Assert.assertNull
import org.junit.Assert.assertTrue
import org.junit.Test
import java.nio.file.Files
import java.util.concurrent.atomic.AtomicInteger

class PlayinfoRegistryTest {

    @After
    fun tearDown() {
        RemoteClient.setCachedConfigForTesting(AcceleratorConfig(enabled = true), 0L)
        PlayinfoRegistry.resetForTesting(clearDir = true)
        AutoVodRouter.resetForTesting()
        BtrSessionManager.resetDefaultRefreshProvider()
    }

    // ---- Issue 3: Bandwidth parsing (JSON + Protobuf + shared persistence + AutoVodRouter) ----

    private class ProtoDashVideo(
        private val baseUrl: String,
        private val backupUrls: List<String> = emptyList(),
        private val bandwidth: Long = 0L
    ) {
        fun getBaseUrl(): String = baseUrl
        fun getBackupUrlList(): List<String> = backupUrls
        fun getBandwidth(): Long = bandwidth
    }

    private class ProtoPlayViewWithBandwidth(
        private val cid: Long,
        private val video: ProtoDashVideo,
        private val audio: ProtoDashVideo
    ) {
        fun getCid(): Long = cid
        fun getDashVideo(): ProtoDashVideo = video
        fun getDashAudio(): ProtoDashVideo = audio
    }

    @Test
    fun testBandwidthParsedFromJsonAndProtobufAndPersistedCrossProcess() {
        val tempDir = Files.createTempDirectory("playinfo_bw_test").toFile()
        try {
            PlayinfoRegistry.configureSharedStateDir(tempDir)
            val json = """
                {
                  "code": 0,
                  "data": {
                    "cid": 55667788,
                    "dash": {
                      "video": [
                        {
                          "id": 80,
                          "bandwidth": 8450000,
                          "baseUrl": "https://upos-sz-mirrorcos.bilivideo.com/upgcxcode/55/66/55667788/55667788-1-30080.m4s?deadline=1900000100"
                        }
                      ],
                      "audio": [
                        {
                          "id": 30280,
                          "bandwidth": 192000,
                          "baseUrl": "https://upos-sz-mirrorcos.bilivideo.com/upgcxcode/55/66/55667788/55667788-1-30280.m4s?deadline=1900000100"
                        }
                      ]
                    }
                  }
                }
            """.trimIndent()
            val entries = PlayinfoRegistry.ingestJson(json)
            assertEquals(2, entries.size)
            val videoUrl = "https://upos-sz-mirrorcos.bilivideo.com/upgcxcode/55/66/55667788/55667788-1-30080.m4s"
            val audioUrl = "https://upos-sz-mirrorcos.bilivideo.com/upgcxcode/55/66/55667788/55667788-1-30280.m4s"
            assertEquals(8_450_000L, PlayinfoRegistry.lookup(videoUrl)!!.bandwidth)
            assertEquals(55667788L, PlayinfoRegistry.lookup(videoUrl)!!.cid)
            assertEquals(192_000L, PlayinfoRegistry.lookup(audioUrl)!!.bandwidth)

            // AutoVodRouter should have ingested real video bandwidth (> default 3.75 Mbps)
            val autoSession = AutoVodRouter.getSessionForTesting("55667788")
            assertNotNull("ingestJson must feed AutoVodRouter session", autoSession)
            assertEquals(8_450_000.0, autoSession!!.requiredBps, 1.0)

            // Cross-process disk reload preserves bandwidth & cid
            PlayinfoRegistry.resetForTesting(clearDir = false)
            val reloaded = PlayinfoRegistry.lookup(videoUrl)!!
            assertEquals(8_450_000L, reloaded.bandwidth)
            assertEquals(55667788L, reloaded.cid)

            // Protobuf DASH bandwidth extraction + AutoVodRouter ingestion
            val pbVideoUrl = "https://upos-sz-mirrorcos.bilivideo.com/upgcxcode/77/88/77889900/77889900-1-30112.m4s?deadline=1900000200"
            val pbAudioUrl = "https://upos-sz-mirrorcos.bilivideo.com/upgcxcode/77/88/77889900/77889900-1-30280.m4s?deadline=1900000200"
            val reply = ProtoPlayViewWithBandwidth(
                cid = 77889900L,
                video = ProtoDashVideo(baseUrl = pbVideoUrl, bandwidth = 12_340_000L),
                audio = ProtoDashVideo(baseUrl = pbAudioUrl, bandwidth = 320_000L)
            )
            val pbEntries = PlayinfoRegistry.ingestPlayViewReply(reply, "grpc-blocking")
            assertEquals(2, pbEntries.size)
            assertEquals(12_340_000L, PlayinfoRegistry.lookup(pbVideoUrl)!!.bandwidth)
            assertEquals(320_000L, PlayinfoRegistry.lookup(pbAudioUrl)!!.bandwidth)
            assertEquals(77889900L, PlayinfoRegistry.lookup(pbVideoUrl)!!.cid)
        } finally {
            tempDir.deleteRecursively()
        }
    }

    // ---- Issue 4: Custom proxyHost unwrapping & scheduler /v1/resource/ exclusion & IPv6 live ----

    @Test
    fun testCustomProxyHostLookupAndSchedulerV1ResourceExclusion() {
        RemoteClient.setCachedConfigForTesting(
            AcceleratorConfig(enabled = true, proxyHost = "custom-relay.bilivideo.com")
        )
        val innerUrl = "https://xy123.mcdn.bilivideo.cn/v1/resource/900123-1-30080.m4s?deadline=1900000500"
        val json = """
            {
              "code": 0,
              "data": {
                "dash": {
                  "video": [
                    {
                      "id": 80,
                      "baseUrl": "$innerUrl",
                      "is_drm": true
                    }
                  ]
                }
              }
            }
        """.trimIndent()
        assertTrue(PlayinfoRegistry.ingestJson(json).isNotEmpty())

        val wrappedWithCustomProxy =
            "https://custom-relay.bilivideo.com/?url=" + java.net.URLEncoder.encode(innerUrl, "UTF-8")
        val found = PlayinfoRegistry.lookup(wrappedWithCustomProxy)
        assertNotNull("Custom configured proxyHost wrapper must unwrap for PlayinfoRegistry lookup", found)
        assertTrue(found!!.hasDrm)

        // Scheduler /v1/resource/ MUST NOT be unwrapped
        assertNull(
            "szbdyd /v1/resource/ must be excluded from scheduler unwrapping",
            PlayinfoRegistry.recoverSchedulerSourceUrl(
                "https://xy123.szbdyd.com/v1/resource/900123-1-30080.m4s?xy_usource=upos-sz-mirrorcos.bilivideo.com"
            )
        )
        assertNull(
            "mountaintoys /v1/resource/ must be excluded from scheduler unwrapping",
            PlayinfoRegistry.recoverSchedulerSourceUrl(
                "https://xy123.mountaintoys.cn/v1/resource/900123-1-30080.m4s?xy_usource=upos-sz-mirrorcos.bilivideo.com"
            )
        )
        // Normal /upgcxcode/ scheduler URL still unwraps
        assertEquals(
            "https://upos-sz-mirrorcos.bilivideo.com/upgcxcode/1/2/3.m4s?xy_usource=upos-sz-mirrorcos.bilivideo.com",
            PlayinfoRegistry.recoverSchedulerSourceUrl(
                "https://xy123.mountaintoys.cn/upgcxcode/1/2/3.m4s?xy_usource=upos-sz-mirrorcos.bilivideo.com"
            )
        )

        // IPv6 PCDN live URL must be filtered out without substringBefore(':') truncation
        PlayinfoRegistry.ingestLiveGroups(
            listOf(
                listOf(
                    "https://[240e:1234::1]/live-bvc/999/index.m3u8?expires=9",
                    "https://d1--cn-gotcha208.bilivideo.com/live-bvc/999/index.m3u8?expires=9",
                    "https://d1--cn-gotcha204.bilivideo.com/live-bvc/999/index.m3u8?expires=9"
                )
            )
        )
        val liveBackups = PlayinfoRegistry.lookupLiveBackups(
            "https://d1--cn-gotcha208.bilivideo.com/live-bvc/999/index.m3u8?expires=9"
        )
        assertEquals(
            listOf("https://d1--cn-gotcha204.bilivideo.com/live-bvc/999/index.m3u8?expires=9"),
            liveBackups
        )
    }

    // ---- Issue 15: Cross-process refresh bridge + defaultRefreshProvider + gRPC refresher priority ----

    @Test
    fun testCrossProcessRefreshBridgeAndBtrDefaultRefreshProvider() {
        val nowSec = System.currentTimeMillis() / 1000L
        val oldDeadline = nowSec + 60L
        val newDeadline = nowSec + 7200L
        val oldUrl = "https://upos-sz-mirrorcos.bilivideo.com/upgcxcode/33/44/334455/334455-1-30080.m4s?deadline=$oldDeadline&sign=old"
        val freshUrl = "https://upos-sz-mirrorcos.bilivideo.com/upgcxcode/33/44/334455/334455-1-30080.m4s?deadline=$newDeadline&sign=fresh"
        val freshBackup = "https://upos-sz-mirrorali.bilivideo.com/upgcxcode/33/44/334455/334455-1-30080.m4s?deadline=$newDeadline&sign=fresh"

        // Simulate Process A (main process) holding the refresher and handling incoming cross-process requests
        val refreshInvocations = AtomicInteger(0)
        PlayinfoRegistry.crossProcessRefreshBridge = { req ->
            refreshInvocations.incrementAndGet()
            assertEquals("/upgcxcode/33/44/334455/334455-1-30080.m4s", req.pathKey)
            assertEquals(oldDeadline, req.currentDeadlineSec)
            CrossProcessRefreshResult(
                success = true,
                primaryUrl = freshUrl,
                backupUrls = listOf(freshBackup),
                deadlineEpochSec = newDeadline
            )
        }

        // Process B (:ijkservice) creates a BTR session with NO local refreshProvider
        val sid = BtrSessionManager.register(primaryUrl = oldUrl)
        try {
            val session = BtrSessionManager.get(sid)!!
            assertNull("Child process must have no local refreshProvider", session.refreshProvider)
            assertNotNull("BtrSessionManager.defaultRefreshProvider must be wired", BtrSessionManager.defaultRefreshProvider)

            val refreshed = BtrSessionManager.maybeTriggerProactiveRefresh(session, forceOnAuthFailure = false)
            assertTrue("Cross-process proactive refresh must succeed via defaultRefreshProvider", refreshed)
            assertEquals(1, refreshInvocations.get())
            assertEquals(newDeadline, session.urlDeadlineEpochSec)
            assertEquals(freshUrl, session.primaryUrl)
            assertTrue(session.backupUrls.contains(freshBackup))
            assertEquals(newDeadline, PlayinfoRegistry.lookup(oldUrl)!!.deadlineEpochSec)
        } finally {
            BtrSessionManager.remove(sid)
        }
    }

    @Test
    fun testIncomingCrossProcessRequestAndGrpcOverHttpRefresherPriority() {
        val nowSec = System.currentTimeMillis() / 1000L
        val oldDeadline = nowSec + 100L
        val newDeadline = nowSec + 5000L
        val oldUrl = "https://upos-sz-mirrorcos.bilivideo.com/upgcxcode/88/99/889900/889900-1-30080.m4s?deadline=$oldDeadline"
        val freshUrl = "https://upos-sz-mirrorcos.bilivideo.com/upgcxcode/88/99/889900/889900-1-30080.m4s?deadline=$newDeadline"

        val grpcCalls = AtomicInteger(0)
        val httpCalls = AtomicInteger(0)

        val grpcReply = ProtoPlayViewWithBandwidth(
            cid = 889900L,
            video = ProtoDashVideo(baseUrl = oldUrl, bandwidth = 6_000_000L),
            audio = ProtoDashVideo(baseUrl = "https://upos-sz-mirrorcos.bilivideo.com/upgcxcode/88/99/889900/889900-1-30280.m4s?deadline=$oldDeadline", bandwidth = 128_000L)
        )
        val freshGrpcReply = ProtoPlayViewWithBandwidth(
            cid = 889900L,
            video = ProtoDashVideo(baseUrl = freshUrl, bandwidth = 6_000_000L),
            audio = ProtoDashVideo(baseUrl = "https://upos-sz-mirrorcos.bilivideo.com/upgcxcode/88/99/889900/889900-1-30280.m4s?deadline=$newDeadline", bandwidth = 128_000L)
        )
        PlayinfoRegistry.ingestPlayViewReply(
            grpcReply,
            sourceTag = "grpc-blocking",
            refreshCallback = PlayinfoRefreshCallback {
                grpcCalls.incrementAndGet()
                PlayinfoRegistry.ingestPlayViewReply(freshGrpcReply, sourceTag = "grpc-refresh").isNotEmpty()
            },
            refreshSource = PlayinfoRegistry.RefreshSource.GRPC
        )

        // Subsequent HTTP ingest for the same track must NOT downgrade the gRPC refresher
        val httpJson = """{"code":0,"data":{"cid":889900,"dash":{"video":[{"id":80,"baseUrl":"$oldUrl"}]}}}"""
        PlayinfoRegistry.ingestJson(
            httpJson,
            refreshCallback = PlayinfoRefreshCallback {
                httpCalls.incrementAndGet()
                true
            },
            refreshSource = PlayinfoRegistry.RefreshSource.HTTP
        )

        val result = PlayinfoRegistry.handleIncomingCrossProcessRefreshRequest(
            CrossProcessRefreshRequest(
                requestId = "req-1",
                url = oldUrl,
                pathKey = "/upgcxcode/88/99/889900/889900-1-30080.m4s",
                cid = 889900L,
                epId = 0L,
                currentDeadlineSec = oldDeadline,
                timeoutMs = 5_000L
            )
        )
        assertTrue(result.success)
        assertEquals(freshUrl, result.primaryUrl)
        assertEquals(newDeadline, result.deadlineEpochSec)
        assertEquals("gRPC refresher must be retained over HTTP refresher", 1, grpcCalls.get())
        assertEquals("HTTP refresher must not replace gRPC refresher", 0, httpCalls.get())

        // Without bridge or appContext, requestRefresh for unknown URL safely returns false without crashing
        assertFalse(PlayinfoRegistry.requestRefresh("https://upos-sz-mirrorcos.bilivideo.com/upgcxcode/0/0/unknown.m4s"))
    }

    // ---- Issue 16: Non-sticky isPreview/hasDrm, sniffing keywords, depth 20, obfuscated isPerStreamScope, try_look ----

    @Test
    fun testPreviewAndDrmAreNotStickyAcrossNewerPlayinfoAndSharedPersistence() {
        val tempDir = Files.createTempDirectory("playinfo_nonsticky_test").toFile()
        try {
            PlayinfoRegistry.configureSharedStateDir(tempDir)
            val previewUrl = "https://upos-sz-mirrorcos.bilivideo.com/upgcxcode/12/34/555000.m4s?deadline=1900009000&preview=1"
            val fullUrl = "https://upos-sz-mirrorcos.bilivideo.com/upgcxcode/12/34/555000.m4s?deadline=1900008000"

            val previewJson = """
                {
                  "code": 0,
                  "data": {
                    "is_preview": 1,
                    "is_drm": true,
                    "dash": {
                      "video": [
                        { "id": 80, "baseUrl": "$previewUrl" }
                      ]
                    }
                  }
                }
            """.trimIndent()
            PlayinfoRegistry.ingestJson(previewJson)
            val before = PlayinfoRegistry.lookup(fullUrl)!!
            assertTrue(before.isPreview)
            assertTrue(before.hasDrm)

            // User logs in / upgrades to VIP -> newer playinfo clears is_preview & is_drm immediately
            val fullJson = """
                {
                  "code": 0,
                  "data": {
                    "is_preview": 0,
                    "is_drm": false,
                    "dash": {
                      "video": [
                        { "id": 80, "baseUrl": "$fullUrl" }
                      ]
                    }
                  }
                }
            """.trimIndent()
            PlayinfoRegistry.ingestJson(fullJson)
            val after = PlayinfoRegistry.lookup(fullUrl)!!
            assertFalse("Newer playinfo must immediately clear isPreview", after.isPreview)
            assertFalse("Newer playinfo must immediately clear hasDrm", after.hasDrm)
            assertEquals("Newer non-preview URL must replace old preview URL even if old preview had higher deadline", fullUrl, after.primaryUrl)

            // Reload from shared disk in simulated second process -> must ALSO see cleared flags
            PlayinfoRegistry.resetForTesting(clearDir = false)
            val reloaded = PlayinfoRegistry.lookup(fullUrl)!!
            assertFalse("Shared disk state must not stick old isPreview=true", reloaded.isPreview)
            assertFalse("Shared disk state must not stick old hasDrm=true", reloaded.hasDrm)
            assertEquals(fullUrl, reloaded.primaryUrl)
        } finally {
            tempDir.deleteRecursively()
        }
    }

    @Test
    fun testExpandedSniffingKeywordsAndTryLookNotPreview() {
        val keywords = listOf("mountaintoys", "szbdyd", "nexusedgeio", "ahdohpiechei", "mcdn")
        for (kw in keywords) {
            val json = """{"code":0,"data":{"dash":{"video":[{"id":80,"baseUrl":"https://edge.$kw.example/stream/123456.m4s?deadline=1900000100"}]}}}"""
            assertTrue("PlayinfoRegistry must sniff keyword '$kw'", PlayinfoRegistry.looksLikePlayinfoJson(json))
        }

        // JSON try_look=true / try_look=1 must NOT set isPreview=true
        val tryLookUrl = "https://upos-sz-mirrorcos.bilivideo.com/upgcxcode/12/34/trylook-1080p.m4s?deadline=1900000100"
        val tryLookJson = """
            {
              "code": 0,
              "data": {
                "try_look": true,
                "is_preview": 0,
                "dash": {
                  "video": [
                    { "id": 80, "try_look": 1, "baseUrl": "$tryLookUrl" }
                  ]
                }
              }
            }
        """.trimIndent()
        PlayinfoRegistry.ingestJson(tryLookJson)
        assertFalse("JSON try_look must NOT be treated as isPreview", PlayinfoRegistry.lookup(tryLookUrl)!!.isPreview)

        // Protobuf getTryLook()=true & try_look field must NOT set isPreview=true
        class FakeTryLookReply(
            @Suppress("unused") val try_look: Boolean = true,
            private val url: String = "https://upos-sz-mirrorcos.bilivideo.com/upgcxcode/12/34/pb-trylook.m4s?deadline=1900000100"
        ) {
            fun getTryLook(): Boolean = true
            fun getBaseUrl(): String = url
        }
        PlayinfoRegistry.ingestPlayViewReply(FakeTryLookReply(), "grpc-trylook")
        assertFalse(
            "Protobuf try_look must NOT be treated as isPreview",
            PlayinfoRegistry.lookup("https://upos-sz-mirrorcos.bilivideo.com/upgcxcode/12/34/pb-trylook.m4s")!!.isPreview
        )
    }

    // Deep nested protobuf (> depth 7, within max depth 20)
    private class DeepWrapper(val child: Any)

    @Test
    fun testDeepProtobufTraversalBeyondDepth7UpToDepth20() {
        val deepUrl = "https://upos-sz-mirrorcos.bilivideo.com/upgcxcode/99/11/deep-14.m4s?deadline=1900000999"
        var node: Any = ProtoDashVideo(baseUrl = deepUrl, bandwidth = 5_500_000L)
        repeat(14) {
            node = DeepWrapper(node)
        }
        val entries = PlayinfoRegistry.ingestPlayViewReply(node, "deep-playview-unite")
        assertEquals("Deep depth-14 protobuf PlayView must be parsed (max depth 20)", 1, entries.size)
        val track = PlayinfoRegistry.lookup(deepUrl)
        assertNotNull(track)
        assertEquals(5_500_000L, track!!.bandwidth)
    }

    // Completely obfuscated class & field names for isPerStreamScope isolation test
    private class ObfMeta(@Suppress("unused") val is_drm: Boolean)
    private class ObfMedia(
        @Suppress("unused") val u: String,
        @Suppress("unused") val bandwidth: Long
    )
    // Intentionally place media BEFORE metadata to test order-independent isolation
    private class ObfStreamItem(
        @Suppress("unused") val a: ObfMedia?,
        @Suppress("unused") val b: ObfMeta
    )
    private class ObfViewMeta(@Suppress("unused") val is_preview: Boolean)
    private class ObfVideoHolder(@Suppress("unused") val l: List<ObfStreamItem>)
    private class ObfRootReply(
        @Suppress("unused") val x: ObfVideoHolder,
        @Suppress("unused") val y: ObfViewMeta?
    )

    @Test
    fun testObfuscatedProtobufPerStreamScopeIsolatesDrmIncludingEmptyVipStreams() {
        val drmUrl = "https://upos-sz-mirrorcos.bilivideo.com/upgcxcode/22/33/obf-drm.m4s?deadline=1900000100"
        val cleanUrl = "https://upos-sz-mirrorcos.bilivideo.com/upgcxcode/22/33/obf-clean.m4s?deadline=1900000100"

        val root = ObfRootReply(
            x = ObfVideoHolder(
                listOf(
                    // Empty VIP-only DRM stream (no playable dashVideo URL) must NOT pollute global flags
                    ObfStreamItem(a = null, b = ObfMeta(is_drm = true)),
                    // DRM stream with media declared BEFORE metadata
                    ObfStreamItem(a = ObfMedia(u = drmUrl, bandwidth = 10_000_000L), b = ObfMeta(is_drm = true)),
                    // Clean 1080p stream must remain hasDrm = false
                    ObfStreamItem(a = ObfMedia(u = cleanUrl, bandwidth = 4_000_000L), b = ObfMeta(is_drm = false))
                )
            ),
            y = ObfViewMeta(is_preview = false)
        )

        val entries = PlayinfoRegistry.ingestPlayViewReply(root, "obf-reply")
        assertEquals(2, entries.size)
        val drmEntry = PlayinfoRegistry.lookup(drmUrl)!!
        val cleanEntry = PlayinfoRegistry.lookup(cleanUrl)!!
        assertTrue("Obfuscated DRM stream must still be flagged DRM even when media precedes meta", drmEntry.hasDrm)
        assertFalse("Obfuscated clean sibling stream must NOT be polluted by DRM or empty VIP sibling", cleanEntry.hasDrm)
        assertFalse(cleanEntry.isPreview)
        assertEquals(10_000_000L, drmEntry.bandwidth)
        assertEquals(4_000_000L, cleanEntry.bandwidth)

        // Global obfuscated ViewInfo is_preview=true MUST still propagate to all streams
        PlayinfoRegistry.resetForTesting(clearDir = true)
        val previewRoot = ObfRootReply(
            x = ObfVideoHolder(
                listOf(ObfStreamItem(a = ObfMedia(u = cleanUrl, bandwidth = 4_000_000L), b = ObfMeta(is_drm = false)))
            ),
            y = ObfViewMeta(is_preview = true)
        )
        PlayinfoRegistry.ingestPlayViewReply(previewRoot, "obf-preview-reply")
        assertTrue("Global ViewInfo is_preview must propagate to obfuscated streams", PlayinfoRegistry.lookup(cleanUrl)!!.isPreview)
    }
}
