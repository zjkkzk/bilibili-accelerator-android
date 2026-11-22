package io.github.jh_mmm.biliaccelerator.btr

import io.github.jh_mmm.biliaccelerator.core.AcceleratorConfig
import io.github.jh_mmm.biliaccelerator.core.RewriteResult
import org.junit.Assert.assertEquals
import org.junit.Assert.assertFalse
import org.junit.Assert.assertNotEquals
import org.junit.Assert.assertNotNull
import org.junit.Assert.assertNull
import org.junit.Assert.assertTrue
import org.junit.Test
import java.io.ByteArrayOutputStream
import java.net.ServerSocket
import java.net.URLEncoder
import java.util.Collections
import java.util.concurrent.CountDownLatch
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicInteger
import java.util.concurrent.atomic.AtomicLong

class BtrLiveFixTest {

    @Test
    fun testKnownFmp4HostsIncludesDomesticAndOverseasNodes() {
        val hosts = BtrLiveCore.KNOWN_FMP4_HOSTS
        assertEquals(
            listOf(
                "d1--cn-gotcha204.bilivideo.com",
                "d1--cn-gotcha208.bilivideo.com",
                "d1--ov-gotcha208.bilivideo.com",
                "d1--ov-gotcha208b.bilivideo.com"
            ),
            hosts
        )
        assertTrue(BtrLiveCore.isKnownFmp4Host("d1--cn-gotcha204.bilivideo.com"))
        assertTrue(BtrLiveCore.isKnownFmp4Host("d1--cn-gotcha208.bilivideo.com"))
        assertTrue(BtrLiveCore.isKnownFmp4Host("d1--ov-gotcha208.bilivideo.com"))
        assertTrue(BtrLiveCore.isKnownFmp4Host("d1--ov-gotcha208b.bilivideo.com"))
        assertFalse(BtrLiveCore.isKnownFmp4Host("d1--cn-gotcha104.bilivideo.com"))
        assertFalse(BtrLiveCore.isKnownFmp4Host("ov-gotcha07.bilivideo.com"))

        for (src in hosts) {
            for (dst in hosts) {
                assertTrue(
                    "All KNOWN_FMP4_HOSTS should be mutually signature compatible ($src -> $dst)",
                    BtrLiveCore.areHostsSignatureCompatible(src, dst)
                )
            }
        }
    }

    @Test
    fun testResolveTargetUrlForHostPreservesPerHostAuthQueryAndBasePath() {
        val primaryPlaylist =
            "https://d1--cn-gotcha204.bilivideo.com/live-bvc/111111/live_123_1500/index.m3u8?expires=1700000000&cdn=cn-gotcha204&sign=sig%2F204%2B%3D&trid=tr_204"
        val backupPlaylistDiffCdn =
            "https://cn-gdgz-fx-01-01.bilivideo.com/live-bvc/222222/live_123_1500/index.m3u8?wsSecret=sec%2Fgd%2B1%3D&wsTime=1700000000&trid=tr_gd&extra=ext_gd"
        val backupPlaylistGotcha104 =
            "https://d1--cn-gotcha104.bilivideo.com/live-bvc/111111/live_123_1500/index.m3u8?expires=1700000000&cdn=cn-gotcha104&sign=sig_104&trid=tr_104"

        val liveHostUrls = mapOf(
            "d1--cn-gotcha204.bilivideo.com" to primaryPlaylist,
            "cn-gdgz-fx-01-01.bilivideo.com" to backupPlaylistDiffCdn,
            "d1--cn-gotcha104.bilivideo.com" to backupPlaylistGotcha104
        )

        val segmentUrl =
            "https://d1--cn-gotcha204.bilivideo.com/live-bvc/111111/live_123_1500/h105.m4s?expires=1700000000&cdn=cn-gotcha204&sign=sig%2F204%2B%3D&trid=tr_204"

        // 1. Different-CDN backup host in liveHostUrls preserves its own scheme/host/basePath/auth query + segment filename
        val resolvedGdgz = BtrLiveCore.resolveTargetUrlForHost(
            segmentUrl = segmentUrl,
            targetHost = "cn-gdgz-fx-01-01.bilivideo.com",
            liveHostUrls = liveHostUrls,
            primaryUrl = primaryPlaylist
        )
        assertEquals(
            "https://cn-gdgz-fx-01-01.bilivideo.com/live-bvc/222222/live_123_1500/h105.m4s?wsSecret=sec%2Fgd%2B1%3D&wsTime=1700000000&trid=tr_gd&extra=ext_gd",
            resolvedGdgz
        )

        // 2. Another different-CDN backup host with same base directory replaces query with its own auth query
        val resolved104 = BtrLiveCore.resolveTargetUrlForHost(
            segmentUrl = segmentUrl,
            targetHost = "d1--cn-gotcha104.bilivideo.com",
            liveHostUrls = liveHostUrls,
            primaryUrl = primaryPlaylist
        )
        assertEquals(
            "https://d1--cn-gotcha104.bilivideo.com/live-bvc/111111/live_123_1500/h105.m4s?expires=1700000000&cdn=cn-gotcha104&sign=sig_104&trid=tr_104",
            resolved104
        )

        // 3. Same-group KNOWN_FMP4_HOSTS (domestic & overseas) work via swapHost even without explicit entry in liveHostUrls
        for (fmp4Peer in listOf(
            "d1--cn-gotcha208.bilivideo.com",
            "d1--ov-gotcha208.bilivideo.com",
            "d1--ov-gotcha208b.bilivideo.com"
        )) {
            val resolvedPeer = BtrLiveCore.resolveTargetUrlForHost(
                segmentUrl = segmentUrl,
                targetHost = fmp4Peer,
                liveHostUrls = liveHostUrls,
                primaryUrl = primaryPlaylist
            )
            assertEquals(
                "https://$fmp4Peer/live-bvc/111111/live_123_1500/h105.m4s?expires=1700000000&cdn=cn-gotcha204&sign=sig%2F204%2B%3D&trid=tr_204",
                resolvedPeer
            )
        }

        // 4. Unmapped different-CDN host (not in liveHostUrls and not in KNOWN_FMP4_HOSTS) returns null to avoid 403 signature mismatch
        val resolvedUnmapped = BtrLiveCore.resolveTargetUrlForHost(
            segmentUrl = segmentUrl,
            targetHost = "ov-gotcha07.bilivideo.com",
            liveHostUrls = liveHostUrls,
            primaryUrl = primaryPlaylist
        )
        assertNull(
            "Unmapped different-CDN host must return null rather than sending mismatched signature",
            resolvedUnmapped
        )
    }

    @Test
    fun testDownloadLiveSegmentAndProbeCandidatesUsePerHostAuthQueriesAndSkipIncompatibleUnmappedHosts() {
        BtrLiveEngine.clearCache()
        val expectedPayload = ByteArray(4096) { ((it * 31 + 7) and 0xFF).toByte() }
        val primaryQueries = Collections.synchronizedList(mutableListOf<String>())
        val backupQueries = Collections.synchronizedList(mutableListOf<String>())
        val backupPaths = Collections.synchronizedList(mutableListOf<String>())
        val primaryMode = AtomicInteger(0) // 0 = stall to trigger hedge on backup, 1 = 200 OK

        val primaryServer = ServerSocket(0)
        val backupServer = ServerSocket(0)

        val tPrimary = Thread {
            while (!primaryServer.isClosed) {
                val sock = try { primaryServer.accept() } catch (_: Throwable) { break }
                Thread {
                    sock.use { s ->
                        val reader = s.getInputStream().bufferedReader()
                        val reqLine = reader.readLine() ?: return@use
                        while (true) {
                            val line = reader.readLine() ?: break
                            if (line.isEmpty()) break
                        }
                        val target = reqLine.split(" ").getOrNull(1).orEmpty()
                        primaryQueries.add(target.substringAfter('?', ""))
                        val out = s.getOutputStream()
                        if (primaryMode.get() == 0) {
                            // Stall primary so hedge attempt on backup wins
                            runCatching { Thread.sleep(1500L) }
                        }
                        if (!target.contains("wsSecret=primary_sec")) {
                            out.write("HTTP/1.1 403 Forbidden\r\nContent-Length: 0\r\nConnection: close\r\n\r\n".toByteArray())
                        } else {
                            out.write("HTTP/1.1 200 OK\r\nContent-Type: video/iso.segment\r\nContent-Length: ${expectedPayload.size}\r\nConnection: close\r\n\r\n".toByteArray())
                            out.write(expectedPayload)
                        }
                        out.flush()
                    }
                }.apply { isDaemon = true; start() }
            }
        }.apply { isDaemon = true; start() }

        val tBackup = Thread {
            while (!backupServer.isClosed) {
                val sock = try { backupServer.accept() } catch (_: Throwable) { break }
                Thread {
                    sock.use { s ->
                        val reader = s.getInputStream().bufferedReader()
                        val reqLine = reader.readLine() ?: return@use
                        var rangeHeader = ""
                        while (true) {
                            val line = reader.readLine() ?: break
                            if (line.isEmpty()) break
                            if (line.startsWith("Range:", ignoreCase = true)) {
                                rangeHeader = line.substringAfter(':').trim()
                            }
                        }
                        val target = reqLine.split(" ").getOrNull(1).orEmpty()
                        backupPaths.add(target.substringBefore('?'))
                        backupQueries.add(target.substringAfter('?', ""))
                        val out = s.getOutputStream()
                        // Strictly verify backup host receives its own wsSecret=backup_sec&trid=backup_trid (returns 403 if primary token was sent!)
                        if (!target.contains("wsSecret=backup_sec") || !target.contains("trid=backup_trid")) {
                            out.write("HTTP/1.1 403 Forbidden\r\nContent-Length: 0\r\nConnection: close\r\n\r\n".toByteArray())
                        } else if (rangeHeader.startsWith("bytes=0-2047")) {
                            val probeSlice = expectedPayload.copyOfRange(0, 2048)
                            out.write("HTTP/1.1 206 Partial Content\r\nContent-Type: video/iso.segment\r\nContent-Range: bytes 0-2047/${expectedPayload.size}\r\nContent-Length: ${probeSlice.size}\r\nConnection: close\r\n\r\n".toByteArray())
                            out.write(probeSlice)
                        } else {
                            out.write("HTTP/1.1 200 OK\r\nContent-Type: video/iso.segment\r\nContent-Length: ${expectedPayload.size}\r\nConnection: close\r\n\r\n".toByteArray())
                            out.write(expectedPayload)
                        }
                        out.flush()
                    }
                }.apply { isDaemon = true; start() }
            }
        }.apply { isDaemon = true; start() }

        var sid = ""
        try {
            val primaryUrl =
                "http://127.0.0.1:${primaryServer.localPort}/live-bvc/111/live_room/index.m3u8?wsSecret=primary_sec&trid=primary_trid"
            val backupUrl =
                "http://localhost:${backupServer.localPort}/live-bvc/222/live_room/index.m3u8?wsSecret=backup_sec&trid=backup_trid&extra=backup_ext"

            sid = BtrSessionManager.register(
                primaryUrl = primaryUrl,
                backupUrls = listOf(backupUrl),
                isLive = true
            )
            val session = BtrSessionManager.get(sid)!!
            val pool = session.liveHostPool!!

            // Add an unmapped different-CDN host that has no entry in session.liveHostUrls and is not in KNOWN_FMP4_HOSTS
            pool.add("unmapped-other-cdn.bilivideo.com", proven = false)

            val sampleSegUrl =
                "http://127.0.0.1:${primaryServer.localPort}/live-bvc/111/live_room/seg201.m4s?wsSecret=primary_sec&trid=primary_trid"

            // 1. Test probeCandidates: must probe localhost using backupUrl's port, basePath (/live-bvc/222/live_room/seg201.m4s), and auth query,
            //    and must skip unmapped-other-cdn.bilivideo.com without penalizing or banning it
            BtrLiveEngine.probeCandidates(session, sampleSegUrl)
            val deadline = System.currentTimeMillis() + 3000L
            while (System.currentTimeMillis() < deadline) {
                val st = pool.status().firstOrNull { it.host == "localhost" }?.state
                if (st == "healthy") break
                Thread.sleep(20L)
            }
            val localhostState = pool.status().firstOrNull { it.host == "localhost" }?.state
            assertEquals("Backup host with its own auth query must pass probe as healthy", "healthy", localhostState)
            assertTrue(
                "Probe on backup host must preserve segment filename with backup base path",
                backupPaths.contains("/live-bvc/222/live_room/seg201.m4s")
            )
            assertTrue(
                "Probe on backup host must use backup host's auth query params",
                backupQueries.any { it.contains("wsSecret=backup_sec") && it.contains("trid=backup_trid") && it.contains("extra=backup_ext") }
            )
            val unmappedState = pool.status().firstOrNull { it.host == "unmapped-other-cdn.bilivideo.com" }?.state
            assertEquals(
                "Unmapped different-CDN host without signed URL must not be probed with mismatched token or penalized",
                "untested",
                unmappedState
            )

            // 2. Test downloadLiveSegment hedged race: primary (127.0.0.1) stalls -> hedge on backup (localhost) wins using backup auth query
            val seg202Url =
                "http://127.0.0.1:${primaryServer.localPort}/live-bvc/111/live_room/seg202.m4s?wsSecret=primary_sec&trid=primary_trid"
            val downloaded = BtrLiveEngine.downloadLiveSegment(
                session = session,
                url = seg202Url,
                urgent = true
            )
            assertNotNull(downloaded)
            assertEquals("localhost", downloaded.host)
            assertTrue(expectedPayload.contentEquals(downloaded.bytes))
            assertTrue(
                "Hedged segment download on backup host must request /live-bvc/222/live_room/seg202.m4s",
                backupPaths.contains("/live-bvc/222/live_room/seg202.m4s")
            )
            assertTrue(
                "Hedged segment download on backup host must carry backup auth query and never leak primary_sec",
                backupQueries.all { it.contains("wsSecret=backup_sec") && !it.contains("primary_sec") }
            )
        } finally {
            if (sid.isNotBlank()) BtrSessionManager.remove(sid)
            BtrLiveEngine.clearCache()
            runCatching { primaryServer.close() }
            runCatching { backupServer.close() }
            tPrimary.interrupt()
            tBackup.interrupt()
        }
    }

    // =========================================================================
    // Issue 12: Live first-byte & total timeout must penalize/ban dead hosts
    // =========================================================================

    @Test
    fun testIssue12FirstByteTimeoutPenalizesAndBansBlackholeHost() {
        BtrLiveEngine.clearCache()
        val payload = ByteArray(1024) { (it and 0xFF).toByte() }
        val blackholeServer = ServerSocket(0)
        val healthyServer = ServerSocket(0)

        val tBlackhole = Thread {
            while (!blackholeServer.isClosed) {
                val sock = try { blackholeServer.accept() } catch (_: Throwable) { break }
                Thread {
                    sock.use { s ->
                        val reader = s.getInputStream().bufferedReader()
                        while (true) {
                            val line = reader.readLine() ?: break
                            if (line.isEmpty()) break
                        }
                        // Blackhole: never send first byte
                        runCatching { Thread.sleep(5000L) }
                    }
                }.apply { isDaemon = true; start() }
            }
        }.apply { isDaemon = true; start() }

        val tHealthy = Thread {
            while (!healthyServer.isClosed) {
                val sock = try { healthyServer.accept() } catch (_: Throwable) { break }
                Thread {
                    sock.use { s ->
                        val reader = s.getInputStream().bufferedReader()
                        while (true) {
                            val line = reader.readLine() ?: break
                            if (line.isEmpty()) break
                        }
                        val out = s.getOutputStream()
                        out.write("HTTP/1.1 200 OK\r\nContent-Type: video/iso.segment\r\nContent-Length: ${payload.size}\r\nConnection: close\r\n\r\n".toByteArray())
                        out.write(payload)
                        out.flush()
                    }
                }.apply { isDaemon = true; start() }
            }
        }.apply { isDaemon = true; start() }

        var sid = ""
        try {
            val primaryUrl = "http://127.0.0.1:${blackholeServer.localPort}/live-bvc/100/room1/index.m3u8?sign=p"
            val backupUrl = "http://localhost:${healthyServer.localPort}/live-bvc/200/room1/index.m3u8?sign=b"
            sid = BtrSessionManager.register(primaryUrl = primaryUrl, backupUrls = listOf(backupUrl), isLive = true)
            val session = BtrSessionManager.get(sid)!!
            val pool = session.liveHostPool!!

            // First-byte timeout (180ms < hedge 400ms on non-urgent prefetch): blackhole cancels via watchdog and MUST be penalized
            val seg1 = BtrLiveEngine.downloadLiveSegment(
                session = session,
                url = "http://127.0.0.1:${blackholeServer.localPort}/live-bvc/100/room1/h1.m4s?sign=p",
                urgent = false,
                firstByteTimeoutMs = 180L,
                segmentTimeoutMs = 3000L
            )
            assertEquals("localhost", seg1.host)
            val stateAfterFirst = pool.status().first { it.host == "127.0.0.1" }.state
            assertEquals(
                "First-byte watchdog timeout must penalize host (not swallow via isCanceled)",
                "blocked",
                stateAfterFirst
            )

            // Second consecutive first-byte timeout on the same blackhole host must ban it (emptyReplies >= 2):
            // Both hosts are in backoff, with 127.0.0.1 proven so 127.0.0.1 is guaranteed to be tried first.
            val twoTimeoutPool = BtrLiveHostPool()
            twoTimeoutPool.add("127.0.0.1", proven = true)
            twoTimeoutPool.failure("127.0.0.1", 0L) // emptyReplies = 1, blocked
            twoTimeoutPool.add("localhost", proven = false)
            twoTimeoutPool.failure("localhost", 100L) // blocked, unproven -> ranked after 127.0.0.1
            session.liveHostPool = twoTimeoutPool

            val seg2 = BtrLiveEngine.downloadLiveSegment(
                session = session,
                url = "http://127.0.0.1:${blackholeServer.localPort}/live-bvc/100/room1/h2.m4s?sign=p",
                urgent = false,
                firstByteTimeoutMs = 180L,
                segmentTimeoutMs = 3000L
            )
            assertEquals("localhost", seg2.host)
            assertEquals(
                "2nd empty-reply first-byte timeout must permanently ban dead blackhole host",
                "banned",
                twoTimeoutPool.status().first { it.host == "127.0.0.1" }.state
            )
        } finally {
            if (sid.isNotBlank()) BtrSessionManager.remove(sid)
            BtrLiveEngine.clearCache()
            runCatching { blackholeServer.close() }
            runCatching { healthyServer.close() }
            tBlackhole.interrupt()
            tHealthy.interrupt()
        }
    }

    @Test
    fun testIssue12TotalSegmentTimeoutAndFlvTimeoutPenalizeHost() {
        BtrLiveEngine.clearCache()
        val reqCount = AtomicInteger(0)
        val slowServer = ServerSocket(0)

        val tSlow = Thread {
            while (!slowServer.isClosed) {
                val sock = try { slowServer.accept() } catch (_: Throwable) { break }
                Thread {
                    sock.use { s ->
                        val reader = s.getInputStream().bufferedReader()
                        while (true) {
                            val line = reader.readLine() ?: break
                            if (line.isEmpty()) break
                        }
                        val n = reqCount.incrementAndGet()
                        val out = s.getOutputStream()
                        if (n == 1) {
                            // 1st attempt: send first byte immediately (disarms fbWatchdog) then stall until segmentTimeoutMs
                            out.write("HTTP/1.1 200 OK\r\nContent-Type: video/iso.segment\r\nContent-Length: 4096\r\nConnection: close\r\n\r\n".toByteArray())
                            out.write(ByteArray(16) { 1 })
                            out.flush()
                            runCatching { Thread.sleep(5000L) }
                        } else {
                            // Fallback request fails immediately so test doesn't wait on OkHttp readTimeout
                            out.write("HTTP/1.1 503 Service Unavailable\r\nContent-Length: 0\r\nConnection: close\r\n\r\n".toByteArray())
                            out.flush()
                        }
                    }
                }.apply { isDaemon = true; start() }
            }
        }.apply { isDaemon = true; start() }

        val flvBlackhole = ServerSocket(0)
        val tFlv = Thread {
            while (!flvBlackhole.isClosed) {
                val sock = try { flvBlackhole.accept() } catch (_: Throwable) { break }
                Thread {
                    sock.use { s ->
                        val reader = s.getInputStream().bufferedReader()
                        while (true) {
                            val line = reader.readLine() ?: break
                            if (line.isEmpty()) break
                        }
                        runCatching { Thread.sleep(5000L) }
                    }
                }.apply { isDaemon = true; start() }
            }
        }.apply { isDaemon = true; start() }

        var sid = ""
        var flvSid = ""
        try {
            // 1. Total segment timeout penalizes slow-drip host
            val url = "http://127.0.0.1:${slowServer.localPort}/live-bvc/100/room/h10.m4s?sign=s"
            sid = BtrSessionManager.register(primaryUrl = url, isLive = true)
            val session = BtrSessionManager.get(sid)!!
            val pool = BtrLiveHostPool().apply { add("127.0.0.1", proven = true) }
            session.liveHostPool = pool
            session.liveHostUrls.clear()
            session.liveHostUrls["127.0.0.1"] = url

            var threw = false
            try {
                BtrLiveEngine.downloadLiveSegment(
                    session = session,
                    url = url,
                    urgent = true,
                    firstByteTimeoutMs = 2000L,
                    segmentTimeoutMs = 300L
                )
            } catch (_: Throwable) {
                threw = true
            }
            assertTrue("Expected total timeout + 503 fallback to throw", threw)
            assertEquals(
                "Total segment timeout must penalize active host",
                "blocked",
                pool.status().first { it.host == "127.0.0.1" }.state
            )

            // 2. FLV total timeout penalizes dead FLV host
            val flvUrl = "http://127.0.0.1:${flvBlackhole.localPort}/live-bvc/100/live_123.flv?sign=f"
            flvSid = BtrSessionManager.register(primaryUrl = flvUrl, isLive = true)
            val flvSession = BtrSessionManager.get(flvSid)!!
            val flvPool = flvSession.liveHostPool!!
            val out = ByteArrayOutputStream()
            val ok = BtrLiveEngine.pipeLiveFlvStream(
                session = flvSession,
                headers = emptyMap(),
                clientOut = out,
                timeoutMs = 250L
            )
            assertFalse(ok)
            assertEquals(
                "FLV total timeout must penalize unresponsive host",
                "blocked",
                flvPool.status().first { it.host == "127.0.0.1" }.state
            )
        } finally {
            if (sid.isNotBlank()) BtrSessionManager.remove(sid)
            if (flvSid.isNotBlank()) BtrSessionManager.remove(flvSid)
            BtrLiveEngine.clearCache()
            runCatching { slowServer.close() }
            runCatching { flvBlackhole.close() }
            tSlow.interrupt()
            tFlv.interrupt()
        }
    }

    // =========================================================================
    // Issue 13: VOD customCdnPool must NOT break live pool or force swapHost
    // =========================================================================

    @Test
    fun testIssue13VodCustomCdnPoolIgnoredForLiveSession() {
        assertTrue(BtrLiveCore.isVodOnlyHost("upos-sz-mirrorcos.bilivideo.com"))
        assertTrue(BtrLiveCore.isVodOnlyHost("upos-sz-estgoss.bilivideo.com"))
        assertTrue(BtrLiveCore.isVodOnlyHost("proxy-tf-all-ws.bilivideo.com"))
        assertFalse(BtrLiveCore.isVodOnlyHost("d1--cn-gotcha204.bilivideo.com"))

        assertFalse(BtrLiveCore.isLiveCompatibleCustomHost("upos-sz-mirrorcos.bilivideo.com"))
        assertFalse(BtrLiveCore.isLiveCompatibleCustomHost("proxy-tf-all-ws.bilivideo.com"))
        assertFalse(BtrLiveCore.isLiveCompatibleCustomHost("xy111x222x333x444xy.mcdn.bilivideo.cn"))
        assertTrue(BtrLiveCore.isLiveCompatibleCustomHost("d1--cn-gotcha204.bilivideo.com"))
        assertTrue(BtrLiveCore.isLiveCompatibleCustomHost("d1--ov-gotcha208b.bilivideo.com"))

        val filtered = BtrLiveCore.filterLiveCustomHosts(
            listOf(
                "upos-sz-mirrorcos.bilivideo.com",
                "upos-sz-mirrorhw.bilivideo.com",
                "proxy-tf-all-ws.bilivideo.com"
            )
        )
        assertTrue("VOD-only customCdnPool must filter to empty for live", filtered.isEmpty())

        val vodConfig = AcceleratorConfig(
            customCdnPool = listOf("upos-sz-mirrorcos.bilivideo.com", "upos-sz-mirrorhw.bilivideo.com")
        )
        val liveSegUrl = "https://d1--cn-gotcha204.bilivideo.com/live-bvc/111111/live_123/h100.m4s?sign=abc"
        val sid = BtrSessionManager.register(primaryUrl = liveSegUrl, isLive = true, config = vodConfig)
        try {
            val session = BtrSessionManager.get(sid)!!
            assertFalse("VOD customCdnPool must NOT set liveCustomPoolOnly=true", session.liveCustomPoolOnly)
            val hosts = session.liveHostPool!!.status().map { it.host }.toSet()
            assertFalse("VOD mirror must NOT be added to liveHostPool", hosts.contains("upos-sz-mirrorcos.bilivideo.com"))
            assertFalse("VOD mirror must NOT be added to liveHostPool", hosts.contains("upos-sz-mirrorhw.bilivideo.com"))
            assertTrue("KNOWN_FMP4_HOSTS should remain enabled", hosts.contains("d1--cn-gotcha208.bilivideo.com"))

            // Even with customPoolOnly=true, resolveTargetUrlForHost must reject VOD-only host
            assertNull(
                BtrLiveCore.resolveTargetUrlForHost(
                    segmentUrl = liveSegUrl,
                    targetHost = "upos-sz-mirrorcos.bilivideo.com",
                    customPoolOnly = true
                )
            )
        } finally {
            BtrSessionManager.remove(sid)
        }
    }

    // =========================================================================
    // Issue 14: FLV hedge auth, Range passthrough, speculative misses, cache key,
    //           PROXY_WRAP_REGEX, isP2pUrl/swapHost ports, prefetch accel & IPC
    // =========================================================================

    @Test
    fun testIssue14FlvHedgeAuthCompatibility() {
        val flvPrimary = "https://d1--cn-gotcha204.bilivideo.com/live-bvc/111111/live_123.flv?expires=1700&sign=flv_primary"
        val flvBackup = "https://cn-gdgz-fx-01-01.bilivideo.com/live-bvc/222222/live_123.flv?wsSecret=flv_backup&wsTime=1700"
        val liveHostUrls = mapOf(
            "d1--cn-gotcha204.bilivideo.com" to flvPrimary,
            "cn-gdgz-fx-01-01.bilivideo.com" to flvBackup
        )

        // FLV must NOT reuse signature across KNOWN_FMP4_HOSTS without an explicit entry in liveHostUrls
        assertNull(
            "FLV must not blindly swapHost onto unmapped KNOWN_FMP4_HOSTS peer",
            BtrLiveCore.resolveTargetUrlForHost(
                segmentUrl = flvPrimary,
                targetHost = "d1--cn-gotcha208.bilivideo.com",
                liveHostUrls = liveHostUrls,
                primaryUrl = flvPrimary
            )
        )
        // FLV with explicit per-host signed URL in liveHostUrls MUST resolve to that host's own URL
        assertEquals(
            flvBackup,
            BtrLiveCore.resolveTargetUrlForHost(
                segmentUrl = flvPrimary,
                targetHost = "cn-gdgz-fx-01-01.bilivideo.com",
                liveHostUrls = liveHostUrls,
                primaryUrl = flvPrimary
            )
        )

        // End-to-end pipeLiveFlvStream: unmapped host in pool must NEVER be contacted; mapped backup wins with its own token
        BtrLiveEngine.clearCache()
        val backupTokens = Collections.synchronizedList(mutableListOf<String>())
        val primaryFlvServer = ServerSocket(0)
        val backupFlvServer = ServerSocket(0)
        val flvBody = "FLV\u0001\u0005\u0000\u0000\u0000\u0009testpayload".toByteArray()

        val tP = Thread {
            while (!primaryFlvServer.isClosed) {
                val sock = try { primaryFlvServer.accept() } catch (_: Throwable) { break }
                Thread {
                    sock.use { s ->
                        val r = s.getInputStream().bufferedReader()
                        while (true) {
                            val l = r.readLine() ?: break
                            if (l.isEmpty()) break
                        }
                        // Primary fails with 503 immediately so hedge tries backup
                        s.getOutputStream().write("HTTP/1.1 503 Service Unavailable\r\nContent-Length: 0\r\nConnection: close\r\n\r\n".toByteArray())
                        s.getOutputStream().flush()
                    }
                }.apply { isDaemon = true; start() }
            }
        }.apply { isDaemon = true; start() }

        val tB = Thread {
            while (!backupFlvServer.isClosed) {
                val sock = try { backupFlvServer.accept() } catch (_: Throwable) { break }
                Thread {
                    sock.use { s ->
                        val r = s.getInputStream().bufferedReader()
                        val reqLine = r.readLine() ?: return@use
                        while (true) {
                            val l = r.readLine() ?: break
                            if (l.isEmpty()) break
                        }
                        val target = reqLine.split(" ").getOrNull(1).orEmpty()
                        backupTokens.add(target)
                        val out = s.getOutputStream()
                        if (!target.contains("wsSecret=flv_b")) {
                            out.write("HTTP/1.1 403 Forbidden\r\nContent-Length: 0\r\nConnection: close\r\n\r\n".toByteArray())
                        } else {
                            out.write("HTTP/1.1 200 OK\r\nContent-Type: video/x-flv\r\nConnection: close\r\n\r\n".toByteArray())
                            out.write(flvBody)
                        }
                        out.flush()
                    }
                }.apply { isDaemon = true; start() }
            }
        }.apply { isDaemon = true; start() }

        var sid = ""
        val prevSink = BtrLiveEngine.notifyRewriteSink
        BtrLiveEngine.notifyRewriteSink = {}
        try {
            val pUrl = "http://127.0.0.1:${primaryFlvServer.localPort}/live-bvc/111/live_123.flv?sign=flv_p"
            val bUrl = "http://localhost:${backupFlvServer.localPort}/live-bvc/222/live_123.flv?wsSecret=flv_b"
            sid = BtrSessionManager.register(primaryUrl = pUrl, backupUrls = listOf(bUrl), isLive = true)
            val session = BtrSessionManager.get(sid)!!
            session.liveHostPool!!.add("unmapped-flv-cdn.bilivideo.com", proven = true)

            val clientOut = ByteArrayOutputStream()
            BtrLiveEngine.pipeLiveFlvStream(session = session, headers = emptyMap(), clientOut = clientOut)
            val respStr = clientOut.toString(Charsets.ISO_8859_1.name())
            assertTrue("Expected 200 OK FLV response from mapped backup", respStr.startsWith("HTTP/1.1 200 OK"))
            assertTrue("Expected FLV payload", respStr.contains("testpayload"))
            assertTrue("Backup must receive its own wsSecret=flv_b", backupTokens.any { it.contains("wsSecret=flv_b") })
            assertEquals(
                "Unmapped FLV host must be skipped and never penalized",
                "healthy",
                session.liveHostPool!!.status().first { it.host == "unmapped-flv-cdn.bilivideo.com" }.state
            )
        } finally {
            BtrLiveEngine.notifyRewriteSink = prevSink
            if (sid.isNotBlank()) BtrSessionManager.remove(sid)
            BtrLiveEngine.clearCache()
            runCatching { primaryFlvServer.close() }
            runCatching { backupFlvServer.close() }
            tP.interrupt()
            tB.interrupt()
        }
    }

    @Test
    fun testIssue14PipeSegmentRangePassthroughAndCachedRange() {
        BtrLiveEngine.clearCache()
        val fullPayload = ByteArray(100) { it.toByte() }
        val receivedRanges = Collections.synchronizedList(mutableListOf<String>())
        val server = ServerSocket(0)

        val tServer = Thread {
            while (!server.isClosed) {
                val sock = try { server.accept() } catch (_: Throwable) { break }
                Thread {
                    sock.use { s ->
                        val r = s.getInputStream().bufferedReader()
                        r.readLine() ?: return@use
                        var rangeHdr = ""
                        while (true) {
                            val l = r.readLine() ?: break
                            if (l.isEmpty()) break
                            if (l.startsWith("Range:", ignoreCase = true)) {
                                rangeHdr = l.substringAfter(':').trim()
                            }
                        }
                        receivedRanges.add(rangeHdr)
                        val out = s.getOutputStream()
                        if (rangeHdr == "bytes=10-19") {
                            val slice = fullPayload.copyOfRange(10, 20)
                            out.write(
                                ("HTTP/1.1 206 Partial Content\r\n" +
                                    "Content-Type: video/iso.segment\r\n" +
                                    "Content-Range: bytes 10-19/${fullPayload.size}\r\n" +
                                    "Content-Length: ${slice.size}\r\n" +
                                    "Connection: close\r\n\r\n").toByteArray()
                            )
                            out.write(slice)
                        } else {
                            out.write("HTTP/1.1 200 OK\r\nContent-Type: video/iso.segment\r\nContent-Length: ${fullPayload.size}\r\nConnection: close\r\n\r\n".toByteArray())
                            out.write(fullPayload)
                        }
                        out.flush()
                    }
                }.apply { isDaemon = true; start() }
            }
        }.apply { isDaemon = true; start() }

        var sid = ""
        val prevSink = BtrLiveEngine.notifyRewriteSink
        BtrLiveEngine.notifyRewriteSink = {}
        try {
            val segUrl = "http://127.0.0.1:${server.localPort}/live-bvc/111/room/h50.m4s?sign=r"
            sid = BtrSessionManager.register(primaryUrl = segUrl, isLive = true)
            val session = BtrSessionManager.get(sid)!!
            val encoded = URLEncoder.encode(segUrl, "UTF-8")

            // 1. Cache-miss ranged request -> must forward Range upstream and return 206 + exact 10 bytes
            val missOut = ByteArrayOutputStream()
            BtrLiveEngine.pipeSegment(
                session = session,
                requestPath = "/stream/$sid/segment.m4s?url=$encoded",
                headers = mapOf("range" to "bytes=10-19"),
                clientOut = missOut
            )
            val missBytes = missOut.toByteArray()
            val missStr = String(missBytes, Charsets.ISO_8859_1)
            val missHeader = missStr.substringBefore("\r\n\r\n")
            val missBody = missBytes.copyOfRange(missHeader.length + 4, missBytes.size)
            assertTrue("Cache-miss ranged request must return 206 Partial Content", missHeader.startsWith("HTTP/1.1 206 Partial Content"))
            assertTrue("Must include Content-Range: bytes 10-19/100", missHeader.contains("Content-Range: bytes 10-19/100"))
            assertTrue("Must pass Range header to upstream", receivedRanges.contains("bytes=10-19"))
            assertTrue("Body must match exact bytes 10..19", fullPayload.copyOfRange(10, 20).contentEquals(missBody))

            // 2. Cache-hit ranged request -> must slice cached segment and return 206 without returning 200 full bytes
            val cachedUrl = "http://127.0.0.1:${server.localPort}/live-bvc/111/room/h51.m4s?sign=r"
            BtrLiveEngine.putCacheForTest(
                cachedUrl,
                CachedLiveSegment(bytes = fullPayload, contentType = "video/iso.segment", host = "127.0.0.1"),
                sessionId = sid
            )
            val hitOut = ByteArrayOutputStream()
            BtrLiveEngine.pipeSegment(
                session = session,
                requestPath = "/stream/$sid/segment.m4s?url=${URLEncoder.encode(cachedUrl, "UTF-8")}",
                headers = mapOf("range" to "bytes=20-29"),
                clientOut = hitOut
            )
            val hitBytes = hitOut.toByteArray()
            val hitHeader = String(hitBytes, Charsets.ISO_8859_1).substringBefore("\r\n\r\n")
            val hitBody = hitBytes.copyOfRange(hitHeader.length + 4, hitBytes.size)
            assertTrue("Cache-hit ranged request must return 206 Partial Content", hitHeader.startsWith("HTTP/1.1 206 Partial Content"))
            assertTrue(hitHeader.contains("Content-Range: bytes 20-29/100"))
            assertTrue(fullPayload.copyOfRange(20, 30).contentEquals(hitBody))

            // 3. Out-of-bounds range on cached segment -> 416 Range Not Satisfiable
            val oobOut = ByteArrayOutputStream()
            BtrLiveEngine.pipeSegment(
                session = session,
                requestPath = "/stream/$sid/segment.m4s?url=${URLEncoder.encode(cachedUrl, "UTF-8")}",
                headers = mapOf("range" to "bytes=500-600"),
                clientOut = oobOut
            )
            assertTrue(String(oobOut.toByteArray(), Charsets.ISO_8859_1).startsWith("HTTP/1.1 416 Range Not Satisfiable"))
        } finally {
            BtrLiveEngine.notifyRewriteSink = prevSink
            if (sid.isNotBlank()) BtrSessionManager.remove(sid)
            BtrLiveEngine.clearCache()
            runCatching { server.close() }
            tServer.interrupt()
        }
    }

    @Test
    fun testIssue14SpeculativeMissesNotResetOnPlaylistAdvanceAndStopsAfterSixMisses() {
        BtrLiveEngine.clearCache()
        val playlistStep = AtomicInteger(0)
        val requestedSpeculativePaths = Collections.synchronizedList(mutableListOf<String>())
        val server = ServerSocket(0)

        val tServer = Thread {
            while (!server.isClosed) {
                val sock = try { server.accept() } catch (_: Throwable) { break }
                Thread {
                    sock.use { s ->
                        val r = s.getInputStream().bufferedReader()
                        val reqLine = r.readLine() ?: return@use
                        while (true) {
                            val l = r.readLine() ?: break
                            if (l.isEmpty()) break
                        }
                        val target = reqLine.split(" ").getOrNull(1).orEmpty().substringBefore('?')
                        val out = s.getOutputStream()
                        if (target.endsWith(".m3u8")) {
                            val step = playlistStep.incrementAndGet()
                            val segNum = 1700000000L + step * 10L
                            val m3u8 = "#EXTM3U\n#EXT-X-VERSION:7\n#EXT-X-MAP:URI=\"init.m4s\"\n#EXTINF:1.0,\n$segNum.m4s\n"
                            val bytes = m3u8.toByteArray()
                            out.write("HTTP/1.1 200 OK\r\nContent-Type: application/vnd.apple.mpegurl\r\nContent-Length: ${bytes.size}\r\nConnection: close\r\n\r\n".toByteArray())
                            out.write(bytes)
                        } else if (target.endsWith("init.m4s") || target.endsWith("0.m4s")) {
                            val seg = ByteArray(64) { 7 }
                            out.write("HTTP/1.1 200 OK\r\nContent-Type: video/iso.segment\r\nContent-Length: ${seg.size}\r\nConnection: close\r\n\r\n".toByteArray())
                            out.write(seg)
                        } else {
                            // Speculative +1 segment (ends with 1.m4s) -> 404
                            requestedSpeculativePaths.add(target)
                            out.write("HTTP/1.1 404 Not Found\r\nContent-Length: 0\r\nConnection: close\r\n\r\n".toByteArray())
                        }
                        out.flush()
                    }
                }.apply { isDaemon = true; start() }
            }
        }.apply { isDaemon = true; start() }

        var sid = ""
        try {
            val playlistUrl = "http://127.0.0.1:${server.localPort}/live-bvc/111/tsroom/index.m3u8"
            sid = BtrSessionManager.register(primaryUrl = playlistUrl, isLive = true)
            val session = BtrSessionManager.get(sid)!!
            // Pre-seed 5 misses, then advance playlist -> liveSpeculativeMisses MUST NOT reset to 0!
            session.liveSpeculativeMisses.set(5)
            // Pre-cache init.m4s and the upcoming segment (1700000010.m4s) so pumpPrefetches immediately triggers speculative 1700000011.m4s
            BtrLiveEngine.putCacheForTest(
                "http://127.0.0.1:${server.localPort}/live-bvc/111/tsroom/init.m4s",
                CachedLiveSegment(ByteArray(32), "video/iso.segment", "127.0.0.1"),
                sessionId = sid
            )
            BtrLiveEngine.putCacheForTest(
                "http://127.0.0.1:${server.localPort}/live-bvc/111/tsroom/1700000010.m4s",
                CachedLiveSegment(ByteArray(32), "video/iso.segment", "127.0.0.1"),
                sessionId = sid
            )

            BtrLiveEngine.pipePlaylist(
                session = session,
                requestPath = "/stream/$sid.m3u8",
                headers = emptyMap(),
                clientOut = ByteArrayOutputStream(),
                proxyPort = 18080
            )

            // Wait for the 6th speculative miss to complete
            val deadline = System.currentTimeMillis() + 3000L
            while (System.currentTimeMillis() < deadline && session.liveSpeculativeMisses.get() < 6) {
                Thread.sleep(20L)
            }
            assertEquals("liveSpeculativeMisses must reach 6 without being reset on lastNum advance", 6, session.liveSpeculativeMisses.get())
            val countAfterSix = requestedSpeculativePaths.size

            // Advance playlist again (1700000020.m4s, already cached) -> must NOT reset misses or send 1700000021.m4s
            BtrLiveEngine.putCacheForTest(
                "http://127.0.0.1:${server.localPort}/live-bvc/111/tsroom/1700000020.m4s",
                CachedLiveSegment(ByteArray(32), "video/iso.segment", "127.0.0.1"),
                sessionId = sid
            )
            BtrLiveEngine.pipePlaylist(
                session = session,
                requestPath = "/stream/$sid.m3u8",
                headers = emptyMap(),
                clientOut = ByteArrayOutputStream(),
                proxyPort = 18080
            )
            Thread.sleep(200L)
            assertEquals("liveSpeculativeMisses must stay at 6 after playlist lastNum advances", 6, session.liveSpeculativeMisses.get())
            assertEquals("No further speculative 404 requests should be sent once misses == 6", countAfterSix, requestedSpeculativePaths.size)
        } finally {
            if (sid.isNotBlank()) BtrSessionManager.remove(sid)
            BtrLiveEngine.clearCache()
            runCatching { server.close() }
            tServer.interrupt()
        }
    }

    @Test
    fun testIssue14NormalizedCacheKeyAcrossNodeFailoverAndInitMap() {
        BtrLiveEngine.clearCache()
        val urlNodeA = "https://d1--cn-gotcha204.bilivideo.com/live-bvc/111111/live_123_1500/h200.m4s?expires=1700&sign=aaa"
        val urlNodeB = "https://cn-gdgz-fx-01-01.bilivideo.com/live-bvc/222222/live_123_1500/h200.m4s?wsSecret=bbb&wsTime=1701"
        val urlWrapped = "https://xxx.smtcdns.net/d1--cn-gotcha208.bilivideo.com/live-bvc/333333/live_123_1500/h200.m4s?trid=ccc"
        val urlOtherQuality = "https://d1--cn-gotcha204.bilivideo.com/live-bvc/111111/live_123_4000/h200.m4s?expires=1700&sign=aaa"

        val keyA = BtrLiveCore.normalizeCacheKey(urlNodeA)
        assertEquals("/live-bvc/live_123_1500/h200.m4s", keyA)
        assertEquals(keyA, BtrLiveCore.normalizeCacheKey(urlNodeB))
        assertEquals(keyA, BtrLiveCore.normalizeCacheKey(urlWrapped))
        assertNotEquals("Different quality subdirs must keep distinct cache keys", keyA, BtrLiveCore.normalizeCacheKey(urlOtherQuality))

        // Prefetched segment + protected #EXT-X-MAP stored under node A must hit cache when requested via node B
        val sid = "test-norm-cache"
        val mapA = "https://d1--cn-gotcha204.bilivideo.com/live-bvc/111111/live_123_1500/h0.m4s?sign=a"
        val mapB = "https://cn-gdgz-fx-01-01.bilivideo.com/live-bvc/222222/live_123_1500/h0.m4s?wsSecret=b"
        try {
            val seg = CachedLiveSegment(ByteArray(16) { 9 }, "video/iso.segment", "d1--cn-gotcha204.bilivideo.com")
            val mapSeg = CachedLiveSegment(ByteArray(8) { 3 }, "video/iso.segment", "d1--cn-gotcha204.bilivideo.com")
            BtrLiveEngine.protectMapUrl(mapA, sessionId = sid)
            BtrLiveEngine.putCacheForTest(mapA, mapSeg, sessionId = sid)
            BtrLiveEngine.putCacheForTest(urlNodeA, seg, sessionId = sid)

            assertNotNull("Segment cached on node A must hit via node B URL", BtrLiveEngine.getCachedForTest(urlNodeB, sessionId = sid))
            assertNotNull("Init map cached on node A must hit via node B URL", BtrLiveEngine.getCachedForTest(mapB, sessionId = sid))
        } finally {
            BtrLiveEngine.clearCache()
        }
    }

    @Test
    fun testIssue14ProxyWrapRegexStrictAndPortHandling() {
        // 1. Strict PROXY_WRAP_REGEX
        assertEquals(
            "https://d1--cn-gotcha204.bilivideo.com/live-bvc/123/h1.m4s?sign=a",
            BtrLiveCore.unwrapProxyUrl("https://relay01.smtcdns.net/d1--cn-gotcha204.bilivideo.com/live-bvc/123/h1.m4s?sign=a")
        )
        assertEquals(
            "https://sub.d1--cn-gotcha204.bilivideo.cn:443/live-bvc/123/h1.m4s",
            BtrLiveCore.unwrapProxyUrl("https://relay01.smtcdns.com/sub.d1--cn-gotcha204.bilivideo.cn:443/live-bvc/123/h1.m4s")
        )
        assertEquals(
            "Ordinary dot-containing path /live.v1/... must NOT be destructively unwrapped",
            "",
            BtrLiveCore.unwrapProxyUrl("https://relay01.smtcdns.net/live.v1/123/h1.m4s")
        )
        assertEquals(
            "Loopback host inside smtcdns path must NOT be unwrapped",
            "",
            BtrLiveCore.unwrapProxyUrl("https://relay01.smtcdns.net/127.0.0.1:8080/live-bvc/123/h1.m4s")
        )
        assertEquals(
            "Arbitrary non-bilivideo domain must NOT be unwrapped",
            "",
            BtrLiveCore.unwrapProxyUrl("https://relay01.smtcdns.net/evil.example.com/live-bvc/123/h1.m4s")
        )
        assertEquals(
            "Out-of-range port must NOT be unwrapped",
            "",
            BtrLiveCore.unwrapProxyUrl("https://relay01.smtcdns.net/d1--cn-gotcha204.bilivideo.com:99999/live-bvc/123/h1.m4s")
        )

        // 2. isP2pUrl non-standard port & os=mcdn detection
        assertTrue(BtrLiveCore.isP2pUrl("https://d1--cn-gotcha204.bilivideo.com:8443/live-bvc/123/h1.m4s"))
        assertTrue(BtrLiveCore.isP2pUrl("https://d1--cn-gotcha204.bilivideo.com/live-bvc/123/h1.m4s?os=mcdn&sign=1"))
        assertFalse(BtrLiveCore.isP2pUrl("https://d1--cn-gotcha204.bilivideo.com:443/live-bvc/123/h1.m4s"))
        assertFalse(BtrLiveCore.isP2pUrl("https://d1--cn-gotcha204.bilivideo.com/live-bvc/123/h1.m4s?os=upos"))
        assertFalse("Loopback test server with custom port must not be flagged as P2P", BtrLiveCore.isP2pUrl("http://127.0.0.1:18080/live-bvc/123/h1.m4s"))

        // 3. swapHost must explicitly strip non-standard port even when target host == original host
        assertEquals(
            "https://d1--cn-gotcha204.bilivideo.com/live-bvc/123/h1.m4s?sign=a%2Fb%2B%3D",
            BtrLiveCore.swapHost(
                "https://d1--cn-gotcha204.bilivideo.com:8443/live-bvc/123/h1.m4s?sign=a%2Fb%2B%3D",
                "d1--cn-gotcha204.bilivideo.com"
            )
        )
    }

    @Test
    fun testIssue14RegularPrefetchAccelerationAndNoPrefetchIpcSpam() {
        BtrLiveEngine.clearCache()
        val payload = ByteArray(512) { (it and 0xFF).toByte() }
        val primaryArrived = CountDownLatch(1)
        val backupArrivedAt = AtomicLong(0L)
        val primaryServer = ServerSocket(0)
        val backupServer = ServerSocket(0)

        val tPrimary = Thread {
            while (!primaryServer.isClosed) {
                val sock = try { primaryServer.accept() } catch (_: Throwable) { break }
                Thread {
                    sock.use { s ->
                        val r = s.getInputStream().bufferedReader()
                        while (true) {
                            val l = r.readLine() ?: break
                            if (l.isEmpty()) break
                        }
                        primaryArrived.countDown()
                        // Stall primary so backup hedge wins
                        runCatching { Thread.sleep(2000L) }
                    }
                }.apply { isDaemon = true; start() }
            }
        }.apply { isDaemon = true; start() }

        val tBackup = Thread {
            while (!backupServer.isClosed) {
                val sock = try { backupServer.accept() } catch (_: Throwable) { break }
                Thread {
                    sock.use { s ->
                        val r = s.getInputStream().bufferedReader()
                        while (true) {
                            val l = r.readLine() ?: break
                            if (l.isEmpty()) break
                        }
                        backupArrivedAt.compareAndSet(0L, System.currentTimeMillis())
                        val out = s.getOutputStream()
                        out.write("HTTP/1.1 200 OK\r\nContent-Type: video/iso.segment\r\nContent-Length: ${payload.size}\r\nConnection: close\r\n\r\n".toByteArray())
                        out.write(payload)
                        out.flush()
                    }
                }.apply { isDaemon = true; start() }
            }
        }.apply { isDaemon = true; start() }

        var sid = ""
        val reportedEvents = Collections.synchronizedList(mutableListOf<RewriteResult>())
        val prevSink = BtrLiveEngine.notifyRewriteSink
        BtrLiveEngine.notifyRewriteSink = { reportedEvents.add(it) }
        try {
            val pUrl = "http://127.0.0.1:${primaryServer.localPort}/live-bvc/111/room/index.m3u8?sign=p"
            val bUrl = "http://localhost:${backupServer.localPort}/live-bvc/222/room/index.m3u8?sign=b"
            sid = BtrSessionManager.register(primaryUrl = pUrl, backupUrls = listOf(bUrl), isLive = true)
            val session = BtrSessionManager.get(sid)!!
            val segUrl = "http://127.0.0.1:${primaryServer.localPort}/live-bvc/111/room/h300.m4s?sign=p"

            // 1. Start a NORMAL (non-speculative, urgent=false, 400ms hedge) background prefetch
            val prefetchStartedAt = System.currentTimeMillis()
            val prefetchDone = CountDownLatch(1)
            Thread {
                runCatching {
                    BtrLiveEngine.downloadLiveSegment(
                        session = session,
                        url = segUrl,
                        urgent = false,
                        isSpeculative = false
                    )
                }
                prefetchDone.countDown()
            }.apply { isDaemon = true; start() }

            assertTrue("Primary request should start quickly", primaryArrived.await(1500L, TimeUnit.MILLISECONDS))

            // 2. Player hits the same segment urgently right away -> must accelerate normal prefetch hedge from 400ms to ~150ms
            val urgentResult = BtrLiveEngine.downloadLiveSegment(
                session = session,
                url = segUrl,
                urgent = true,
                isSpeculative = false
            )
            assertEquals("localhost", urgentResult.host)
            assertTrue(prefetchDone.await(1500L, TimeUnit.MILLISECONDS))
            val hedgeElapsed = backupArrivedAt.get() - prefetchStartedAt
            assertTrue(
                "Normal prefetch must accelerate hedge from 400ms to ~150ms when hit by urgent request (actual=${hedgeElapsed}ms)",
                hedgeElapsed in 100L..350L
            )

            // 3. Background prefetch completion must NOT spam notifyRewrite
            assertEquals("Background prefetch / downloadLiveSegment alone must not emit notifyRewrite", 0, reportedEvents.size)

            // 4. Only when player actually consumes via pipeSegment should notifyRewrite fire (and only once per segment)
            val encoded = URLEncoder.encode(segUrl, "UTF-8")
            BtrLiveEngine.pipeSegment(
                session = session,
                requestPath = "/stream/$sid/segment.m4s?url=$encoded",
                headers = emptyMap(),
                clientOut = ByteArrayOutputStream()
            )
            assertEquals("Consuming segment via pipeSegment must emit notifyRewrite once", 1, reportedEvents.size)

            // Re-reading the same cached segment must not duplicate notifyRewrite
            BtrLiveEngine.pipeSegment(
                session = session,
                requestPath = "/stream/$sid/segment.m4s?url=$encoded",
                headers = emptyMap(),
                clientOut = ByteArrayOutputStream()
            )
            assertEquals("Duplicate read of already-reported segment must not spam notifyRewrite", 1, reportedEvents.size)
        } finally {
            BtrLiveEngine.notifyRewriteSink = prevSink
            if (sid.isNotBlank()) BtrSessionManager.remove(sid)
            BtrLiveEngine.clearCache()
            runCatching { primaryServer.close() }
            runCatching { backupServer.close() }
            tPrimary.interrupt()
            tBackup.interrupt()
        }
    }
}
