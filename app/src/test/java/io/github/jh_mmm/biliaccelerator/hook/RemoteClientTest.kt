package io.github.jh_mmm.biliaccelerator.hook

import android.content.ContextWrapper
import android.content.SharedPreferences
import io.github.jh_mmm.biliaccelerator.core.AcceleratorConfig
import io.github.jh_mmm.biliaccelerator.core.RewriteResult
import io.github.jh_mmm.biliaccelerator.provider.StatsProvider
import org.junit.After
import org.junit.Assert.assertEquals
import org.junit.Assert.assertFalse
import org.junit.Assert.assertNotNull
import org.junit.Assert.assertNull
import org.junit.Assert.assertTrue
import org.junit.Before
import org.junit.Test

class RemoteClientTest {

    @Before
    fun setUp() {
        RemoteClient.resetFlushStateForTesting()
    }

    @After
    fun tearDown() {
        RemoteClient.resetFlushStateForTesting()
    }

    private fun createResult(id: String): RewriteResult {
        return RewriteResult(
            changed = true,
            originalUrl = "https://upos-sz-mirrorcos.bilivideo.com/test/$id.m4s?auth=secret",
            finalUrl = "https://upos-sz-mirrorcos.bilivideo.com/test/$id.m4s?auth=secret",
            originalHost = "upos-sz-mirrorcos.bilivideo.com",
            targetHost = "upos-sz-mirrorcos.bilivideo.com",
            reason = id,
            isPcdn = false,
            isMcdn = false,
            segmentKey = id
        )
    }

    @Test
    fun testSanitizeForIpcCleansSensitiveUrls() {
        val raw = createResult("seg-100")
        val sanitized = RemoteClient.sanitizeForIpc(raw)
        assertEquals("", sanitized.originalUrl)
        assertEquals("", sanitized.finalUrl)
        assertEquals("seg-100", sanitized.segmentKey)
        assertEquals("seg-100", sanitized.reason)
        assertTrue(sanitized.changed)
    }

    @Test
    fun testHeadOrderPreservationOnRateLimitReenqueue() {
        // Bug A verification:
        // When batch [item1, item2, item3] fails to send (e.g. broadcast rate limit),
        // and new items [item4, item5] are added to pendingBatch while in flight,
        // re-enqueuing the unsent batch MUST place item1, item2, item3 back at the HEAD
        // in exact chronological order (item1, item2, item3, item4, item5),
        // instead of appending at the tail (which resulted in item4, item5, item1, item2, item3).

        val item1 = createResult("order-1")
        val item2 = createResult("order-2")
        val item3 = createResult("order-3")
        val unsentBatch = listOf(item1, item2, item3)

        // New items arrive during in-flight network call
        val item4 = createResult("order-4")
        val item5 = createResult("order-5")
        RemoteClient.addPendingBatchForTesting(item4)
        RemoteClient.addPendingBatchForTesting(item5)

        // Simulate rate-limit re-enqueue at head
        RemoteClient.reenqueueBatchAtHead(unsentBatch)

        val pending = RemoteClient.getPendingBatchForTesting()
        assertEquals(5, pending.size)

        // Verify exact head order preservation (FIFO)
        assertEquals("order-1", pending[0].reason)
        assertEquals("order-2", pending[1].reason)
        assertEquals("order-3", pending[2].reason)
        assertEquals("order-4", pending[3].reason)
        assertEquals("order-5", pending[4].reason)
    }

    @Test
    fun testMultipleBatchesReenqueuePreservesChronoOrder() {
        val batchA = listOf(createResult("batchA-1"), createResult("batchA-2"))
        val batchB = listOf(createResult("batchB-1"), createResult("batchB-2"))

        // Add batchB to tail first
        for (item in batchB) {
            RemoteClient.addPendingBatchForTesting(item)
        }

        // Re-enqueue batchA to head
        RemoteClient.reenqueueBatchAtHead(batchA)

        val pending = RemoteClient.getPendingBatchForTesting()
        assertEquals(4, pending.size)
        assertEquals("batchA-1", pending[0].reason)
        assertEquals("batchA-2", pending[1].reason)
        assertEquals("batchB-1", pending[2].reason)
        assertEquals("batchB-2", pending[3].reason)
    }

    @Test
    fun testDebounceFlushSchedulingUnderBatchMaxSize() {
        // Under BATCH_MAX_SIZE (10), notifyRewrite schedules delayed flush (FLUSH_INTERVAL_MS)
        RemoteClient.notifyRewrite(createResult("item-1"))

        assertEquals(1, RemoteClient.getPendingBatchForTesting().size)
        assertFalse(RemoteClient.isImmediateFlushScheduledForTesting())
        assertNotNull(RemoteClient.getPendingFlushFutureForTesting())
        assertFalse(RemoteClient.getPendingFlushFutureForTesting()!!.isDone)

        // Adding more items (< 10) resets/reschedules the delayed future without triggering immediate flush
        for (i in 2..5) {
            RemoteClient.notifyRewrite(createResult("item-$i"))
        }

        assertEquals(5, RemoteClient.getPendingBatchForTesting().size)
        assertFalse(RemoteClient.isImmediateFlushScheduledForTesting())
        assertNotNull(RemoteClient.getPendingFlushFutureForTesting())
    }

    @Test
    fun testDeduplicatedImmediateFlushSchedulingOnBatchThreshold() {
        // Bug B verification:
        // When batch reaches BATCH_MAX_SIZE (10), pendingFlushFuture must be atomically cancelled/cleared,
        // and immediate flush must be scheduled. Rapid successive arrivals while immediate flush is pending
        // must NOT double-trigger multiple back-to-back flush tasks.

        // Add 9 items (below threshold, delayed future is active)
        for (i in 1..9) {
            RemoteClient.notifyRewrite(createResult("threshold-$i"))
        }
        val delayedFuture = RemoteClient.getPendingFlushFutureForTesting()
        assertNotNull("Delayed future should be scheduled for < 10 items", delayedFuture)
        assertFalse(delayedFuture!!.isCancelled)
        assertFalse(RemoteClient.isImmediateFlushScheduledForTesting())

        // 10th item triggers BATCH_MAX_SIZE threshold; hold the single-threaded reportScheduler
        // so we can deterministically inspect the queued immediate flush state before it runs.
        val started = java.util.concurrent.CountDownLatch(1)
        val release = java.util.concurrent.CountDownLatch(1)
        RemoteClient.pauseReportSchedulerForTesting(started, release)
        try {
            assertTrue("Scheduler barrier should engage", started.await(2, java.util.concurrent.TimeUnit.SECONDS))

            RemoteClient.notifyRewrite(createResult("threshold-10"))

            // Delayed future MUST be cancelled and cleared atomically
            assertTrue("Delayed future must be cancelled on threshold reach", delayedFuture.isCancelled)
            assertNull("pendingFlushFuture must be cleared to null", RemoteClient.getPendingFlushFutureForTesting())

            // Immediate flush must be scheduled
            assertTrue(RemoteClient.isImmediateFlushScheduledForTesting())

            // Add items 11..15 in rapid succession before flush runs
            for (i in 11..15) {
                RemoteClient.notifyRewrite(createResult("threshold-$i"))
            }

            // isImmediateFlushScheduled must remain true and deduplicated
            assertTrue(RemoteClient.isImmediateFlushScheduledForTesting())
            assertNull("No delayed future should be scheduled while immediate flush is pending", RemoteClient.getPendingFlushFutureForTesting())
        } finally {
            release.countDown()
        }

        // Wait for scheduler to execute
        RemoteClient.awaitReportSchedulerForTesting()

        // After flush executes, isImmediateFlushScheduled returns to false
        assertFalse(RemoteClient.isImmediateFlushScheduledForTesting())
    }

    @Test
    fun testFlushBatchGuardPreventsConcurrentExecution() {
        assertFalse(RemoteClient.isFlushingForTesting())

        // When queue is empty, direct flushBatch does nothing and isFlushing returns false
        RemoteClient.flushBatchDirectForTesting()
        assertFalse(RemoteClient.isFlushingForTesting())
    }

    private class InMemorySharedPreferences : SharedPreferences {
        val data = LinkedHashMap<String, Any?>()

        override fun getAll(): MutableMap<String, *> = LinkedHashMap(data)
        override fun getString(key: String?, defValue: String?): String? = (data[key] as? String) ?: defValue
        @Suppress("UNCHECKED_CAST")
        override fun getStringSet(key: String?, defValues: MutableSet<String>?): MutableSet<String>? =
            (data[key] as? MutableSet<String>) ?: defValues
        override fun getInt(key: String?, defValue: Int): Int = (data[key] as? Int) ?: defValue
        override fun getLong(key: String?, defValue: Long): Long = (data[key] as? Long) ?: defValue
        override fun getFloat(key: String?, defValue: Float): Float = (data[key] as? Float) ?: defValue
        override fun getBoolean(key: String?, defValue: Boolean): Boolean = (data[key] as? Boolean) ?: defValue
        override fun contains(key: String?): Boolean = data.containsKey(key)
        override fun registerOnSharedPreferenceChangeListener(listener: SharedPreferences.OnSharedPreferenceChangeListener?) {}
        override fun unregisterOnSharedPreferenceChangeListener(listener: SharedPreferences.OnSharedPreferenceChangeListener?) {}

        override fun edit(): SharedPreferences.Editor = object : SharedPreferences.Editor {
            private val pending = LinkedHashMap<String, Any?>()
            private var clearAll = false

            override fun putString(key: String?, value: String?) = apply { if (key != null) pending[key] = value }
            override fun putStringSet(key: String?, values: MutableSet<String>?) = apply { if (key != null) pending[key] = values }
            override fun putInt(key: String?, value: Int) = apply { if (key != null) pending[key] = value }
            override fun putLong(key: String?, value: Long) = apply { if (key != null) pending[key] = value }
            override fun putFloat(key: String?, value: Float) = apply { if (key != null) pending[key] = value }
            override fun putBoolean(key: String?, value: Boolean) = apply { if (key != null) pending[key] = value }
            override fun remove(key: String?) = apply { if (key != null) pending[key] = null }
            override fun clear() = apply { clearAll = true }
            override fun commit(): Boolean {
                apply()
                return true
            }
            override fun apply() {
                if (clearAll) data.clear()
                for ((k, v) in pending) {
                    if (v == null) data.remove(k) else data[k] = v
                }
            }
        }
    }

    private fun createPrefsContext(prefsMap: MutableMap<String, InMemorySharedPreferences> = HashMap()): ContextWrapper {
        return object : ContextWrapper(null) {
            override fun getSharedPreferences(name: String?, mode: Int): SharedPreferences {
                return prefsMap.getOrPut(name ?: "default") { InMemorySharedPreferences() }
            }
            override fun getApplicationContext() = this
        }
    }

    @Test
    fun testStatsProviderPersistsSelectionModeAndStallRecovery() {
        val prefsMap = HashMap<String, InMemorySharedPreferences>()
        val ctx = createPrefsContext(prefsMap)

        // Default values before anything is saved
        val defaults = StatsProvider.loadConfig(ctx)
        assertEquals("auto", defaults.selectionMode)
        assertTrue(defaults.stallRecovery)

        // Save manual mode with stallRecovery=false and legacy mcdnStrategy="replace"
        val custom = AcceleratorConfig(
            enabled = true,
            mcdnStrategy = "replace",
            selectionMode = "fixed",
            stallRecovery = false
        )
        StatsProvider.saveConfig(ctx, custom)

        val reloaded = StatsProvider.loadConfig(ctx)
        assertEquals("manual", reloaded.selectionMode)
        assertFalse(reloaded.stallRecovery)
        assertEquals("proxy-all", reloaded.mcdnStrategy)

        // Toggle back to auto + stallRecovery=true
        StatsProvider.saveConfig(ctx, reloaded.copy(selectionMode = "auto", stallRecovery = true))
        val reloadedAuto = StatsProvider.loadConfig(ctx)
        assertEquals("auto", reloadedAuto.selectionMode)
        assertTrue(reloadedAuto.stallRecovery)

        // Blank persisted targetHost/proxyHost are normalized back to safe defaults on loadConfig
        prefsMap[StatsProvider.PREFS_CONFIG]!!.edit()
            .putString(StatsProvider.KEY_TARGET_HOST, "   ")
            .putString(StatsProvider.KEY_PROXY_HOST, "")
            .apply()
        val normalizedBlank = StatsProvider.loadConfig(ctx)
        assertEquals("upos-sz-mirrorcos.bilivideo.com", normalizedBlank.targetHost)
        assertEquals("proxy-tf-all-ws.bilivideo.com", normalizedBlank.proxyHost)
    }

    @Test
    fun testRemoteClientNormalizesConfigAcrossParseCacheAndDiskWrite() {
        val prefsMap = HashMap<String, InMemorySharedPreferences>()
        val ctx = createPrefsContext(prefsMap)

        // 1. parseConfigJson normalizes both mcdnStrategy and selectionMode
        val parsed = RemoteClient.parseConfigJson(
            """{"enabled":true,"mcdnStrategy":"replace","selectionMode":"fixed","stallRecovery":false}"""
        )
        assertNotNull(parsed)
        assertEquals("proxy-all", parsed!!.mcdnStrategy)
        assertEquals("manual", parsed.selectionMode)
        assertFalse(parsed.stallRecovery)

        // 2. saveConfigToLocalCache writes normalized JSON to disk
        RemoteClient.saveConfigToLocalCache(
            ctx,
            AcceleratorConfig(mcdnStrategy = "replace", selectionMode = "fixed", stallRecovery = false)
        )
        val rawStored = prefsMap["bili_accelerator_target_cache"]?.getString("cached_config_json", null)
        assertNotNull(rawStored)
        assertTrue("Disk cache must store normalized mcdnStrategy", rawStored!!.contains("\"mcdnStrategy\":\"proxy-all\""))
        assertTrue("Disk cache must store normalized selectionMode", rawStored.contains("\"selectionMode\":\"manual\""))
        assertFalse("Disk cache must not store legacy replace", rawStored.contains("\"replace\""))

        // 3. loadConfigFromLocalCache normalizes legacy un-normalized JSON and rewrites it to disk
        prefsMap["bili_accelerator_target_cache"]!!.edit()
            .putString(
                "cached_config_json",
                """{"enabled":true,"mcdnStrategy":"replace","selectionMode":"FIXED","stallRecovery":false}"""
            )
            .apply()
        RemoteClient.loadConfigFromLocalCache(ctx)
        val loaded = RemoteClient.getCachedConfig()
        assertEquals("proxy-all", loaded.mcdnStrategy)
        assertEquals("manual", loaded.selectionMode)
        assertFalse(loaded.stallRecovery)

        val rewrittenStored = prefsMap["bili_accelerator_target_cache"]!!.getString("cached_config_json", null)!!
        assertTrue(rewrittenStored.contains("\"mcdnStrategy\":\"proxy-all\""))
        assertTrue(rewrittenStored.contains("\"selectionMode\":\"manual\""))
    }
}
