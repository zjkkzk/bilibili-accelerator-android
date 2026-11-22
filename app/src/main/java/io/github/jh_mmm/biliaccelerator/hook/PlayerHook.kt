package io.github.jh_mmm.biliaccelerator.hook

import android.net.Uri
import android.util.Log
import io.github.jh_mmm.biliaccelerator.btr.BtrSessionManager
import io.github.jh_mmm.biliaccelerator.core.AcceleratorConfig
import io.github.jh_mmm.biliaccelerator.core.AutoVodRouter
import io.github.jh_mmm.biliaccelerator.core.BiliAcceleratorCore
import io.github.jh_mmm.biliaccelerator.core.PlayinfoRegistry
import io.github.jh_mmm.biliaccelerator.core.PlayinfoTrackEntry
import io.github.jh_mmm.biliaccelerator.core.RewriteResult
import io.github.libxposed.api.XposedModule
import java.io.FileDescriptor
import java.lang.ref.ReferenceQueue
import java.lang.ref.WeakReference
import java.lang.reflect.Constructor
import java.lang.reflect.Method
import java.util.Collections
import java.util.IdentityHashMap
import java.util.LinkedHashMap
import java.util.WeakHashMap
import java.util.concurrent.ConcurrentHashMap
import kotlin.math.max

object PlayerHook {

    private const val TAG = "BiliAccelerator-Player"

    private const val MEDIA_PREPARED = 1
    private const val MEDIA_PLAYBACK_COMPLETE = 2
    private const val MEDIA_BUFFERING_UPDATE = 3
    private const val MEDIA_ERROR = 100
    private const val MEDIA_INFO = 200
    private const val MEDIA_INFO_VIDEO_RENDERING_START = 3
    private const val MEDIA_INFO_BUFFERING_START = 701
    private const val MEDIA_INFO_BUFFERING_END = 702
    private const val MEDIA_INFO_NETWORK_BANDWIDTH = 703

    @Volatile
    var isHooked: Boolean = false
        internal set

    private val hookedBuilderClasses = Collections.newSetFromMap(WeakHashMap<Class<*>, Boolean>())
    private val hookedPlayerClasses = Collections.newSetFromMap(WeakHashMap<Class<*>, Boolean>())
    private val hookedMethods = Collections.newSetFromMap(WeakHashMap<Method, Boolean>())
    private val hookedConstructors = Collections.newSetFromMap(WeakHashMap<Constructor<*>, Boolean>())

    /**
     * 基于对象引用恒等性 (===) 与 WeakReference 的线程安全映射，
     * 防止目标 Builder/Player 类重写 equals/hashCode 导致的状态丢失或错乱。
     */
    private class IdentityWeakMap<V : Any> {
        private class WeakIdentityKey(
            referent: Any,
            queue: ReferenceQueue<Any>? = null
        ) : WeakReference<Any>(referent, queue) {
            val identityHash: Int = System.identityHashCode(referent)
            override fun hashCode(): Int = identityHash
            override fun equals(other: Any?): Boolean {
                if (this === other) return true
                if (other !is WeakIdentityKey) return false
                val a = get() ?: return false
                val b = other.get() ?: return false
                return a === b
            }
        }

        private val queue = ReferenceQueue<Any>()
        private val map = ConcurrentHashMap<WeakIdentityKey, V>()

        private fun expungeStale() {
            while (true) {
                val ref = queue.poll() ?: break
                @Suppress("UNCHECKED_CAST")
                map.remove(ref as WeakIdentityKey)
            }
        }

        operator fun get(key: Any?): V? {
            if (key == null) return null
            expungeStale()
            return map[WeakIdentityKey(key)]
        }

        operator fun set(key: Any, value: V): V? {
            expungeStale()
            return map.put(WeakIdentityKey(key, queue), value)
        }

        fun remove(key: Any?): V? {
            if (key == null) return null
            expungeStale()
            return map.remove(WeakIdentityKey(key))
        }

        fun containsKey(key: Any?): Boolean = get(key) != null

        fun entriesSnapshot(): List<Pair<Any, V>> {
            expungeStale()
            val out = ArrayList<Pair<Any, V>>()
            for ((k, v) in map.entries) {
                val referent = k.get()
                if (referent != null) {
                    out.add(referent to v)
                }
            }
            return out
        }
    }

    private data class AutoVodBinding(
        val cid: String,
        val activeUrl: String,
        val isAudio: Boolean = false,
        val isDirectDataSource: Boolean = false
    )

    private val STREAM_SESSION_ID_REGEX = Regex("""/stream/([a-zA-Z0-9_-]+)(?:/|\.[a-zA-Z0-9]+|$)""")

    private val builderPendingBackups = IdentityWeakMap<List<String>>()
    private val builderRawBackups = IdentityWeakMap<List<String>>()
    private val builderPrimaryUrl = IdentityWeakMap<String>()
    private val builderDeferredBackupMethod = IdentityWeakMap<Method>()
    private val builderLastBackupMethod = IdentityWeakMap<Method>()
    private val builderNativeConvergedToBtr = IdentityWeakMap<Boolean>()
    private val builderResolvedNonBtr = IdentityWeakMap<Boolean>()
    private val builderCleanLiveUrls = IdentityWeakMap<List<String>>()
    private val builderBtrUrl = IdentityWeakMap<String>()
    private val builderRequestHeaders = IdentityWeakMap<MutableMap<String, String>>()
    private val builderAutoVod = IdentityWeakMap<AutoVodBinding>()
    private val builderInflightHandle = IdentityWeakMap<AutoVodRouter.InflightRequestHandle>()
    private val segmentToSessionId = IdentityWeakMap<String>()
    private val segmentToBuilder = IdentityWeakMap<Any>()
    private val segmentToAutoVod = IdentityWeakMap<AutoVodBinding>()
    private val playerToSessionId = IdentityWeakMap<String>()
    private val playerToAutoVod = IdentityWeakMap<AutoVodBinding>()
    private val playerToInflightHandle = IdentityWeakMap<AutoVodRouter.InflightRequestHandle>()
    private val playerResolvedNonBtr = IdentityWeakMap<Boolean>()
    private val suppressBackupHook = ThreadLocal.withInitial { false }
    private val livePathCleanUrls = object : LinkedHashMap<String, List<String>>(16, 0.75f, true) {
        override fun removeEldestEntry(eldest: MutableMap.MutableEntry<String, List<String>>?): Boolean = size > 32
    }

    private val autoVodHostListener = AutoVodRouter.ActiveHostListener { cid, oldHost, newHost, newUrl, trigger ->
        onAutoVodActiveHostChanged(cid, oldHost, newHost, newUrl, trigger)
    }

    internal fun ensureAutoVodListenerRegistered() {
        AutoVodRouter.addActiveHostListener(autoVodHostListener)
    }

    /**
     * 响应 AutoVodRouter 竞速切源事件：
     * 1. 更新绑定到同一 cid 的 Builder 备用列表（将胜出节点 newUrl 提权置顶并按优先级重排）；
     * 2. 更新绑定到同一 cid 的 Player 活跃 URL，确保下一次重试/切段/卡顿恢复立即路由至新节点。
     */
    internal fun onAutoVodActiveHostChanged(
        cid: String,
        oldHost: String,
        newHost: String,
        newUrl: String?,
        trigger: String
    ) {
        if (cid.isBlank() || newHost.isBlank()) return
        val cfg = RemoteClient.getCachedConfig()
        for ((builder, binding) in builderAutoVod.entriesSnapshot()) {
            if (binding.cid != cid || binding.isAudio) continue
            if (!newUrl.isNullOrBlank()) {
                builderAutoVod[builder] = binding.copy(activeUrl = newUrl)
                builderPrimaryUrl[builder] = newUrl
                val currentBackups = builderPendingBackups[builder].orEmpty()
                val merged = (listOf(newUrl) + currentBackups).distinct()
                val prioritized = AutoVodRouter.prioritizeBackups(newUrl, merged, cfg)
                builderPendingBackups[builder] = prioritized
                applyBuilderBackupUrls(builder, prioritized, builderLastBackupMethod[builder])
            }
        }
        for ((player, binding) in playerToAutoVod.entriesSnapshot()) {
            if (binding.cid != cid || binding.isAudio) continue
            if (!newUrl.isNullOrBlank()) {
                playerToAutoVod[player] = binding.copy(activeUrl = newUrl)
                val currentBackups = builderPendingBackups[player].orEmpty()
                if (currentBackups.isNotEmpty()) {
                    val merged = (listOf(newUrl) + currentBackups).distinct()
                    builderPendingBackups[player] = AutoVodRouter.prioritizeBackups(newUrl, merged, cfg)
                }
            }
        }
        Log.i(TAG, "AutoVod active host updated for cid=$cid: $oldHost -> $newHost [trigger=$trigger]")
    }

    internal fun getTrackedAutoVodUrl(target: Any?): String? {
        if (target == null) return null
        return playerToAutoVod[target]?.activeUrl ?: builderAutoVod[target]?.activeUrl
    }

    fun triggerManualRetest(config: AcceleratorConfig = RemoteClient.fetchConfig()): Boolean {
        ensureAutoVodListenerRegistered()
        return AutoVodRouter.triggerManualRetest(config)
    }

    internal fun resolveProxyStreamExtension(url: String): String {
        val pathOnly = (BiliAcceleratorCore.parseUri(url)?.path
            ?: url.substringBefore('?').substringBefore('#')).trim().lowercase()
        return when {
            pathOnly.endsWith(".m3u8") -> "m3u8"
            pathOnly.endsWith(".flv") -> "flv"
            else -> "m4s"
        }
    }

    internal fun computeNativeBackupsForSession(
        btrUrl: String?,
        distinctFiltered: List<String>,
        btrConfigEnabled: Boolean
    ): List<String> {
        if (!btrConfigEnabled) return distinctFiltered
        return if (!btrUrl.isNullOrBlank()) listOf(btrUrl) else emptyList()
    }

    private fun convertBackupsForParamType(paramType: Class<*>, backups: List<String>): Any {
        return when {
            paramType.isArray -> backups.toTypedArray()
            java.util.SortedSet::class.java.isAssignableFrom(paramType) -> java.util.TreeSet(backups)
            java.util.Set::class.java.isAssignableFrom(paramType) -> LinkedHashSet(backups)
            java.util.LinkedList::class.java.isAssignableFrom(paramType) -> java.util.LinkedList(backups)
            else -> ArrayList(backups)
        }
    }

    private inline fun <T> withBackupHookSuppressed(block: () -> T): T {
        val prev = suppressBackupHook.get() ?: false
        suppressBackupHook.set(true)
        return try {
            block()
        } finally {
            suppressBackupHook.set(prev)
        }
    }

    internal fun applyBuilderBackupUrls(
        builder: Any,
        backups: List<String>,
        preferredMethod: Method? = null
    ): Boolean {
        val targetMethod = preferredMethod ?: (builder.javaClass.declaredMethods + builder.javaClass.methods)
            .firstOrNull { it.name == "setBackupUrls" && it.parameterTypes.size == 1 }
            ?: return false
        val convertedArg = convertBackupsForParamType(targetMethod.parameterTypes[0], backups)
        return withBackupHookSuppressed {
            try {
                targetMethod.isAccessible = true
                targetMethod.invoke(builder, convertedArg)
                true
            } catch (_: Throwable) {
                false
            }
        }
    }

    internal fun clearBuilderBtrState(builder: Any?) {
        if (builder == null) return
        builderBtrUrl.remove(builder)
        builderResolvedNonBtr.remove(builder)
        playerToSessionId.remove(builder)
        BtrSessionManager.unbindBuilderSession(builder)
    }

    internal fun flushDeferredBackups(builder: Any?, btrUrl: String?, config: AcceleratorConfig? = null) {
        if (builder == null) return
        val deferredMethod = builderDeferredBackupMethod.remove(builder)
        val fallbackMethod = deferredMethod ?: builderLastBackupMethod[builder]
        val pendingBackups = builderPendingBackups[builder]
        val wasConvergedToBtr = builderNativeConvergedToBtr.remove(builder) == true
        if (!btrUrl.isNullOrBlank()) {
            if (deferredMethod != null || pendingBackups != null) {
                if (applyBuilderBackupUrls(builder, listOf(btrUrl), fallbackMethod)) {
                    builderNativeConvergedToBtr[builder] = true
                }
            }
        } else if ((deferredMethod != null || wasConvergedToBtr) && pendingBackups != null) {
            val effectiveBackups = if (config != null && AutoVodRouter.shouldUseAutoRouting(config)) {
                val primary = builderPrimaryUrl[builder] ?: pendingBackups.firstOrNull().orEmpty()
                AutoVodRouter.prioritizeBackups(primary, pendingBackups, config)
            } else {
                pendingBackups
            }
            applyBuilderBackupUrls(builder, effectiveBackups, fallbackMethod)
        }
    }

    internal fun flushDeferredBackupsBeforeBuild(builder: Any?) {
        if (builder != null && builderDeferredBackupMethod.containsKey(builder)) {
            flushDeferredBackups(builder, builderBtrUrl[builder])
        }
    }

    internal fun onSegmentBuilt(builder: Any?, builtSegment: Any?) {
        if (builder == null || builtSegment == null) return
        segmentToBuilder[builtSegment] = builder
        val sid = BtrSessionManager.getSessionForBuilder(builder)
        if (!sid.isNullOrBlank()) {
            segmentToSessionId[builtSegment] = sid
        }
        val autoBinding = builderAutoVod[builder]
        if (autoBinding != null) {
            segmentToAutoVod[builtSegment] = autoBinding
        }
    }

    private fun trackAutoVodSelection(
        target: Any?,
        activeUrl: String,
        playinfoTrack: PlayinfoTrackEntry?,
        isDirectDataSource: Boolean
    ) {
        ensureAutoVodListenerRegistered()
        val isAudio = playinfoTrack?.isAudio == true || BiliAcceleratorCore.isAudioTrack(activeUrl)
        val fileKey = AutoVodRouter.fileKey(activeUrl) ?: playinfoTrack?.fileKey
        val cid = playinfoTrack?.cid?.takeIf { it > 0L }?.toString()
            ?: AutoVodRouter.cidOf(fileKey)
            ?: fileKey
            ?: return
        val binding = AutoVodBinding(
            cid = cid,
            activeUrl = activeUrl,
            isAudio = isAudio,
            isDirectDataSource = isDirectDataSource
        )
        val requiredBps = playinfoTrack?.bandwidth?.takeIf { it > 0L }?.toDouble() ?: 0.0
        val handle = if (!isAudio) {
            runCatching { AutoVodRouter.beginInflightRequest(url = activeUrl, requiredBps = requiredBps) }.getOrNull()
        } else {
            null
        }
        if (target != null) {
            if (isDirectDataSource) {
                playerToAutoVod[target] = binding
                playerResolvedNonBtr[target] = true
                if (handle != null) {
                    playerToInflightHandle[target] = handle
                }
            } else {
                builderAutoVod[target] = binding
                if (handle != null) {
                    builderInflightHandle[target] = handle
                }
            }
        }
    }

    internal fun interceptSetBackupUrls(
        builder: Any?,
        method: Method,
        rawArg: Any?,
        config: AcceleratorConfig = RemoteClient.fetchConfig(),
        onRewrite: ((RewriteResult) -> Unit)? = { RemoteClient.notifyRewrite(it) },
        proceedOriginal: () -> Any?,
        proceedWithArg: (Any) -> Any?
    ): Any? {
        if (suppressBackupHook.get() == true) {
            return proceedOriginal()
        }
        val rawList: List<String>? = when (rawArg) {
            is Collection<*> -> rawArg.filterIsInstance<String>()
            is Array<*> -> rawArg.filterIsInstance<String>()
            else -> null
        }
        if (rawList == null || rawList.isEmpty()) {
            builder?.let {
                builderDeferredBackupMethod.remove(it)
                builderNativeConvergedToBtr.remove(it)
                builderPendingBackups.remove(it)
                builderRawBackups.remove(it)
            }
            return withBackupHookSuppressed { proceedOriginal() }
        }

        if (!config.enabled) {
            builder?.let {
                builderDeferredBackupMethod.remove(it)
                builderNativeConvergedToBtr.remove(it)
                builderPendingBackups.remove(it)
                builderRawBackups.remove(it)
            }
            return withBackupHookSuppressed { proceedOriginal() }
        }

        val hasDrmBackup = rawList.any { rawUrl ->
            PlayinfoRegistry.lookup(rawUrl)?.hasDrm == true ||
                BiliAcceleratorCore.hasDrmSignal(rawUrl)
        }
        if (hasDrmBackup) {
            builder?.let {
                builderDeferredBackupMethod.remove(it)
                builderNativeConvergedToBtr.remove(it)
                builderPendingBackups.remove(it)
                builderRawBackups.remove(it)
                clearBuilderBtrState(it)
                builderResolvedNonBtr[it] = true
            }
            return withBackupHookSuppressed { proceedOriginal() }
        }

        val distinctRaw = rawList.distinct()
        val effectiveConfig = AutoVodRouter.effectiveRewriteConfig(config)
        val filteredList = if (
            effectiveConfig.blockPcdn || effectiveConfig.proxyMcdn || effectiveConfig.forceUpos || (effectiveConfig.rewriteAkamai && effectiveConfig.enableBtr)
        ) {
            filterAndFillBackupUrls(rawList, effectiveConfig, onRewrite)
        } else {
            distinctRaw
        }
        val distinctFiltered = if (AutoVodRouter.shouldUseAutoRouting(config)) {
            ensureAutoVodListenerRegistered()
            val primaryForOrder = builder?.let { builderPrimaryUrl[it] } ?: filteredList.firstOrNull().orEmpty()
            AutoVodRouter.prioritizeBackups(primaryForOrder, filteredList, config)
        } else {
            filteredList
        }

        builder?.let {
            builderRawBackups[it] = distinctRaw
            builderPendingBackups[it] = distinctFiltered
            builderLastBackupMethod[it] = method
            val cleanLiveList = distinctFiltered.filter { cand ->
                val host = BiliAcceleratorCore.normalizeHost(BiliAcceleratorCore.parseUri(cand)?.host)
                BiliAcceleratorCore.isLiveMediaUrl("", cand) &&
                    !BiliAcceleratorCore.isSlowLiveHost(host, cand, config.portHeuristic)
            }
            if (cleanLiveList.isNotEmpty()) {
                builderCleanLiveUrls[it] = cleanLiveList
                for (cand in cleanLiveList) {
                    recordCleanLiveUrl(cand)
                }
            }
        }

        val rawBoundSessionId = builder?.let { BtrSessionManager.getSessionForBuilder(it) }
        val boundSessionId = rawBoundSessionId?.takeIf { BtrSessionManager.get(it) != null }
        if (builder != null && rawBoundSessionId != null && boundSessionId == null) {
            clearBuilderBtrState(builder)
            builderResolvedNonBtr[builder] = true
        }
        val btrConfigActive = config.enableBtr
        if (btrConfigActive) {
            if (boundSessionId != null) {
                BtrSessionManager.updateBackups(boundSessionId, distinctFiltered)
                val btrUrl = builderBtrUrl[builder]
                val nativeBackups = computeNativeBackupsForSession(btrUrl, distinctFiltered, true)
                builderDeferredBackupMethod.remove(builder)
                val res = withBackupHookSuppressed {
                    proceedWithArg(convertBackupsForParamType(method.parameterTypes[0], nativeBackups))
                }
                builderNativeConvergedToBtr[builder] = true
                return res
            } else if (builder != null && builderResolvedNonBtr[builder] != true) {
                // Builder 构建期 setBackupUrls 早于 setUrl：暂缓原生调用，待 setUrl 判定是否走 BTR 后再统一收敛或放行
                builderDeferredBackupMethod[builder] = method
                val retType = method.returnType
                if (retType == Void.TYPE) {
                    return null
                }
                if (retType.isAssignableFrom(builder.javaClass)) {
                    return builder
                }
                return withBackupHookSuppressed {
                    proceedWithArg(convertBackupsForParamType(method.parameterTypes[0], emptyList()))
                }
            } else if (builder == null) {
                BtrSessionManager.updateBackupsByCandidateUrls(distinctFiltered)
            }
        }

        if (builder != null) {
            builderDeferredBackupMethod.remove(builder)
            builderNativeConvergedToBtr.remove(builder)
        }
        if (distinctFiltered.isNotEmpty()) {
            return withBackupHookSuppressed {
                proceedWithArg(convertBackupsForParamType(method.parameterTypes[0], distinctFiltered))
            }
        }
        return withBackupHookSuppressed { proceedOriginal() }
    }

    private fun normalizeLivePathKey(url: String): String {
        return BiliAcceleratorCore.parseUri(url)?.path?.trim()?.trimEnd('/')?.lowercase().orEmpty()
    }

    private fun recordCleanLiveUrl(url: String) {
        val pathKey = normalizeLivePathKey(url)
        if (pathKey.isBlank()) return
        synchronized(livePathCleanUrls) {
            val existing = livePathCleanUrls[pathKey].orEmpty()
            if (!existing.contains(url)) {
                livePathCleanUrls[pathKey] = (existing + url).take(8)
            }
        }
    }

    private fun findCleanLiveUrlForUrl(
        url: String,
        builderInstance: Any?,
        portHeuristic: Boolean
    ): String? {
        val targetPath = normalizeLivePathKey(url)
        val builderCandidates = builderInstance?.let { builderCleanLiveUrls[it] }.orEmpty()
        val pathCandidates = if (targetPath.isNotBlank()) {
            synchronized(livePathCleanUrls) { livePathCleanUrls[targetPath].orEmpty() }
        } else emptyList()
        val combined = (builderCandidates + pathCandidates + PlayinfoRegistry.lookupLiveBackups(url)).distinct()
        return combined.firstOrNull { cand ->
            val candPath = normalizeLivePathKey(cand)
            val host = BiliAcceleratorCore.normalizeHost(BiliAcceleratorCore.parseUri(cand)?.host)
            (targetPath.isBlank() || candPath == targetPath) &&
                !BiliAcceleratorCore.isSlowLiveHost(host, cand, portHeuristic)
        }
    }

    // Both spellings are observed across different Bilibili versions (Assert vs Asset)
    private val SEGMENT_BUILDER_CLASSES = listOf(
        "tv.danmaku.ijk.media.player.IjkMediaAsset\$MediaAssertSegment\$Builder",
        "tv.danmaku.ijk.media.player.IjkMediaAsset\$MediaAssetSegment\$Builder",
        "tv.danmaku.ijk.media.player.IjkMediaAsset\$MediaSegment\$Builder"
    )

    fun init(module: XposedModule, classLoader: ClassLoader) {
        ensureAutoVodListenerRegistered()
        var hooked = false
        for (className in SEGMENT_BUILDER_CLASSES) {
            val builderClass = try {
                classLoader.loadClass(className)
            } catch (_: Throwable) {
                null
            } ?: continue

            hookSegmentBuilder(module, builderClass)
            hooked = true
            HookStatusTracker.recordStatus("player_segment", className)
            Log.i(TAG, "Successfully hooked player segment builder: $className")
        }

        // Fallback: Hook IjkMediaPlayer directly if segment builder is missing or bypassed
        val fallbackHooked = hookFallbackPlayer(module, classLoader)
        if (fallbackHooked) {
            hooked = true
        }

        if (hooked) {
            isHooked = true
        } else {
            Log.i(TAG, "Standard IjkMediaAsset builder not found, relying on player fallback hooks")
        }
    }

    private fun hookSegmentBuilder(module: XposedModule, clazz: Class<*>) {
        synchronized(hookedBuilderClasses) {
            if (!hookedBuilderClasses.add(clazz)) return
        }
        // 1. Hook all constructors (distinct + 全局去重防止继承方法重复注入)
        val constructors = (clazz.declaredConstructors + clazz.constructors).distinct()
        for (constructor in constructors) {
            if (!synchronized(hookedConstructors) { hookedConstructors.add(constructor) }) continue
            try {
                module.hook(constructor).intercept { chain ->
                    val newArgs = rewriteArgIfMedia(chain.args, chain.thisObject, methodName = "<init>")
                    if (newArgs != null) {
                        chain.proceed(newArgs)
                    } else {
                        chain.proceed()
                    }
                }
            } catch (t: Throwable) {
                Log.d(TAG, "Could not hook constructor: ${t.message}")
            }
        }

        // 2. Hook setter methods: setUrl, setBaseUrl, setResolveUrl
        val setterNames = setOf("setUrl", "setBaseUrl", "setResolveUrl")
        val methods = (clazz.declaredMethods + clazz.methods).distinct()
        for (m in methods) {
            if (m.name in setterNames) {
                if (!synchronized(hookedMethods) { hookedMethods.add(m) }) continue
                try {
                    module.hook(m).intercept { chain ->
                        val newArgs = rewriteArgIfMedia(chain.args, chain.thisObject, methodName = m.name)
                        if (newArgs != null) {
                            chain.proceed(newArgs)
                        } else {
                            chain.proceed()
                        }
                    }
                } catch (_: Throwable) {}
            }
        }

        // 3. Hook setBackupUrls(Collection/List or Array)
        val backupMethods = (clazz.declaredMethods + clazz.methods).distinct().filter {
            it.name == "setBackupUrls" && it.parameterTypes.size == 1
        }

        for (method in backupMethods) {
            if (!synchronized(hookedMethods) { hookedMethods.add(method) }) continue
            try {
                module.hook(method).intercept { chain ->
                    interceptSetBackupUrls(
                        builder = chain.thisObject,
                        method = method,
                        rawArg = chain.args.firstOrNull(),
                        proceedOriginal = { chain.proceed() },
                        proceedWithArg = { convertedArg ->
                            val newArgs = chain.args.toTypedArray().apply { this[0] = convertedArg }
                            chain.proceed(newArgs)
                        }
                    )
                }
            } catch (t: Throwable) {
                Log.w(TAG, "Failed to hook ${method.name}: ${t.message}")
            }
        }

        // 3.5 捕获 Builder 上设置的自定义 HTTP Header / User-Agent / Referer，避免 BTR 请求丢失客户端特征头导致 403
        val headerMapSetters = methods.filter {
            (it.name == "setHeaders" || it.name == "setHttpHeaders" || it.name == "addHeaders") &&
                it.parameterTypes.size == 1 && Map::class.java.isAssignableFrom(it.parameterTypes[0])
        }
        for (m in headerMapSetters) {
            if (!synchronized(hookedMethods) { hookedMethods.add(m) }) continue
            try {
                module.hook(m).intercept { chain ->
                    val b = chain.thisObject
                    val rawMap = chain.args.firstOrNull() as? Map<*, *>
                    if (b != null && rawMap != null && rawMap.isNotEmpty()) {
                        val existing = builderRequestHeaders[b] ?: LinkedHashMap<String, String>().also { builderRequestHeaders[b] = it }
                        for ((k, v) in rawMap) {
                            val ks = k?.toString()?.trim()?.takeIf { it.isNotEmpty() } ?: continue
                            val vs = v?.toString()?.trim()?.takeIf { it.isNotEmpty() } ?: continue
                            existing[ks] = vs
                        }
                    }
                    chain.proceed()
                }
            } catch (_: Throwable) {}
        }

        val singleHeaderSetters = methods.filter {
            (it.name == "setUserAgent" || it.name == "setReferer") &&
                it.parameterTypes.size == 1 && it.parameterTypes[0] == String::class.java
        }
        for (m in singleHeaderSetters) {
            if (!synchronized(hookedMethods) { hookedMethods.add(m) }) continue
            try {
                val hdrKey = if (m.name == "setUserAgent") "User-Agent" else "Referer"
                module.hook(m).intercept { chain ->
                    val b = chain.thisObject
                    val value = (chain.args.firstOrNull() as? String)?.trim()?.takeIf { it.isNotEmpty() }
                    if (b != null && value != null) {
                        val existing = builderRequestHeaders[b] ?: LinkedHashMap<String, String>().also { builderRequestHeaders[b] = it }
                        existing[hdrKey] = value
                    }
                    chain.proceed()
                }
            } catch (_: Throwable) {}
        }

        // 4. Hook build()/create()：刷出暂缓备用地址并将 Builder 的 BTR/AutoVod 绑定透传至构建出的 Segment 实例
        val buildMethods = methods.filter { (it.name == "build" || it.name == "create") && it.parameterTypes.isEmpty() }
        for (m in buildMethods) {
            if (!synchronized(hookedMethods) { hookedMethods.add(m) }) continue
            try {
                module.hook(m).intercept { chain ->
                    flushDeferredBackupsBeforeBuild(chain.thisObject)
                    val built = chain.proceed()
                    onSegmentBuilt(chain.thisObject, built)
                    built
                }
            } catch (_: Throwable) {}
        }
    }

    fun filterAndFillBackupUrls(
        rawList: List<String>,
        config: AcceleratorConfig,
        onRewrite: ((RewriteResult) -> Unit)? = null
    ): List<String> {
        return BiliAcceleratorCore.filterAndFillBackupUrls(rawList, config, onRewrite)
    }

    private data class BuilderHints(
        val hasDrm: Boolean = false,
        val isPreview: Boolean = false,
        val isEpisode: Boolean = false
    )

    private fun inspectBuilderHints(target: Any?): BuilderHints {
        if (target == null) return BuilderHints()
        var hasDrm = false
        var isPreview = false
        var isEpisode = false
        try {
            var cls: Class<*>? = target.javaClass
            var depth = 0
            while (cls != null && cls != Any::class.java && depth < 3) {
                for (f in cls.declaredFields) {
                    val name = f.name.lowercase()
                    if (!name.contains("drm") && !name.contains("preview") &&
                        !name.contains("epid") && !name.contains("seasonid") &&
                        !name.contains("ep_id") && !name.contains("season_id")
                    ) continue
                    runCatching {
                        f.isAccessible = true
                        val v = f.get(target)
                        when {
                            name.contains("drm") -> {
                                val active = when (v) {
                                    is Boolean -> v
                                    is Number -> v.toInt() > 0
                                    is String -> v.isNotBlank() && v != "0" && !v.equals("false", true)
                                    else -> v != null
                                }
                                if (active) hasDrm = true
                            }
                            name.contains("preview") -> {
                                val active = when (v) {
                                    is Boolean -> v
                                    is Number -> v.toInt() > 0
                                    is String -> v == "1" || v.equals("true", true)
                                    else -> false
                                }
                                if (active) isPreview = true
                            }
                            name.contains("epid") || name.contains("ep_id") || name.contains("season") -> {
                                val active = when (v) {
                                    is Number -> v.toLong() > 0L
                                    is String -> v.isNotBlank() && v != "0"
                                    else -> false
                                }
                                if (active) isEpisode = true
                            }
                        }
                    }
                }
                cls = cls.superclass
                depth++
            }
        } catch (_: Throwable) {}
        return BuilderHints(hasDrm = hasDrm, isPreview = isPreview, isEpisode = isEpisode)
    }

    private fun shouldReportResult(result: RewriteResult): Boolean {
        return result.reason != "ignored" &&
                result.reason != "parse-failed" &&
                result.reason != "already-local-proxy" &&
                result.reason != "already-proxied"
    }

    private data class BtrRewriteOutcome(
        val btrUrl: String,
        val effectiveResult: RewriteResult
    )

    private fun tryRewriteToBtr(
        originalUrl: String,
        result: RewriteResult,
        config: AcceleratorConfig,
        builderInstance: Any? = null,
        requestHeaders: Map<*, *>? = null
    ): BtrRewriteOutcome? {
        if (!config.enabled || !config.enableBtr) {
            return null
        }
        if (!BiliAcceleratorCore.isMediaUrl(originalUrl)) {
            return null
        }
        // 防重入：严禁对已重定向至本地代理回环的 URL 重复改写
        if (originalUrl.contains("127.0.0.1:") || originalUrl.contains("localhost:") ||
            (result.changed && (result.finalUrl.contains("127.0.0.1:") || result.finalUrl.contains("localhost:")))) {
            return null
        }
        val candidateTarget = if (result.changed) result.finalUrl else originalUrl
        if (result.reason == "parse-failed" || BiliAcceleratorCore.parseUri(candidateTarget) == null) {
            return null
        }
        val isLive = result.reason.startsWith("live-") ||
                BiliAcceleratorCore.isLiveMediaUrl("", originalUrl) ||
                (result.changed && BiliAcceleratorCore.isLiveMediaUrl("", result.finalUrl))

        // 直播流多源并发加速控制：未开启时保持原生绕过
        if (isLive && !config.enableLiveAcceleration) {
            return null
        }

        val hints = inspectBuilderHints(builderInstance)
        val playinfoTrack: PlayinfoTrackEntry? = PlayinfoRegistry.lookup(originalUrl)
            ?: if (result.changed) PlayinfoRegistry.lookup(result.finalUrl) else null
        val isEpisode = hints.isEpisode ||
                (playinfoTrack?.isEpisode == true) ||
                BiliAcceleratorCore.isEpisodeMediaUrl(originalUrl) ||
                (result.changed && BiliAcceleratorCore.isEpisodeMediaUrl(result.finalUrl))
        val isPreview = hints.isPreview ||
                (playinfoTrack?.isPreview == true) ||
                BiliAcceleratorCore.isEpisodePreview(originalUrl) ||
                (result.changed && BiliAcceleratorCore.isEpisodePreview(result.finalUrl))
        val hasDrm = hints.hasDrm ||
                (playinfoTrack?.hasDrm == true) ||
                BiliAcceleratorCore.hasDrmSignal(originalUrl) ||
                (result.changed && BiliAcceleratorCore.hasDrmSignal(result.finalUrl))

        // 所有试看流（含剧集、付费课程/课堂、充电专属等非剧集试看）一律跳过 BTR，交由原生播放器处理
        if (isPreview) {
            Log.i(TAG, "Preview stream detected, leaving to native player")
            return null
        }

        // 剧集专属模式控制：未开启时严格遵循 BTR 原版调度放行至原生播放器
        if (isEpisode && !config.enableEpisodeAcceleration) {
            Log.i(TAG, "Episode acceleration disabled in config, leaving to native player")
            return null
        }

        // 版权保护/DRM 加密流（Widevine/ChinaDRM/PSSH/is_drm/Playinfo/Builder DRM 属性）绝对不重写至 BTR 本地代理，交由原生直连放行
        if (hasDrm) {
            Log.i(TAG, "DRM protected stream detected, bypassing BTR proxy")
            return null
        }

        if (!RemoteClient.ensureBtrProxyStarted(config)) {
            return null
        }

        val rawBackups = builderInstance?.let { builderRawBackups[it] }.orEmpty()
        val pendingBackups = builderInstance?.let { builderPendingBackups[it] }.orEmpty().toMutableList()
        val nowSec = System.currentTimeMillis() / 1000L
        val origDeadlineSec = BiliAcceleratorCore.extractUrlDeadlineSeconds(originalUrl)
        val isPlayinfoUrlsFresh = playinfoTrack != null && (
            playinfoTrack.deadlineEpochSec <= 0L ||
                (playinfoTrack.deadlineEpochSec > nowSec &&
                    (origDeadlineSec <= 0L || playinfoTrack.deadlineEpochSec >= origDeadlineSec))
            )

        if (isPlayinfoUrlsFresh && playinfoTrack.backupUrls.isNotEmpty()) {
            val filteredPlayinfoBackups = if (config.blockPcdn || config.proxyMcdn || config.forceUpos || (config.rewriteAkamai && config.enableBtr)) {
                filterAndFillBackupUrls(playinfoTrack.backupUrls, config, null)
            } else {
                playinfoTrack.backupUrls
            }
            for (b in filteredPlayinfoBackups) {
                if (b.isNotBlank() && b != originalUrl && !pendingBackups.contains(b)) {
                    pendingBackups.add(b)
                }
            }
        }

        val cleanOfficialBackup = findPromotableOfficialBackup(originalUrl, result, rawBackups, isLive)
        var targetUrl: String
        var effectiveResult: RewriteResult
        if (cleanOfficialBackup != null) {
            targetUrl = cleanOfficialBackup
            val promotedHost = BiliAcceleratorCore.normalizeHost(BiliAcceleratorCore.parseUri(cleanOfficialBackup)?.host)
            effectiveResult = result.copy(
                changed = cleanOfficialBackup != originalUrl,
                finalUrl = cleanOfficialBackup,
                targetHost = promotedHost,
                reason = "pcdn-promote"
            )
            Log.i(TAG, "Promoted official backup mirror to primary: $cleanOfficialBackup")
        } else if (!isLive && result.reason == "akamai-redirect" && isAllAkamaiDonorCase(config, originalUrl, rawBackups, if (isPlayinfoUrlsFresh) listOfNotNull(playinfoTrack?.primaryUrl) + playinfoTrack?.backupUrls.orEmpty() else emptyList())) {
            // 对齐上游 BTR cdn-resolver：仅当所有候选均为 akamaized.net（无非 Akamai 官方 donor）且开启 rewriteAkamai+BTR 时，
            // Akamai 地址作为 donor 合成 UPOS 镜像；原 Akamai 地址保留为回退，403 节点交由 BanList / 失败追踪淘汰
            targetUrl = result.finalUrl
            effectiveResult = result
        } else if (!isLive && result.reason == "akamai-redirect") {
            // 存在非 Akamai 官方 donor 或未开启 rewriteAkamai 时，保留原签名 Akamai 地址作为主源 (akamai-pass)
            targetUrl = originalUrl
            effectiveResult = result.copy(
                changed = false,
                finalUrl = originalUrl,
                targetHost = result.originalHost,
                reason = "akamai-pass"
            )
        } else {
            targetUrl = if (result.changed) result.finalUrl else originalUrl
            effectiveResult = result
        }

        // 对齐 BTR / realzza：BTR 模式下发生域名改写（Akamai/force-upos/或无干净 UPOS 时的 PCDN/MCDN/解包）时，
        // 保留原始地址置于末尾作为 403 兜底，避免改写后的 UPOS 节点拒绝边缘签名导致无源可退
        val shouldKeepOriginalFallback = !isLive && originalUrl != targetUrl && (
            result.reason == "akamai-redirect" ||
                BiliAcceleratorCore.isAkamaiHost(result.originalHost) ||
                result.reason == "force-upos" ||
                (result.changed && cleanOfficialBackup == null)
            )
        if (shouldKeepOriginalFallback && !pendingBackups.contains(originalUrl)) {
            pendingBackups.add(originalUrl)
        }

        if (isLive) {
            val cleanCandidate = findCleanLiveUrlForUrl(targetUrl, builderInstance, config.portHeuristic)
            if (!cleanCandidate.isNullOrBlank() && cleanCandidate != targetUrl && !pendingBackups.contains(cleanCandidate)) {
                pendingBackups.add(cleanCandidate)
            }
            // getRoomPlayInfo 登记的官方多线路 (保留各节点 extra 鉴权)，即使 Ijk 仅传入单一主 URL 也可补全备用源
            for (liveBackup in PlayinfoRegistry.lookupLiveBackups(targetUrl)) {
                if (liveBackup != targetUrl && !pendingBackups.contains(liveBackup)) {
                    pendingBackups.add(liveBackup)
                }
            }
        }

        pendingBackups.removeAll { candidate ->
            if (candidate == targetUrl) {
                true
            } else {
                val candHost = BiliAcceleratorCore.normalizeHost(BiliAcceleratorCore.parseUri(candidate)?.host)
                !BiliAcceleratorCore.isAkamaiHost(candHost) && candidate.contains("hdnts=", ignoreCase = true)
            }
        }

        val headerUa = requestHeaders?.entries?.firstOrNull { it.key?.toString()?.equals("user-agent", ignoreCase = true) == true }?.value?.toString()
        val headerRef = requestHeaders?.entries?.firstOrNull { it.key?.toString()?.equals("referer", ignoreCase = true) == true }?.value?.toString()
        val isAudio = !isLive && (playinfoTrack?.isAudio == true || BiliAcceleratorCore.isAudioTrack(targetUrl))

        val proxyPort = io.github.jh_mmm.biliaccelerator.btr.BtrLocalProxyServer.port
        val ext = resolveProxyStreamExtension(targetUrl)

        // 若同一 Builder 连续调用构造器 / setUrl / setResolveUrl，优先复用或替换旧 Session，杜绝孤立会话膨胀
        if (builderInstance != null) {
            val existingId = BtrSessionManager.getSessionForBuilder(builderInstance)
            val existingSession = existingId?.let { BtrSessionManager.get(it) }
            if (existingSession != null && existingSession.primaryUrl == targetUrl && existingSession.isLive == isLive) {
                if (pendingBackups.isNotEmpty()) {
                    BtrSessionManager.updateBackups(existingSession.sessionId, pendingBackups)
                }
                if (!headerUa.isNullOrBlank()) existingSession.userAgent = headerUa
                if (!headerRef.isNullOrBlank()) existingSession.referer = headerRef
                val reusedUrl = "http://127.0.0.1:$proxyPort/stream/${existingSession.sessionId}.$ext"
                builderResolvedNonBtr.remove(builderInstance)
                playerResolvedNonBtr.remove(builderInstance)
                playerToSessionId[builderInstance] = existingSession.sessionId
                builderBtrUrl[builderInstance] = reusedUrl
                flushDeferredBackups(builderInstance, reusedUrl, config)
                return BtrRewriteOutcome(reusedUrl, effectiveResult)
            }
            if (existingId != null) {
                BtrSessionManager.remove(existingId)
            }
        }

        if (!isLive) {
            BtrSessionManager.refreshSignatureIfSamePath(targetUrl, pendingBackups)
        }

        val sessionId = BtrSessionManager.register(
            primaryUrl = targetUrl,
            backupUrls = pendingBackups,
            userAgent = headerUa,
            isAudio = isAudio,
            isEpisode = isEpisode,
            isLive = isLive,
            config = config
        )
        if (!headerRef.isNullOrBlank()) {
            BtrSessionManager.get(sessionId)?.referer = headerRef
        }
        val btrUrl = "http://127.0.0.1:$proxyPort/stream/$sessionId.$ext"
        if (builderInstance != null) {
            builderResolvedNonBtr.remove(builderInstance)
            playerResolvedNonBtr.remove(builderInstance)
            BtrSessionManager.bindBuilderSession(builderInstance, sessionId)
            playerToSessionId[builderInstance] = sessionId
            builderBtrUrl[builderInstance] = btrUrl
            flushDeferredBackups(builderInstance, btrUrl, config)
        }
        return BtrRewriteOutcome(btrUrl, effectiveResult)
    }

    internal fun rewriteArgIfMedia(
        args: List<Any?>,
        builderInstance: Any? = null,
        configOverride: AcceleratorConfig? = null,
        onRewrite: ((RewriteResult) -> Unit)? = { RemoteClient.notifyRewrite(it) },
        methodName: String? = null
    ): Array<Any?>? {
        val strIndex = args.indexOfFirst { it is String && BiliAcceleratorCore.hasMediaSignal(it) }
        if (strIndex == -1) {
            // setResolveUrl(null / "") 或无有效 URL 字符串参数时绝不清空已有 BTR 状态或回滚 backupUrls
            val hasNonBlankStringArg = args.any { it is String && it.isNotBlank() }
            if (!hasNonBlankStringArg) {
                return null
            }
            if (methodName == "setResolveUrl" && args.none { it is String && it.contains("://") }) {
                return null
            }
            clearBuilderBtrState(builderInstance)
            flushDeferredBackups(builderInstance, btrUrl = null)
            return null
        }

        val originalUrl = args[strIndex] as String
        builderInstance?.let { builderPrimaryUrl[it] = originalUrl }
        val config = configOverride ?: RemoteClient.fetchConfig()
        if (!config.enabled) {
            clearBuilderBtrState(builderInstance)
            flushDeferredBackups(builderInstance, btrUrl = null, config = config)
            builderInstance?.let { builderResolvedNonBtr[it] = true }
            return null
        }

        val parsed = BiliAcceleratorCore.parseUri(originalUrl)
        val isLive = parsed != null && BiliAcceleratorCore.isLiveMediaUrl(parsed.path, originalUrl)
        val result = BiliAcceleratorCore.rewriteUrl(originalUrl, config)
        if (result.reason == "already-local-proxy") {
            return null
        }
        val playinfoTrack = PlayinfoRegistry.lookup(originalUrl)
            ?: if (result.changed) PlayinfoRegistry.lookup(result.finalUrl) else null
        if (playinfoTrack?.hasDrm == true) {
            clearBuilderBtrState(builderInstance)
            flushDeferredBackups(builderInstance, btrUrl = null, config = config)
            builderInstance?.let { builderResolvedNonBtr[it] = true }
            Log.i(TAG, "Playinfo DRM detected in media rewrite, leaving stream untouched")
            return null
        }

        val capturedHeaders = builderInstance?.let { builderRequestHeaders[it] }
        if (isLive) {
            if (config.enableLiveAcceleration) {
                val btrOutcome = tryRewriteToBtr(originalUrl, result, config, builderInstance, capturedHeaders)
                if (btrOutcome != null) {
                    if (btrOutcome.effectiveResult.changed && shouldReportResult(btrOutcome.effectiveResult)) {
                        onRewrite?.invoke(btrOutcome.effectiveResult)
                    }
                    Log.i(TAG, "Rewrote live stream to BTR proxy: ${result.originalHost} -> 127.0.0.1:${io.github.jh_mmm.biliaccelerator.btr.BtrLocalProxyServer.port}")
                    val newArgs = args.toTypedArray()
                    newArgs[strIndex] = btrOutcome.btrUrl
                    return newArgs
                }
            }
            clearBuilderBtrState(builderInstance)
            flushDeferredBackups(builderInstance, btrUrl = null, config = config)
            builderInstance?.let { builderResolvedNonBtr[it] = true }
            if (result.changed) {
                onRewrite?.invoke(result)
                val newArgs = args.toTypedArray()
                newArgs[strIndex] = result.finalUrl
                return newArgs
            }
            if (result.isPcdn && config.blockPcdn) {
                val cleanLiveUrl = findCleanLiveUrlForUrl(originalUrl, builderInstance, config.portHeuristic)
                val cleanLiveHost = cleanLiveUrl?.let { BiliAcceleratorCore.normalizeHost(BiliAcceleratorCore.parseUri(it)?.host) }
                if (!cleanLiveUrl.isNullOrBlank() && !cleanLiveHost.isNullOrBlank() && cleanLiveUrl != originalUrl) {
                    val liveResult = RewriteResult(
                        changed = true,
                        originalUrl = originalUrl,
                        finalUrl = cleanLiveUrl,
                        originalHost = parsed.host,
                        targetHost = cleanLiveHost,
                        reason = "live-pcdn-filter",
                        isPcdn = true,
                        isMcdn = result.isMcdn,
                        segmentKey = result.segmentKey
                    )
                    onRewrite?.invoke(liveResult)
                    val newArgs = args.toTypedArray()
                    newArgs[strIndex] = cleanLiveUrl
                    return newArgs
                }
            }
            if (shouldReportResult(result)) onRewrite?.invoke(result)
            return null
        }

        val btrOutcome = tryRewriteToBtr(originalUrl, result, config, builderInstance, capturedHeaders)
        if (btrOutcome != null) {
            if (btrOutcome.effectiveResult.changed && shouldReportResult(btrOutcome.effectiveResult)) {
                onRewrite?.invoke(btrOutcome.effectiveResult)
            }
            Log.i(TAG, "Rewrote media url to BTR proxy: ${result.originalHost} -> 127.0.0.1:${io.github.jh_mmm.biliaccelerator.btr.BtrLocalProxyServer.port}")
            val newArgs = args.toTypedArray()
            newArgs[strIndex] = btrOutcome.btrUrl
            return newArgs
        }

        clearBuilderBtrState(builderInstance)
        builderInstance?.let { builderResolvedNonBtr[it] = true }
        val rawBackups = builderInstance?.let { builderRawBackups[it] }.orEmpty()

        // 非 BTR 智能模式 (selectionMode == "auto")：由 AutoVodRouter 全权决策，
        // 若 selectVodUrl 判定保留健康官方/海外节点 (changed = false)，严禁落入原始 config (forceUpos = true) 强行覆盖！
        if (AutoVodRouter.shouldUseAutoRouting(config)) {
            val autoRes = AutoVodRouter.selectVodUrl(originalUrl, rawBackups, playinfoTrack, config)
            val effectiveUrl = if (autoRes.changed && autoRes.finalUrl != originalUrl) autoRes.finalUrl else originalUrl
            trackAutoVodSelection(builderInstance, effectiveUrl, playinfoTrack, isDirectDataSource = false)
            flushDeferredBackups(builderInstance, btrUrl = null, config = config)
            if (autoRes.changed && autoRes.finalUrl != originalUrl) {
                if (shouldReportResult(autoRes)) onRewrite?.invoke(autoRes)
                Log.i(TAG, "Auto VOD routing: ${autoRes.originalHost} -> ${autoRes.targetHost} [${autoRes.reason}]")
                val newArgs = args.toTypedArray()
                newArgs[strIndex] = autoRes.finalUrl
                return newArgs
            }
            if (shouldReportResult(autoRes)) onRewrite?.invoke(autoRes)
            return null
        }

        flushDeferredBackups(builderInstance, btrUrl = null, config = config)

        // 未进入 BTR 时（如剧集加速关闭、试看流、本地代理启动失败）按非 BTR 规则 (enableBtr = false) 回退，
        // 绝不将 BTR 专属的 akamai-redirect UPOS URL 交给原生播放器导致 HTTP 403。
        val nonBtrResult = if (config.enableBtr) {
            BiliAcceleratorCore.rewriteUrl(originalUrl, config.copy(enableBtr = false))
        } else {
            result
        }
        val cleanOfficialBackup = if (nonBtrResult.changed || nonBtrResult.isPcdn || nonBtrResult.isMcdn || BiliAcceleratorCore.isRelayUrl(originalUrl)) {
            findPromotableOfficialBackup(originalUrl, nonBtrResult, rawBackups, isLive = false)
        } else {
            null
        }
        if (nonBtrResult.changed || (cleanOfficialBackup != null && cleanOfficialBackup != originalUrl)) {
            val finalUrl = cleanOfficialBackup ?: nonBtrResult.finalUrl
            val finalTargetHost = BiliAcceleratorCore.normalizeHost(BiliAcceleratorCore.parseUri(finalUrl)?.host)
            val reportedResult = if (cleanOfficialBackup != null) {
                nonBtrResult.copy(
                    changed = cleanOfficialBackup != originalUrl,
                    finalUrl = finalUrl,
                    targetHost = finalTargetHost,
                    reason = "pcdn-promote",
                    isMcdn = false
                )
            } else {
                nonBtrResult
            }
            if (shouldReportResult(reportedResult)) onRewrite?.invoke(reportedResult)
            Log.i(TAG, "Rewrote media url: ${nonBtrResult.originalHost} -> $finalTargetHost [${reportedResult.reason}]")
            val newArgs = args.toTypedArray()
            newArgs[strIndex] = finalUrl
            return newArgs
        }
        if (shouldReportResult(nonBtrResult)) onRewrite?.invoke(nonBtrResult)
        return null
    }

    /** All-Akamai donor: rewriteAkamai + BTR 开启，且候选中不存在任何非 Akamai 的干净官方 UPOS donor。 */
    internal fun isAllAkamaiDonorCase(
        config: AcceleratorConfig,
        originalUrl: String,
        rawBackups: List<String>,
        playinfoUrls: List<String>
    ): Boolean {
        if (!config.rewriteAkamai || !config.enableBtr) return false
        if (!BiliAcceleratorCore.isAkamaiHost(BiliAcceleratorCore.normalizeHost(BiliAcceleratorCore.parseUri(originalUrl)?.host))) return false
        if (BiliAcceleratorCore.hasDrmSignal(originalUrl) || BiliAcceleratorCore.isEpisodePreview(originalUrl)) return false
        // 必须检查未经 filterAndFill 改写的原始候选（并解包 smtcdns / 调度器），避免 Akamai 备选已被改写成 UPOS 后误判存在 donor
        return (rawBackups + playinfoUrls)
            .map { u ->
                val step1 = BiliAcceleratorCore.unwrapSmtcdnsUrl(u).ifBlank { u }
                BiliAcceleratorCore.extractSchedulerSourceUrl(step1) ?: step1
            }
            .none { BiliAcceleratorCore.isCleanUposDonor(it) }
    }

    internal fun findPromotableOfficialBackup(
        originalUrl: String,
        result: RewriteResult,
        rawBackups: List<String>,
        isLive: Boolean
    ): String? {
        if (isLive) return null
        val shouldPromote = result.isPcdn ||
            result.isMcdn ||
            BiliAcceleratorCore.isRelayUrl(originalUrl) ||
            result.reason == "akamai-redirect"
        if (!shouldPromote) return null
        val nowSec = System.currentTimeMillis() / 1000L
        val origDeadlineSec = BiliAcceleratorCore.extractUrlDeadlineSeconds(originalUrl)
        val playinfoEntry = PlayinfoRegistry.lookup(originalUrl)
            ?: if (result.changed) PlayinfoRegistry.lookup(result.finalUrl) else null
        val isPlayinfoFresh = playinfoEntry != null && (
            playinfoEntry.deadlineEpochSec <= 0L ||
                (playinfoEntry.deadlineEpochSec > nowSec &&
                    (origDeadlineSec <= 0L || playinfoEntry.deadlineEpochSec >= origDeadlineSec))
            )
        val playinfoUrls = if (isPlayinfoFresh) {
            listOfNotNull(playinfoEntry?.primaryUrl) + playinfoEntry?.backupUrls.orEmpty()
        } else {
            emptyList()
        }
        val candidateBackups = (rawBackups + playinfoUrls)
            .map { u ->
                val step1 = BiliAcceleratorCore.unwrapSmtcdnsUrl(u).ifBlank { u }
                BiliAcceleratorCore.extractSchedulerSourceUrl(step1) ?: step1
            }
            .distinct()
        return candidateBackups.firstOrNull { backup ->
            if (!BiliAcceleratorCore.isCleanUposDonor(backup)) return@firstOrNull false
            val candDeadline = BiliAcceleratorCore.extractUrlDeadlineSeconds(backup)
            // 禁止从旧缓存中提权已过期或早于当前 URL 有效期的备选地址，防止提权即 403
            if (candDeadline > 0L && candDeadline <= nowSec) return@firstOrNull false
            if (origDeadlineSec > 0L && candDeadline in 1 until origDeadlineSec) return@firstOrNull false
            true
        }
    }

    internal fun rewriteFallbackDataSource(
        original: String?,
        playerInstance: Any?,
        headersMap: Map<*, *>? = null,
        configOverride: AcceleratorConfig? = null,
        onRewrite: ((RewriteResult) -> Unit)? = { RemoteClient.notifyRewrite(it) }
    ): String? {
        if (original == null || !BiliAcceleratorCore.hasMediaSignal(original) || !BiliAcceleratorCore.isMediaUrl(original)) {
            clearBuilderBtrState(playerInstance)
            return null
        }
        if (PlayinfoRegistry.lookup(original)?.hasDrm == true) {
            clearBuilderBtrState(playerInstance)
            playerInstance?.let { playerResolvedNonBtr[it] = true }
            return null
        }
        val config = configOverride ?: RemoteClient.fetchConfig()
        if (!config.enabled) {
            clearBuilderBtrState(playerInstance)
            playerInstance?.let { playerResolvedNonBtr[it] = true }
            return null
        }
        val parsed = BiliAcceleratorCore.parseUri(original)
        val isLive = BiliAcceleratorCore.isLiveMediaUrl(parsed?.path.orEmpty(), original)
        val result = BiliAcceleratorCore.rewriteUrl(original, config)
        if (result.reason == "already-local-proxy") {
            return null
        }
        val playinfoTrack = PlayinfoRegistry.lookup(original)
            ?: if (result.changed) PlayinfoRegistry.lookup(result.finalUrl) else null
        if (playinfoTrack?.hasDrm == true) {
            clearBuilderBtrState(playerInstance)
            playerInstance?.let { playerResolvedNonBtr[it] = true }
            return null
        }
        val btrOutcome = tryRewriteToBtr(original, result, config, playerInstance, headersMap)
        if (btrOutcome != null) {
            if (btrOutcome.effectiveResult.changed && shouldReportResult(btrOutcome.effectiveResult)) {
                onRewrite?.invoke(btrOutcome.effectiveResult)
            }
            Log.i(TAG, "Rewrote IjkMediaPlayer dataSource to BTR: 127.0.0.1:${io.github.jh_mmm.biliaccelerator.btr.BtrLocalProxyServer.port}")
            return btrOutcome.btrUrl
        }
        clearBuilderBtrState(playerInstance)
        playerInstance?.let { playerResolvedNonBtr[it] = true }

        if (isLive) {
            if (result.changed) {
                onRewrite?.invoke(result)
                return result.finalUrl
            }
            if (result.isPcdn && config.blockPcdn) {
                val cleanLiveUrl = findCleanLiveUrlForUrl(original, playerInstance, config.portHeuristic)
                val cleanLiveHost = cleanLiveUrl?.let { BiliAcceleratorCore.normalizeHost(BiliAcceleratorCore.parseUri(it)?.host) }
                if (!cleanLiveUrl.isNullOrBlank() && !cleanLiveHost.isNullOrBlank() && cleanLiveUrl != original) {
                    val liveResult = RewriteResult(
                        changed = true,
                        originalUrl = original,
                        finalUrl = cleanLiveUrl,
                        originalHost = parsed?.host.orEmpty(),
                        targetHost = cleanLiveHost,
                        reason = "live-pcdn-filter",
                        isPcdn = true,
                        isMcdn = result.isMcdn,
                        segmentKey = result.segmentKey
                    )
                    onRewrite?.invoke(liveResult)
                    return cleanLiveUrl
                }
            }
            if (shouldReportResult(result)) onRewrite?.invoke(result)
            return null
        }

        val rawBackups = playerInstance?.let { builderRawBackups[it] }.orEmpty()
        if (AutoVodRouter.shouldUseAutoRouting(config)) {
            val autoRes = AutoVodRouter.selectVodUrl(original, rawBackups, playinfoTrack, config)
            val effectiveUrl = if (autoRes.changed && autoRes.finalUrl != original) autoRes.finalUrl else original
            trackAutoVodSelection(playerInstance, effectiveUrl, playinfoTrack, isDirectDataSource = true)
            if (autoRes.changed && autoRes.finalUrl != original) {
                if (shouldReportResult(autoRes)) onRewrite?.invoke(autoRes)
                Log.i(TAG, "Auto VOD routing (dataSource): ${autoRes.originalHost} -> ${autoRes.targetHost} [${autoRes.reason}]")
                return autoRes.finalUrl
            }
            if (shouldReportResult(autoRes)) onRewrite?.invoke(autoRes)
            return null
        }

        val nonBtrResult = if (config.enableBtr) {
            BiliAcceleratorCore.rewriteUrl(original, config.copy(enableBtr = false))
        } else {
            result
        }
        val cleanOfficialBackup = if (nonBtrResult.changed || nonBtrResult.isPcdn || nonBtrResult.isMcdn || BiliAcceleratorCore.isRelayUrl(original)) {
            findPromotableOfficialBackup(original, nonBtrResult, rawBackups, isLive = false)
        } else {
            null
        }
        if (nonBtrResult.changed || (cleanOfficialBackup != null && cleanOfficialBackup != original)) {
            val finalUrl = cleanOfficialBackup ?: nonBtrResult.finalUrl
            val finalTargetHost = BiliAcceleratorCore.normalizeHost(BiliAcceleratorCore.parseUri(finalUrl)?.host)
            val reportedResult = if (cleanOfficialBackup != null) {
                nonBtrResult.copy(
                    changed = cleanOfficialBackup != original,
                    finalUrl = finalUrl,
                    targetHost = finalTargetHost,
                    reason = "pcdn-promote",
                    isMcdn = false
                )
            } else {
                nonBtrResult
            }
            if (shouldReportResult(reportedResult)) onRewrite?.invoke(reportedResult)
            Log.i(TAG, "Rewrote IjkMediaPlayer dataSource: ${nonBtrResult.originalHost} -> $finalTargetHost [${reportedResult.reason}]")
            return finalUrl
        }
        if (shouldReportResult(nonBtrResult)) onRewrite?.invoke(nonBtrResult)
        return null
    }

    // ---- IjkMediaPlayer <-> Builder/Segment session binding & callbacks -----

    private data class DiscoveredAssetBinding(
        var videoSessionId: String? = null,
        var audioSessionId: String? = null,
        var videoAutoVod: AutoVodBinding? = null,
        var videoBuilder: Any? = null
    )

    private fun extractSessionIdFromProxyUrl(url: String): String? {
        if (!url.contains("/stream/")) return null
        if (!url.contains("127.0.0.1") && !url.contains("localhost")) return null
        return STREAM_SESSION_ID_REGEX.find(url)?.groupValues?.getOrNull(1)?.takeIf { it.isNotBlank() }
    }

    private fun recordDiscoveredSessionId(sid: String, out: DiscoveredAssetBinding) {
        val session = BtrSessionManager.get(sid) ?: return
        if (!session.isAudio) {
            if (out.videoSessionId == null) out.videoSessionId = sid
        } else {
            if (out.audioSessionId == null) out.audioSessionId = sid
        }
    }

    private fun scanObjectForBindings(
        node: Any?,
        depth: Int,
        maxDepth: Int,
        visited: MutableSet<Any>,
        out: DiscoveredAssetBinding
    ) {
        if (node == null || depth > maxDepth) return
        if (node is Number || node is Boolean || node is Enum<*> || node is Class<*>) return
        if (node is String) {
            extractSessionIdFromProxyUrl(node)?.let { recordDiscoveredSessionId(it, out) }
            return
        }
        if (!visited.add(node)) return

        segmentToSessionId[node]?.let { recordDiscoveredSessionId(it, out) }
        BtrSessionManager.getSessionForBuilder(node)?.let { recordDiscoveredSessionId(it, out) }
        segmentToAutoVod[node]?.let { b ->
            if (!b.isAudio && out.videoAutoVod == null) {
                out.videoAutoVod = b
                out.videoBuilder = segmentToBuilder[node]
            }
        }
        builderAutoVod[node]?.let { b ->
            if (!b.isAudio && out.videoAutoVod == null) {
                out.videoAutoVod = b
                out.videoBuilder = node
            }
        }
        if (out.videoSessionId != null) return

        when (node) {
            is Iterable<*> -> {
                for (item in node) {
                    scanObjectForBindings(item, depth + 1, maxDepth, visited, out)
                    if (out.videoSessionId != null) return
                }
                return
            }
            is Array<*> -> {
                for (item in node) {
                    scanObjectForBindings(item, depth + 1, maxDepth, visited, out)
                    if (out.videoSessionId != null) return
                }
                return
            }
            is Map<*, *> -> {
                for (v in node.values) {
                    scanObjectForBindings(v, depth + 1, maxDepth, visited, out)
                    if (out.videoSessionId != null) return
                }
                return
            }
        }

        val clsName = node.javaClass.name
        if (clsName.startsWith("java.") || clsName.startsWith("android.") || clsName.startsWith("kotlin.")) {
            return
        }

        var cls: Class<*>? = node.javaClass
        var superDepth = 0
        while (cls != null && cls != Any::class.java && superDepth < 3) {
            val fields = runCatching { cls.declaredFields }.getOrNull().orEmpty()
            for (f in fields) {
                if (java.lang.reflect.Modifier.isStatic(f.modifiers)) continue
                val fType = f.type
                if (fType.isPrimitive || Number::class.java.isAssignableFrom(fType) || fType == Boolean::class.javaObjectType) {
                    continue
                }
                val v = runCatching {
                    f.isAccessible = true
                    f.get(node)
                }.getOrNull() ?: continue
                scanObjectForBindings(v, depth + 1, maxDepth, visited, out)
                if (out.videoSessionId != null) return
            }
            cls = cls.superclass
            superDepth++
        }
    }

    internal fun bindPlayerToAsset(player: Any?, assetOrSegment: Any?) {
        if (player == null || assetOrSegment == null) return
        val discovered = DiscoveredAssetBinding()
        val visited = Collections.newSetFromMap(IdentityHashMap<Any, Boolean>())
        scanObjectForBindings(assetOrSegment, depth = 0, maxDepth = 4, visited = visited, out = discovered)
        val bestSid = discovered.videoSessionId ?: discovered.audioSessionId
        if (!bestSid.isNullOrBlank()) {
            playerToSessionId[player] = bestSid
            BtrSessionManager.bindBuilderSession(player, bestSid)
            playerResolvedNonBtr.remove(player)
            playerToAutoVod.remove(player)
            return
        }
        val autoBinding = discovered.videoAutoVod
        if (autoBinding != null) {
            playerToAutoVod[player] = autoBinding
            playerResolvedNonBtr[player] = true
            playerToSessionId.remove(player)
            BtrSessionManager.unbindBuilderSession(player)
            discovered.videoBuilder?.let { b ->
                builderInflightHandle[b]?.let { h ->
                    playerToInflightHandle[player] = h
                }
            }
        }
    }

    internal fun resolvePlayerSessionId(player: Any?): String? {
        if (player == null) return null
        val direct = playerToSessionId[player] ?: BtrSessionManager.getSessionForBuilder(player)
        if (!direct.isNullOrBlank()) {
            if (BtrSessionManager.get(direct) != null) {
                return direct
            }
            playerToSessionId.remove(player)
            BtrSessionManager.unbindBuilderSession(player)
        }
        if (playerResolvedNonBtr[player] == true) {
            return null
        }
        // 兜底：若播放器通过未显式匹配的 setDataSource 重载绑定了 IjkMediaAsset，浅层扫描其持有的资产字段完成绑定
        val discovered = DiscoveredAssetBinding()
        val visited = Collections.newSetFromMap(IdentityHashMap<Any, Boolean>())
        scanObjectForBindings(player, depth = 0, maxDepth = 2, visited = visited, out = discovered)
        val foundSid = discovered.videoSessionId ?: discovered.audioSessionId
        if (!foundSid.isNullOrBlank()) {
            playerToSessionId[player] = foundSid
            BtrSessionManager.bindBuilderSession(player, foundSid)
            return foundSid
        }
        if (discovered.videoAutoVod != null) {
            playerToAutoVod[player] = discovered.videoAutoVod!!
            playerResolvedNonBtr[player] = true
        }
        return null
    }

    private fun invokeZeroArgLongMethod(target: Any, methodName: String): Long? {
        return runCatching {
            val m = target.javaClass.getMethod(methodName)
            (m.invoke(target) as? Number)?.toLong()
        }.getOrNull()
    }

    /**
     * 计算播放器真实的超前缓冲秒数：
     * 1. 优先读取 IjkMediaPlayer.getVideoCachedDuration()（原生返回超前缓冲毫秒数）；
     * 2. 次优使用 MEDIA_BUFFERING_UPDATE 百分比 * getDuration() - getCurrentPosition() 计算真实剩余缓冲；
     * 3. 若均不可用返回 null，绝不把 0..100% 总时长百分比当作 0..15s 缓冲池。
     */
    internal fun resolvePlayerBufferAheadSeconds(player: Any?, percent: Int? = null): Double? {
        if (player == null) return null
        val cachedMs = invokeZeroArgLongMethod(player, "getVideoCachedDuration")
        if (cachedMs != null && cachedMs >= 0L) {
            return cachedMs.toDouble() / 1000.0
        }
        if (percent != null) {
            val durationMs = invokeZeroArgLongMethod(player, "getDuration") ?: -1L
            val positionMs = invokeZeroArgLongMethod(player, "getCurrentPosition") ?: -1L
            if (durationMs > 0L && positionMs >= 0L) {
                val bufferedPosMs = durationMs.toDouble() * percent.coerceIn(0, 100) / 100.0
                return max(0.0, (bufferedPosMs - positionMs.toDouble()) / 1000.0)
            }
        }
        return null
    }

    internal fun handlePlayerInfoEvent(
        player: Any?,
        what: Int,
        extra: Int,
        config: AcceleratorConfig = RemoteClient.getCachedConfig(),
        nowMs: Long = System.currentTimeMillis()
    ) {
        val sid = resolvePlayerSessionId(player)
        val activeUrl = player?.let { playerToAutoVod[it]?.activeUrl }
        when (what) {
            MEDIA_INFO_BUFFERING_START -> {
                if (sid != null) {
                    BtrSessionManager.notifyPlayerStall(sid, nowMs)
                }
                val bufS = resolvePlayerBufferAheadSeconds(player) ?: 0.0
                runCatching {
                    AutoVodRouter.reportPlayerStall(
                        urlOrHost = activeUrl,
                        bufferAheadS = bufS,
                        config = config,
                        nowMs = nowMs
                    )
                }
            }
            MEDIA_INFO_BUFFERING_END, MEDIA_INFO_VIDEO_RENDERING_START -> {
                runCatching {
                    AutoVodRouter.reportPlayerBufferingEnd(urlOrHost = activeUrl, nowMs = nowMs)
                }
                if (what == MEDIA_INFO_BUFFERING_END) {
                    val openHandle = player?.let { playerToInflightHandle.remove(it) }
                    if (openHandle != null && !openHandle.ended) {
                        val bufS = resolvePlayerBufferAheadSeconds(player) ?: 10.0
                        runCatching {
                            AutoVodRouter.endInflightRequest(
                                handle = openHandle,
                                httpStatus = 200,
                                loadedBytes = max(openHandle.loadedBytes, 1L),
                                aborted = false,
                                bufferAheadS = bufS,
                                config = config,
                                nowMs = nowMs
                            )
                        }
                    }
                }
            }
            MEDIA_INFO_NETWORK_BANDWIDTH -> {
                if (extra > 0) {
                    val bufS = resolvePlayerBufferAheadSeconds(player) ?: 10.0
                    val openHandle = player?.let { playerToInflightHandle[it] }
                    if (openHandle != null && !openHandle.ended) {
                        val deltaBytes = (extra.toLong() * 1000L) / 8L
                        runCatching {
                            AutoVodRouter.reportInflightProgress(
                                handle = openHandle,
                                loadedBytes = openHandle.loadedBytes + deltaBytes,
                                bufferAheadS = bufS,
                                config = config,
                                nowMs = nowMs
                            )
                        }
                    }
                    runCatching {
                        AutoVodRouter.reportBandwidthHint(
                            kbps = extra,
                            urlOrHost = activeUrl,
                            config = config,
                            nowMs = nowMs
                        )
                    }
                }
            }
        }
    }

    internal fun handlePlayerBufferingUpdate(
        player: Any?,
        percent: Int,
        config: AcceleratorConfig = RemoteClient.getCachedConfig(),
        nowMs: Long = System.currentTimeMillis()
    ) {
        val bufferAheadS = resolvePlayerBufferAheadSeconds(player, percent) ?: return
        val sid = resolvePlayerSessionId(player)
        if (sid != null && bufferAheadS < AutoVodRouter.STUCK_URGENT_BUFFER_S) {
            BtrSessionManager.notifyPlayerBufferHunger(sid, nowMs = nowMs)
        }
        val activeUrl = player?.let { playerToAutoVod[it]?.activeUrl }
        if (AutoVodRouter.shouldUseAutoRouting(config)) {
            val openHandle = player?.let { playerToInflightHandle[it] }
            if (openHandle != null && !openHandle.ended) {
                val tcpBytesPerSec = player?.let { invokeZeroArgLongMethod(it, "getTcpSpeed") } ?: 0L
                if (tcpBytesPerSec > 0L) {
                    runCatching {
                        AutoVodRouter.reportInflightProgress(
                            handle = openHandle,
                            loadedBytes = openHandle.loadedBytes + tcpBytesPerSec,
                            bufferAheadS = bufferAheadS,
                            config = config,
                            nowMs = nowMs
                        )
                    }
                }
            }
            runCatching {
                AutoVodRouter.reportPlayerBufferAhead(
                    bufferAheadS = bufferAheadS,
                    urlOrHost = activeUrl,
                    config = config,
                    nowMs = nowMs
                )
            }
        }
    }

    internal fun handlePlayerErrorEvent(
        player: Any?,
        config: AcceleratorConfig = RemoteClient.getCachedConfig(),
        nowMs: Long = System.currentTimeMillis()
    ) {
        val activeUrl = player?.let { playerToAutoVod[it]?.activeUrl }
        runCatching { AutoVodRouter.reportPlayerBufferingEnd(urlOrHost = activeUrl, nowMs = nowMs) }
        val openHandle = player?.let { playerToInflightHandle.remove(it) }
        if (openHandle != null && !openHandle.ended) {
            runCatching {
                AutoVodRouter.endInflightRequest(
                    handle = openHandle,
                    httpStatus = 0,
                    loadedBytes = 0L,
                    aborted = false,
                    bufferAheadS = 0.0,
                    config = config,
                    nowMs = nowMs
                )
            }
        } else {
            runCatching {
                AutoVodRouter.reportTransferError(
                    urlOrHost = activeUrl,
                    config = config,
                    nowMs = nowMs
                )
            }
        }
    }

    internal fun handlePlayerLifecycleEvent(
        player: Any?,
        eventWhat: Int,
        config: AcceleratorConfig = RemoteClient.getCachedConfig(),
        nowMs: Long = System.currentTimeMillis()
    ) {
        if (eventWhat != MEDIA_PREPARED && eventWhat != MEDIA_PLAYBACK_COMPLETE) return
        val activeUrl = player?.let { playerToAutoVod[it]?.activeUrl }
        runCatching { AutoVodRouter.reportPlayerBufferingEnd(urlOrHost = activeUrl, nowMs = nowMs) }
        val openHandle = player?.let { playerToInflightHandle.remove(it) }
        if (openHandle != null && !openHandle.ended) {
            val bufS = resolvePlayerBufferAheadSeconds(player) ?: 10.0
            runCatching {
                AutoVodRouter.endInflightRequest(
                    handle = openHandle,
                    httpStatus = 200,
                    loadedBytes = max(openHandle.loadedBytes, 1L),
                    aborted = false,
                    bufferAheadS = bufS,
                    config = config,
                    nowMs = nowMs
                )
            }
        }
    }

    internal fun handlePlayerResetOrRelease(player: Any?) {
        if (player == null) return
        val activeUrl = playerToAutoVod[player]?.activeUrl
        runCatching { AutoVodRouter.reportPlayerBufferingEnd(urlOrHost = activeUrl) }
        val openHandle = playerToInflightHandle.remove(player)
        if (openHandle != null && !openHandle.ended) {
            runCatching {
                AutoVodRouter.endInflightRequest(
                    handle = openHandle,
                    httpStatus = 0,
                    loadedBytes = openHandle.loadedBytes,
                    aborted = true
                )
            }
        }
        playerToSessionId.remove(player)
        playerToAutoVod.remove(player)
        playerResolvedNonBtr.remove(player)
        BtrSessionManager.unbindBuilderSession(player)
    }

    private fun hookFallbackPlayer(module: XposedModule, classLoader: ClassLoader): Boolean {
        val ijkPlayerClass = try {
            classLoader.loadClass("tv.danmaku.ijk.media.player.IjkMediaPlayer")
        } catch (_: Throwable) {
            null
        } ?: return false

        synchronized(hookedPlayerClasses) {
            if (!hookedPlayerClasses.add(ijkPlayerClass)) return true
        }

        var fallbackHooked = false

        try {
            val allMethods = (ijkPlayerClass.declaredMethods + ijkPlayerClass.methods).distinct()

            // 1. Hook setDataSource(String path) 及 setDataSource(String path, Map<String, String> headers) 等所有首参为 String 的重载
            val stringMethods = allMethods.filter {
                it.name == "setDataSource" && it.parameterTypes.isNotEmpty() && it.parameterTypes[0] == String::class.java
            }
            for (m in stringMethods) {
                if (!synchronized(hookedMethods) { hookedMethods.add(m) }) continue
                try {
                    module.hook(m).intercept { chain ->
                        val original = chain.args.getOrNull(0) as? String
                        val headersMap = chain.args.getOrNull(1) as? Map<*, *>
                        val rewritten = rewriteFallbackDataSource(original, chain.thisObject, headersMap)
                        if (rewritten != null) {
                            val newArgs = chain.args.toTypedArray().apply { this[0] = rewritten }
                            return@intercept chain.proceed(newArgs)
                        }
                        chain.proceed()
                    }
                    fallbackHooked = true
                } catch (_: Throwable) {}
            }

            // 2. Hook setDataSource(Context context, Uri uri) 及 setDataSource(Context, Uri, Map) 等重载
            val uriMethods = allMethods.filter {
                it.name == "setDataSource" && it.parameterTypes.size >= 2 && it.parameterTypes[1] == Uri::class.java
            }
            for (m in uriMethods) {
                if (!synchronized(hookedMethods) { hookedMethods.add(m) }) continue
                try {
                    module.hook(m).intercept { chain ->
                        val originalUri = chain.args.getOrNull(1) as? Uri
                        val headersMap = chain.args.getOrNull(2) as? Map<*, *>
                        val rewritten = rewriteFallbackDataSource(originalUri?.toString(), chain.thisObject, headersMap)
                        if (rewritten != null) {
                            val newArgs = chain.args.toTypedArray().apply { this[1] = Uri.parse(rewritten) }
                            return@intercept chain.proceed(newArgs)
                        }
                        chain.proceed()
                    }
                    fallbackHooked = true
                } catch (_: Throwable) {}
            }

            // 2.5 Hook IjkMediaPlayer 接收 IjkMediaAsset / IMediaDataSource 等对象的重载，建立 IjkMediaPlayer -> BTR Session / AutoVod 绑定
            val assetSetterMethods = allMethods.filter { m ->
                (m.name == "setDataSource" || m.name == "setMediaAsset" || m.name == "setAsset") &&
                    m.parameterTypes.isNotEmpty() &&
                    m.parameterTypes[0] != String::class.java &&
                    m.parameterTypes[0] != FileDescriptor::class.java &&
                    !(m.parameterTypes.size >= 2 && m.parameterTypes[1] == Uri::class.java)
            }
            for (m in assetSetterMethods) {
                if (!synchronized(hookedMethods) { hookedMethods.add(m) }) continue
                try {
                    module.hook(m).intercept { chain ->
                        val player = chain.thisObject
                        for (arg in chain.args) {
                            if (arg != null && arg !is Number && arg !is Boolean && arg !is String) {
                                runCatching { bindPlayerToAsset(player, arg) }
                            }
                        }
                        chain.proceed()
                    }
                    fallbackHooked = true
                } catch (_: Throwable) {}
            }

            // 2.6 Hook reset/release/stop：释放播放器绑定并中止在途统计与卡顿计时器
            val resetMethods = allMethods.filter {
                (it.name == "reset" || it.name == "release" || it.name == "stop") && it.parameterTypes.isEmpty()
            }
            for (m in resetMethods) {
                if (!synchronized(hookedMethods) { hookedMethods.add(m) }) continue
                try {
                    module.hook(m).intercept { chain ->
                        runCatching { handlePlayerResetOrRelease(chain.thisObject) }
                        chain.proceed()
                    }
                } catch (_: Throwable) {}
            }

            // 3. Hook IjkMediaPlayer / AbstractMediaPlayer buffering, stall, bandwidth & error callbacks
            //    优先拦截 postEventFromNative；仅当未挂载 postEventFromNative 时才回退到 notifyOn*，避免重复触发
            val eventClasses = listOfNotNull(ijkPlayerClass, ijkPlayerClass.superclass)
            var hookedPostEvent = false
            for (cls in eventClasses) {
                for (m in cls.declaredMethods) {
                    if (m.name == "postEventFromNative" && m.parameterTypes.size >= 4) {
                        if (!synchronized(hookedMethods) { hookedMethods.add(m) }) {
                            hookedPostEvent = true
                            continue
                        }
                        try {
                            module.hook(m).intercept { chain ->
                                val weakPlayer = (chain.args.getOrNull(0) as? WeakReference<*>)?.get()
                                val what = chain.args.getOrNull(1) as? Int
                                val arg1 = chain.args.getOrNull(2) as? Int
                                val arg2 = chain.args.getOrNull(3) as? Int
                                val cfg = RemoteClient.getCachedConfig()
                                when (what) {
                                    MEDIA_INFO -> if (arg1 != null) {
                                        handlePlayerInfoEvent(weakPlayer, arg1, arg2 ?: 0, cfg)
                                    }
                                    MEDIA_BUFFERING_UPDATE -> if (arg1 != null) {
                                        handlePlayerBufferingUpdate(weakPlayer, arg1, cfg)
                                    }
                                    MEDIA_ERROR -> {
                                        handlePlayerErrorEvent(weakPlayer, cfg)
                                    }
                                    MEDIA_PREPARED, MEDIA_PLAYBACK_COMPLETE -> {
                                        handlePlayerLifecycleEvent(weakPlayer, what, cfg)
                                    }
                                }
                                chain.proceed()
                            }
                            hookedPostEvent = true
                        } catch (_: Throwable) {}
                    }
                }
            }
            if (!hookedPostEvent) {
                for (cls in eventClasses) {
                    for (m in cls.declaredMethods) {
                        try {
                            when (m.name) {
                                "notifyOnInfo" -> if (m.parameterTypes.size == 2 && synchronized(hookedMethods) { hookedMethods.add(m) }) {
                                    module.hook(m).intercept { chain ->
                                        val what = chain.args.getOrNull(0) as? Int
                                        val extra = chain.args.getOrNull(1) as? Int ?: 0
                                        if (what != null) {
                                            handlePlayerInfoEvent(chain.thisObject, what, extra, RemoteClient.getCachedConfig())
                                        }
                                        chain.proceed()
                                    }
                                }
                                "notifyOnBufferingUpdate" -> if (m.parameterTypes.size == 1 && synchronized(hookedMethods) { hookedMethods.add(m) }) {
                                    module.hook(m).intercept { chain ->
                                        val pct = chain.args.getOrNull(0) as? Int
                                        if (pct != null) {
                                            handlePlayerBufferingUpdate(chain.thisObject, pct, RemoteClient.getCachedConfig())
                                        }
                                        chain.proceed()
                                    }
                                }
                                "notifyOnError" -> if (m.parameterTypes.size == 2 && synchronized(hookedMethods) { hookedMethods.add(m) }) {
                                    module.hook(m).intercept { chain ->
                                        handlePlayerErrorEvent(chain.thisObject, RemoteClient.getCachedConfig())
                                        chain.proceed()
                                    }
                                }
                                "notifyOnPrepared" -> if (m.parameterTypes.isEmpty() && synchronized(hookedMethods) { hookedMethods.add(m) }) {
                                    module.hook(m).intercept { chain ->
                                        handlePlayerLifecycleEvent(chain.thisObject, MEDIA_PREPARED, RemoteClient.getCachedConfig())
                                        chain.proceed()
                                    }
                                }
                                "notifyOnCompletion" -> if (m.parameterTypes.isEmpty() && synchronized(hookedMethods) { hookedMethods.add(m) }) {
                                    module.hook(m).intercept { chain ->
                                        handlePlayerLifecycleEvent(chain.thisObject, MEDIA_PLAYBACK_COMPLETE, RemoteClient.getCachedConfig())
                                        chain.proceed()
                                    }
                                }
                            }
                        } catch (_: Throwable) {}
                    }
                }
            }

            if (fallbackHooked) {
                HookStatusTracker.recordStatus("player_fallback", "tv.danmaku.ijk.media.player.IjkMediaPlayer")
                Log.i(TAG, "Successfully hooked IjkMediaPlayer setDataSource methods")
            }
        } catch (t: Throwable) {
            Log.w(TAG, "hookFallbackPlayer failed: ${t.message}")
        }
        return fallbackHooked
    }
}
