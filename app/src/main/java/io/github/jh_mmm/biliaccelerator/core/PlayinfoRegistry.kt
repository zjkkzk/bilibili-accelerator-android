package io.github.jh_mmm.biliaccelerator.core

import android.app.PendingIntent
import android.content.BroadcastReceiver
import android.content.Context
import android.content.Intent
import android.content.IntentFilter
import android.os.Build
import android.os.Process
import com.google.gson.Gson
import com.google.gson.JsonArray
import com.google.gson.JsonElement
import com.google.gson.JsonObject
import com.google.gson.JsonParser
import com.google.gson.reflect.TypeToken
import io.github.jh_mmm.biliaccelerator.btr.BtrSessionManager
import io.github.jh_mmm.biliaccelerator.hook.RemoteClient
import java.io.File
import java.lang.reflect.Method
import java.nio.file.Files
import java.nio.file.StandardCopyOption
import java.util.Collections
import java.util.IdentityHashMap
import java.util.UUID
import java.util.concurrent.CompletableFuture
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.Executors
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.atomic.AtomicLong
import java.util.zip.CRC32

data class PlayinfoTrackEntry(
    val pathKey: String,
    val fileKey: String,
    val primaryUrl: String,
    val backupUrls: List<String>,
    val deadlineEpochSec: Long,
    val isAudio: Boolean,
    val hasDrm: Boolean,
    val isPreview: Boolean,
    val isEpisode: Boolean,
    val cid: Long = 0L,
    val epId: Long = 0L,
    val updatedAtMs: Long = System.currentTimeMillis(),
    val bandwidth: Long = 0L
)

fun interface PlayinfoRefreshCallback {
    fun refresh(): Boolean
}

data class CrossProcessRefreshRequest(
    val requestId: String,
    val url: String,
    val pathKey: String,
    val cid: Long,
    val epId: Long,
    val currentDeadlineSec: Long,
    val timeoutMs: Long
)

data class CrossProcessRefreshResult(
    val success: Boolean,
    val primaryUrl: String = "",
    val backupUrls: List<String> = emptyList(),
    val deadlineEpochSec: Long = 0L
)

data class SharedConcurrencyState(
    val learnedConcurrency: Int = 0,
    val hardCapConcurrency: Int = 0,
    val hardCapUntilMs: Long = 0L,
    val updatedAtMs: Long = 0L
)

private data class SharedTrackSummary(
    val pathKey: String = "",
    val fileKey: String = "",
    val primaryUrl: String = "",
    val backupUrls: List<String> = emptyList(),
    val isAudio: Boolean = false,
    val hasDrm: Boolean = false,
    val isPreview: Boolean = false,
    val isEpisode: Boolean = false,
    val deadlineEpochSec: Long = 0L,
    val updatedAtMs: Long = 0L,
    val cid: Long = 0L,
    val epId: Long = 0L,
    val bandwidth: Long = 0L
)

private data class RegisteredRefresher(
    val callback: PlayinfoRefreshCallback,
    val isGrpc: Boolean,
    val registeredAtMs: Long
)

object PlayinfoRegistry {

    enum class RefreshSource {
        GRPC,
        HTTP,
        OTHER
    }

    private const val MAX_ENTRIES = 512
    private const val ENTRY_TTL_MS = 6L * 60L * 60L * 1000L
    private const val MAX_SHARED_ENTRIES = 128
    private const val MAX_PROTO_DEPTH = 20
    private const val SHARED_DIR_NAME = "btr_shared_state"
    private const val TRACKS_FILE_NAME = "playinfo_tracks.json"
    private const val CONCURRENCY_FILE_NAME = "concurrency_state.json"

    internal const val ACTION_PLAYINFO_REFRESH_REQUEST = "io.github.jh_mmm.biliaccelerator.PLAYINFO_REFRESH_REQUEST"
    internal const val ACTION_PLAYINFO_REFRESH_RESULT = "io.github.jh_mmm.biliaccelerator.PLAYINFO_REFRESH_RESULT"
    private const val EXTRA_REQUEST_ID = "request_id"
    private const val EXTRA_SENDER_PID = "sender_pid"
    private const val EXTRA_TARGET_URL = "target_url"
    private const val EXTRA_PATH_KEY = "path_key"
    private const val EXTRA_CID = "cid"
    private const val EXTRA_EP_ID = "ep_id"
    private const val EXTRA_CURRENT_DEADLINE = "current_deadline"
    private const val EXTRA_REFRESH_OK = "refresh_ok"
    private const val EXTRA_PRIMARY_URL = "primary_url"
    private const val EXTRA_BACKUP_URLS_JSON = "backup_urls_json"
    private const val EXTRA_DEADLINE = "deadline"
    private const val EXTRA_AUTH_TOKEN = "auth_token"
    internal const val CROSS_PROCESS_REFRESH_TIMEOUT_MS = 8_000L

    private val gson = Gson()
    private val byPathKey = ConcurrentHashMap<String, PlayinfoTrackEntry>()
    private val byParentFileKey = ConcurrentHashMap<String, PlayinfoTrackEntry>()
    private val byFileKey = ConcurrentHashMap<String, PlayinfoTrackEntry>()
    private val refreshersByPath = ConcurrentHashMap<String, RegisteredRefresher>()
    private val refreshersByParentFile = ConcurrentHashMap<String, RegisteredRefresher>()
    private val refreshersByFile = ConcurrentHashMap<String, RegisteredRefresher>()
    private val refreshersByCid = ConcurrentHashMap<Long, RegisteredRefresher>()
    private val refreshersByEpId = ConcurrentHashMap<Long, RegisteredRefresher>()
    private val pendingRefreshRequests = ConcurrentHashMap<String, CompletableFuture<CrossProcessRefreshResult>>()
    private val handledCrossProcessRequestIds = ConcurrentHashMap<String, Long>()
    private val refreshRelayExecutor = Executors.newCachedThreadPool { r ->
        Thread(r, "Playinfo-RefreshRelay").apply { isDaemon = true }
    }

    @Volatile
    private var appContext: Context? = null
    private val refreshReceiverRegistered = AtomicBoolean(false)

    @Volatile
    internal var crossProcessRefreshBridge: ((CrossProcessRefreshRequest) -> CrossProcessRefreshResult)? = null

    private val DISTINCTIVE_ID_REGEX = Regex("""\d{5,}""")
    private val GENERIC_FILE_KEYS = setOf(
        "index.m3u8", "master.m3u8", "playlist.m3u8", "video.m4s", "audio.m4s",
        "stream.m4s", "stream.mp4", "stream.flv", "init.m4s", "chunk.m4s"
    )

    @Volatile
    private var sharedStateDir: File? = null
    private val sharedFileLock = Any()
    private val lastTracksFileCheckMs = AtomicLong(0L)
    @Volatile
    private var lastTracksFileMtime = 0L
    @Volatile
    private var lastTracksFileLength = -1L
    @Volatile
    private var lastTracksContentHash = 0L
    private val lastConcurrencyFileCheckMs = AtomicLong(0L)
    @Volatile
    private var lastConcurrencyFileMtime = 0L
    @Volatile
    private var lastConcurrencyFileLength = -1L

    fun configureSharedStateDir(baseDir: File?) {
        if (baseDir == null) return
        runCatching {
            val dir = File(baseDir, SHARED_DIR_NAME)
            if (!dir.exists()) dir.mkdirs()
            if (dir.isDirectory) {
                sharedStateDir = dir
            }
        }
    }

    fun attachAppContext(context: Context?) {
        if (context == null) return
        val app = runCatching { context.applicationContext ?: context }.getOrDefault(context)
        appContext = app
        if (sharedStateDir == null) {
            configureSharedStateDir(runCatching { app.noBackupFilesDir ?: app.cacheDir }.getOrNull())
        }
        registerCrossProcessRefreshReceiver(app)
    }

    private fun registerCrossProcessRefreshReceiver(context: Context) {
        if (!refreshReceiverRegistered.compareAndSet(false, true)) return
        runCatching {
            val myPkg = context.packageName ?: return@runCatching
            val receiver = object : BroadcastReceiver() {
                override fun onReceive(ctx: Context?, intent: Intent?) {
                    if (intent == null) return
                    val action = intent.action ?: return
                    if (action != ACTION_PLAYINFO_REFRESH_REQUEST && action != ACTION_PLAYINFO_REFRESH_RESULT) return
                    val senderPid = intent.getIntExtra(EXTRA_SENDER_PID, -1)
                    val myPid = runCatching { Process.myPid() }.getOrDefault(-1)
                    if (senderPid > 0 && myPid > 0 && senderPid == myPid) return

                    val authToken: PendingIntent? = if (Build.VERSION.SDK_INT >= Build.VERSION_CODES.TIRAMISU) {
                        runCatching { intent.getParcelableExtra(EXTRA_AUTH_TOKEN, PendingIntent::class.java) }.getOrNull()
                    } else {
                        @Suppress("DEPRECATION")
                        runCatching { intent.getParcelableExtra<PendingIntent>(EXTRA_AUTH_TOKEN) }.getOrNull()
                    }
                    val creatorPkg = authToken?.creatorPackage
                    val sentFromPkg = if (Build.VERSION.SDK_INT >= Build.VERSION_CODES.UPSIDE_DOWN_CAKE) {
                        runCatching { sentFromPackage }.getOrNull()
                    } else null
                    val authorized = creatorPkg == myPkg || sentFromPkg == myPkg
                    if (!authorized) return

                    if (action == ACTION_PLAYINFO_REFRESH_RESULT) {
                        val reqId = intent.getStringExtra(EXTRA_REQUEST_ID) ?: return
                        val pending = pendingRefreshRequests.remove(reqId) ?: return
                        val ok = intent.getBooleanExtra(EXTRA_REFRESH_OK, false)
                        val primary = intent.getStringExtra(EXTRA_PRIMARY_URL).orEmpty()
                        val backupsJson = intent.getStringExtra(EXTRA_BACKUP_URLS_JSON).orEmpty()
                        val backups: List<String> = if (backupsJson.isNotBlank()) {
                            runCatching {
                                val type = object : TypeToken<List<String>>() {}.type
                                gson.fromJson<List<String>>(backupsJson, type) ?: emptyList()
                            }.getOrDefault(emptyList())
                        } else emptyList()
                        val deadline = intent.getLongExtra(EXTRA_DEADLINE, 0L)
                        pending.complete(CrossProcessRefreshResult(ok, primary, backups, deadline))
                        return
                    }

                    val reqId = intent.getStringExtra(EXTRA_REQUEST_ID) ?: return
                    val targetUrl = intent.getStringExtra(EXTRA_TARGET_URL).orEmpty()
                    val pathKey = intent.getStringExtra(EXTRA_PATH_KEY).orEmpty()
                    val cid = intent.getLongExtra(EXTRA_CID, 0L)
                    val epId = intent.getLongExtra(EXTRA_EP_ID, 0L)
                    val currentDeadline = intent.getLongExtra(EXTRA_CURRENT_DEADLINE, 0L)

                    val hasLocalRefresher = findLocalRefresherEntry(targetUrl, pathKey, cid, epId) != null
                    val hasFresherCached = findFresherTrack(targetUrl, currentDeadline) != null
                    if (!hasLocalRefresher && !hasFresherCached) return

                    val nowMs = System.currentTimeMillis()
                    if (handledCrossProcessRequestIds.size > 128) {
                        handledCrossProcessRequestIds.entries.removeIf { nowMs - it.value > 30_000L }
                    }
                    if (handledCrossProcessRequestIds.putIfAbsent(reqId, nowMs) != null) return

                    val req = CrossProcessRefreshRequest(
                        requestId = reqId,
                        url = targetUrl,
                        pathKey = pathKey,
                        cid = cid,
                        epId = epId,
                        currentDeadlineSec = currentDeadline,
                        timeoutMs = CROSS_PROCESS_REFRESH_TIMEOUT_MS
                    )
                    refreshRelayExecutor.execute {
                        val result = handleIncomingCrossProcessRefreshRequest(req)
                        sendSamePackageBroadcast(context, ACTION_PLAYINFO_REFRESH_RESULT) { replyIntent ->
                            replyIntent.putExtra(EXTRA_REQUEST_ID, reqId)
                            replyIntent.putExtra(EXTRA_REFRESH_OK, result.success)
                            replyIntent.putExtra(EXTRA_PRIMARY_URL, result.primaryUrl)
                            replyIntent.putExtra(EXTRA_BACKUP_URLS_JSON, gson.toJson(result.backupUrls))
                            replyIntent.putExtra(EXTRA_DEADLINE, result.deadlineEpochSec)
                        }
                    }
                }
            }
            val filter = IntentFilter().apply {
                addAction(ACTION_PLAYINFO_REFRESH_REQUEST)
                addAction(ACTION_PLAYINFO_REFRESH_RESULT)
            }
            if (Build.VERSION.SDK_INT >= Build.VERSION_CODES.TIRAMISU) {
                context.registerReceiver(receiver, filter, Context.RECEIVER_NOT_EXPORTED)
            } else {
                @Suppress("UnspecifiedRegisterReceiverFlag")
                context.registerReceiver(receiver, filter)
            }
        }.onFailure {
            refreshReceiverRegistered.set(false)
        }
    }

    private fun sendSamePackageBroadcast(context: Context, action: String, populate: (Intent) -> Unit): Boolean {
        return runCatching {
            val pkg = context.packageName ?: return false
            val authIntent = Intent("io.github.jh_mmm.biliaccelerator.PLAYINFO_AUTH").apply { setPackage(pkg) }
            val authToken = PendingIntent.getBroadcast(
                context,
                action.hashCode() and 0x7FFF,
                authIntent,
                PendingIntent.FLAG_IMMUTABLE or PendingIntent.FLAG_UPDATE_CURRENT
            )
            val intent = Intent(action).apply {
                setPackage(pkg)
                putExtra(EXTRA_SENDER_PID, runCatching { Process.myPid() }.getOrDefault(-1))
                putExtra(EXTRA_AUTH_TOKEN, authToken)
                populate(this)
            }
            context.sendBroadcast(intent)
            true
        }.getOrDefault(false)
    }

    internal fun resetForTesting(clearDir: Boolean = true) {
        byPathKey.clear()
        byParentFileKey.clear()
        byFileKey.clear()
        refreshersByPath.clear()
        refreshersByParentFile.clear()
        refreshersByFile.clear()
        refreshersByCid.clear()
        refreshersByEpId.clear()
        pendingRefreshRequests.clear()
        handledCrossProcessRequestIds.clear()
        crossProcessRefreshBridge = null
        liveByPath.clear()
        lastTracksFileCheckMs.set(0L)
        lastTracksFileMtime = 0L
        lastTracksFileLength = -1L
        lastTracksContentHash = 0L
        lastConcurrencyFileCheckMs.set(0L)
        lastConcurrencyFileMtime = 0L
        lastConcurrencyFileLength = -1L
        if (clearDir) {
            sharedStateDir = null
            appContext = null
            refreshReceiverRegistered.set(false)
        }
    }

    fun looksLikePlayinfoJson(text: String?): Boolean {
        if (text.isNullOrBlank() || text.length < 48) return false
        val hasMediaContainer = text.contains("\"dash\"") ||
            text.contains("\"durl\"") ||
            text.contains("\"video_info\"") ||
            text.contains("\"vod_info\"") ||
            text.contains("\"stream_list\"") ||
            (text.contains("\"playurl_info\"") && text.contains("\"url_info\""))
        if (!hasMediaContainer) return false
        return text.contains("bilivideo") ||
            text.contains("akamaized") ||
            text.contains("upgcxcode") ||
            text.contains("bstarstatic") ||
            text.contains("mountaintoys") ||
            text.contains("szbdyd") ||
            text.contains("nexusedgeio") ||
            text.contains("ahdohpiechei") ||
            text.contains("mcdn") ||
            text.contains("is_drm") ||
            text.contains("drm_tech_type") ||
            text.contains("is_preview")
    }

    private fun unwrapPlayinfoLookupUrl(rawUrl: String): String {
        val step1 = BiliAcceleratorCore.unwrapSmtcdnsUrl(rawUrl).ifBlank { rawUrl }
        val configuredProxy = runCatching { RemoteClient.getCachedConfig().proxyHost }.getOrNull()
        val wrapped = BiliAcceleratorCore.extractProxyWrappedInnerUrl(step1, configuredProxy)
        if (!wrapped.isNullOrBlank()) {
            return BiliAcceleratorCore.unwrapSmtcdnsUrl(wrapped).ifBlank { wrapped }
        }
        return step1
    }

    private fun normalizeKeys(rawUrl: String): Triple<String, String, String> {
        val effectiveUrl = unwrapPlayinfoLookupUrl(rawUrl)
        val parsed = BiliAcceleratorCore.parseUri(effectiveUrl) ?: return Triple("", "", "")
        val path = parsed.path.trim().trimEnd('/').lowercase()
        val fileKey = path.substringAfterLast('/')
        val parentAndFile = path.trimStart('/').split('/').let { segs ->
            if (segs.size >= 2) "${segs[segs.size - 2]}/${segs.last()}" else ""
        }
        return Triple(path, parentAndFile, fileKey)
    }

    private fun isDistinctiveFileKey(fileKey: String): Boolean {
        if (fileKey.isBlank()) return false
        val lower = fileKey.lowercase()
        if (lower in GENERIC_FILE_KEYS || lower.startsWith("chunk-") || lower.startsWith("chunk_")) return false
        return DISTINCTIVE_ID_REGEX.containsMatchIn(lower) || lower.length >= 16
    }

    private fun parentFileKeyFromPath(pathKey: String): String {
        val trimmed = pathKey.trimStart('/')
        val segs = trimmed.split('/')
        return if (segs.size >= 2) "${segs[segs.size - 2]}/${segs.last()}" else ""
    }

    fun normalizePathAndFileKey(rawUrl: String): Pair<String, String> {
        val (path, _, fileKey) = normalizeKeys(rawUrl)
        return path to fileKey
    }

    private fun extractEarliestDeadline(urls: List<String>): Long {
        var earliest = 0L
        for (u in urls) {
            val dl = BiliAcceleratorCore.extractUrlDeadlineSeconds(u)
            if (dl > 0L && (earliest == 0L || dl < earliest)) {
                earliest = dl
            }
        }
        return earliest
    }

    fun parsePlayinfoJson(
        jsonText: String,
        sourceUrl: String = "",
        nowMs: Long = System.currentTimeMillis()
    ): List<PlayinfoTrackEntry> {
        if (!looksLikePlayinfoJson(jsonText)) return emptyList()
        val rootElem = runCatching { JsonParser.parseString(jsonText) }.getOrNull() ?: return emptyList()
        if (!rootElem.isJsonObject) return emptyList()
        val rootObj = rootElem.asJsonObject

        val dataObj = rootObj.obj("data")
        val resultObj = rootObj.obj("result")
        val videoInfoObj = dataObj?.obj("video_info") ?: resultObj?.obj("video_info")
        val vodInfoObj = dataObj?.obj("vod_info") ?: resultObj?.obj("vod_info") ?: rootObj.obj("vod_info")
        val viewInfoObj = dataObj?.obj("view_info") ?: resultObj?.obj("view_info") ?: rootObj.obj("view_info")
        val bizInfoObj = dataObj?.obj("play_view_business_info") ?: resultObj?.obj("play_view_business_info")
        val supplementObj = dataObj?.obj("supplement") ?: resultObj?.obj("supplement")
        val bodyObj = videoInfoObj ?: vodInfoObj ?: dataObj ?: resultObj ?: rootObj

        val containers = listOfNotNull(rootObj, dataObj, resultObj, videoInfoObj, vodInfoObj, viewInfoObj, bizInfoObj, supplementObj, bodyObj)

        val rootDrm = containers.any { hasJsonDrmSignal(it) }
        val rootPreview = containers.any { hasJsonPreviewSignal(it) }
        val rootEpisode = BiliAcceleratorCore.isEpisodeMediaUrl(sourceUrl) ||
            sourceUrl.contains("/pgc/", ignoreCase = true) ||
            sourceUrl.contains("/ogv/", ignoreCase = true) ||
            sourceUrl.contains("/bangumi/", ignoreCase = true) ||
            containers.any { hasJsonEpisodeSignal(it) }

        val epId = containers.firstNotNullOfOrNull { c ->
            c.longVal("ep_id") ?: c.longVal("epid") ?: c.longVal("episode_id")
                ?: c.obj("ogv_episode_info")?.longVal("episode_id")
                ?: c.obj("episode_info")?.longVal("ep_id")
                ?: c.obj("episode_info")?.longVal("episode_id")
        } ?: 0L
        val cid = containers.firstNotNullOfOrNull { c ->
            c.longVal("cid") ?: c.obj("arc")?.longVal("cid")
        } ?: 0L

        val entries = mutableListOf<PlayinfoTrackEntry>()
        val dashContainers = listOfNotNull(
            bodyObj.obj("dash"),
            dataObj?.obj("dash"),
            resultObj?.obj("dash"),
            rootObj.obj("dash")
        ).distinct()

        for (dash in dashContainers) {
            collectDashArray(dash.arr("video"), isAudioHint = false, rootDrm, rootPreview, rootEpisode, cid, epId, nowMs, entries)
            collectDashArray(dash.arr("audio"), isAudioHint = true, rootDrm, rootPreview, rootEpisode, cid, epId, nowMs, entries)
            dash.obj("dolby")?.arr("audio")?.let {
                collectDashArray(it, isAudioHint = true, rootDrm, rootPreview, rootEpisode, cid, epId, nowMs, entries)
            }
            for (hiResKey in listOf("flac", "loss_less_item")) {
                dash.obj(hiResKey)?.let { flac ->
                    flac.arr("audio")?.let {
                        collectDashArray(it, isAudioHint = true, rootDrm, rootPreview, rootEpisode, cid, epId, nowMs, entries)
                    }
                    flac.obj("audio")?.let { singleFlac ->
                        val arr = JsonArray().apply { add(singleFlac) }
                        collectDashArray(arr, isAudioHint = true, rootDrm, rootPreview, rootEpisode, cid, epId, nowMs, entries)
                    }
                }
            }
        }

        // stream_list + dash_audio / dolby / loss_less_item (PlayViewUnite / playerunite JSON)
        for (c in containers) {
            c.arr("stream_list")?.let { streamList ->
                for (elem in streamList) {
                    val streamObj = elem.asObjOrNull() ?: continue
                    val streamInfo = streamObj.obj("stream_info")
                    val streamDrm = rootDrm || (streamInfo != null && hasJsonDrmSignal(streamInfo))
                    val streamPreview = rootPreview || (streamInfo != null && hasJsonPreviewSignal(streamInfo))
                    val streamFallbackBandwidth = (streamInfo?.longVal("bandwidth")
                        ?: streamInfo?.longVal("band_width")
                        ?: streamInfo?.longVal("bitrate")
                        ?: streamInfo?.longVal("bit_rate")
                        ?: 0L).coerceAtLeast(0L)
                    streamObj.obj("dash_video")?.let { dv ->
                        addTrackFromJsonObject(
                            dv,
                            isAudioHint = false,
                            streamDrm,
                            streamPreview,
                            rootEpisode,
                            cid,
                            epId,
                            nowMs,
                            entries,
                            fallbackBandwidth = streamFallbackBandwidth
                        )
                    }
                    streamObj.obj("segment_video")?.arr("segment")?.let { segs ->
                        collectDashArray(
                            segs,
                            isAudioHint = false,
                            streamDrm,
                            streamPreview,
                            rootEpisode,
                            cid,
                            epId,
                            nowMs,
                            entries,
                            fallbackBandwidth = streamFallbackBandwidth
                        )
                    }
                }
            }
            c.arr("dash_audio")?.let {
                collectDashArray(it, isAudioHint = true, rootDrm, rootPreview, rootEpisode, cid, epId, nowMs, entries)
            }
            c.obj("dolby")?.arr("audio")?.let {
                collectDashArray(it, isAudioHint = true, rootDrm, rootPreview, rootEpisode, cid, epId, nowMs, entries)
            }
            c.obj("loss_less_item")?.let { lli ->
                lli.arr("audio")?.let {
                    collectDashArray(it, isAudioHint = true, rootDrm, rootPreview, rootEpisode, cid, epId, nowMs, entries)
                }
                lli.obj("audio")?.let { singleHiRes ->
                    val arr = JsonArray().apply { add(singleHiRes) }
                    collectDashArray(arr, isAudioHint = true, rootDrm, rootPreview, rootEpisode, cid, epId, nowMs, entries)
                }
            }
        }

        // durl / durls (MP4/FLV fallback)
        for (c in containers) {
            c.arr("durl")?.let { collectDashArray(it, isAudioHint = false, rootDrm, rootPreview, rootEpisode, cid, epId, nowMs, entries) }
            c.arr("durls")?.forEach { group ->
                group.asObjOrNull()?.arr("durl")?.let {
                    collectDashArray(it, isAudioHint = false, rootDrm, rootPreview, rootEpisode, cid, epId, nowMs, entries)
                }
            }
        }

        return entries.distinctBy { "${it.pathKey}|${it.primaryUrl}" }
    }

    private fun collectDashArray(
        arr: JsonArray?,
        isAudioHint: Boolean,
        rootDrm: Boolean,
        rootPreview: Boolean,
        rootEpisode: Boolean,
        cid: Long,
        epId: Long,
        nowMs: Long,
        out: MutableList<PlayinfoTrackEntry>,
        fallbackBandwidth: Long = 0L
    ) {
        if (arr == null) return
        for (elem in arr) {
            val obj = elem.asObjOrNull() ?: continue
            addTrackFromJsonObject(
                obj,
                isAudioHint,
                rootDrm,
                rootPreview,
                rootEpisode,
                cid,
                epId,
                nowMs,
                out,
                fallbackBandwidth = fallbackBandwidth
            )
        }
    }

    private fun appendResolvedTrackEntries(
        rawPrimary: String,
        rawBackups: List<String>,
        hasDrm: Boolean,
        isPreview: Boolean,
        isEpisode: Boolean,
        isAudio: Boolean,
        cid: Long,
        epId: Long,
        nowMs: Long,
        out: MutableList<PlayinfoTrackEntry>,
        bandwidth: Long = 0L
    ) {
        // 调度域名 (mountaintoys.cn / szbdyd.com) 在被剥离前先通过 xy_usource 还原原生 CDN
        val unwrappedPrimary = BiliAcceleratorCore.unwrapSmtcdnsUrl(rawPrimary).ifBlank { rawPrimary }
            .let { recoverSchedulerSourceUrl(it) ?: it }
        val unwrappedBackups = rawBackups
            .filter { it.isNotBlank() }
            .map { BiliAcceleratorCore.unwrapSmtcdnsUrl(it).ifBlank { it } }
            .map { recoverSchedulerSourceUrl(it) ?: it }
            .distinct()
        val allNormalized = (listOf(unwrappedPrimary) + unwrappedBackups).filter { it.isNotBlank() }.distinct()
        if (allNormalized.isEmpty()) return

        val hasCleanOfficial = !hasDrm && !isPreview && allNormalized.any { BiliAcceleratorCore.isCleanUposDonor(it) }
        val effectivePrimary = if (!hasDrm && !isPreview && !BiliAcceleratorCore.isCleanUposDonor(unwrappedPrimary)) {
            allNormalized.firstOrNull { BiliAcceleratorCore.isCleanUposDonor(it) } ?: unwrappedPrimary
        } else {
            unwrappedPrimary
        }

        val effectiveBackups = allNormalized.filter { it != effectivePrimary }.let { remaining ->
            if (hasCleanOfficial) {
                remaining.filter { url ->
                    val parsed = BiliAcceleratorCore.parseUri(url) ?: return@filter false
                    val host = BiliAcceleratorCore.normalizeHost(parsed.host)
                    val oddPort = parsed.port != -1 && parsed.port != 80 && parsed.port != 443
                    val hasOsMcdn = parsed.queryParam("os")?.equals("mcdn", ignoreCase = true) == true
                    !BiliAcceleratorCore.isIpAddress(host) &&
                        !BiliAcceleratorCore.isKnownP2pHost(host) &&
                        !BiliAcceleratorCore.isMcdnHost(host) &&
                        !BiliAcceleratorCore.isSchedulerHost(host) &&
                        !oddPort &&
                        !hasOsMcdn
                }
            } else {
                remaining
            }
        }.distinct()

        val deadlineUrls = (listOf(effectivePrimary) + effectiveBackups).ifEmpty { allNormalized }
        val deadline = extractEarliestDeadline(deadlineUrls)
        val seenKeys = HashSet<String>()
        val keyUrls = (listOf(rawPrimary) + rawBackups + allNormalized).filter { it.isNotBlank() }.distinct()
        for (u in keyUrls) {
            val (pathKey, fileKey) = normalizePathAndFileKey(u)
            if (pathKey.isBlank() || !seenKeys.add(pathKey)) continue
            out.add(
                PlayinfoTrackEntry(
                    pathKey = pathKey,
                    fileKey = fileKey,
                    primaryUrl = effectivePrimary,
                    backupUrls = effectiveBackups,
                    deadlineEpochSec = deadline,
                    isAudio = isAudio,
                    hasDrm = hasDrm,
                    isPreview = isPreview,
                    isEpisode = isEpisode,
                    cid = cid,
                    epId = epId,
                    updatedAtMs = nowMs,
                    bandwidth = bandwidth.coerceAtLeast(0L)
                )
            )
        }
    }

    private fun addTrackFromJsonObject(
        obj: JsonObject,
        isAudioHint: Boolean,
        rootDrm: Boolean,
        rootPreview: Boolean,
        rootEpisode: Boolean,
        cid: Long,
        epId: Long,
        nowMs: Long,
        out: MutableList<PlayinfoTrackEntry>,
        fallbackBandwidth: Long = 0L
    ) {
        val primary = obj.strVal("baseUrl") ?: obj.strVal("base_url") ?: obj.strVal("url") ?: return
        if (primary.isBlank() || !BiliAcceleratorCore.hasMediaSignal(primary)) return
        val backups = mutableListOf<String>()
        for (key in listOf("backupUrl", "backup_url", "backup_url_list")) {
            obj.arr(key)?.forEach { b ->
                val s = b.asStringOrNull()
                if (!s.isNullOrBlank() && s != primary) backups.add(s)
            }
        }
        val allUrls = listOf(primary) + backups
        val hasDrm = rootDrm || hasJsonDrmSignal(obj) || allUrls.any { BiliAcceleratorCore.hasDrmSignal(it) }
        val isPreview = rootPreview || hasJsonPreviewSignal(obj) || allUrls.any { BiliAcceleratorCore.isEpisodePreview(it) }
        val isEpisode = rootEpisode || allUrls.any { BiliAcceleratorCore.isEpisodeMediaUrl(it) }
        val isAudio = isAudioHint || BiliAcceleratorCore.isAudioTrack(primary)
        val parsedBandwidth = (obj.longVal("bandwidth")
            ?: obj.longVal("band_width")
            ?: obj.longVal("bitrate")
            ?: obj.longVal("bit_rate")
            ?: fallbackBandwidth).coerceAtLeast(0L)
        appendResolvedTrackEntries(
            rawPrimary = primary,
            rawBackups = backups,
            hasDrm = hasDrm,
            isPreview = isPreview,
            isEpisode = isEpisode,
            isAudio = isAudio,
            cid = cid,
            epId = epId,
            nowMs = nowMs,
            out = out,
            bandwidth = parsedBandwidth
        )
    }

    fun ingestJson(
        jsonText: String,
        sourceUrl: String = "",
        refreshCallback: PlayinfoRefreshCallback? = null,
        refreshSource: RefreshSource = RefreshSource.OTHER
    ): List<PlayinfoTrackEntry> {
        ingestLiveGroups(parseLivePlayinfoJson(jsonText))
        val entries = parsePlayinfoJson(jsonText, sourceUrl)
        if (entries.isEmpty()) return emptyList()
        storeEntries(entries, refreshCallback, refreshSource)
        runCatching { AutoVodRouter.ingestPlayinfoJson(jsonText) }
        runCatching { AutoVodRouter.ingestPlayinfoTracks(entries) }
        return entries
    }

    // ───────────────────────── Live (getRoomPlayInfo) ─────────────────────────

    private const val MAX_LIVE_PATHS = 256
    private data class LiveEntry(val urls: List<String>, val updatedAtMs: Long)
    private val liveByPath = ConcurrentHashMap<String, LiveEntry>()

    internal fun clearLiveForTesting() = liveByPath.clear()

    /**
     * 还原 mountaintoys.cn / szbdyd.com 调度域名 URL 中 xy_usource 携带的原生 B 站 CDN。
     * 带 os=mcdn 的中继 URL、/v1/resource/ 边缘路径或 xy_usource 非 B 站 CDN 时返回 null。
     */
    internal fun recoverSchedulerSourceUrl(url: String): String? {
        return BiliAcceleratorCore.extractSchedulerSourceUrl(url)?.takeIf { it.isNotBlank() && it != url }
    }

    private fun isPcdnLiveUrl(url: String): Boolean {
        val parsed = BiliAcceleratorCore.parseUri(url) ?: return true
        val host = BiliAcceleratorCore.normalizeHost(parsed.host)
        if (host.isBlank()) return true
        val oddPort = parsed.port != -1 && parsed.port != 80 && parsed.port != 443
        val osMcdn = parsed.queryParam("os")?.equals("mcdn", ignoreCase = true) == true
        return BiliAcceleratorCore.isIpAddress(host) ||
            BiliAcceleratorCore.isKnownP2pHost(host) ||
            BiliAcceleratorCore.isMcdnHost(host) ||
            BiliAcceleratorCore.isSchedulerHost(host) ||
            oddPort || osMcdn
    }

    private fun joinLiveUrl(host: String, baseUrl: String, extra: String): String? {
        if (host.isBlank() || baseUrl.isBlank()) return null
        val h = if (host.startsWith("http", ignoreCase = true)) host.trimEnd('/') else "https://" + host.trimEnd('/')
        val path = if (baseUrl.startsWith("/")) baseUrl else "/$baseUrl"
        val sep = if (path.contains('?')) {
            if (path.endsWith("?") || path.endsWith("&") || extra.startsWith("?") || extra.startsWith("&")) "" else "&"
        } else if (extra.isBlank() || extra.startsWith("?")) "" else "?"
        return h + path + sep + extra
    }

    /** playurl_info.playurl.stream[].format[].codec[] -> 每个 codec 的所有官方线路 (保留各节点 extra 鉴权) */
    fun parseLivePlayinfoJson(jsonText: String): List<List<String>> {
        if (!jsonText.contains("\"url_info\"")) return emptyList()
        val root = runCatching { JsonParser.parseString(jsonText) }.getOrNull()?.asObjOrNull() ?: return emptyList()
        val data = root.obj("data")
        val playurlInfo = data?.obj("playurl_info") ?: data?.obj("data")?.obj("playurl_info")
            ?: root.obj("playurl_info") ?: root.obj("result")?.obj("playurl_info")
        val streams = playurlInfo?.obj("playurl")?.arr("stream") ?: return emptyList()
        val groups = mutableListOf<List<String>>()
        for (s in streams) {
            for (f in s.asObjOrNull()?.arr("format") ?: continue) {
                for (c in f.asObjOrNull()?.arr("codec") ?: continue) {
                    val codec = c.asObjOrNull() ?: continue
                    val base = codec.strVal("base_url") ?: continue
                    val urls = mutableListOf<String>()
                    for (ui in codec.arr("url_info") ?: continue) {
                        val info = ui.asObjOrNull() ?: continue
                        joinLiveUrl(info.strVal("host").orEmpty(), base, info.strVal("extra").orEmpty())
                            ?.let { urls.add(it) }
                    }
                    if (urls.isNotEmpty()) groups.add(urls)
                }
            }
        }
        return groups
    }

    /** protobuf: 任意节点含 getBaseUrl + getUrlInfoList(getHost/getExtra) 即视为 live codec */
    fun parseLivePlayinfoProto(reply: Any?): List<List<String>> {
        if (reply == null) return emptyList()
        val groups = mutableListOf<List<String>>()
        val visited = Collections.newSetFromMap(IdentityHashMap<Any, Boolean>())
        walkLiveProto(reply, 0, visited, groups)
        return groups
    }

    private fun walkLiveProto(node: Any?, depth: Int, visited: MutableSet<Any>, out: MutableList<List<String>>) {
        if (node == null || depth > MAX_PROTO_DEPTH) return
        if (node is String || node is Number || node is Boolean || node is Enum<*>) return
        if (!visited.add(node)) return
        if (node is Iterable<*>) { node.forEach { walkLiveProto(it, depth + 1, visited, out) }; return }
        if (node is Array<*>) { node.forEach { walkLiveProto(it, depth + 1, visited, out) }; return }
        if (node is Map<*, *>) { node.values.forEach { walkLiveProto(it, depth + 1, visited, out) }; return }
        val cls = node.javaClass
        if (cls.name.startsWith("java.") || cls.name.startsWith("android.")) return
        val methods = runCatching { cls.methods }.getOrNull().orEmpty().filter { it.parameterTypes.isEmpty() }
        val baseM = methods.firstOrNull { it.name == "getBaseUrl" }
        val infoM = methods.firstOrNull { it.name == "getUrlInfoList" }
        if (baseM != null && infoM != null) {
            val base = invokeZeroArg(baseM, node) as? String
            val infos = invokeZeroArg(infoM, node) as? Iterable<*>
            if (!base.isNullOrBlank() && infos != null) {
                val urls = mutableListOf<String>()
                for (info in infos) {
                    if (info == null) continue
                    val host = runCatching { info.javaClass.getMethod("getHost").invoke(info) as? String }.getOrNull().orEmpty()
                    val extra = runCatching { info.javaClass.getMethod("getExtra").invoke(info) as? String }.getOrNull().orEmpty()
                    joinLiveUrl(host, base, extra)?.let { urls.add(it) }
                }
                if (urls.isNotEmpty()) out.add(urls)
            }
            return
        }
        for (m in methods) {
            val n = m.name
            if (!n.startsWith("get") || n == "getClass" || n == "getDefaultInstanceForType" || n == "getParserForType" ||
                n.endsWith("OrBuilder") || n.endsWith("Count") || n.endsWith("Bytes")
            ) continue
            val ret = m.returnType
            if (ret.isPrimitive || ret == String::class.java || Number::class.java.isAssignableFrom(ret)) continue
            val child = invokeZeroArg(m, node) ?: continue
            if (child !== node) walkLiveProto(child, depth + 1, visited, out)
        }
    }

    /** 对每个 codec 分组: 还原调度域名、剔除 PCDN、保留 extra，按 path 存储 */
    internal fun ingestLiveGroups(groups: List<List<String>>, nowMs: Long = System.currentTimeMillis()): Int {
        var stored = 0
        for (group in groups) {
            val cleaned = group
                .map { BiliAcceleratorCore.unwrapSmtcdnsUrl(it).ifBlank { it } }
                .map { recoverSchedulerSourceUrl(it) ?: it }
                .filter { !isPcdnLiveUrl(it) }
                .distinct()
            if (cleaned.isEmpty()) continue
            val paths = cleaned.mapNotNull { livePathKey(it) }.toSet()
            if (liveByPath.size > MAX_LIVE_PATHS) {
                val cutoff = nowMs - ENTRY_TTL_MS
                liveByPath.entries.removeIf { it.value.updatedAtMs < cutoff }
                if (liveByPath.size > MAX_LIVE_PATHS) liveByPath.clear()
            }
            for (p in paths) {
                liveByPath[p] = LiveEntry(cleaned.filter { livePathKey(it) == p }, nowMs)
                stored++
            }
        }
        return stored
    }

    private fun livePathKey(url: String): String? =
        BiliAcceleratorCore.parseUri(url)?.path?.trim()?.trimEnd('/')?.lowercase()?.takeIf { it.isNotBlank() }

    /** 返回与 url 同一 live 流路径的其它官方线路 (不含 url 本身，保持原顺序与 extra 鉴权) */
    fun lookupLiveBackups(url: String): List<String> {
        val key = livePathKey(url) ?: return emptyList()
        val e = liveByPath[key] ?: return emptyList()
        if (System.currentTimeMillis() - e.updatedAtMs > ENTRY_TTL_MS) return emptyList()
        return e.urls.filter { it != url }
    }

    fun parsePlayViewReply(
        reply: Any?,
        sourceTag: String = "",
        nowMs: Long = System.currentTimeMillis()
    ): List<PlayinfoTrackEntry> {
        if (reply == null) return emptyList()
        val className = reply.javaClass.name.lowercase()
        val isEpisodeRoot = BiliAcceleratorCore.isEpisodeMediaUrl(sourceTag) ||
            className.contains(".pgc.") ||
            className.contains(".ogv.") ||
            className.contains("bangumi")

        val visited = Collections.newSetFromMap(IdentityHashMap<Any, Boolean>())
        val flags = ProtoFlags(isEpisode = isEpisodeRoot)
        val rawTracks = mutableListOf<RawProtoTrack>()

        inspectProtoNode(
            node = reply,
            depth = 0,
            visited = visited,
            flags = flags,
            tracks = rawTracks,
            isAudioContext = false,
            inheritedDrm = false,
            inheritedPreview = false,
            fromStreamCollection = false
        )

        if (rawTracks.isEmpty()) return emptyList()
        val entries = mutableListOf<PlayinfoTrackEntry>()
        for (t in rawTracks) {
            val allUrls = (listOf(t.primaryUrl) + t.backupUrls).filter { it.isNotBlank() }.distinct()
            if (allUrls.isEmpty()) continue
            val hasDrm = flags.hasDrm || t.hasDrm || allUrls.any { BiliAcceleratorCore.hasDrmSignal(it) }
            val isPreview = flags.isPreview || t.isPreview || allUrls.any { BiliAcceleratorCore.isEpisodePreview(it) }
            val isEpisode = flags.isEpisode || t.isEpisode || allUrls.any { BiliAcceleratorCore.isEpisodeMediaUrl(it) }
            val isAudio = t.isAudio || BiliAcceleratorCore.isAudioTrack(t.primaryUrl)
            appendResolvedTrackEntries(
                rawPrimary = t.primaryUrl,
                rawBackups = t.backupUrls,
                hasDrm = hasDrm,
                isPreview = isPreview,
                isEpisode = isEpisode,
                isAudio = isAudio,
                cid = flags.cid,
                epId = flags.epId,
                nowMs = nowMs,
                out = entries,
                bandwidth = t.bandwidth
            )
        }
        return entries.distinctBy { "${it.pathKey}|${it.primaryUrl}" }
    }

    fun ingestPlayViewReply(
        reply: Any?,
        sourceTag: String = "",
        refreshCallback: PlayinfoRefreshCallback? = null,
        refreshSource: RefreshSource = RefreshSource.GRPC
    ): List<PlayinfoTrackEntry> {
        val liveName = reply?.javaClass?.name.orEmpty()
        if (liveName.contains("RoomPlayInfo", ignoreCase = true) ||
            (liveName.contains(".live.", ignoreCase = true) && liveName.contains("PlayInfo", ignoreCase = true))
        ) {
            ingestLiveGroups(parseLivePlayinfoProto(reply))
        }
        val entries = parsePlayViewReply(reply, sourceTag)
        if (entries.isEmpty()) return emptyList()
        storeEntries(entries, refreshCallback, refreshSource)
        runCatching { AutoVodRouter.ingestPlayinfoTracks(entries) }
        return entries
    }

    private data class ProtoFlags(
        var hasDrm: Boolean = false,
        var isPreview: Boolean = false,
        var isEpisode: Boolean = false,
        var cid: Long = 0L,
        var epId: Long = 0L,
        var bandwidth: Long = 0L
    )

    private data class RawProtoTrack(
        val primaryUrl: String,
        val backupUrls: List<String>,
        val isAudio: Boolean,
        val hasDrm: Boolean = false,
        val isPreview: Boolean = false,
        val isEpisode: Boolean = false,
        val bandwidth: Long = 0L
    )

    private fun isStreamClassName(clsName: String): Boolean {
        return clsName.endsWith("stream") ||
            clsName.contains("\$stream") ||
            clsName.contains("dashitem") ||
            clsName.contains("dashvideo") ||
            clsName.contains("segmentvideo") ||
            clsName.contains("responseurl")
    }

    private fun containsMediaUrlShallow(
        node: Any?,
        maxDepth: Int,
        seen: MutableSet<Any> = Collections.newSetFromMap(IdentityHashMap())
    ): Boolean {
        if (node == null || maxDepth < 0) return false
        if (node is String) return BiliAcceleratorCore.hasMediaSignal(node)
        if (node is Number || node is Boolean || node is Enum<*>) return false
        if (!seen.add(node)) return false
        if (node is Iterable<*>) {
            for (item in node) {
                if (containsMediaUrlShallow(item, maxDepth - 1, seen)) return true
            }
            return false
        }
        if (node is Array<*>) {
            for (item in node) {
                if (containsMediaUrlShallow(item, maxDepth - 1, seen)) return true
            }
            return false
        }
        if (node is Map<*, *>) {
            for (v in node.values) {
                if (containsMediaUrlShallow(v, maxDepth - 1, seen)) return true
            }
            return false
        }
        val cls = node.javaClass
        val clsName = cls.name.lowercase()
        if (clsName.startsWith("java.") || clsName.startsWith("android.")) return false
        val fields = runCatching { cls.declaredFields }.getOrNull().orEmpty()
        for (f in fields) {
            val fname = f.name.lowercase()
            if (fname.contains("memoized") || fname == "unknownfields" || fname == "default_instance" || fname == "parser") continue
            val v = runCatching {
                f.isAccessible = true
                f.get(node)
            }.getOrNull() ?: continue
            if (v is String) {
                if (BiliAcceleratorCore.hasMediaSignal(v)) return true
            } else if (maxDepth > 0 && v !is Number && v !is Boolean && v !is Enum<*>) {
                if (containsMediaUrlShallow(v, maxDepth - 1, seen)) return true
            }
        }
        val methods = runCatching { cls.methods }.getOrNull().orEmpty()
        for (m in methods) {
            if (m.parameterTypes.isNotEmpty()) continue
            val name = m.name
            if (name == "getBaseUrl" || name == "getUrl" || name == "getBackupUrlList" || name == "getBackupUrl") {
                val v = invokeZeroArg(m, node) ?: continue
                if (v is String && BiliAcceleratorCore.hasMediaSignal(v)) return true
                if (v is Iterable<*> && v.any { it is String && BiliAcceleratorCore.hasMediaSignal(it) }) return true
            }
        }
        return false
    }

    private fun isProbableStreamElement(item: Any): Boolean {
        if (item is String || item is Number || item is Boolean || item is Enum<*>) return false
        val clsName = item.javaClass.name.lowercase()
        if (clsName.startsWith("java.") || clsName.startsWith("android.")) return false
        if (isStreamClassName(clsName)) return true
        return containsMediaUrlShallow(item, maxDepth = 2)
    }

    private fun inspectProtoNode(
        node: Any?,
        depth: Int,
        visited: MutableSet<Any>,
        flags: ProtoFlags,
        tracks: MutableList<RawProtoTrack>,
        isAudioContext: Boolean,
        inheritedDrm: Boolean = false,
        inheritedPreview: Boolean = false,
        fromStreamCollection: Boolean = false
    ) {
        if (node == null || depth > MAX_PROTO_DEPTH) return
        if (node is String || node is Number || node is Boolean || node is Enum<*>) return
        if (!visited.add(node)) return

        if (node is Iterable<*>) {
            val items = node.toList()
            val isStreamList = items.any { it != null && isProbableStreamElement(it) }
            for (item in items) {
                inspectProtoNode(
                    item,
                    depth + 1,
                    visited,
                    flags,
                    tracks,
                    isAudioContext,
                    inheritedDrm,
                    inheritedPreview,
                    fromStreamCollection = isStreamList
                )
            }
            return
        }
        if (node is Array<*>) {
            val isStreamList = node.any { it != null && isProbableStreamElement(it) }
            for (item in node) {
                inspectProtoNode(
                    item,
                    depth + 1,
                    visited,
                    flags,
                    tracks,
                    isAudioContext,
                    inheritedDrm,
                    inheritedPreview,
                    fromStreamCollection = isStreamList
                )
            }
            return
        }
        if (node is Map<*, *>) {
            for (v in node.values) {
                inspectProtoNode(v, depth + 1, visited, flags, tracks, isAudioContext, inheritedDrm, inheritedPreview)
            }
            return
        }

        val cls = node.javaClass
        val clsName = cls.name.lowercase()
        if (clsName.startsWith("java.") || clsName.startsWith("android.")) return

        if (clsName.contains("pgc") || clsName.contains("ogv") || clsName.contains("bangumi")) {
            flags.isEpisode = true
        }

        val localAudio = isAudioContext || clsName.contains("audio") || clsName.contains("dolby") || clsName.contains("flac")
        var localDrm = inheritedDrm
        var localPreview = inheritedPreview
        var localBandwidth = 0L

        val methods = runCatching { cls.methods }.getOrNull().orEmpty()
        var foundBaseUrl: String? = null
        val foundBackups = mutableListOf<String>()
        val childGetters = mutableListOf<Pair<String, Any>>()
        var enumDrmTechResult: Boolean? = null
        var rawDrmTechValue: Int? = null
        var hasStreamInfoGetter = false
        var hasMediaChildGetter = false

        // First pass: inspect scalar getters, URLs, and flags before recursing into children
        for (m in methods) {
            if (m.parameterTypes.isNotEmpty()) continue
            val name = m.name
            if (name == "getClass" || name == "hashCode" || name == "toString" || name == "getDefaultInstanceForType" || name == "getSerializedSize" || name == "getParserForType") {
                continue
            }
            when (name) {
                "getIsDrm", "hasDrm", "getWidevine" -> {
                    if (isTruthyValue(invokeZeroArg(m, node))) localDrm = true
                }
                "getDrmTechTypeValue", "getDrmTypeValue" -> {
                    val v = (invokeZeroArg(m, node) as? Number)?.toInt()
                    if (v != null) rawDrmTechValue = v
                }
                "getDrmTechType", "getDrmType" -> {
                    val v = invokeZeroArg(m, node)
                    val active = isDrmEnumOrValueActive(v)
                    if (v is Enum<*> || v is String) {
                        enumDrmTechResult = active
                    }
                    if (active) localDrm = true
                }
                "getPssh" -> {
                    val v = invokeZeroArg(m, node)?.toString()
                    if (!v.isNullOrBlank()) localDrm = true
                }
                "getIsPreview", "getPreview", "getIsTrial" -> {
                    if (isTruthyValue(invokeZeroArg(m, node))) localPreview = true
                }
                "getWatchTimeLimit" -> {
                    val v = (invokeZeroArg(m, node) as? Number)?.toLong() ?: 0L
                    if (v > 0L) localPreview = true
                }
                "getEpId", "getEpisodeId", "getSeasonId" -> {
                    val v = (invokeZeroArg(m, node) as? Number)?.toLong() ?: 0L
                    if (v > 0L) {
                        flags.isEpisode = true
                        if (name != "getSeasonId" && flags.epId == 0L) flags.epId = v
                    }
                }
                "getCid" -> {
                    val v = (invokeZeroArg(m, node) as? Number)?.toLong() ?: 0L
                    if (v > 0L && flags.cid == 0L) flags.cid = v
                }
                "getBandwidth", "getBandWidth", "getBitrate", "getBitRate" -> {
                    val v = (invokeZeroArg(m, node) as? Number)?.toLong() ?: 0L
                    if (v > 0L) localBandwidth = v
                }
                "getVideoType", "getVideoTypeValue" -> {
                    val s = invokeZeroArg(m, node)?.toString()?.uppercase().orEmpty()
                    if (s.contains("PGC") || s.contains("OGV") || s == "2") flags.isEpisode = true
                }
                "getBaseUrl", "getUrl" -> {
                    val s = invokeZeroArg(m, node) as? String
                    if (!s.isNullOrBlank() && BiliAcceleratorCore.hasMediaSignal(s)) {
                        foundBaseUrl = s
                    }
                }
                "getBackupUrlList", "getBackupUrl" -> {
                    val v = invokeZeroArg(m, node)
                    if (v is Iterable<*>) {
                        v.filterIsInstance<String>().filter { BiliAcceleratorCore.hasMediaSignal(it) }.forEach { foundBackups.add(it) }
                    } else if (v is String && BiliAcceleratorCore.hasMediaSignal(v)) {
                        foundBackups.add(v)
                    }
                }
                else -> {
                    if (name.startsWith("get") && !name.endsWith("OrBuilder") && !name.endsWith("Count") && !name.endsWith("Bytes")) {
                        val ret = m.returnType
                        if (!ret.isPrimitive && ret != String::class.java && !Number::class.java.isAssignableFrom(ret)) {
                            val child = invokeZeroArg(m, node)
                            if (child != null && child !== node) {
                                if (name.contains("StreamInfo", ignoreCase = true)) hasStreamInfoGetter = true
                                if (name.contains("DashVideo", ignoreCase = true) || name.contains("SegmentVideo", ignoreCase = true)) {
                                    hasMediaChildGetter = true
                                }
                                childGetters.add(name to child)
                            }
                        }
                    }
                }
            }
        }
        if (enumDrmTechResult == null && (rawDrmTechValue ?: 0) > 0) {
            localDrm = true
        }

        // Also scan declared fields for obfuscated Protobuf builds (e.g. R8 keeping field names or media URLs)
        val childFields = mutableListOf<Pair<String, Any>>()
        val fields = runCatching { cls.declaredFields }.getOrNull().orEmpty()
        for (f in fields) {
            val fname = f.name.lowercase()
            if (fname.contains("memoized") || fname == "unknownfields" || fname == "default_instance" || fname == "parser") continue
            val valObj = runCatching {
                f.isAccessible = true
                f.get(node)
            }.getOrNull() ?: continue

            when {
                fname.contains("drmtechtype") || fname.contains("drm_tech_type") || fname.contains("drmtype") || fname.contains("drm_type") -> {
                    if (!(valObj is Number && enumDrmTechResult == false) && isDrmEnumOrValueActive(valObj)) {
                        localDrm = true
                    }
                }
                fname.contains("isdrm") || fname.contains("is_drm") || fname.contains("hasdrm") || fname.contains("has_drm") || fname.contains("widevine") -> {
                    if (isTruthyValue(valObj)) localDrm = true
                }
                fname.contains("ispreview") || fname.contains("is_preview") -> {
                    if (isTruthyValue(valObj)) localPreview = true
                }
                fname.contains("bandwidth") || fname.contains("band_width") || fname.contains("bitrate") || fname.contains("bit_rate") -> {
                    val bw = (valObj as? Number)?.toLong() ?: 0L
                    if (bw > 0L && localBandwidth <= 0L) localBandwidth = bw
                }
                fname.contains("epid") || fname.contains("ep_id") || fname.contains("seasonid") || fname.contains("season_id") -> {
                    val n = (valObj as? Number)?.toLong() ?: 0L
                    if (n > 0L) {
                        flags.isEpisode = true
                        if (!fname.contains("season") && flags.epId == 0L) flags.epId = n
                    }
                }
                fname == "cid" || fname == "cid_" -> {
                    val n = (valObj as? Number)?.toLong() ?: 0L
                    if (n > 0L && flags.cid == 0L) flags.cid = n
                }
                valObj is String && BiliAcceleratorCore.hasMediaSignal(valObj) -> {
                    if (fname.contains("backup")) {
                        foundBackups.add(valObj)
                    } else if (foundBaseUrl == null) {
                        foundBaseUrl = valObj
                    }
                }
                valObj is Iterable<*> -> {
                    val strItems = valObj.filterIsInstance<String>().filter { BiliAcceleratorCore.hasMediaSignal(it) }
                    if (strItems.isNotEmpty() && fname.contains("backup")) {
                        foundBackups.addAll(strItems)
                    } else {
                        childFields.add(fname to valObj)
                    }
                }
                !valObj.javaClass.isPrimitive && valObj !is Number && valObj !is Boolean && valObj !is Enum<*> && valObj !is String -> {
                    childFields.add(fname to valObj)
                }
            }
        }

        val hasDirectMediaUrl = foundBaseUrl != null || foundBackups.isNotEmpty()
        val isPerStreamScope = fromStreamCollection ||
            hasDirectMediaUrl ||
            isStreamClassName(clsName) ||
            (hasStreamInfoGetter && hasMediaChildGetter)

        // Inspect metadata sub-objects (e.g. streamInfo / viewInfo / non-media metadata) before media-bearing children
        val orderedGetters = childGetters.sortedBy { (name, child) ->
            when {
                name.contains("StreamInfo", ignoreCase = true) || name.contains("ViewInfo", ignoreCase = true) || name.contains("TrialInfo", ignoreCase = true) -> 0
                isPerStreamScope && !containsMediaUrlShallow(child, maxDepth = 1) -> 0
                else -> 1
            }
        }
        val orderedFields = if (isPerStreamScope) {
            childFields.sortedBy { (fname, valObj) ->
                when {
                    fname.contains("streaminfo") || fname.contains("stream_info") || fname.contains("viewinfo") || fname.contains("trial") -> 0
                    !containsMediaUrlShallow(valObj, maxDepth = 1) -> 0
                    else -> 1
                }
            }
        } else {
            childFields
        }

        val streamContextFlags = if (isPerStreamScope) ProtoFlags(isEpisode = flags.isEpisode) else flags
        val startTrackIdx = tracks.size
        for ((name, child) in orderedGetters) {
            val childAudio = localAudio || name.contains("Audio", ignoreCase = true) || name.contains("Dolby", ignoreCase = true) || name.contains("Flac", ignoreCase = true)
            inspectProtoNode(
                node = child,
                depth = depth + 1,
                visited = visited,
                flags = streamContextFlags,
                tracks = tracks,
                isAudioContext = childAudio,
                inheritedDrm = localDrm,
                inheritedPreview = localPreview
            )
            if (isPerStreamScope) {
                if (streamContextFlags.hasDrm) localDrm = true
                if (streamContextFlags.isPreview) localPreview = true
                if (streamContextFlags.bandwidth > 0L && localBandwidth <= 0L) localBandwidth = streamContextFlags.bandwidth
                if (streamContextFlags.isEpisode) flags.isEpisode = true
                if (streamContextFlags.cid != 0L && flags.cid == 0L) flags.cid = streamContextFlags.cid
                if (streamContextFlags.epId != 0L && flags.epId == 0L) flags.epId = streamContextFlags.epId
            }
        }
        for ((fname, valObj) in orderedFields) {
            val childAudio = localAudio || fname.contains("audio") || fname.contains("dolby") || fname.contains("flac")
            inspectProtoNode(
                node = valObj,
                depth = depth + 1,
                visited = visited,
                flags = streamContextFlags,
                tracks = tracks,
                isAudioContext = childAudio,
                inheritedDrm = localDrm,
                inheritedPreview = localPreview
            )
            if (isPerStreamScope) {
                if (streamContextFlags.hasDrm) localDrm = true
                if (streamContextFlags.isPreview) localPreview = true
                if (streamContextFlags.bandwidth > 0L && localBandwidth <= 0L) localBandwidth = streamContextFlags.bandwidth
                if (streamContextFlags.isEpisode) flags.isEpisode = true
                if (streamContextFlags.cid != 0L && flags.cid == 0L) flags.cid = streamContextFlags.cid
                if (streamContextFlags.epId != 0L && flags.epId == 0L) flags.epId = streamContextFlags.epId
            }
        }

        if (isPerStreamScope) {
            // Retroactively apply stream-scoped DRM/preview/bandwidth flags to child tracks regardless of obfuscated field order
            if (localDrm || localPreview || localBandwidth > 0L) {
                for (i in startTrackIdx until tracks.size) {
                    val t = tracks[i]
                    val nextDrm = t.hasDrm || localDrm
                    val nextPreview = t.isPreview || localPreview
                    val nextBw = if (t.bandwidth > 0L) t.bandwidth else localBandwidth
                    if (nextDrm != t.hasDrm || nextPreview != t.isPreview || nextBw != t.bandwidth) {
                        tracks[i] = t.copy(hasDrm = nextDrm, isPreview = nextPreview, bandwidth = nextBw)
                    }
                }
            }
        } else {
            if (localDrm) flags.hasDrm = true
            if (localPreview) flags.isPreview = true
            if (localBandwidth > 0L && flags.bandwidth <= 0L) flags.bandwidth = localBandwidth
        }

        val primary = foundBaseUrl ?: foundBackups.firstOrNull()
        if (!primary.isNullOrBlank()) {
            val effectiveBandwidth = if (localBandwidth > 0L) localBandwidth else streamContextFlags.bandwidth
            tracks.add(
                RawProtoTrack(
                    primaryUrl = primary,
                    backupUrls = foundBackups.filter { it != primary }.distinct(),
                    isAudio = localAudio,
                    hasDrm = localDrm,
                    isPreview = localPreview,
                    isEpisode = flags.isEpisode,
                    bandwidth = effectiveBandwidth.coerceAtLeast(0L)
                )
            )
        }
    }

    private fun invokeZeroArg(m: Method, target: Any): Any? {
        return runCatching {
            if (!m.isAccessible) m.isAccessible = true
            m.invoke(target)
        }.getOrNull()
    }

    private fun <K : Any> putRefresherPreferringGrpc(
        map: ConcurrentHashMap<K, RegisteredRefresher>,
        key: K,
        incoming: RegisteredRefresher
    ) {
        map.compute(key) { _, existing ->
            when {
                existing == null -> incoming
                incoming.isGrpc -> incoming
                existing.isGrpc && (incoming.registeredAtMs - existing.registeredAtMs) <= ENTRY_TTL_MS -> existing
                else -> incoming
            }
        }
    }

    private fun storeEntries(
        entries: List<PlayinfoTrackEntry>,
        refreshCallback: PlayinfoRefreshCallback?,
        refreshSource: RefreshSource = RefreshSource.OTHER
    ) {
        pruneIfNeeded()
        val now = System.currentTimeMillis()
        val storedList = ArrayList<PlayinfoTrackEntry>(entries.size)
        for (entry in entries) {
            val prev = byPathKey[entry.pathKey]
            val merged = if (prev == null) {
                entry
            } else if (entry.updatedAtMs < prev.updatedAtMs) {
                // Out-of-order older playinfo: preserve fresher flags & URLs from prev
                prev.copy(
                    isEpisode = prev.isEpisode || entry.isEpisode,
                    cid = if (prev.cid > 0L) prev.cid else entry.cid,
                    epId = if (prev.epId > 0L) prev.epId else entry.epId,
                    bandwidth = if (prev.bandwidth > 0L) prev.bandwidth else entry.bandwidth
                )
            } else {
                // Newer (or same-ms) playinfo replaces hasDrm / isPreview instead of sticky ||
                val keepPrevUrls = !prev.hasDrm &&
                    !prev.isPreview &&
                    !entry.hasDrm &&
                    !entry.isPreview &&
                    prev.deadlineEpochSec > entry.deadlineEpochSec &&
                    prev.primaryUrl.isNotBlank()
                entry.copy(
                    primaryUrl = if (keepPrevUrls) prev.primaryUrl else entry.primaryUrl,
                    backupUrls = if (keepPrevUrls) prev.backupUrls else entry.backupUrls,
                    deadlineEpochSec = if (keepPrevUrls) prev.deadlineEpochSec else entry.deadlineEpochSec,
                    hasDrm = entry.hasDrm,
                    isPreview = entry.isPreview,
                    isEpisode = entry.isEpisode || prev.isEpisode,
                    cid = if (entry.cid > 0L) entry.cid else prev.cid,
                    epId = if (entry.epId > 0L) entry.epId else prev.epId,
                    bandwidth = if (entry.bandwidth > 0L) entry.bandwidth else prev.bandwidth
                )
            }
            storedList.add(merged)
            byPathKey[entry.pathKey] = merged
            val parentFileKey = parentFileKeyFromPath(entry.pathKey)
            if (parentFileKey.isNotBlank()) {
                byParentFileKey[parentFileKey] = merged
            }
            if (isDistinctiveFileKey(entry.fileKey)) {
                byFileKey[entry.fileKey] = merged
            }
            if (refreshCallback != null) {
                val registered = RegisteredRefresher(
                    callback = refreshCallback,
                    isGrpc = refreshSource == RefreshSource.GRPC,
                    registeredAtMs = now
                )
                putRefresherPreferringGrpc(refreshersByPath, entry.pathKey, registered)
                if (parentFileKey.isNotBlank()) {
                    putRefresherPreferringGrpc(refreshersByParentFile, parentFileKey, registered)
                }
                if (isDistinctiveFileKey(entry.fileKey)) {
                    putRefresherPreferringGrpc(refreshersByFile, entry.fileKey, registered)
                }
                if (merged.cid > 0L) {
                    putRefresherPreferringGrpc(refreshersByCid, merged.cid, registered)
                }
                if (merged.epId > 0L) {
                    putRefresherPreferringGrpc(refreshersByEpId, merged.epId, registered)
                }
            }
        }

        persistSharedTracks(storedList)

        // Proactively propagate fresh deadlines into any active BtrMediaSession
        val uniquePrimaries = storedList
            .filter { !it.hasDrm && !it.isPreview && it.deadlineEpochSec > 0L }
            .distinctBy { it.pathKey }
        for (e in uniquePrimaries) {
            runCatching {
                BtrSessionManager.refreshSignatureIfSamePath(e.primaryUrl, e.backupUrls)
            }
        }
    }

    fun lookup(url: String, forceSharedReload: Boolean = false): PlayinfoTrackEntry? {
        if (url.isBlank()) return null
        val (pathKey, parentFileKey, fileKey) = normalizeKeys(url)
        if (pathKey.isBlank() && fileKey.isBlank()) return null
        val useBareFileKey = isDistinctiveFileKey(fileKey)

        // Check cross-process shared state (250ms throttled when forceSharedReload=false) so status updates
        // such as clearing isPreview/hasDrm or refreshed deadlines propagate promptly across processes.
        loadSharedTracksIfUpdated(force = forceSharedReload)

        val now = System.currentTimeMillis()
        val found = (if (pathKey.isNotBlank()) byPathKey[pathKey] else null)
            ?: (if (parentFileKey.isNotBlank()) byParentFileKey[parentFileKey] else null)
            ?: (if (useBareFileKey) byFileKey[fileKey] else null)
        return if (found != null && (now - found.updatedAtMs) <= ENTRY_TTL_MS) found else null
    }

    fun findFresherTrack(url: String, currentDeadlineSec: Long): PlayinfoTrackEntry? {
        val meta = lookup(url, forceSharedReload = true) ?: return null
        if (meta.hasDrm || meta.isPreview) return null
        return if (meta.deadlineEpochSec > currentDeadlineSec && meta.primaryUrl.isNotBlank()) meta else null
    }

    private fun isFreshRefresher(reg: RegisteredRefresher?, nowMs: Long = System.currentTimeMillis()): RegisteredRefresher? {
        if (reg == null) return null
        return if (nowMs - reg.registeredAtMs <= ENTRY_TTL_MS) reg else null
    }

    private fun findLocalRefresherEntry(
        url: String,
        pathKeyHint: String = "",
        cidHint: Long = 0L,
        epIdHint: Long = 0L
    ): RegisteredRefresher? {
        val now = System.currentTimeMillis()
        val (normPath, parentFileKey, fileKey) = if (url.isNotBlank()) normalizeKeys(url) else Triple("", "", "")
        val effectivePath = normPath.ifBlank { pathKeyHint }
        val effectiveParent = parentFileKey.ifBlank { if (effectivePath.isNotBlank()) parentFileKeyFromPath(effectivePath) else "" }
        val effectiveFile = fileKey.ifBlank { effectivePath.substringAfterLast('/') }

        if (effectivePath.isNotBlank()) {
            isFreshRefresher(refreshersByPath[effectivePath], now)?.let { return it }
        }
        if (pathKeyHint.isNotBlank() && pathKeyHint != effectivePath) {
            isFreshRefresher(refreshersByPath[pathKeyHint], now)?.let { return it }
        }
        if (effectiveParent.isNotBlank()) {
            isFreshRefresher(refreshersByParentFile[effectiveParent], now)?.let { return it }
        }
        if (isDistinctiveFileKey(effectiveFile)) {
            isFreshRefresher(refreshersByFile[effectiveFile], now)?.let { return it }
        }

        val cachedMeta = (if (effectivePath.isNotBlank()) byPathKey[effectivePath] else null)
            ?: (if (effectiveParent.isNotBlank()) byParentFileKey[effectiveParent] else null)
            ?: (if (isDistinctiveFileKey(effectiveFile)) byFileKey[effectiveFile] else null)
        val cid = if (cidHint > 0L) cidHint else (cachedMeta?.cid ?: 0L)
        val epId = if (epIdHint > 0L) epIdHint else (cachedMeta?.epId ?: 0L)
        if (cid > 0L) {
            isFreshRefresher(refreshersByCid[cid], now)?.let { return it }
        }
        if (epId > 0L) {
            isFreshRefresher(refreshersByEpId[epId], now)?.let { return it }
        }
        return null
    }

    fun findRefreshCallback(url: String): PlayinfoRefreshCallback? {
        return findLocalRefresherEntry(url)?.callback
    }

    internal fun handleIncomingCrossProcessRefreshRequest(req: CrossProcessRefreshRequest): CrossProcessRefreshResult {
        if (req.url.isBlank() && req.pathKey.isBlank() && req.cid <= 0L && req.epId <= 0L) {
            return CrossProcessRefreshResult(false)
        }
        if (req.url.isNotBlank()) {
            val alreadyFresh = findFresherTrack(req.url, req.currentDeadlineSec)
            if (alreadyFresh != null) {
                return CrossProcessRefreshResult(
                    success = true,
                    primaryUrl = alreadyFresh.primaryUrl,
                    backupUrls = alreadyFresh.backupUrls,
                    deadlineEpochSec = alreadyFresh.deadlineEpochSec
                )
            }
        }
        val localRef = findLocalRefresherEntry(req.url, req.pathKey, req.cid, req.epId)
            ?: return CrossProcessRefreshResult(false)
        val invokedOk = runCatching { localRef.callback.refresh() }.getOrDefault(false)
        val updated = (if (req.url.isNotBlank()) lookup(req.url, forceSharedReload = false) else null)
            ?: req.pathKey.takeIf { it.isNotBlank() }?.let { byPathKey[it] }
            ?: byPathKey.values.firstOrNull { entry ->
                !entry.hasDrm && !entry.isPreview &&
                    ((req.cid > 0L && entry.cid == req.cid) || (req.epId > 0L && entry.epId == req.epId))
            }
        if (updated == null || updated.primaryUrl.isBlank() || updated.hasDrm || updated.isPreview) {
            return CrossProcessRefreshResult(false)
        }
        val deadlineAdvanced = req.currentDeadlineSec <= 0L || updated.deadlineEpochSec > req.currentDeadlineSec
        val urlRotated = invokedOk && req.url.isNotBlank() && updated.primaryUrl != req.url
        val success = invokedOk && (deadlineAdvanced || urlRotated)
        return if (success) {
            CrossProcessRefreshResult(
                success = true,
                primaryUrl = updated.primaryUrl,
                backupUrls = updated.backupUrls,
                deadlineEpochSec = updated.deadlineEpochSec
            )
        } else {
            CrossProcessRefreshResult(false)
        }
    }

    private fun applyCrossProcessRefreshedTrack(
        originalUrl: String,
        previousMeta: PlayinfoTrackEntry?,
        result: CrossProcessRefreshResult
    ) {
        if (result.primaryUrl.isBlank()) return
        val now = System.currentTimeMillis()
        val effectiveDeadline = result.deadlineEpochSec.takeIf { it > 0L }
            ?: extractEarliestDeadline(listOf(result.primaryUrl) + result.backupUrls)
        val keys = linkedSetOf<String>()
        val (primaryPath, primaryFile) = normalizePathAndFileKey(result.primaryUrl)
        if (primaryPath.isNotBlank()) keys.add(primaryPath)
        val (origPath, origFile) = normalizePathAndFileKey(originalUrl)
        if (origPath.isNotBlank()) keys.add(origPath)
        if (!previousMeta?.pathKey.isNullOrBlank()) keys.add(previousMeta!!.pathKey)

        for (pk in keys) {
            val fk = pk.substringAfterLast('/').ifBlank { primaryFile.ifBlank { origFile } }
            val entry = PlayinfoTrackEntry(
                pathKey = pk,
                fileKey = fk,
                primaryUrl = result.primaryUrl,
                backupUrls = result.backupUrls,
                deadlineEpochSec = effectiveDeadline,
                isAudio = previousMeta?.isAudio ?: BiliAcceleratorCore.isAudioTrack(result.primaryUrl),
                hasDrm = false,
                isPreview = false,
                isEpisode = previousMeta?.isEpisode ?: BiliAcceleratorCore.isEpisodeMediaUrl(result.primaryUrl),
                cid = previousMeta?.cid ?: 0L,
                epId = previousMeta?.epId ?: 0L,
                updatedAtMs = now,
                bandwidth = previousMeta?.bandwidth ?: 0L
            )
            byPathKey[pk] = entry
            val pfk = parentFileKeyFromPath(pk)
            if (pfk.isNotBlank()) byParentFileKey[pfk] = entry
            if (isDistinctiveFileKey(fk)) byFileKey[fk] = entry
        }
    }

    /**
     * 请求刷新指定媒体 URL 的鉴权签名：
     * 1. 先检查内存及跨进程共享文件是否已有更新鲜的签名；
     * 2. 若当前进程持有本地 refresher（优先 gRPC），直接调用；
     * 3. 否则通过同包跨进程广播桥接（或测试桥接）向持有 refresher 的解析进程发起超时受控的刷新请求，
     *    并将刷新后的主备 URL 同步回本地缓存与活跃的 BtrMediaSession。
     */
    fun requestRefresh(
        url: String,
        currentDeadlineSec: Long = 0L,
        timeoutMs: Long = CROSS_PROCESS_REFRESH_TIMEOUT_MS
    ): Boolean {
        if (url.isBlank()) return false

        val alreadyFresh = findFresherTrack(url, currentDeadlineSec)
        if (alreadyFresh != null) {
            runCatching { BtrSessionManager.refreshSignatureIfSamePath(alreadyFresh.primaryUrl, alreadyFresh.backupUrls) }
            return true
        }

        val meta = lookup(url, forceSharedReload = false)
        val (normPath, _, _) = normalizeKeys(url)
        val pathKey = normPath.ifBlank { meta?.pathKey.orEmpty() }
        val cid = meta?.cid ?: 0L
        val epId = meta?.epId ?: 0L

        val localRef = findLocalRefresherEntry(url, pathKey, cid, epId)
        if (localRef != null) {
            val ok = runCatching { localRef.callback.refresh() }.getOrDefault(false)
            val refreshed = findFresherTrack(url, currentDeadlineSec)
            if (refreshed != null) {
                runCatching { BtrSessionManager.refreshSignatureIfSamePath(refreshed.primaryUrl, refreshed.backupUrls) }
                return true
            }
            if (ok) {
                val updated = lookup(url, forceSharedReload = false)
                if (updated != null && !updated.hasDrm && !updated.isPreview && updated.primaryUrl.isNotBlank() &&
                    (currentDeadlineSec <= 0L || updated.deadlineEpochSec > currentDeadlineSec || updated.primaryUrl != url)
                ) {
                    runCatching { BtrSessionManager.refreshSignatureIfSamePath(updated.primaryUrl, updated.backupUrls) }
                    return true
                }
            }
            return false
        }

        val boundedTimeout = timeoutMs.coerceIn(200L, 15_000L)
        val req = CrossProcessRefreshRequest(
            requestId = UUID.randomUUID().toString(),
            url = url,
            pathKey = pathKey,
            cid = cid,
            epId = epId,
            currentDeadlineSec = currentDeadlineSec,
            timeoutMs = boundedTimeout
        )

        val bridge = crossProcessRefreshBridge
        val result: CrossProcessRefreshResult = if (bridge != null) {
            runCatching { bridge.invoke(req) }.getOrElse { CrossProcessRefreshResult(false) }
        } else {
            val ctx = appContext ?: return false
            registerCrossProcessRefreshReceiver(ctx)
            val future = CompletableFuture<CrossProcessRefreshResult>()
            pendingRefreshRequests[req.requestId] = future
            try {
                val sent = sendSamePackageBroadcast(ctx, ACTION_PLAYINFO_REFRESH_REQUEST) { intent ->
                    intent.putExtra(EXTRA_REQUEST_ID, req.requestId)
                    intent.putExtra(EXTRA_TARGET_URL, req.url)
                    intent.putExtra(EXTRA_PATH_KEY, req.pathKey)
                    intent.putExtra(EXTRA_CID, req.cid)
                    intent.putExtra(EXTRA_EP_ID, req.epId)
                    intent.putExtra(EXTRA_CURRENT_DEADLINE, req.currentDeadlineSec)
                }
                if (!sent) {
                    CrossProcessRefreshResult(false)
                } else {
                    runCatching { future.get(req.timeoutMs, TimeUnit.MILLISECONDS) }
                        .getOrElse { CrossProcessRefreshResult(false) }
                }
            } finally {
                pendingRefreshRequests.remove(req.requestId)
            }
        }

        if (result.success && result.primaryUrl.isNotBlank()) {
            applyCrossProcessRefreshedTrack(url, meta, result)
            runCatching { BtrSessionManager.refreshSignatureIfSamePath(result.primaryUrl, result.backupUrls) }
            return true
        }

        // Fallback: even if IPC reply timed out or was dropped, check if the parser process wrote fresh tracks to disk
        val diskFresher = findFresherTrack(url, currentDeadlineSec)
        if (diskFresher != null) {
            runCatching { BtrSessionManager.refreshSignatureIfSamePath(diskFresher.primaryUrl, diskFresher.backupUrls) }
            return true
        }
        return false
    }

    private fun pruneIfNeeded() {
        val now = System.currentTimeMillis()
        if (byPathKey.size > MAX_ENTRIES) {
            byPathKey.entries.removeIf { now - it.value.updatedAtMs > ENTRY_TTL_MS }
            while (byPathKey.size > MAX_ENTRIES) {
                val oldestKey = byPathKey.entries.minByOrNull { it.value.updatedAtMs }?.key ?: break
                byPathKey.remove(oldestKey)
            }
        }
        if (byParentFileKey.size > MAX_ENTRIES) {
            byParentFileKey.entries.removeIf { now - it.value.updatedAtMs > ENTRY_TTL_MS }
            while (byParentFileKey.size > MAX_ENTRIES) {
                val oldestKey = byParentFileKey.entries.minByOrNull { it.value.updatedAtMs }?.key ?: break
                byParentFileKey.remove(oldestKey)
            }
        }
        if (byFileKey.size > MAX_ENTRIES) {
            byFileKey.entries.removeIf { now - it.value.updatedAtMs > ENTRY_TTL_MS }
            while (byFileKey.size > MAX_ENTRIES) {
                val oldestKey = byFileKey.entries.minByOrNull { it.value.updatedAtMs }?.key ?: break
                byFileKey.remove(oldestKey)
            }
        }
        if (refreshersByPath.size > MAX_ENTRIES) {
            refreshersByPath.entries.removeIf { !byPathKey.containsKey(it.key) || now - it.value.registeredAtMs > ENTRY_TTL_MS }
        }
        if (refreshersByParentFile.size > MAX_ENTRIES) {
            refreshersByParentFile.entries.removeIf { !byParentFileKey.containsKey(it.key) || now - it.value.registeredAtMs > ENTRY_TTL_MS }
        }
        if (refreshersByFile.size > MAX_ENTRIES) {
            refreshersByFile.entries.removeIf { !byFileKey.containsKey(it.key) || now - it.value.registeredAtMs > ENTRY_TTL_MS }
        }
        if (refreshersByCid.size > MAX_ENTRIES) {
            refreshersByCid.entries.removeIf { now - it.value.registeredAtMs > ENTRY_TTL_MS }
            while (refreshersByCid.size > MAX_ENTRIES) {
                val oldest = refreshersByCid.entries.minByOrNull { it.value.registeredAtMs }?.key ?: break
                refreshersByCid.remove(oldest)
            }
        }
        if (refreshersByEpId.size > MAX_ENTRIES) {
            refreshersByEpId.entries.removeIf { now - it.value.registeredAtMs > ENTRY_TTL_MS }
            while (refreshersByEpId.size > MAX_ENTRIES) {
                val oldest = refreshersByEpId.entries.minByOrNull { it.value.registeredAtMs }?.key ?: break
                refreshersByEpId.remove(oldest)
            }
        }
        if (handledCrossProcessRequestIds.size > 128) {
            handledCrossProcessRequestIds.entries.removeIf { now - it.value > 30_000L }
        }
    }

    private fun crc32Of(bytes: ByteArray): Long {
        val crc = CRC32()
        crc.update(bytes)
        return crc.value
    }

    private fun persistSharedTracks(newEntries: List<PlayinfoTrackEntry>) {
        val dir = sharedStateDir ?: return
        runCatching {
            synchronized(sharedFileLock) {
                val file = File(dir, TRACKS_FILE_NAME)
                val existing = readSharedTracksFile(file).toMutableMap()
                val now = System.currentTimeMillis()
                existing.entries.removeIf { now - it.value.updatedAtMs > ENTRY_TTL_MS }
                for (e in newEntries) {
                    if (e.pathKey.isBlank()) continue
                    val prev = existing[e.pathKey]
                    if (prev != null && e.updatedAtMs < prev.updatedAtMs) {
                        continue
                    }
                    val keepPrevUrls = prev != null &&
                        !prev.hasDrm &&
                        !prev.isPreview &&
                        !e.hasDrm &&
                        !e.isPreview &&
                        prev.deadlineEpochSec > e.deadlineEpochSec &&
                        prev.primaryUrl.isNotBlank()
                    val bestPrimary = when {
                        keepPrevUrls -> prev!!.primaryUrl
                        e.primaryUrl.isNotBlank() -> e.primaryUrl
                        else -> prev?.primaryUrl.orEmpty()
                    }
                    val bestBackups = when {
                        keepPrevUrls -> prev!!.backupUrls
                        prev != null && (e.deadlineEpochSec > prev.deadlineEpochSec || e.hasDrm != prev.hasDrm || e.isPreview != prev.isPreview) -> e.backupUrls
                        e.backupUrls.isNotEmpty() -> e.backupUrls
                        else -> prev?.backupUrls.orEmpty()
                    }
                    existing[e.pathKey] = SharedTrackSummary(
                        pathKey = e.pathKey,
                        fileKey = e.fileKey,
                        primaryUrl = bestPrimary,
                        backupUrls = bestBackups,
                        isAudio = e.isAudio,
                        hasDrm = e.hasDrm,
                        isPreview = e.isPreview,
                        isEpisode = e.isEpisode || (prev?.isEpisode == true),
                        deadlineEpochSec = if (keepPrevUrls) prev!!.deadlineEpochSec else e.deadlineEpochSec,
                        updatedAtMs = e.updatedAtMs,
                        cid = if (e.cid > 0L) e.cid else (prev?.cid ?: 0L),
                        epId = if (e.epId > 0L) e.epId else (prev?.epId ?: 0L),
                        bandwidth = if (e.bandwidth > 0L) e.bandwidth else (prev?.bandwidth ?: 0L)
                    )
                }
                val trimmed = existing.values
                    .sortedByDescending { it.updatedAtMs }
                    .take(MAX_SHARED_ENTRIES)
                val jsonBytes = gson.toJson(trimmed).toByteArray(Charsets.UTF_8)
                val tmp = File(dir, "$TRACKS_FILE_NAME.tmp.${android.os.Process.myPid()}.${System.nanoTime()}")
                try {
                    tmp.writeBytes(jsonBytes)
                    try {
                        Files.move(
                            tmp.toPath(),
                            file.toPath(),
                            StandardCopyOption.REPLACE_EXISTING,
                            StandardCopyOption.ATOMIC_MOVE
                        )
                    } catch (_: Throwable) {
                        Files.move(
                            tmp.toPath(),
                            file.toPath(),
                            StandardCopyOption.REPLACE_EXISTING
                        )
                    }
                } finally {
                    if (tmp.exists()) tmp.delete()
                }
                lastTracksFileMtime = file.lastModified()
                lastTracksFileLength = file.length()
                lastTracksContentHash = crc32Of(jsonBytes)
            }
        }
    }

    private fun loadSharedTracksIfUpdated(force: Boolean = false) {
        val dir = sharedStateDir ?: return
        val now = System.currentTimeMillis()
        if (!force && now - lastTracksFileCheckMs.get() < 250L) return
        lastTracksFileCheckMs.set(now)
        runCatching {
            val file = File(dir, TRACKS_FILE_NAME)
            if (!file.exists()) return
            val bytes = file.readBytes()
            if (bytes.isEmpty()) return
            val crc = crc32Of(bytes)
            val len = bytes.size.toLong()
            if (!force && len == lastTracksFileLength && crc == lastTracksContentHash && lastTracksContentHash != 0L) {
                return
            }
            synchronized(sharedFileLock) {
                val type = object : TypeToken<List<SharedTrackSummary>>() {}.type
                val parsedList: List<SharedTrackSummary> = gson.fromJson(String(bytes, Charsets.UTF_8), type) ?: emptyList()
                for (s in parsedList) {
                    if (s.pathKey.isBlank() || (now - s.updatedAtMs) > ENTRY_TTL_MS) continue
                    val existing = byPathKey[s.pathKey]
                    val flagsChangedAtSameTs = existing != null &&
                        s.updatedAtMs == existing.updatedAtMs &&
                        (s.hasDrm != existing.hasDrm || s.isPreview != existing.isPreview)
                    val incomingFresher = existing == null ||
                        s.updatedAtMs > existing.updatedAtMs ||
                        s.deadlineEpochSec > existing.deadlineEpochSec ||
                        flagsChangedAtSameTs
                    if (incomingFresher) {
                        val incomingReplacesFlags = existing == null || s.updatedAtMs >= existing.updatedAtMs
                        val resolvedHasDrm = if (incomingReplacesFlags) s.hasDrm else existing!!.hasDrm
                        val resolvedIsPreview = if (incomingReplacesFlags) s.isPreview else existing!!.isPreview
                        val useIncomingUrls = s.primaryUrl.isNotBlank() && (
                            existing == null ||
                                (incomingReplacesFlags && (existing.hasDrm != s.hasDrm || existing.isPreview != s.isPreview)) ||
                                s.deadlineEpochSec >= existing.deadlineEpochSec
                            )
                        val resolvedBackups = when {
                            existing != null && (s.deadlineEpochSec > existing.deadlineEpochSec || (incomingReplacesFlags && (existing.hasDrm != s.hasDrm || existing.isPreview != s.isPreview))) -> s.backupUrls
                            useIncomingUrls && s.backupUrls.isNotEmpty() -> s.backupUrls
                            else -> existing?.backupUrls.orEmpty()
                        }
                        val resolvedDeadline = if (useIncomingUrls) {
                            s.deadlineEpochSec
                        } else {
                            maxOf(s.deadlineEpochSec, existing?.deadlineEpochSec ?: 0L)
                        }
                        val entry = PlayinfoTrackEntry(
                            pathKey = s.pathKey,
                            fileKey = s.fileKey,
                            primaryUrl = if (useIncomingUrls) s.primaryUrl else existing?.primaryUrl.orEmpty(),
                            backupUrls = resolvedBackups,
                            deadlineEpochSec = resolvedDeadline,
                            isAudio = s.isAudio,
                            hasDrm = resolvedHasDrm,
                            isPreview = resolvedIsPreview,
                            isEpisode = s.isEpisode || (existing?.isEpisode == true),
                            cid = if (s.cid > 0L) s.cid else (existing?.cid ?: 0L),
                            epId = if (s.epId > 0L) s.epId else (existing?.epId ?: 0L),
                            updatedAtMs = maxOf(s.updatedAtMs, existing?.updatedAtMs ?: 0L),
                            bandwidth = if (s.bandwidth > 0L) s.bandwidth else (existing?.bandwidth ?: 0L)
                        )
                        byPathKey[s.pathKey] = entry
                        val parentFileKey = parentFileKeyFromPath(s.pathKey)
                        if (parentFileKey.isNotBlank()) {
                            byParentFileKey[parentFileKey] = entry
                        }
                        if (isDistinctiveFileKey(s.fileKey)) {
                            byFileKey[s.fileKey] = entry
                        }
                    }
                }
                lastTracksFileMtime = file.lastModified()
                lastTracksFileLength = len
                lastTracksContentHash = crc
            }
        }
    }

    private fun readSharedTracksFile(file: File): Map<String, SharedTrackSummary> {
        if (!file.exists()) return emptyMap()
        return runCatching {
            val type = object : TypeToken<List<SharedTrackSummary>>() {}.type
            val list: List<SharedTrackSummary> = gson.fromJson(file.readText(Charsets.UTF_8), type) ?: emptyList()
            list.associateBy { it.pathKey }
        }.getOrDefault(emptyMap())
    }

    fun saveSharedConcurrencyState(state: SharedConcurrencyState) {
        val dir = sharedStateDir ?: return
        runCatching {
            synchronized(sharedFileLock) {
                val file = File(dir, CONCURRENCY_FILE_NAME)
                val tmp = File(dir, "$CONCURRENCY_FILE_NAME.tmp.${android.os.Process.myPid()}")
                tmp.writeText(gson.toJson(state), Charsets.UTF_8)
                if (!tmp.renameTo(file)) {
                    file.writeText(gson.toJson(state), Charsets.UTF_8)
                    tmp.delete()
                }
                lastConcurrencyFileMtime = file.lastModified()
                lastConcurrencyFileLength = file.length()
            }
        }
    }

    fun loadSharedConcurrencyState(): SharedConcurrencyState? {
        val dir = sharedStateDir ?: return null
        return runCatching {
            val file = File(dir, CONCURRENCY_FILE_NAME)
            if (!file.exists()) return null
            lastConcurrencyFileMtime = file.lastModified()
            lastConcurrencyFileLength = file.length()
            gson.fromJson(file.readText(Charsets.UTF_8), SharedConcurrencyState::class.java)
        }.getOrNull()
    }

    fun loadSharedConcurrencyStateIfUpdated(): SharedConcurrencyState? {
        val dir = sharedStateDir ?: return null
        val now = System.currentTimeMillis()
        if (now - lastConcurrencyFileCheckMs.get() < 250L) return null
        lastConcurrencyFileCheckMs.set(now)
        return runCatching {
            val file = File(dir, CONCURRENCY_FILE_NAME)
            if (!file.exists()) return null
            val mtime = file.lastModified()
            val len = file.length()
            if (mtime <= 0L || (mtime == lastConcurrencyFileMtime && len == lastConcurrencyFileLength)) {
                return null
            }
            synchronized(sharedFileLock) {
                lastConcurrencyFileMtime = file.lastModified()
                lastConcurrencyFileLength = file.length()
                gson.fromJson(file.readText(Charsets.UTF_8), SharedConcurrencyState::class.java)
            }
        }.getOrNull()
    }

    fun clearSharedConcurrencyState() {
        val dir = sharedStateDir ?: return
        runCatching {
            synchronized(sharedFileLock) {
                File(dir, CONCURRENCY_FILE_NAME).delete()
                lastConcurrencyFileMtime = 0L
                lastConcurrencyFileLength = -1L
            }
        }
    }

    // ---- JSON & reflection helpers ----

    private fun hasJsonDrmSignal(obj: JsonObject): Boolean {
        if (obj.boolOrIntTrue("is_drm") || obj.boolOrIntTrue("has_drm") || obj.boolOrIntTrue("widevine")) {
            return true
        }
        val drmTech = obj.get("drm_tech_type")
        if (drmTech != null && !drmTech.isJsonNull && drmTech.isJsonPrimitive) {
            val p = drmTech.asJsonPrimitive
            if (p.isNumber && p.asInt > 0) return true
            if (p.isString && isDrmEnumOrValueActive(p.asString)) return true
        }
        val drmType = obj.get("drm_type")
        if (drmType != null && !drmType.isJsonNull && drmType.isJsonPrimitive) {
            val p = drmType.asJsonPrimitive
            if (p.isNumber && p.asInt > 0) return true
            if (p.isString && isDrmEnumOrValueActive(p.asString)) return true
        }
        if (!obj.strVal("pssh").isNullOrBlank()) return true
        val drmList = obj.arr("drm_list")
        if (drmList != null && drmList.size() > 0) return true
        return false
    }

    private fun hasJsonPreviewSignal(obj: JsonObject): Boolean {
        if (obj.boolOrIntTrue("is_preview") || obj.boolOrIntTrue("preview") || obj.boolOrIntTrue("is_trial")) {
            return true
        }
        val limit = obj.longVal("watch_time_limit") ?: 0L
        if (limit > 0L) return true
        val trialInfo = obj.obj("trial_info")
        if (trialInfo != null && (trialInfo.boolOrIntTrue("is_trial") || (trialInfo.longVal("watch_time_limit") ?: 0L) > 0L || (trialInfo.longVal("trial_time") ?: 0L) > 0L)) {
            return true
        }
        return false
    }

    private fun hasJsonEpisodeSignal(obj: JsonObject): Boolean {
        if ((obj.longVal("ep_id") ?: 0L) > 0L ||
            (obj.longVal("epid") ?: 0L) > 0L ||
            (obj.longVal("episode_id") ?: 0L) > 0L ||
            (obj.longVal("season_id") ?: 0L) > 0L ||
            (obj.longVal("season_type") ?: 0L) > 0L
        ) {
            return true
        }
        if (obj.obj("ogv_episode_info") != null || obj.obj("ogv_season_info") != null) return true
        val biz = obj.strVal("business")?.lowercase()
        val videoType = obj.strVal("video_type")?.lowercase()
        return biz == "pgc" || biz == "ogv" || videoType == "pgc" || videoType == "ogv"
    }

    private fun isTruthyValue(v: Any?): Boolean {
        return when (v) {
            is Boolean -> v
            is Number -> v.toInt() > 0
            is String -> v == "1" || v.equals("true", ignoreCase = true)
            else -> false
        }
    }

    private fun isInactiveDrmName(raw: String): Boolean {
        val s = raw.trim().uppercase()
        return s.isEmpty() ||
            s == "0" ||
            s == "FALSE" ||
            s == "NON" ||
            s == "NONE" ||
            s == "NO_DRM" ||
            s == "NONE_DRM" ||
            s == "UNKNOWN" ||
            s == "UNKNOWN_DRM" ||
            s == "DRM_TECH_TYPE_UNKNOWN" ||
            s == "DRM_TECH_TYPE_NONE" ||
            s == "DRM_TECH_TYPE_NON" ||
            s == "UNRECOGNIZED"
    }

    private fun isDrmEnumOrValueActive(v: Any?): Boolean {
        return when (v) {
            null -> false
            is Boolean -> v
            is Number -> v.toInt() > 0
            is Enum<*> -> !isInactiveDrmName(v.name) && v.ordinal > 0
            is String -> !isInactiveDrmName(v)
            else -> false
        }
    }

    private fun JsonElement.asObjOrNull(): JsonObject? = if (isJsonObject) asJsonObject else null
    private fun JsonElement.asStringOrNull(): String? = if (isJsonPrimitive && asJsonPrimitive.isString) asString else null
    private fun JsonObject.obj(name: String): JsonObject? = get(name)?.takeIf { it.isJsonObject }?.asJsonObject
    private fun JsonObject.arr(name: String): JsonArray? = get(name)?.takeIf { it.isJsonArray }?.asJsonArray
    private fun JsonObject.strVal(name: String): String? = get(name)?.takeIf { it.isJsonPrimitive && it.asJsonPrimitive.isString }?.asString
    private fun JsonObject.longVal(name: String): Long? = get(name)?.takeIf { it.isJsonPrimitive }?.let { el ->
        val p = el.asJsonPrimitive
        when {
            p.isNumber -> runCatching { p.asLong }.getOrNull()
            p.isString -> p.asString.toLongOrNull()
            else -> null
        }
    }
    private fun JsonObject.boolOrIntTrue(name: String): Boolean {
        val el = get(name) ?: return false
        if (!el.isJsonPrimitive) return false
        val p = el.asJsonPrimitive
        return when {
            p.isBoolean -> p.asBoolean
            p.isNumber -> p.asInt > 0
            p.isString -> p.asString == "1" || p.asString.equals("true", ignoreCase = true)
            else -> false
        }
    }
}
