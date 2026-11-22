package io.github.jh_mmm.biliaccelerator.btr

import okhttp3.ConnectionPool
import okhttp3.Dispatcher
import okhttp3.OkHttpClient
import okhttp3.Protocol
import java.util.concurrent.TimeUnit

object BtrHttpClient {

    // 连接池复用：支持高达 32 个空闲 HTTP/1.1 Keep-Alive 连接，保持 5 分钟活性
    private val connectionPool = ConnectionPool(
        maxIdleConnections = 32,
        keepAliveDuration = 5,
        timeUnit = TimeUnit.MINUTES
    )

    // 音频流专属独立连接池：8 个空闲连接，与视频流物理隔离，杜绝高码率视频 16 线程占满连接池导致音频饥饿
    private val audioConnectionPool = ConnectionPool(
        maxIdleConnections = 8,
        keepAliveDuration = 5,
        timeUnit = TimeUnit.MINUTES
    )

    // 关键并发解除：OkHttp 默认 maxRequestsPerHost=5 会在多线程拉取单主节点时强行将请求排队，必须提升上限
    private fun createDispatcher(maxRequests: Int = 128, maxPerHost: Int = 32): Dispatcher {
        return Dispatcher().apply {
            this.maxRequests = maxRequests
            this.maxRequestsPerHost = maxPerHost
        }
    }

    // 全局基础客户端：显式强制采用 HTTP/1.1 独立物理连接池（每并发请求独占独立 TCP 通道）。
    // 【技术原理解析与 HTTP/2 权衡说明】：
    // 1. 哔哩哔哩 UPOS CDN 边缘节点对单条 TCP 连接普遍实施严格的 QoS 带宽速率上限（例如单连接 2~4MB/s）；
    // 2. 若启用 HTTP/2 多路复用，8~16 个并发分片流会被 OkHttp 强制折叠复用至同一条底层 TCP 连接上，
    //    导致所有分片共享同一个受限的 TCP 拥塞控制窗口与带宽天花板，彻底丧失 BTR 多线程分片并发提速的核心价值；
    // 3. 此外，HTTP/2 在单条 TCP 发生丢包时会引发跨流队头阻塞（Head-of-Line Blocking），且 UPOS 边缘网关
    //    在面对高频分片 Range 请求时极易频繁回退 RST_STREAM 或 GOAWAY 异常帧；
    // 4. 因此，此处必须显式强制 .protocols(listOf(Protocol.HTTP_1_1))，配合 ConnectionPool 实现真正的
    //    多物理 TCP 窗口并发带宽倍增，同时在同节点后续分片拉取中继续享有 HTTP/1.1 Keep-Alive 长连接免握手复用优势。
    val client: OkHttpClient = OkHttpClient.Builder()
        .dispatcher(createDispatcher(128, 32))
        .connectionPool(connectionPool)
        .protocols(listOf(Protocol.HTTP_1_1))
        .connectTimeout(8_000, TimeUnit.MILLISECONDS)
        .readTimeout(20_000, TimeUnit.MILLISECONDS)
        .writeTimeout(10_000, TimeUnit.MILLISECONDS)
        .followRedirects(true)
        .followSslRedirects(true)
        .retryOnConnectionFailure(false) // 由 BtrChunkEngine 统一控制 Failover 与熔断
        .build()

    const val CHUNK_ATTEMPT_CALL_TIMEOUT_MS = 15_000L
    const val PROBE_CALL_TIMEOUT_MS = 7_000L
    const val FALLBACK_CALL_TIMEOUT_MS = 20_000L
    const val AUDIO_CALL_TIMEOUT_MS = 20_000L

    // 音频专属独立客户端：使用独立连接池与独立调度器，HTTP/1.1 低时延优先，附加 20s 总调用超时防慢速滴流
    val audioClient: OkHttpClient = OkHttpClient.Builder()
        .dispatcher(createDispatcher(32, 16))
        .connectionPool(audioConnectionPool)
        .protocols(listOf(Protocol.HTTP_1_1))
        .connectTimeout(6_000, TimeUnit.MILLISECONDS)
        .readTimeout(15_000, TimeUnit.MILLISECONDS)
        .writeTimeout(10_000, TimeUnit.MILLISECONDS)
        .callTimeout(AUDIO_CALL_TIMEOUT_MS, TimeUnit.MILLISECONDS)
        .followRedirects(true)
        .followSslRedirects(true)
        .retryOnConnectionFailure(false)
        .build()

    // 音频非 Range 长流透传客户端（无总 callTimeout，防止长音频完整流被截断）
    val audioStreamClient: OkHttpClient = audioClient.newBuilder()
        .callTimeout(0, TimeUnit.MILLISECONDS)
        .build()

    // 单分片拉取专用客户端：附加 15s 总调用超时（对齐上游 attemptTimeoutMs = 15000ms），防止慢速滴流节点无限占用并发槽位
    val chunkClient: OkHttpClient = client.newBuilder()
        .callTimeout(CHUNK_ATTEMPT_CALL_TIMEOUT_MS, TimeUnit.MILLISECONDS)
        .build()

    // 音频单分片拉取专用客户端：独立池 + 15s 总调用超时
    val chunkAudioClient: OkHttpClient = audioClient.newBuilder()
        .callTimeout(CHUNK_ATTEMPT_CALL_TIMEOUT_MS, TimeUnit.MILLISECONDS)
        .build()

    // 探针专用客户端：紧凑超时控制，避免慢节点卡死起播
    val probeClient: OkHttpClient = client.newBuilder()
        .connectTimeout(5_000, TimeUnit.MILLISECONDS)
        .readTimeout(6_000, TimeUnit.MILLISECONDS)
        .callTimeout(PROBE_CALL_TIMEOUT_MS, TimeUnit.MILLISECONDS)
        .build()

    // 音频探针专用客户端：隔离音频连接池 + 7s 探针总超时
    val audioProbeClient: OkHttpClient = audioClient.newBuilder()
        .connectTimeout(5_000, TimeUnit.MILLISECONDS)
        .readTimeout(6_000, TimeUnit.MILLISECONDS)
        .callTimeout(PROBE_CALL_TIMEOUT_MS, TimeUnit.MILLISECONDS)
        .build()

    // 兜底直连专用客户端：附加 20s 总调用超时，避免慢速滴流卡死最终兜底
    val fallbackClient: OkHttpClient = client.newBuilder()
        .connectTimeout(8_000, TimeUnit.MILLISECONDS)
        .readTimeout(20_000, TimeUnit.MILLISECONDS)
        .callTimeout(FALLBACK_CALL_TIMEOUT_MS, TimeUnit.MILLISECONDS)
        .build()

    // 直播 FLV 长流透传专用客户端：HTTP/1.1 独立连接 + 30s 读超时，避免上游短暂推流卡顿或 HTTP/2 复用触发断流
    val liveStreamClient: OkHttpClient = client.newBuilder()
        .connectTimeout(6_000, TimeUnit.MILLISECONDS)
        .readTimeout(30_000, TimeUnit.MILLISECONDS)
        .writeTimeout(10_000, TimeUnit.MILLISECONDS)
        .build()
}
