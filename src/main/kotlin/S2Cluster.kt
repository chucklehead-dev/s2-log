@file:UseSerializers(
    StringMapWithEnvVarsSerde::class,
    DurationSerde::class,
    StringWithEnvVarSerde::class,
    PathWithEnvVarSerde::class
)

package chucklehead.xtdb.s2

import chucklehead.xtdb.s2.proto.S2LogConfig
import chucklehead.xtdb.s2.proto.s2LogConfig
import kotlinx.coroutines.*
import kotlinx.coroutines.future.future
import kotlinx.coroutines.guava.await
import kotlinx.serialization.SerialName
import kotlinx.serialization.Serializable
import kotlinx.serialization.UseSerializers
import kotlinx.serialization.modules.PolymorphicModuleBuilder
import kotlinx.serialization.modules.subclass
import org.slf4j.LoggerFactory
import s2.client.StreamClient
import s2.config.AppendRetryPolicy
import s2.config.Config
import s2.types.*
import xtdb.DurationSerde
import xtdb.api.PathWithEnvVarSerde
import xtdb.api.StringMapWithEnvVarsSerde
import xtdb.api.StringWithEnvVarSerde
import xtdb.api.log.Log
import xtdb.api.log.Log.*
import xtdb.api.log.LogClusterAlias
import xtdb.api.log.LogOffset
import xtdb.database.proto.DatabaseConfig
import java.time.Duration
import java.time.Instant
import java.util.concurrent.CompletableFuture
import java.util.concurrent.atomic.AtomicLong
import kotlin.coroutines.CoroutineContext
import kotlin.jvm.optionals.getOrNull
import kotlin.time.Duration.Companion.seconds
import com.google.protobuf.Any as ProtoAny

private val LOGGER = LoggerFactory.getLogger(S2Cluster::class.java)

class S2Cluster(
    private val token: String,
    private val basin: String,
    private val maxAppendInFlightBytes: Int,
    private val appendTimeout: Duration,
    private val readBufferBytes: Int,
    private val retryDelay: Duration,
    coroutineContext: CoroutineContext = Dispatchers.Default
) : Cluster {
    val scope = CoroutineScope(SupervisorJob() + coroutineContext)

    override fun close() {
        LOGGER.trace("Closing S2 cluster")
        runBlocking { withTimeout(5.seconds) { scope.coroutineContext.job.cancelAndJoin() } }
        LOGGER.info("Closed S2 cluster")
    }

    @Serializable
    @SerialName("!S2")
    data class ClusterFactory @JvmOverloads constructor(
        val token: String,
        val basin: String,
        var maxAppendInFlightBytes: Int = (32 * 1024 * 1024),
        var appendTimeout: Duration = Duration.ofSeconds(10),
        var readBufferBytes: Int = (32 * 1024 * 1024),
        var retryDelay: Duration = Duration.ofSeconds(1),
        @kotlinx.serialization.Transient var coroutineContext: CoroutineContext = Dispatchers.Default
    ) : Cluster.Factory<S2Cluster> {

        fun maxAppendInFlightBytes(bytes: Int) = apply { this.maxAppendInFlightBytes = bytes }
        fun appendTimeout(timeout: Duration) = apply { this.appendTimeout = timeout }
        fun readBufferBytes(bytes: Int) = apply { this.readBufferBytes = bytes }
        fun retryDelay(delay: Duration) = apply { this.retryDelay = delay }
        fun coroutineContext(context: CoroutineContext) = apply { this.coroutineContext = context }

        override fun open(): S2Cluster {
            check(token.isNotEmpty()) { "token must not be empty" }
            check(basin.isNotEmpty()) { "basin must not be empty" }

            LOGGER.info("Opening S2 cluster for basin: {}", basin)
            return S2Cluster(token, basin, maxAppendInFlightBytes, appendTimeout, readBufferBytes, retryDelay, coroutineContext)
        }
    }

    private inner class S2Log(
        private val clusterAlias: LogClusterAlias,
        private val stream: String,
        override val epoch: Int
    ) : Log {
        private val streamClient: StreamClient = run {
            val config = Config.newBuilder(token)
                .withCompression(true)
                .withAppendRetryPolicy(AppendRetryPolicy.NO_SIDE_EFFECTS)
                .withMaxAppendInflightBytes(maxAppendInFlightBytes)
                .withRetryDelay(retryDelay)
                .build()
            StreamClient.newBuilder(config, basin, stream).build()
        }
        private val streamAppender = streamClient.managedAppendSession()

        private fun readLatestSubmittedMessage(): LogOffset {
            val tail = streamClient.checkTail().get()
            LOGGER.info("Opened log with tail seq: {} and ts: {}", tail.seqNum, tail.timestamp)
            return tail.seqNum - 1
        }

        private val latestSubmittedOffset0 = AtomicLong(readLatestSubmittedMessage())
        override val latestSubmittedOffset get(): Long {
            val offset = latestSubmittedOffset0.get()
            LOGGER.trace("latest submitted offset: {}", offset)
            return offset
        }

        override fun appendMessage(message: Message): CompletableFuture<MessageMetadata> =
            scope.future {
                val record = AppendRecord.newBuilder()
                    .withBody(message.encode())
                    .build()
                val input = AppendInput.newBuilder().withRecords(listOf(record)).build()
                val output = streamAppender.submit(input, appendTimeout).await()
                val offset = output.end.seqNum - 1
                val latest = latestSubmittedOffset0.updateAndGet { it -> it.coerceAtLeast(offset) }
                val result = MessageMetadata(latest, Instant.ofEpochMilli(output.end.timestamp))
                LOGGER.trace("Appended message, seq: {}, ts: {}", result.logOffset, result.logTimestamp)

                result
            }

        override fun readLastMessage(): Message? {
            val req = ReadRequest.newBuilder()
                .withStart(Start.tailOffset(1))
                .withReadLimit(ReadLimit.count(1))
                .build()

            val output = streamClient.read(req).get()

            return when (output) {
                is Batch -> output.sequencedRecordBatch.records.firstOrNull()?.let { r ->
                    Message.parse(r.body.toByteArray())
                }
                else -> null
            }
        }

        override fun subscribe(subscriber: Subscriber, latestProcessedOffset: LogOffset): Subscription {
            val start = (latestProcessedOffset + 1).coerceAtLeast(0)
            val job = scope.launch {
                val req = ReadSessionRequest.newBuilder()
                    .withHeartbeats(false)
                    .withReadLimit(ReadLimit.NONE)
                    .withStart(Start.SeqNum.seqNum(start))
                    .build()
                LOGGER.info("Starting new subscription at offset: {}", start)
                val session = streamClient.managedReadSession(req, readBufferBytes)

                session.use { s ->
                    runInterruptible(Dispatchers.IO) {
                        while (!session.isClosed) {
                            val output = try {
                                s.get(Duration.ofMinutes(1)).getOrNull()
                            } catch (_: InterruptedException) {
                                LOGGER.warn("subscriber interrupted while polling for new records")
                                throw InterruptedException()
                            }

                            if (output != null && output is Batch) {
                                subscriber.processRecords(
                                    output.sequencedRecordBatch.records
                                        .filterNot { r ->
                                            r.headers.any { h -> h.name.isEmpty }
                                        }.mapNotNull { r ->
                                            Message.parse(r.body.toByteArray())?.let { msg ->
                                                Record(
                                                    r.seqNum,
                                                    Instant.ofEpochMilli(r.timestamp),
                                                    msg
                                                ).also {
                                                    LOGGER.trace("subscriber will process record, seq: {}, ts: {}", it.logOffset, it.logTimestamp)
                                                }
                                            }
                                        })
                            }
                        }
                        LOGGER.info("Closing subscription")
                    }
                }
            }
            return Subscription { runBlocking { withTimeout(5.seconds) { job.cancelAndJoin() } } }
        }

        override fun close() {
            LOGGER.trace("Closing S2 log for stream: {}", stream)
            streamAppender.closeGracefully()
            streamClient.close()
        }
    }

    @Serializable
    @SerialName("!S2")
    data class LogFactory @JvmOverloads constructor(
        val cluster: LogClusterAlias,
        val stream: String,
        var epoch: Int = 0
    ) : Factory {

        fun epoch(epoch: Int) = apply { this.epoch = epoch }

        override fun openLog(clusters: Map<LogClusterAlias, Cluster>): Log {
            val clusterAlias = this.cluster
            val s2Cluster = requireNotNull(clusters[clusterAlias] as? S2Cluster) {
                "missing S2 cluster: '$clusterAlias'"
            }

            check(stream.isNotEmpty()) { "stream must not be empty" }

            LOGGER.info("Opening S2 log for stream: {}", stream)
            return s2Cluster.S2Log(clusterAlias, stream, epoch)
        }

        override fun writeTo(dbConfig: DatabaseConfig.Builder) {
            dbConfig.setOtherLog(ProtoAny.pack(s2LogConfig {
                this.stream = this@LogFactory.stream
                this.epoch = this@LogFactory.epoch
                this.logClusterAlias = cluster
            }, "proto.xtdb.com"))
        }
    }

    /**
     * @suppress
     */
    class Registration : Log.Registration {
        override val protoTag: String get() = "proto.xtdb.com/chucklehead.xtdb.s2.proto.S2LogConfig"

        override fun fromProto(msg: ProtoAny) =
            msg.unpack(S2LogConfig::class.java).let {
                LogFactory(it.logClusterAlias, it.stream, it.epoch)
            }

        override fun registerSerde(builder: PolymorphicModuleBuilder<Factory>) {
            builder.subclass(LogFactory::class)
        }
    }

    /**
     * @suppress
     */
    class ClusterRegistration : Cluster.Registration {
        override fun registerSerde(builder: PolymorphicModuleBuilder<Cluster.Factory<*>>) {
            builder.subclass(ClusterFactory::class)
        }
    }
}
