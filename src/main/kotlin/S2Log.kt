@file:UseSerializers(
    StringMapWithEnvVarsSerde::class,
    DurationSerde::class,
    StringWithEnvVarSerde::class,
    PathWithEnvVarSerde::class
)

package dev.chucklehead

import kotlinx.coroutines.*
import kotlinx.coroutines.future.future
import kotlinx.coroutines.guava.await
import kotlinx.serialization.SerialName
import kotlinx.serialization.Serializable
import kotlinx.serialization.UseSerializers
import s2.client.StreamClient
import s2.config.AppendRetryPolicy
import s2.config.Config
import s2.types.*
import xtdb.DurationSerde
import xtdb.api.PathWithEnvVarSerde
import xtdb.api.StringMapWithEnvVarsSerde
import xtdb.api.StringWithEnvVarSerde
import xtdb.api.Xtdb
import xtdb.api.log.Log
import xtdb.api.log.Log.*
import xtdb.api.log.LogOffset
import xtdb.api.module.XtdbModule
import java.time.Duration
import java.util.concurrent.CompletableFuture
import java.util.concurrent.atomic.AtomicLong
import kotlin.time.Duration.Companion.seconds

class S2Log internal constructor(
    private val client: StreamClient,
    private val appendTimeout: Duration,
    private val readBufferBytes: Int,
    override val epoch: Int
) : Log {
    private val appender = client.managedAppendSession()
    private val scope = CoroutineScope(SupervisorJob() + Dispatchers.Default)

    override fun close() {
        runBlocking { withTimeout(5.seconds) { scope.coroutineContext.job.cancelAndJoin() } }
        appender.closeGracefully()
        client.close()
    }

    private fun readLatestSubmittedMessage(client: StreamClient): LogOffset = client.checkTail().get() - 1

    private val latestSubmittedOffset0 = AtomicLong(readLatestSubmittedMessage(client))
    override val latestSubmittedOffset get() = latestSubmittedOffset0.get()

    override fun appendMessage(message: Message): CompletableFuture<LogOffset> =
        scope.future {
            val record = AppendRecord.newBuilder()
                .withBody(message.encode().array())
                .build()
            val input = AppendInput.newBuilder().withRecords(listOf(record)).build()
            val output = appender.submit(input, appendTimeout).await()
            val offset = output.endSeqNum - 1
            latestSubmittedOffset0.updateAndGet { it -> it.coerceAtLeast(offset) }
        }

    override fun subscribe(subscriber: Subscriber, latestProcessedOffset: LogOffset): Subscription {
        val job = scope.launch {
            val req = ReadSessionRequest.newBuilder()
                .withHeartbeats(false)
                .withReadLimit(ReadLimit.NONE)
                .withStart(Start.SeqNum.seqNum(latestProcessedOffset))
                .build()

            val session = client.managedReadSession(req, readBufferBytes)

            session.use { s ->
                runInterruptible(Dispatchers.IO) {
                    while (true) {

                        val output = try {
                            s.get()
                        } catch (_: InterruptedException) {
                            throw InterruptedException()
                        }

                        output.ifPresent { o ->
                            when (o) {
                                is Batch -> subscriber.processRecords(
                                    o.sequencedRecordBatch.records.map { r ->
                                        Record(
                                            r.seqNum,
                                            r.timestamp,
                                            Message.parse(r.body.asReadOnlyByteBuffer())
                                        )
                                    }
                                )
                            }
                        }

                    }
                }

            }
        }
        return Subscription { runBlocking { withTimeout(5.seconds) { job.cancelAndJoin() } } }
    }

    companion object {
        @JvmStatic
        fun s2(token:String, basin: String, stream: String) =
            Factory(token, basin, stream)

        @JvmSynthetic
        fun Xtdb.Config.s2(
            token: String, basin: String, stream: String,
            configure: Factory.() -> Unit = {}
        ) {
            log = S2Log.s2(token, basin, stream).also(configure)
        }
    }

    @Serializable
    @SerialName("!S2")
    data class Factory(
        val token: String,
        val basin: String,
        val stream: String,
        var maxAppendInFlightBytes: Int = (1 * 1024 * 1024),
        var appendTimeout: Duration = Duration.ofSeconds(5),
        var readBufferBytes: Int = (4 * 1024 * 1024),
        var retryDelay: Duration = Duration.ofSeconds(1),
        var epoch: Int = 0
    ) : Log.Factory {

        fun epoch(epoch: Int) = apply { this.epoch = epoch }
        fun maxAppendInFlightBytes(bytes: Int) = apply { this.maxAppendInFlightBytes = bytes }
        fun appendTimeout(timeout: Duration) = apply { this.appendTimeout = timeout }
        fun readBufferBytes(bytes: Int) = apply { this.readBufferBytes = bytes }
        fun retryDelay(delay: Duration) = apply { this.retryDelay = delay }

        override fun openLog(): S2Log {
            val config = Config.newBuilder(token)
                .withCompression(true)
                .withAppendRetryPolicy(AppendRetryPolicy.NO_SIDE_EFFECTS)
                .withMaxAppendInflightBytes(maxAppendInFlightBytes)
                .withRetryDelay(retryDelay)
                .build()

            val client = StreamClient.newBuilder(config, basin, stream).build()

            return S2Log(client, appendTimeout, readBufferBytes, epoch)
        }
    }

    /**
     * @suppress
     */
    class Registration : XtdbModule.Registration {
        override fun register(registry: XtdbModule.Registry) {
            registry.registerLogFactory(Factory::class)
        }
    }
}