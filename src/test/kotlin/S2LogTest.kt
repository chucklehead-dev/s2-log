package chucklehead.xtdb.s2


import io.mockk.every
import io.mockk.mockk
import kotlinx.coroutines.delay
import kotlinx.coroutines.future.await
import kotlinx.coroutines.test.runTest
import org.junit.jupiter.api.AfterAll
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.BeforeAll
import org.junit.jupiter.api.Tag
import org.junit.jupiter.api.Test
import s2.client.BasinClient
import s2.config.Config
import s2.types.*
import xtdb.api.log.Log.Message
import xtdb.api.log.Log.Record
import xtdb.api.log.Log.Subscriber
import xtdb.api.storage.Storage
import xtdb.log.proto.TrieDetails
import java.nio.ByteBuffer
import java.time.Duration
import java.util.*
import java.util.Collections.synchronizedList
import kotlin.time.Duration.Companion.seconds

@Tag("integration")
class S2LogTest {
    companion object {
        val token = System.getenv("S2_ACCESS_TOKEN")!!
        val basin = System.getenv("S2_BASIN")!!
        private val s2Config = Config.newBuilder(token).build()
        private val client = BasinClient.newBuilder(s2Config, basin).build()
        private val streamConfig = StreamConfig.newBuilder()
            .withStorageClass(StorageClass.STANDARD)
            .withTimestamping(
                Timestamping(TimestampingMode.ARRIVAL, false)
            )
            .withRetentionPolicy(Age(Duration.ofDays(1)))
            .build()
        var streamName = ""

        @JvmStatic
        @BeforeAll
        fun beforeAll() {
            streamName = "integration-${UUID.randomUUID()}"
            val req = CreateStreamRequest.newBuilder()
                .withStreamName(streamName)
                .withStreamConfig(streamConfig)
                .build()
            client.createStream(req).get()
        }

        @JvmStatic
        @AfterAll
        fun afterAll() {
            client.deleteStream(streamName).get()
        }
    }

    @Test
    fun `round-trips-messages`() = runTest(timeout = 30.seconds) {
        val msgs = synchronizedList(mutableListOf<List<Record>>())

        val subscriber = mockk<Subscriber> {
            every { processRecords(capture(msgs)) } returns Unit
            every { latestProcessedMsgId } returns -1
        }

        fun trieDetails(key: String, size: Long) =
            TrieDetails.newBuilder()
                .setTableName("my-table").setTrieKey(key)
                .setDataFileSize(size)
                .build()

        val addedTrieDetails = listOf(trieDetails("foo", 12), trieDetails("bar", 18))

        S2Log.s2(S2LogTest.token, S2LogTest.basin, S2LogTest.streamName)
            .openLog().use { log ->
                log.subscribe(subscriber, 0).use { _ ->
                    val txPayload = ByteBuffer.allocate(9).put(-1).putLong(42).flip()
                    log.appendMessage(Message.Tx(txPayload)).await()

                    log.appendMessage(Message.FlushBlock(12)).await()

                    log.appendMessage(Message.TriesAdded(Storage.VERSION, addedTrieDetails)).await()

                    while (msgs.flatten().size < 3) delay(100)
                }
            }

        assertEquals(3, msgs.flatten().size)

        val allMsgs = msgs.flatten()

        allMsgs[0].message.let {
            check(it is Message.Tx)
            assertEquals(42, it.payload.getLong(1))
        }

        allMsgs[1].message.let {
            check(it is Message.FlushBlock)
            assertEquals(12, it.expectedBlockTxId)
        }

        allMsgs[2].message.let {
            check(it is Message.TriesAdded)
            assertEquals(addedTrieDetails,it.tries)
        }

    }
}
