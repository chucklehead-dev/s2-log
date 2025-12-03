package chucklehead.xtdb.s2


import io.mockk.every
import io.mockk.mockk
import kotlinx.coroutines.delay
import kotlinx.coroutines.future.await
import kotlinx.coroutines.test.runTest
import org.junit.jupiter.api.AfterAll
import org.junit.jupiter.api.Assertions.*
import org.junit.jupiter.api.BeforeAll
import org.junit.jupiter.api.RepeatedTest
import org.junit.jupiter.api.Tag
import org.junit.jupiter.api.Test
import s2.client.BasinClient
import s2.config.Config
import s2.types.*
import xtdb.api.log.Log
import xtdb.api.log.Log.Message
import xtdb.api.log.Log.Record
import xtdb.api.log.Log.Subscriber
import xtdb.api.storage.Storage
import xtdb.database.Database
import xtdb.database.proto.DatabaseConfig
import xtdb.log.proto.TrieDetails
import xtdb.util.asPath
import java.nio.ByteBuffer
import java.time.Duration
import java.util.*
import java.util.Collections.synchronizedList
import kotlin.time.Duration.Companion.seconds

@Tag("integration")
class S2ClusterTest {
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

    private fun txMessage(id: Byte) = Message.Tx(byteArrayOf(-1, id))

    @Test
    fun `cluster-factory-creates-cluster`() {
        S2Cluster.ClusterFactory(token, basin).open().use { cluster ->
            assertNotNull(cluster)
        }
    }

    @Test
    fun `log-factory-requires-cluster`() {
        S2Cluster.ClusterFactory(token, basin).open().use { cluster ->
            S2Cluster.LogFactory("s2", streamName)
                .openLog(mapOf("s2" to cluster))
                .use { log ->
                    assertNotNull(log)
                }
        }
    }

    @Test
    fun `read-last-message-empty-stream`() {
        // Create a fresh stream for this test
        val freshStreamName = "integration-empty-${UUID.randomUUID()}"
        val req = CreateStreamRequest.newBuilder()
            .withStreamName(freshStreamName)
            .withStreamConfig(streamConfig)
            .build()
        client.createStream(req).get()

        try {
            S2Cluster.ClusterFactory(token, basin).open().use { cluster ->
                S2Cluster.LogFactory("s2", freshStreamName)
                    .openLog(mapOf("s2" to cluster))
                    .use { log ->
                        assertNull(log.readLastMessage())
                    }
            }
        } finally {
            client.deleteStream(freshStreamName).get()
        }
    }

    @Test
    fun `read-last-message-returns-last`() = runTest(timeout = 30.seconds) {
        S2Cluster.ClusterFactory(token, basin).open().use { cluster ->
            S2Cluster.LogFactory("s2", streamName)
                .openLog(mapOf("s2" to cluster))
                .use { log ->
                    // Append a message
                    val txPayload = ByteBuffer.allocate(9).put(-1).putLong(42).flip().array()
                    log.appendMessage(Message.Tx(txPayload)).await()

                    // Read it back
                    val msg = log.readLastMessage()
                    assertNotNull(msg)
                    check(msg is Message.Tx)
                    assertEquals(42, ByteBuffer.wrap(msg.payload).getLong(1))
                }
        }
    }

    @Test
    fun `read-last-message-returns-last-after-multiple-appends`() = runTest(timeout = 30.seconds) {
        // Create a fresh stream for this test to ensure clean state
        val freshStreamName = "integration-multi-${UUID.randomUUID()}"
        val req = CreateStreamRequest.newBuilder()
            .withStreamName(freshStreamName)
            .withStreamConfig(streamConfig)
            .build()
        client.createStream(req).get()

        try {
            S2Cluster.ClusterFactory(token, basin).open().use { cluster ->
                S2Cluster.LogFactory("s2", freshStreamName)
                    .openLog(mapOf("s2" to cluster))
                    .use { log ->
                        log.appendMessage(txMessage(1)).await()
                        log.appendMessage(txMessage(2)).await()
                        log.appendMessage(txMessage(3)).await()

                        val lastMessage = log.readLastMessage()
                        check(lastMessage is Message.Tx)
                        assertArrayEquals(byteArrayOf(-1, 3), lastMessage.payload)
                    }
            }
        } finally {
            client.deleteStream(freshStreamName).get()
        }
    }

    @Test
    fun `write-to-serializes-config`() {
        val logFactory = S2Cluster.LogFactory("s2", "my-stream", epoch = 5)
        val builder = DatabaseConfig.newBuilder()
        logFactory.writeTo(builder)

        val config = builder.build()
        assertTrue(config.hasOtherLog())
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

        val databaseConfig = Database.Config(
            Log.localLog("log-path".asPath), Storage.local("storage-path".asPath)
        )

        S2Cluster.ClusterFactory(token, basin).open().use { cluster ->
            S2Cluster.LogFactory("s2", streamName)
                .openLog(mapOf("s2" to cluster))
                .use { log ->
                    log.subscribe(subscriber, 0).use { _ ->
                        val txPayload = ByteBuffer.allocate(9).put(-1).putLong(42).flip().array()
                        log.appendMessage(Message.Tx(txPayload)).await()

                        log.appendMessage(Message.FlushBlock(12L)).await()

                        log.appendMessage(Message.TriesAdded(Storage.VERSION, 0, addedTrieDetails)).await()

                        log.appendMessage(Message.AttachDatabase("foo", databaseConfig)).await()

                        while (msgs.flatten().size < 4) delay(100)
                    }
                }
        }

        assertEquals(4, msgs.flatten().size)

        val allMsgs = msgs.flatten()

        allMsgs[0].message.let {
            check(it is Message.Tx)
            assertEquals(42, ByteBuffer.wrap(it.payload).getLong(1))
        }

        allMsgs[1].message.let {
            check(it is Message.FlushBlock)
            assertEquals(12L, it.expectedBlockIdx)
        }

        allMsgs[2].message.let {
            check(it is Message.TriesAdded)
            assertEquals(addedTrieDetails, it.tries)
        }

        allMsgs[3].message.let {
            check(it is Message.AttachDatabase)
            assertEquals("foo", it.dbName)
            assertEquals(databaseConfig, it.config)
        }
    }

    @Tag("integration")
    @RepeatedTest(100)
    fun `close should cancel all subscription coroutines without leaking`() = runTest(timeout = 10.seconds) {
        // Create a fresh stream for this test
        val freshStreamName = "integration-leak-${UUID.randomUUID()}"
        val req = CreateStreamRequest.newBuilder()
            .withStreamName(freshStreamName)
            .withStreamConfig(streamConfig)
            .build()
        client.createStream(req).get()

        try {
            S2Cluster.ClusterFactory(token, basin).open().use { cluster ->
                S2Cluster.LogFactory("s2", freshStreamName)
                    .openLog(mapOf("s2" to cluster))
                    .use { log ->
                        val records = mutableListOf<Record>()
                        val subscription = log.subscribe(
                            object : Subscriber {
                                override val latestProcessedMsgId: Long = -1
                                override val latestSubmittedMsgId: Long = -1
                                override fun processRecords(recs: List<Record>) {
                                    recs.forEach { records.add(it) }
                                }
                            },
                            0
                        )

                        log.appendMessage(Message.FlushBlock(1)).await()

                        subscription.close()
                    }
            }
        } finally {
            client.deleteStream(freshStreamName).get()
        }
    }
}
