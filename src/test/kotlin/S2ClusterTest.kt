package chucklehead.xtdb.s2


import io.mockk.every
import io.mockk.mockk
import kotlinx.coroutines.delay
import kotlinx.coroutines.future.await
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.withTimeout
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Assertions.*
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.RepeatedTest
import org.junit.jupiter.api.Tag
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.TestInstance
import org.junit.jupiter.api.TestInstance.Lifecycle.PER_CLASS
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
@TestInstance(PER_CLASS)
class S2ClusterTest {
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
    
    @BeforeEach
    fun beforeEach() {
        streamName = "integration-${UUID.randomUUID()}"
        println("Creating stream: $streamName in basin: $basin")
        val req = CreateStreamRequest.newBuilder()
            .withStreamName(streamName)
            .withStreamConfig(streamConfig)
            .build()
        client.createStream(req).get()
        println("Stream created successfully")
    }

    @AfterEach
    fun afterEach() {
        client.deleteStream(streamName).get()
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
        S2Cluster.ClusterFactory(token, basin).open().use { cluster ->
            S2Cluster.LogFactory("s2", streamName)
                .openLog(mapOf("s2" to cluster))
                .use { log ->
                    assertNull(log.readLastMessage())
                }
        }
    }

    @Test
    fun `read-last-message-returns-last`() = runBlocking {
        withTimeout(30.seconds) {
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
    }

    @Test
    fun `read-last-message-returns-last-after-multiple-appends`() = runBlocking {
        withTimeout(30.seconds) {
            S2Cluster.ClusterFactory(token, basin).open().use { cluster ->
            S2Cluster.LogFactory("s2", streamName)
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
    fun `round-trips-messages`() = runBlocking {
        withTimeout(30.seconds) {
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
                    log.subscribe(subscriber, -1).use { _ ->
                        val txPayload = ByteBuffer.allocate(9).put(-1).putLong(42).flip().array()
                        log.appendMessage(Message.Tx(txPayload)).await()

                        log.appendMessage(Message.FlushBlock(12L)).await()

                        log.appendMessage(Message.TriesAdded(Storage.VERSION, 0, addedTrieDetails)).await()

                        log.appendMessage(Message.AttachDatabase("foo", databaseConfig)).await()

                        while (synchronized(msgs) { msgs.flatten().size } < 4) delay(100)
                    }
                }
            }

            val allMsgs = synchronized(msgs) { msgs.flatten() }

            assertEquals(4, allMsgs.size)

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
    }

    @Tag("integration")
    @RepeatedTest(100)
    fun `close should cancel all subscription coroutines without leaking`() = runBlocking {
        withTimeout(10.seconds) {
            S2Cluster.ClusterFactory(token, basin).open().use { cluster ->
            S2Cluster.LogFactory("s2", streamName)
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
        }
    }
}
