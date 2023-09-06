package com.projectronin.kafka

import com.fasterxml.jackson.databind.JsonMappingException
import com.projectronin.kafka.config.RoninConsumerKafkaProperties
import com.projectronin.kafka.data.KafkaHeaders
import com.projectronin.kafka.data.RoninEvent
import com.projectronin.kafka.data.RoninEventResult
import com.projectronin.kafka.exceptions.ConsumerExceptionHandler
import io.micrometer.core.instrument.ImmutableTag
import io.micrometer.core.instrument.simple.SimpleMeterRegistry
import io.mockk.every
import io.mockk.mockk
import io.mockk.verify
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.clients.consumer.OffsetAndMetadata
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.errors.WakeupException
import org.hamcrest.MatcherAssert.assertThat
import org.hamcrest.Matchers.contains
import org.hamcrest.Matchers.containsInAnyOrder
import org.hamcrest.Matchers.instanceOf
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Assertions.assertDoesNotThrow
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertThrows
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test
import java.time.Duration
import java.time.Instant
import java.util.concurrent.CountDownLatch
import java.util.concurrent.Executors
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicBoolean

class RoninConsumerProcessTests {
    private data class Stuff(val id: String)

    private val kafkaConsumer = mockk<KafkaConsumer<String, ByteArray>> {
        every { subscribe(listOf("topic.1", "topic.2")) } returns Unit
        every { commitSync(any<Map<TopicPartition, OffsetAndMetadata>>()) } returns Unit
        every { wakeup() } returns Unit
        every { close() } returns Unit
    }
    private val metrics = SimpleMeterRegistry()
    private val roninConsumer = RoninConsumer(
        listOf("topic.1", "topic.2"),
        mapOf("stuff" to Stuff::class),
        kafkaConsumer = kafkaConsumer,
        meterRegistry = metrics,
        kafkaProperties = RoninConsumerKafkaProperties()
    )
    private val fixedInstant = Instant.ofEpochSecond(1660000000)

    private val executor = Executors.newSingleThreadExecutor()

    @AfterEach
    fun shutdownExecutor() {
        executor.shutdownNow()
    }

    @Test
    fun `receives events`() {
        every { kafkaConsumer.poll(any<Duration>()) } returns MockUtils.records(
            MockUtils.record("stuff", "key1.1", "{\"id\": \"one\"}"),
            MockUtils.record("stuff", "key1.2", "{\"id\": \"two\"}", "resourceType/resourceId"),
            MockUtils.record("stuff", "last", "{\"id\": \"three\"}"),
        )

        val processed = mutableListOf<RoninEvent<*>>()
        roninConsumer.process { e ->
            processed.add(e)
            if (e.getSubject() == "last") {
                roninConsumer.stop()
            }
            RoninEventResult.ACK
        }

        assertThat(
            processed,
            contains(
                RoninEvent(
                    id = "1",
                    time = fixedInstant,
                    specVersion = "3",
                    dataSchema = "4",
                    dataContentType = "5",
                    source = "6",
                    type = "stuff",
                    data = Stuff("one"),
                    subject = "key1.1"
                ),
                RoninEvent(
                    id = "1",
                    time = fixedInstant,
                    specVersion = "3",
                    dataSchema = "4",
                    dataContentType = "5",
                    source = "6",
                    type = "stuff",
                    data = Stuff("two"),
                    subject = "resourceType/resourceId",
                    resourceType = "resourceType",
                    resourceId = "resourceId"
                ),
                RoninEvent(
                    id = "1",
                    time = fixedInstant,
                    specVersion = "3",
                    dataSchema = "4",
                    dataContentType = "5",
                    source = "6",
                    type = "stuff",
                    data = Stuff("three"),
                    subject = "last"
                ),
            )
        )

        assertEquals(1, metrics[RoninConsumer.Metrics.POLL_TIMER].timer().count())
        assertEquals(1, metrics[RoninConsumer.Metrics.POLL_MESSAGE_DISTRIBUTION].summary().count())
        assertEquals(3, metrics[RoninConsumer.Metrics.MESSAGE_QUEUE_TIMER].timer().count())
        assertEquals(3, metrics[RoninConsumer.Metrics.MESSAGE_HANDLER_TIMER].timer().count())
        assertEquals(3.0, metrics[RoninConsumer.Metrics.HANDLER_ACK].counter().count())
        val meters = metrics.meters.associate { it.id.name to it.id.tags }
        assertThat(
            meters.keys,
            containsInAnyOrder(
                RoninConsumer.Metrics.POLL_TIMER,
                RoninConsumer.Metrics.POLL_MESSAGE_DISTRIBUTION,
                RoninConsumer.Metrics.MESSAGE_QUEUE_TIMER,
                RoninConsumer.Metrics.MESSAGE_HANDLER_TIMER,
                RoninConsumer.Metrics.HANDLER_ACK,
            )
        )
        assertEquals(meters[RoninConsumer.Metrics.POLL_TIMER], emptyList<ImmutableTag>())
        assertEquals(meters[RoninConsumer.Metrics.POLL_MESSAGE_DISTRIBUTION], emptyList<ImmutableTag>())
        assertThat(
            meters[RoninConsumer.Metrics.MESSAGE_QUEUE_TIMER],
            containsInAnyOrder(ImmutableTag("topic", "topic"), ImmutableTag("ce_type", "stuff"))
        )
        assertThat(
            meters[RoninConsumer.Metrics.MESSAGE_HANDLER_TIMER],
            containsInAnyOrder(ImmutableTag("topic", "topic"), ImmutableTag("ce_type", "stuff"))
        )
        assertThat(
            meters[RoninConsumer.Metrics.HANDLER_ACK],
            containsInAnyOrder(ImmutableTag("topic", "topic"), ImmutableTag("ce_type", "stuff"))
        )
    }

    // basically just here for test coverage, but checks that things still work without a meter registry
    @Test
    fun `receives events - no metrics`() {
        val roninConsumer = RoninConsumer(
            listOf("topic.1", "topic.2"),
            mapOf("stuff" to Stuff::class),
            kafkaConsumer = kafkaConsumer,
            kafkaProperties = RoninConsumerKafkaProperties()
        )
        every { kafkaConsumer.poll(any<Duration>()) } returns MockUtils.records(
            MockUtils.record("stuff", "key1.1", "{\"id\": \"one\"}"),
            MockUtils.record("stuff", "key1.2", "{\"id\": \"two\"}"),
            MockUtils.record("stuff", "last", "{\"id\": \"three\"}"),
        )

        val processed = mutableListOf<RoninEvent<*>>()
        roninConsumer.process { e ->
            processed.add(e)
            if (e.getSubject() == "last") {
                roninConsumer.stop()
            }
            RoninEventResult.ACK
        }

        assertThat(
            processed,
            contains(
                RoninEvent("1", fixedInstant, "3", "4", "5", "6", "stuff", Stuff("one"), "key1.1"),
                RoninEvent("1", fixedInstant, "3", "4", "5", "6", "stuff", Stuff("two"), "key1.2"),
                RoninEvent("1", fixedInstant, "3", "4", "5", "6", "stuff", Stuff("three"), "last"),
            )
        )
        verify(exactly = 3) { kafkaConsumer.commitSync(mapOf(TopicPartition("topic", 1) to OffsetAndMetadata(43))) }
    }

    @Test
    fun `test poll once`() {
        val roninConsumer = RoninConsumer(
            listOf("topic.1", "topic.2"),
            mapOf("stuff" to Stuff::class),
            kafkaConsumer = kafkaConsumer,
            kafkaProperties = RoninConsumerKafkaProperties()
        )
        every { kafkaConsumer.poll(any<Duration>()) } returns MockUtils.records(
            MockUtils.record("stuff", "key1.1", "{\"id\": \"one\"}"),
            MockUtils.record("stuff", "key1.2", "{\"id\": \"two\"}"),
            MockUtils.record("stuff", "last", "{\"id\": \"three\"}"),
        )

        val processed = mutableListOf<RoninEvent<*>>()
        roninConsumer.pollOnce { e ->
            processed.add(e)
            RoninEventResult.ACK
        }

        verify(exactly = 3) { kafkaConsumer.commitSync(mapOf(TopicPartition("topic", 1) to OffsetAndMetadata(43))) }
    }

    @Test
    fun `unknown type is skipped`() {
        every { kafkaConsumer.poll(any<Duration>()) } returns MockUtils.records(
            MockUtils.record("nope", "key1.1", "{\"id\": \"one\"}"),
            MockUtils.record("stuff", "last", "{\"id\": \"three\"}"),
        )

        val processed = mutableListOf<RoninEvent<*>>()
        roninConsumer.process { e ->
            processed.add(e)
            if (e.getSubject() == "last") {
                roninConsumer.stop()
            }
            RoninEventResult.ACK
        }

        assertThat(
            processed,
            contains(
                RoninEvent("1", fixedInstant, "3", "4", "5", "6", "stuff", Stuff("three"), "last"),
            )
        )

        assertEquals(1, metrics[RoninConsumer.Metrics.POLL_TIMER].timer().count())
        assertEquals(1, metrics[RoninConsumer.Metrics.POLL_MESSAGE_DISTRIBUTION].summary().count())
        assertEquals(1, metrics[RoninConsumer.Metrics.MESSAGE_QUEUE_TIMER].timer().count())
        assertEquals(1, metrics[RoninConsumer.Metrics.MESSAGE_HANDLER_TIMER].timer().count())
        assertEquals(1.0, metrics[RoninConsumer.Metrics.EXCEPTION_UNKNOWN_TYPE].counter().count())
        assertThat(
            metrics.meters.find { it.id.name == RoninConsumer.Metrics.EXCEPTION_UNKNOWN_TYPE }?.id?.tags,
            containsInAnyOrder(ImmutableTag("topic", "topic"), ImmutableTag("ce_type", "nope"))
        )
        verify(exactly = 2) { kafkaConsumer.commitSync(any<Map<TopicPartition, OffsetAndMetadata>>()) }
    }

    @Test
    fun `record with missing headers is skipped`() {
        every { kafkaConsumer.poll(any<Duration>()) } returns MockUtils.records(
            mockk {
                every { topic() } returns "topic.1"
                every { partition() } returns 1
                every { key() } returns "key.1"
                every { value() } returns "{\"id\": \"one\"}".toByteArray()
                every { headers() } returns mockk {
                    val h = mutableListOf(
                        MockUtils.Header(KafkaHeaders.id, "1"),
                    )
                    every { iterator() } returns h.iterator()
                }
                every { offset() } returns 42
                every { timestamp() } returns 1659999750
            },
            MockUtils.record("stuff", "last", "{\"id\": \"three\"}"),
        )

        val processed = mutableListOf<RoninEvent<*>>()
        roninConsumer.process { e ->
            processed.add(e)
            if (e.getSubject() == "last") {
                roninConsumer.stop()
            }
            RoninEventResult.ACK
        }

        assertThat(
            processed,
            contains(
                RoninEvent("1", fixedInstant, "3", "4", "5", "6", "stuff", Stuff("three"), "last"),
            )
        )

        assertEquals(1, metrics[RoninConsumer.Metrics.POLL_TIMER].timer().count())
        assertEquals(1, metrics[RoninConsumer.Metrics.POLL_MESSAGE_DISTRIBUTION].summary().count())
        assertEquals(1, metrics[RoninConsumer.Metrics.MESSAGE_QUEUE_TIMER].timer().count())
        assertEquals(1, metrics[RoninConsumer.Metrics.MESSAGE_HANDLER_TIMER].timer().count())
        assertEquals(1.0, metrics[RoninConsumer.Metrics.EXCEPTION_MISSING_HEADER].counter().count())
        assertThat(
            metrics.meters.find { it.id.name == RoninConsumer.Metrics.EXCEPTION_MISSING_HEADER }?.id?.tags,
            containsInAnyOrder(ImmutableTag("topic", "topic.1"), ImmutableTag("ce_type", "null"))
        )
        verify(exactly = 2) { kafkaConsumer.commitSync(any<Map<TopicPartition, OffsetAndMetadata>>()) }
    }

    @Test
    fun `deserialization failure without error handler`() {
        every { kafkaConsumer.poll(any<Duration>()) } returns MockUtils.records(
            MockUtils.record("stuff", "key1.2", "{\"nope\": 3}"),
            MockUtils.record("stuff", "last", "{\"id\": \"three\"}"),
        )

        val processed = mutableListOf<RoninEvent<*>>()
        roninConsumer.process { e ->
            processed.add(e)
            if (e.getSubject() == "last") {
                roninConsumer.stop()
            }
            RoninEventResult.ACK
        }

        assertThat(
            processed,
            contains(
                RoninEvent("1", fixedInstant, "3", "4", "5", "6", "stuff", Stuff("three"), "last"),
            )
        )

        assertEquals(1, metrics[RoninConsumer.Metrics.POLL_TIMER].timer().count())
        assertEquals(1, metrics[RoninConsumer.Metrics.POLL_MESSAGE_DISTRIBUTION].summary().count())
        assertEquals(2, metrics[RoninConsumer.Metrics.MESSAGE_QUEUE_TIMER].timer().count())
        assertEquals(1, metrics[RoninConsumer.Metrics.MESSAGE_HANDLER_TIMER].timer().count())
        assertEquals(1.0, metrics[RoninConsumer.Metrics.EXCEPTION_DESERIALIZATION].counter().count())
        assertThat(
            metrics.meters.find { it.id.name == RoninConsumer.Metrics.EXCEPTION_DESERIALIZATION }?.id?.tags,
            containsInAnyOrder(ImmutableTag("topic", "topic"), ImmutableTag("ce_type", "stuff"))
        )
        verify(exactly = 2) { kafkaConsumer.commitSync(any<Map<TopicPartition, OffsetAndMetadata>>()) }
    }

    @Test
    fun `deserialization failure with error handler`() {
        val exceptionRecords = mutableListOf<ConsumerRecord<String, ByteArray>>()
        val exceptions = mutableListOf<Throwable>()
        val roninConsumer = RoninConsumer(
            listOf("topic.1", "topic.2"),
            mapOf("stuff" to Stuff::class),
            kafkaConsumer = kafkaConsumer,
            exceptionHandler = object : ConsumerExceptionHandler {
                override fun recordHandlingException(record: ConsumerRecord<String, ByteArray>, t: Throwable) {
                    exceptionRecords.add(record)
                    exceptions.add(t)
                }

                override fun eventProcessingException(events: List<RoninEvent<*>>, t: Throwable) {
                    TODO("Not yet implemented")
                }
            },
            meterRegistry = metrics,
            kafkaProperties = RoninConsumerKafkaProperties()
        )
        val exceptionRecord = MockUtils.record("stuff", "key1.2", "{\"nope\": 3}")
        every { kafkaConsumer.poll(any<Duration>()) } returns MockUtils.records(
            exceptionRecord,
            MockUtils.record("stuff", "last", "{\"id\": \"three\"}"),
        )

        val processed = mutableListOf<RoninEvent<*>>()
        roninConsumer.process { e ->
            processed.add(e)
            if (e.getSubject() == "last") {
                roninConsumer.stop()
            }
            RoninEventResult.ACK
        }

        assertThat(
            processed,
            contains(
                RoninEvent("1", fixedInstant, "3", "4", "5", "6", "stuff", Stuff("three"), "last"),
            )
        )
        verify(exactly = 2) { kafkaConsumer.commitSync(any<Map<TopicPartition, OffsetAndMetadata>>()) }
        assertThat(exceptionRecords, contains(exceptionRecord))
        assertThat(exceptions, contains(instanceOf(JsonMappingException::class.java)))

        assertEquals(1, metrics[RoninConsumer.Metrics.POLL_TIMER].timer().count())
        assertEquals(1, metrics[RoninConsumer.Metrics.POLL_MESSAGE_DISTRIBUTION].summary().count())
        assertEquals(2, metrics[RoninConsumer.Metrics.MESSAGE_QUEUE_TIMER].timer().count())
        assertEquals(1, metrics[RoninConsumer.Metrics.MESSAGE_HANDLER_TIMER].timer().count())
        assertEquals(1.0, metrics[RoninConsumer.Metrics.EXCEPTION_DESERIALIZATION].counter().count())
    }

    @Test
    fun `processing error without error handler`() {
        every { kafkaConsumer.poll(any<Duration>()) } returns MockUtils.records(
            MockUtils.record("stuff", "key1.2", "{\"id\": \"two\"}"),
            MockUtils.record("stuff", "last", "{\"id\": \"three\"}"),
        )

        val processed = mutableListOf<RoninEvent<*>>()
        roninConsumer.process { e ->
            processed.add(e)
            when (e.getSubject()) {
                "key1.2" -> throw RuntimeException("kaboom")
                "last" -> roninConsumer.stop()
            }
            RoninEventResult.ACK
        }

        assertThat(
            processed,
            contains(
                RoninEvent("1", fixedInstant, "3", "4", "5", "6", "stuff", Stuff("two"), "key1.2"),
            )
        )

        assertEquals(1, metrics[RoninConsumer.Metrics.POLL_TIMER].timer().count())
        assertEquals(1, metrics[RoninConsumer.Metrics.POLL_MESSAGE_DISTRIBUTION].summary().count())
        assertEquals(1, metrics[RoninConsumer.Metrics.MESSAGE_QUEUE_TIMER].timer().count())
        assertEquals(1.0, metrics[RoninConsumer.Metrics.HANDLER_UNHANDLED_EXCEPTION].counter().count())
        assertThat(
            metrics.meters.find { it.id.name == RoninConsumer.Metrics.HANDLER_UNHANDLED_EXCEPTION }?.id?.tags,
            containsInAnyOrder(ImmutableTag("topic", "topic"), ImmutableTag("ce_type", "stuff"))
        )
        verify(exactly = 0) { kafkaConsumer.commitSync(any<Map<TopicPartition, OffsetAndMetadata>>()) }
    }

    @Test
    fun `processing error with error handler`() {
        val exceptionEvents = mutableListOf<RoninEvent<*>>()
        val exceptions = mutableListOf<Throwable>()
        val roninConsumer = RoninConsumer(
            listOf("topic.1", "topic.2"),
            mapOf("stuff" to Stuff::class),
            kafkaConsumer = kafkaConsumer,
            exceptionHandler = object : ConsumerExceptionHandler {
                override fun recordHandlingException(record: ConsumerRecord<String, ByteArray>, t: Throwable) {
                    TODO("Not yet implemented")
                }

                override fun eventProcessingException(events: List<RoninEvent<*>>, t: Throwable) {
                    exceptionEvents.addAll(events)
                    exceptions.add(t)
                }
            },
            meterRegistry = metrics,
            kafkaProperties = RoninConsumerKafkaProperties()
        )

        every { kafkaConsumer.poll(any<Duration>()) } returns MockUtils.records(
            MockUtils.record("stuff", "key1.2", "{\"id\": \"two\"}"),
            MockUtils.record("stuff", "last", "{\"id\": \"three\"}"),
        )

        val processed = mutableListOf<RoninEvent<*>>()
        val exception = RuntimeException("kaboom")
        roninConsumer.process { e ->
            processed.add(e)
            when (e.getSubject()) {
                "key1.2" -> throw exception
                "last" -> roninConsumer.stop()
            }
            RoninEventResult.ACK
        }

        val exceptionEvent =
            RoninEvent("1", fixedInstant, "3", "4", "5", "6", "stuff", Stuff("two"), "key1.2")
        assertThat(
            processed,
            contains(
                exceptionEvent,
            )
        )
        assertThat(exceptionEvents, contains(exceptionEvent))
        assertThat(exceptions, contains(exception))

        assertEquals(1, metrics[RoninConsumer.Metrics.POLL_TIMER].timer().count())
        assertEquals(1, metrics[RoninConsumer.Metrics.POLL_MESSAGE_DISTRIBUTION].summary().count())
        assertEquals(1, metrics[RoninConsumer.Metrics.MESSAGE_QUEUE_TIMER].timer().count())
        assertEquals(1.0, metrics[RoninConsumer.Metrics.HANDLER_UNHANDLED_EXCEPTION].counter().count())
        verify(exactly = 0) { kafkaConsumer.commitSync(any<Map<TopicPartition, OffsetAndMetadata>>()) }
    }

    @Test
    fun `processing error with throwing error handler`() {
        val roninConsumer = RoninConsumer(
            listOf("topic.1", "topic.2"),
            mapOf("stuff" to Stuff::class),
            kafkaConsumer = kafkaConsumer,
            exceptionHandler = object : ConsumerExceptionHandler {
                override fun recordHandlingException(record: ConsumerRecord<String, ByteArray>, t: Throwable) {
                    TODO("Not yet implemented")
                }

                override fun eventProcessingException(events: List<RoninEvent<*>>, t: Throwable) {
                    throw RuntimeException("kaboom kaboom")
                }
            },
            kafkaProperties = RoninConsumerKafkaProperties()
        )

        every { kafkaConsumer.poll(any<Duration>()) } returns mockk {
            val records = mutableListOf(
                MockUtils.record("stuff", "key1.2", "{\"id\": \"two\"}"),
            )
            every { iterator() } returns records.iterator()
        }

        roninConsumer.process { throw RuntimeException("source kaboom") }

        verify(exactly = 0) { kafkaConsumer.commitSync(any<Map<TopicPartition, OffsetAndMetadata>>()) }
        assertEquals(RoninConsumer.Status.STOPPED, roninConsumer.status())
    }

    @Test
    fun `process throws exception if process is already running`() {
        every { kafkaConsumer.poll(any<Duration>()) } returns MockUtils.records()

        executor.submit {
            roninConsumer.process { RoninEventResult.ACK }
        }

        // make sure it's actually running on the background thread - otherwise this thread could win and start first
        while (roninConsumer.status() != RoninConsumer.Status.PROCESSING) { Thread.sleep(10) }

        val exception = assertThrows(IllegalStateException::class.java) {
            roninConsumer.process { RoninEventResult.ACK }
        }

        assertEquals("process() can only be called once per instance", exception.message)

        roninConsumer.stop()
    }

    @Test
    fun `process throws exception after consumer is stopped`() {
        every { kafkaConsumer.poll(any<Duration>()) } returns MockUtils.records()

        executor.submit {
            roninConsumer.process { RoninEventResult.ACK }
        }
        roninConsumer.stop()

        assertEquals(RoninConsumer.Status.STOPPED, roninConsumer.status())
        val exception = assertThrows(IllegalStateException::class.java) {
            roninConsumer.process { RoninEventResult.ACK }
        }
        assertEquals("process() can only be called once per instance", exception.message)
    }

    @Test
    fun `processOnce throws exception when consumer is stopped`() {
        every { kafkaConsumer.poll(any<Duration>()) } returns MockUtils.records()

        roninConsumer.stop()
        val exception = assertThrows(IllegalStateException::class.java) {
            roninConsumer.pollOnce { RoninEventResult.ACK }
        }
        assertEquals("pollOnce() cannot be called after the instance is stopped", exception.message)
    }

    @Test
    fun `stop wakes up KafkaConsumer and exits processing loop`() {
        every { kafkaConsumer.poll(any<Duration>()) } returns MockUtils.records()

        val future = executor.submit {
            roninConsumer.process { RoninEventResult.ACK }
        }

        // Make sure the consumer actually starts processing before we stop it
        while (roninConsumer.status() != RoninConsumer.Status.PROCESSING) { Thread.sleep(10) }

        roninConsumer.stop()

        verify(exactly = 1) { kafkaConsumer.wakeup() }
        assertEquals(RoninConsumer.Status.STOPPED, roninConsumer.status())
        assertDoesNotThrow { future.get(100, TimeUnit.MILLISECONDS) }
        verify(exactly = 1) { kafkaConsumer.close() }
    }

    @Test
    fun `process rethrows WakeupExceptions when not shutting down`() {
        every { kafkaConsumer.poll(any<Duration>()) } throws WakeupException()

        assertThrows(WakeupException::class.java) {
            roninConsumer.process { RoninEventResult.ACK }
        }
    }

    @Test
    fun `process ignores WakeupExceptions during shutdown`() {
        // We need KafkaConsumer.poll() to throw a WakeupException _after_ we've stopped the consumer. To accomplish
        // this, we have the poll() mock block, and then we have KafkaConsumer.wakeup() wake it up, similarly to how
        // KafkaConsumer really works.
        val latch = CountDownLatch(1)
        val threwException = AtomicBoolean(false)
        every { kafkaConsumer.poll(any<Duration>()) } answers {
            latch.await()
            threwException.set(true)
            throw WakeupException()
        }
        every { kafkaConsumer.wakeup() } answers { latch.countDown() }

        val future = executor.submit {
            roninConsumer.process { RoninEventResult.ACK }
        }

        // Make sure the consumer actually starts processing (aka we're waiting on kafkaConsumer.poll())
        while (roninConsumer.status() != RoninConsumer.Status.PROCESSING) { Thread.sleep(10) }

        roninConsumer.stop()

        // make sure we actually threw the WakeupException
        assertTrue(threwException.get())
        // But that KafkaConsumer.process didn't throw it
        assertDoesNotThrow { future.get(100, TimeUnit.MILLISECONDS) }
    }

    // basically just here for test coverage, but checks that things still work without a meter registry
    @Test
    fun `receives events - resource set`() {
        val roninConsumer = RoninConsumer(
            listOf("topic.1", "topic.2"),
            mapOf("stuff" to Stuff::class),
            kafkaConsumer = kafkaConsumer,
            kafkaProperties = RoninConsumerKafkaProperties()
        )
        every { kafkaConsumer.poll(any<Duration>()) } returns MockUtils.records(
            MockUtils.record("stuff", "key1.1", "{\"id\": \"one\"}"),
            MockUtils.record("stuff", "key1.2", "{\"id\": \"two\"}"),
            MockUtils.record("stuff", "last", "{\"id\": \"three\"}"),
        )

        val processed = mutableListOf<RoninEvent<*>>()
        roninConsumer.process { e ->
            processed.add(e)
            if (e.getSubject() == "last") {
                roninConsumer.stop()
            }
            RoninEventResult.ACK
        }

        assertThat(
            processed,
            contains(
                RoninEvent("1", fixedInstant, "3", "4", "5", "6", "stuff", Stuff("one"), "key1.1"),
                RoninEvent("1", fixedInstant, "3", "4", "5", "6", "stuff", Stuff("two"), "key1.2"),
                RoninEvent("1", fixedInstant, "3", "4", "5", "6", "stuff", Stuff("three"), "last"),
            )
        )
        verify(exactly = 3) { kafkaConsumer.commitSync(mapOf(TopicPartition("topic", 1) to OffsetAndMetadata(43))) }
    }
}
