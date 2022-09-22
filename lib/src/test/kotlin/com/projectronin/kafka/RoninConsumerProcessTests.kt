package com.projectronin.kafka

import com.fasterxml.jackson.databind.JsonMappingException
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
import org.hamcrest.MatcherAssert.assertThat
import org.hamcrest.Matchers.contains
import org.hamcrest.Matchers.containsInAnyOrder
import org.hamcrest.Matchers.instanceOf
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test
import java.time.Duration
import java.time.Instant

class RoninConsumerProcessTests {
    data class Stuff(override val id: String) : RoninEvent.Data<String>

    private val kafkaConsumer = mockk<KafkaConsumer<String, ByteArray>> {
        every { subscribe(listOf("topic.1", "topic.2")) } returns Unit
        every { commitSync(any<Map<TopicPartition, OffsetAndMetadata>>()) } returns Unit
    }
    private val metrics = SimpleMeterRegistry()
    private val roninConsumer = RoninConsumer(
        listOf("topic.1", "topic.2"),
        mapOf("stuff" to Stuff::class),
        kafkaConsumer = kafkaConsumer,
        meterRegistry = metrics
    )
    private val fixedInstant = Instant.ofEpochSecond(1660000000)

    @Test
    fun `receives events`() {
        every { kafkaConsumer.poll(any<Duration>()) } returns MockUtils.records(
            MockUtils.record("stuff", "key1.1", "{\"id\": \"one\"}"),
            MockUtils.record("stuff", "key1.2", "{\"id\": \"two\"}"),
            MockUtils.record("stuff", "last", "{\"id\": \"three\"}"),
        )

        val processed = mutableListOf<RoninEvent<*>>()
        roninConsumer.process { e ->
            processed.add(e)
            if (e.subject == "last") {
                roninConsumer.stop()
            }
            RoninEventResult.ACK
        }

        assertThat(
            processed,
            contains(
                RoninEvent("1", fixedInstant, "3", "4", "5", "6", "stuff", "key1.1", Stuff("one")),
                RoninEvent("1", fixedInstant, "3", "4", "5", "6", "stuff", "key1.2", Stuff("two")),
                RoninEvent("1", fixedInstant, "3", "4", "5", "6", "stuff", "last", Stuff("three")),
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
            kafkaConsumer = kafkaConsumer
        )
        every { kafkaConsumer.poll(any<Duration>()) } returns MockUtils.records(
            MockUtils.record("stuff", "key1.1", "{\"id\": \"one\"}"),
            MockUtils.record("stuff", "key1.2", "{\"id\": \"two\"}"),
            MockUtils.record("stuff", "last", "{\"id\": \"three\"}"),
        )

        val processed = mutableListOf<RoninEvent<*>>()
        roninConsumer.process { e ->
            processed.add(e)
            if (e.subject == "last") {
                roninConsumer.stop()
            }
            RoninEventResult.ACK
        }

        assertThat(
            processed,
            contains(
                RoninEvent("1", fixedInstant, "3", "4", "5", "6", "stuff", "key1.1", Stuff("one")),
                RoninEvent("1", fixedInstant, "3", "4", "5", "6", "stuff", "key1.2", Stuff("two")),
                RoninEvent("1", fixedInstant, "3", "4", "5", "6", "stuff", "last", Stuff("three")),
            )
        )
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
            if (e.subject == "last") {
                roninConsumer.stop()
            }
            RoninEventResult.ACK
        }

        assertThat(
            processed,
            contains(
                RoninEvent("1", fixedInstant, "3", "4", "5", "6", "stuff", "last", Stuff("three")),
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
            if (e.subject == "last") {
                roninConsumer.stop()
            }
            RoninEventResult.ACK
        }

        assertThat(
            processed,
            contains(
                RoninEvent("1", fixedInstant, "3", "4", "5", "6", "stuff", "last", Stuff("three")),
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
            if (e.subject == "last") {
                roninConsumer.stop()
            }
            RoninEventResult.ACK
        }

        assertThat(
            processed,
            contains(
                RoninEvent("1", fixedInstant, "3", "4", "5", "6", "stuff", "last", Stuff("three")),
            )
        )

        assertEquals(1, metrics[RoninConsumer.Metrics.POLL_TIMER].timer().count())
        assertEquals(1, metrics[RoninConsumer.Metrics.POLL_MESSAGE_DISTRIBUTION].summary().count())
        assertEquals(2, metrics[RoninConsumer.Metrics.MESSAGE_QUEUE_TIMER].timer().count())
        assertEquals(1, metrics[RoninConsumer.Metrics.MESSAGE_HANDLER_TIMER].timer().count())
        assertEquals(1.0, metrics[RoninConsumer.Metrics.EXCEPTION_DESERIALIZATION].counter().count())
        assertThat(
            metrics.meters.find { it.id.name == RoninConsumer.Metrics.EXCEPTION_DESERIALIZATION }?.id?.tags,
            containsInAnyOrder(ImmutableTag("topic", "topic"))
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
            meterRegistry = metrics
        )
        val exceptionRecord = MockUtils.record("stuff", "key1.2", "{\"nope\": 3}")
        every { kafkaConsumer.poll(any<Duration>()) } returns MockUtils.records(
            exceptionRecord,
            MockUtils.record("stuff", "last", "{\"id\": \"three\"}"),
        )

        val processed = mutableListOf<RoninEvent<*>>()
        roninConsumer.process { e ->
            processed.add(e)
            if (e.subject == "last") {
                roninConsumer.stop()
            }
            RoninEventResult.ACK
        }

        assertThat(
            processed,
            contains(
                RoninEvent("1", fixedInstant, "3", "4", "5", "6", "stuff", "last", Stuff("three")),
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
            when (e.subject) {
                "key1.2" -> throw RuntimeException("kaboom")
                "last" -> roninConsumer.stop()
            }
            RoninEventResult.ACK
        }

        assertThat(
            processed,
            contains(
                RoninEvent("1", fixedInstant, "3", "4", "5", "6", "stuff", "key1.2", Stuff("two")),
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
            meterRegistry = metrics
        )

        every { kafkaConsumer.poll(any<Duration>()) } returns MockUtils.records(
            MockUtils.record("stuff", "key1.2", "{\"id\": \"two\"}"),
            MockUtils.record("stuff", "last", "{\"id\": \"three\"}"),
        )

        val processed = mutableListOf<RoninEvent<*>>()
        val exception = RuntimeException("kaboom")
        roninConsumer.process { e ->
            processed.add(e)
            when (e.subject) {
                "key1.2" -> throw exception
                "last" -> roninConsumer.stop()
            }
            RoninEventResult.ACK
        }

        val exceptionEvent =
            RoninEvent("1", fixedInstant, "3", "4", "5", "6", "stuff", "key1.2", Stuff("two"))
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
            }
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
}
