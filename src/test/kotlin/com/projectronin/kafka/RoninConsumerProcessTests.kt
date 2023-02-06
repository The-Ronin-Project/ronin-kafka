package com.projectronin.kafka

import com.projectronin.kafka.config.RoninConsumerKafkaProperties
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
import org.apache.kafka.clients.consumer.MockConsumer
import org.apache.kafka.clients.consumer.OffsetAndMetadata
import org.apache.kafka.clients.consumer.OffsetResetStrategy
import org.apache.kafka.common.KafkaException
import org.apache.kafka.common.TopicPartition
import org.hamcrest.MatcherAssert.assertThat
import org.hamcrest.Matchers.contains
import org.hamcrest.Matchers.containsInAnyOrder
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test
import java.time.Duration
import java.time.Instant

class RoninConsumerProcessTests {
    data class Stuff(val id: String)

    private val kafkaConsumer = mockk<KafkaConsumer<String, RoninEvent<*>>> {
        every { subscribe(listOf("topic.1", "topic.2")) } returns Unit
        every { commitSync(any<Map<TopicPartition, OffsetAndMetadata>>()) } returns Unit
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

    @Test
    fun `receives events`() {
        every { kafkaConsumer.poll(any<Duration>()) } returns MockUtils.records(
            MockUtils.record(
                "stuff", "key1.1",
                RoninEvent(
                    id = "1",
                    time = fixedInstant,
                    dataSchema = "4",
                    dataContentType = "5",
                    source = "6",
                    type = "stuff",
                    subject = "key1.1",
                    data = Stuff("one")
                )
            ),
            MockUtils.record(
                "stuff", "key1.2",
                RoninEvent(
                    id = "2",
                    time = fixedInstant,
                    dataSchema = "4",
                    dataContentType = "5",
                    source = "6",
                    type = "stuff",
                    subject = "key1.2",
                    data = Stuff("two")
                )
            ),
            MockUtils.record(
                "stuff", "last",
                RoninEvent(
                    id = "3",
                    time = fixedInstant,
                    dataSchema = "4",
                    dataContentType = "5",
                    source = "6",
                    type = "stuff",
                    subject = "last",
                    data = Stuff("three")
                )
            ),
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
                RoninEvent("1", fixedInstant, "1.0", "4", "5", "6", "stuff", "key1.1", Stuff("one")),
                RoninEvent("2", fixedInstant, "1.0", "4", "5", "6", "stuff", "key1.2", Stuff("two")),
                RoninEvent("3", fixedInstant, "1.0", "4", "5", "6", "stuff", "last", Stuff("three")),
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
            MockUtils.record(
                "stuff", "key1.1",
                RoninEvent(
                    id = "1",
                    time = fixedInstant,
                    dataSchema = "4",
                    dataContentType = "5",
                    source = "6",
                    type = "stuff",
                    subject = "key1.1",
                    data = Stuff("one")
                )
            ),
            MockUtils.record(
                "stuff", "key1.2",
                RoninEvent(
                    id = "2",
                    time = fixedInstant,
                    dataSchema = "4",
                    dataContentType = "5",
                    source = "6",
                    type = "stuff",
                    subject = "key1.2",
                    data = Stuff("two")
                )
            ),
            MockUtils.record(
                "stuff", "last",
                RoninEvent(
                    id = "3",
                    time = fixedInstant,
                    dataSchema = "4",
                    dataContentType = "5",
                    source = "6",
                    type = "stuff",
                    subject = "last",
                    data = Stuff("three")
                )
            ),
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
                RoninEvent("1", fixedInstant, "1.0", "4", "5", "6", "stuff", "key1.1", Stuff("one")),
                RoninEvent("2", fixedInstant, "1.0", "4", "5", "6", "stuff", "key1.2", Stuff("two")),
                RoninEvent("3", fixedInstant, "1.0", "4", "5", "6", "stuff", "last", Stuff("three")),
            )
        )
        verify(exactly = 3) { kafkaConsumer.commitSync(mapOf(TopicPartition("topic", 1) to OffsetAndMetadata(43))) }
    }

    @Test
    fun `processing error without error handler`() {
        every { kafkaConsumer.poll(any<Duration>()) } returns MockUtils.records(
            MockUtils.record(
                "stuff", "key1.2",
                RoninEvent(
                    id = "2",
                    time = fixedInstant,
                    dataSchema = "4",
                    dataContentType = "5",
                    source = "6",
                    type = "stuff",
                    subject = "key1.2",
                    data = Stuff("two")
                )
            ),
            MockUtils.record(
                "stuff", "last",
                RoninEvent(
                    id = "3",
                    time = fixedInstant,
                    dataSchema = "4",
                    dataContentType = "5",
                    source = "6",
                    type = "stuff",
                    subject = "last",
                    data = Stuff("three")
                )
            ),
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
                RoninEvent("2", fixedInstant, "1.0", "4", "5", "6", "stuff", "key1.2", Stuff("two")),
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
                override fun recordHandlingException(record: ConsumerRecord<String, *>, t: Throwable) {
                    TODO("Not yet implemented")
                }

                override fun eventProcessingException(events: List<RoninEvent<*>>, t: Throwable) {
                    exceptionEvents.addAll(events)
                    exceptions.add(t)
                }

                override fun pollException(t: Throwable) {
                    TODO("Not yet implemented")
                }

                override fun deserializationException(t: Throwable) {
                    TODO("Not yet implemented")
                }
            },
            meterRegistry = metrics,
            kafkaProperties = RoninConsumerKafkaProperties()
        )

        every { kafkaConsumer.poll(any<Duration>()) } returns MockUtils.records(
            MockUtils.record(
                "stuff", "key1.2",
                RoninEvent(
                    id = "2",
                    time = fixedInstant,
                    dataSchema = "4",
                    dataContentType = "5",
                    source = "6",
                    type = "stuff",
                    subject = "key1.2",
                    data = Stuff("two")
                )
            ),
            MockUtils.record(
                "stuff", "last",
                RoninEvent(
                    id = "3",
                    time = fixedInstant,
                    dataSchema = "4",
                    dataContentType = "5",
                    source = "6",
                    type = "stuff",
                    subject = "last",
                    data = Stuff("three")
                )
            ),
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
            RoninEvent("2", fixedInstant, "1.0", "4", "5", "6", "stuff", "key1.2", Stuff("two"))
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
                override fun recordHandlingException(record: ConsumerRecord<String, *>, t: Throwable) {
                    TODO("Not yet implemented")
                }

                override fun eventProcessingException(events: List<RoninEvent<*>>, t: Throwable) {
                    throw RuntimeException("kaboom kaboom")
                }

                override fun pollException(t: Throwable) {
                    TODO("Not yet implemented")
                }

                override fun deserializationException(t: Throwable) {
                    TODO("Not yet implemented")
                }
            },
            kafkaProperties = RoninConsumerKafkaProperties()
        )

        every { kafkaConsumer.poll(any<Duration>()) } returns mockk {
            val records = mutableListOf(
                MockUtils.record(
                    "stuff", "key1.2",
                    RoninEvent(
                        dataSchema = "https://projectronin.com/data-schema",
                        source = "tests",
                        type = "data.created",
                        subject = "key1.2",
                        data = Stuff("two")
                    )
                ),
            )
            every { iterator() } returns records.iterator()
        }

        roninConsumer.process { throw RuntimeException("source kaboom") }

        verify(exactly = 0) { kafkaConsumer.commitSync(any<Map<TopicPartition, OffsetAndMetadata>>()) }
        assertEquals(RoninConsumer.Status.STOPPED, roninConsumer.status())
    }

    @Test
    fun `deserialization error`() {
        val consumer = MockConsumer<String, RoninEvent<*>>(OffsetResetStrategy.LATEST)
        consumer.schedulePollTask { consumer.setPollException(KafkaException("Something is wrong")) }

        val roninConsumer = RoninConsumer(
            listOf("topic.1", "topic.2"),
            mapOf("stuff" to Stuff::class),
            kafkaConsumer = consumer,
            exceptionHandler = object : ConsumerExceptionHandler {
                override fun recordHandlingException(record: ConsumerRecord<String, *>, t: Throwable) {
                    TODO("Not yet implemented")
                }

                override fun eventProcessingException(events: List<RoninEvent<*>>, t: Throwable) {
                    throw RuntimeException("kaboom kaboom")
                }

                override fun pollException(t: Throwable) {
                }

                override fun deserializationException(t: Throwable) {
                    TODO("Not yet implemented")
                }
            },
            kafkaProperties = RoninConsumerKafkaProperties(),
            meterRegistry = metrics
        )

        val processed = mutableListOf<RoninEvent<*>>()
        roninConsumer.process { e ->
            processed.add(e)
            if (e.subject == "last") {
                roninConsumer.stop()
            }
            RoninEventResult.ACK
        }

        assertEquals(1.0, metrics[RoninConsumer.Metrics.EXCEPTION_POLL].counter().count())
        verify(exactly = 0) { kafkaConsumer.commitSync(any<Map<TopicPartition, OffsetAndMetadata>>()) }
    }
}
