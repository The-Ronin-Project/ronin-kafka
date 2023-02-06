package com.projectronin.kafka

import com.projectronin.kafka.config.RoninProducerKafkaProperties
import com.projectronin.kafka.data.RoninEvent
import io.micrometer.core.instrument.ImmutableTag
import io.micrometer.core.instrument.simple.SimpleMeterRegistry
import io.mockk.every
import io.mockk.mockk
import io.mockk.slot
import io.mockk.verify
import org.apache.kafka.clients.producer.Callback
import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.clients.producer.RecordMetadata
import org.apache.kafka.common.TopicPartition
import org.hamcrest.MatcherAssert.assertThat
import org.hamcrest.Matchers.containsInAnyOrder
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertNull
import org.junit.jupiter.api.Test
import java.time.Instant
import java.util.concurrent.CompletableFuture

class RoninProducerTests {
    data class Stuff(val id: Int)

    private val kafkaProducer = mockk<KafkaProducer<String, RoninEvent<Stuff>>>()
    private val metrics = SimpleMeterRegistry()
    private val roninProducer = RoninProducer(
        "topic",
        "source",
        "dataschema",
        kafkaProducer = kafkaProducer,
        meterRegistry = metrics,
        kafkaProperties = RoninProducerKafkaProperties()
    )
    private val fixedInstant = Instant.ofEpochSecond(1660000000)

    @Test
    fun `send RoninEvent - success`() {
        val recordSlot = slot<ProducerRecord<String, RoninEvent<Stuff>>>()
        val metadata = mockk<RecordMetadata>()
        every { kafkaProducer.send(capture(recordSlot), any()) } answers {
            val block = secondArg<Callback>()
            block.onCompletion(
                RecordMetadata(TopicPartition("topic", 1), 1L, 1, System.currentTimeMillis(), 4, 4),
                null
            )
            CompletableFuture.completedFuture(metadata)
        }
        val response = roninProducer
            .send(
                RoninEvent(
                    id = "1",
                    time = fixedInstant,
                    specVersion = "4.2",
                    dataSchema = "le-schema",
                    dataContentType = "stuff",
                    source = "tests",
                    type = "dummy",
                    subject = "subject",
                    data = Stuff(3)
                )
            )

        verify(exactly = 1) { kafkaProducer.send(any(), any()) }
        with(recordSlot.captured) {
            assertEquals("topic", topic())
            assertNull(partition())
            assertEquals("subject", key())
            assertEquals(3, value().data.id)
        }
        assertEquals(metadata, response.get())

        assertEquals(1, metrics[RoninProducer.Metrics.SEND_TIMER].timer().count())
        val meters = metrics.meters.associate { it.id.name to it.id.tags }
        assertThat(meters.keys, containsInAnyOrder(RoninProducer.Metrics.SEND_TIMER))
        assertThat(
            meters[RoninProducer.Metrics.SEND_TIMER],
            containsInAnyOrder(
                ImmutableTag("topic", "topic"),
                ImmutableTag("ce_type", "dummy"),
                ImmutableTag("success", "true"),
            )
        )
    }

    // basically just here for test coverage, but checks that things still work without a meter registry
    @Test
    fun `send RoninEvent - success - no metrics`() {
        val roninProducer = RoninProducer(
            "topic",
            "source",
            "dataschema",
            kafkaProducer = kafkaProducer,
            kafkaProperties = RoninProducerKafkaProperties()
        )
        val recordSlot = slot<ProducerRecord<String, RoninEvent<Stuff>>>()
        val metadata = mockk<RecordMetadata>()
        every { kafkaProducer.send(capture(recordSlot), any()) } answers {
            val block = secondArg<Callback>()
            block.onCompletion(
                RecordMetadata(TopicPartition("topic", 1), 1L, 1, System.currentTimeMillis(), 4, 4),
                null
            )
            CompletableFuture.completedFuture(metadata)
        }
        val response = roninProducer
            .send(
                RoninEvent(
                    id = "1",
                    time = fixedInstant,
                    specVersion = "4.2",
                    dataSchema = "le-schema",
                    dataContentType = "stuff",
                    source = "tests",
                    type = "dummy",
                    subject = "subject",
                    data = Stuff(3)
                )
            )

        verify(exactly = 1) { kafkaProducer.send(any(), any()) }
        with(recordSlot.captured) {
            assertEquals("topic", topic())
            assertNull(partition())
            assertEquals("subject", key())
            assertEquals(3, value().data.id)
        }
        assertEquals(metadata, response.get())
    }

    @Test
    fun `send RoninEvent_Data - failure`() {
        val recordSlot = slot<ProducerRecord<String, RoninEvent<Stuff>>>()
        val metadata = mockk<RecordMetadata>()
        every { kafkaProducer.send(capture(recordSlot), any()) } answers {
            val block = secondArg<Callback>()
            block.onCompletion(
                RecordMetadata(TopicPartition("topic", 1), 1L, 1, System.currentTimeMillis(), 4, 4),
                RuntimeException("kaboom")
            )
            CompletableFuture.completedFuture(metadata)
        }
        val response = roninProducer
            .send(
                type = "dummy",
                subject = "subject",
                data = Stuff(3)
            )

        verify(exactly = 1) { kafkaProducer.send(any(), any()) }
        with(recordSlot.captured) {
            assertEquals("topic", topic())
            assertNull(partition())
            assertEquals("subject", key())
            assertEquals(3, value().data.id)
        }
        assertEquals(metadata, response.get())

        assertEquals(1, metrics[RoninProducer.Metrics.SEND_TIMER].timer().count())
        val meters = metrics.meters.associate { it.id.name to it.id.tags }
        assertThat(meters.keys, containsInAnyOrder(RoninProducer.Metrics.SEND_TIMER))
        assertThat(
            meters[RoninProducer.Metrics.SEND_TIMER],
            containsInAnyOrder(
                ImmutableTag("topic", "topic"),
                ImmutableTag("ce_type", "dummy"),
                ImmutableTag("success", "false"),
            )
        )
    }

    @Test
    fun flush() {
        every { kafkaProducer.flush() } returns Unit
        roninProducer.flush()
        verify(exactly = 1) { kafkaProducer.flush() }

        assertEquals(1, metrics[RoninProducer.Metrics.FLUSH_TIMER].timer().count())
        val meters = metrics.meters.associate { it.id.name to it.id.tags }
        assertThat(meters.keys, containsInAnyOrder(RoninProducer.Metrics.FLUSH_TIMER))
        assertEquals(meters[RoninProducer.Metrics.FLUSH_TIMER], emptyList<ImmutableTag>())
    }
}
