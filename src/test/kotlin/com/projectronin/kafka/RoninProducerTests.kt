package com.projectronin.kafka

import com.projectronin.kafka.config.RoninProducerKafkaProperties
import com.projectronin.kafka.data.KafkaHeaders
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
import org.apache.kafka.common.header.Headers
import org.hamcrest.MatcherAssert.assertThat
import org.hamcrest.Matchers.containsInAnyOrder
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertNotNull
import org.junit.jupiter.api.Assertions.assertNull
import org.junit.jupiter.api.Test
import java.time.Instant
import java.util.concurrent.CompletableFuture

class RoninProducerTests {
    private val kafkaProducer = mockk<KafkaProducer<String, ByteArray>>()
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

    private fun Headers.getString(key: String) = this.lastHeader(key).value().decodeToString()

    @Test
    fun `send RoninEvent - success`() {
        val recordSlot = slot<ProducerRecord<String, ByteArray>>()
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
                    data = object {
                        val id: Int = 3
                    },
                    subject = "subject"
                )
            )

        verify(exactly = 1) { kafkaProducer.send(any(), any()) }
        with(recordSlot.captured) {
            assertEquals("topic", topic())
            assertNull(partition())
            assertEquals("subject", key())
            assertEquals("{\"id\":3}", value().decodeToString())
            assertEquals("1", headers().getString("ce_id"))
            assertEquals("tests", headers().getString("ce_source"))
            assertEquals("4.2", headers().getString("ce_specversion"))
            assertEquals("dummy", headers().getString("ce_type"))
            assertEquals("stuff", headers().getString("content-type"))
            assertEquals("le-schema", headers().getString("ce_dataschema"))
            assertEquals("2022-08-08T23:06:40Z", headers().getString("ce_time"))
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
        val recordSlot = slot<ProducerRecord<String, ByteArray>>()
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
                    data = object {
                        val id: Int = 3
                    },
                    subject = "subject"
                )
            )

        verify(exactly = 1) { kafkaProducer.send(any(), any()) }
        with(recordSlot.captured) {
            assertEquals("topic", topic())
            assertNull(partition())
            assertEquals("subject", key())
            assertEquals("{\"id\":3}", value().decodeToString())
            assertEquals("1", headers().getString(KafkaHeaders.id))
            assertEquals("tests", headers().getString(KafkaHeaders.source))
            assertEquals("4.2", headers().getString(KafkaHeaders.specVersion))
            assertEquals("dummy", headers().getString(KafkaHeaders.type))
            assertEquals("stuff", headers().getString(KafkaHeaders.contentType))
            assertEquals("le-schema", headers().getString(KafkaHeaders.dataSchema))
            assertEquals("2022-08-08T23:06:40Z", headers().getString(KafkaHeaders.time))
        }
        assertEquals(metadata, response.get())
    }

    @Test
    fun `send RoninEvent_Data - failure`() {
        val recordSlot = slot<ProducerRecord<String, ByteArray>>()
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
                data = object {
                    val id: Int = 3
                }
            )

        verify(exactly = 1) { kafkaProducer.send(any(), any()) }
        with(recordSlot.captured) {
            assertEquals("topic", topic())
            assertNull(partition())
            assertEquals("subject", key())
            assertEquals("{\"id\":3}", value().decodeToString())
            assertNotNull(headers().getString(KafkaHeaders.id))
            assertEquals("source", headers().getString(KafkaHeaders.source))
            assertEquals("1.0", headers().getString(KafkaHeaders.specVersion))
            assertEquals("dummy", headers().getString(KafkaHeaders.type))
            assertEquals("application/json", headers().getString(KafkaHeaders.contentType))
            assertEquals("dataschema", headers().getString(KafkaHeaders.dataSchema))
            assertNotNull(headers().getString(KafkaHeaders.time))
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

    @Test
    fun useKeyWhenSupplied() {
        val recordSlot = slot<ProducerRecord<String, ByteArray>>()
        val metadata = mockk<RecordMetadata>()
        every { kafkaProducer.send(capture(recordSlot), any()) } answers {
            val block = secondArg<Callback>()
            block.onCompletion(
                RecordMetadata(TopicPartition("topic", 1), 1L, 1, System.currentTimeMillis(), 4, 4),
                RuntimeException("kaboom")
            )
            CompletableFuture.completedFuture(metadata)
        }

        val event = RoninEvent(
            dataSchema = "dataschema", source = "source", type = "dummy", data = object { val id: Int = 3 },
            subject = "subject"
        )

        roninProducer.send(event, "key1")

        verify(exactly = 1) { kafkaProducer.send(any(), any()) }
        with(recordSlot.captured) {
            assertEquals("key1", key())
            assertEquals("subject", headers().getString(KafkaHeaders.subject))
        }
    }

    @Test
    fun useKeyWhenSuppliedSubjectNull() {
        val recordSlot = slot<ProducerRecord<String, ByteArray>>()
        val metadata = mockk<RecordMetadata>()
        every { kafkaProducer.send(capture(recordSlot), any()) } answers {
            val block = secondArg<Callback>()
            block.onCompletion(
                RecordMetadata(TopicPartition("topic", 1), 1L, 1, System.currentTimeMillis(), 4, 4),
                RuntimeException("kaboom")
            )
            CompletableFuture.completedFuture(metadata)
        }

        val event = RoninEvent(
            dataSchema = "dataschema", source = "source", type = "dummy",
            data = object { val id: Int = 3 }
        )

        roninProducer.send(event, "key1")

        verify(exactly = 1) { kafkaProducer.send(any(), any()) }
        with(recordSlot.captured) {
            assertEquals("key1", key())
            assertNull(headers().find { h -> h.key().equals(KafkaHeaders.subject) })
        }
    }

    @Test
    fun noKeyWhenNoKeyNoSubject() {
        val recordSlot = slot<ProducerRecord<String, ByteArray>>()
        val metadata = mockk<RecordMetadata>()
        every { kafkaProducer.send(capture(recordSlot), any()) } answers {
            val block = secondArg<Callback>()
            block.onCompletion(
                RecordMetadata(TopicPartition("topic", 1), 1L, 1, System.currentTimeMillis(), 4, 4),
                RuntimeException("kaboom")
            )
            CompletableFuture.completedFuture(metadata)
        }

        val event = RoninEvent(
            dataSchema = "dataschema", source = "source", type = "dummy",
            data = object { val id: Int = 3 }
        )

        roninProducer.send(event)

        verify(exactly = 1) { kafkaProducer.send(any(), any()) }
        with(recordSlot.captured) {
            assertEquals(null, key())
            assertNull(headers().find { h -> h.key().equals(KafkaHeaders.subject) })
        }
    }

    @Test
    fun useSubjectWhenNoKey() {
        val recordSlot = slot<ProducerRecord<String, ByteArray>>()
        val metadata = mockk<RecordMetadata>()
        every { kafkaProducer.send(capture(recordSlot), any()) } answers {
            val block = secondArg<Callback>()
            block.onCompletion(
                RecordMetadata(TopicPartition("topic", 1), 1L, 1, System.currentTimeMillis(), 4, 4),
                RuntimeException("kaboom")
            )
            CompletableFuture.completedFuture(metadata)
        }

        val event = RoninEvent(
            dataSchema = "dataschema", source = "source", type = "dummy", data = object { val id: Int = 3 },
            subject = "subject"
        )

        roninProducer.send(event)

        verify(exactly = 1) { kafkaProducer.send(any(), any()) }
        with(recordSlot.captured) {
            assertEquals("subject", key())
            assertEquals("subject", headers().getString(KafkaHeaders.subject))
        }
    }
}
