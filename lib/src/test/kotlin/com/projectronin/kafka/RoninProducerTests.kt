package com.projectronin.kafka

import com.projectronin.kafka.data.RoninEvent
import io.mockk.every
import io.mockk.mockk
import io.mockk.slot
import io.mockk.verify
import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.clients.producer.RecordMetadata
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertNotNull
import org.junit.jupiter.api.Assertions.assertNull
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test
import java.time.Instant
import java.util.concurrent.CompletableFuture

class RoninProducerTests {
    private val kafkaProducer = mockk<KafkaProducer<String, String>>()
    private val roninProducer = RoninProducer(
        "topic",
        "source",
        "dataschema",
        kafkaProducer = kafkaProducer
    )
    private val fixedInstant = Instant.ofEpochSecond(1660000000)

    @Test
    fun `send RoninEvent`() {
        val recordSlot = slot<ProducerRecord<String, String>>()
        val metadata = mockk<RecordMetadata>()
        every { kafkaProducer.send(capture(recordSlot), any()) } returns CompletableFuture.completedFuture(metadata)
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
                    data = object : RoninEvent.Data<Int> {
                        override val id: Int = 3
                    }
                )
            )

        verify(exactly = 1) { kafkaProducer.send(any(), any()) }
        with(recordSlot.captured) {
            assertEquals("topic", topic())
            assertNull(partition())
            assertEquals("subject", key())
            assertEquals("{\"id\":3}", value())
            assertTrue("1".toByteArray(Charsets.UTF_8).contentEquals(headers().lastHeader("ce_id").value()))
            assertTrue("tests".toByteArray(Charsets.UTF_8).contentEquals(headers().lastHeader("ce_source").value()))
            assertTrue("4.2".toByteArray(Charsets.UTF_8).contentEquals(headers().lastHeader("ce_specversion").value()))
            assertTrue("dummy".toByteArray(Charsets.UTF_8).contentEquals(headers().lastHeader("ce_type").value()))
            assertTrue("stuff".toByteArray(Charsets.UTF_8).contentEquals(headers().lastHeader("content-type").value()))
            assertTrue(
                "le-schema".toByteArray(Charsets.UTF_8).contentEquals(headers().lastHeader("ce_dataschema").value())
            )
            assertTrue(
                "2022-08-08T23:06:40Z".toByteArray(Charsets.UTF_8)
                    .contentEquals(headers().lastHeader("ce_time").value())
            )
        }
        assertEquals(metadata, response.get())
    }

    @Test
    fun `send RoninEvent_Data`() {
        val recordSlot = slot<ProducerRecord<String, String>>()
        val metadata = mockk<RecordMetadata>()
        every { kafkaProducer.send(capture(recordSlot), any()) } returns CompletableFuture.completedFuture(metadata)
        val response = roninProducer
            .send(
                type = "dummy",
                subject = "subject",
                data = object : RoninEvent.Data<Int> {
                    override val id: Int = 3
                }
            )

        verify(exactly = 1) { kafkaProducer.send(any(), any()) }
        with(recordSlot.captured) {
            assertEquals("topic", topic())
            assertNull(partition())
            assertEquals("subject", key())
            assertEquals("{\"id\":3}", value())
            assertNotNull(headers().lastHeader("ce_id").value())
            assertTrue("source".toByteArray(Charsets.UTF_8).contentEquals(headers().lastHeader("ce_source").value()))
            assertTrue("1.0".toByteArray(Charsets.UTF_8).contentEquals(headers().lastHeader("ce_specversion").value()))
            assertTrue("dummy".toByteArray(Charsets.UTF_8).contentEquals(headers().lastHeader("ce_type").value()))
            assertTrue(
                "application/json"
                    .toByteArray(Charsets.UTF_8)
                    .contentEquals(headers().lastHeader("content-type").value())
            )
            assertTrue(
                "dataschema".toByteArray(Charsets.UTF_8).contentEquals(headers().lastHeader("ce_dataschema").value())
            )
            assertNotNull(headers().lastHeader("ce_time").value())
        }
        assertEquals(metadata, response.get())
    }

    @Test
    fun flush() {
        every { kafkaProducer.flush() } returns Unit
        roninProducer.flush()
        verify(exactly = 1) { kafkaProducer.flush() }
    }
}
