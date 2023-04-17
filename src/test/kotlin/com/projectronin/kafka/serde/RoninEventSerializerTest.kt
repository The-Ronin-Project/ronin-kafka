package com.projectronin.kafka.serde

import com.projectronin.kafka.MockUtils
import com.projectronin.kafka.RoninConsumer
import com.projectronin.kafka.RoninProducer
import com.projectronin.kafka.config.RoninConsumerKafkaProperties
import com.projectronin.kafka.config.RoninProducerKafkaProperties
import com.projectronin.kafka.data.RoninEvent
import com.projectronin.kafka.data.RoninEventResult
import io.mockk.every
import io.mockk.mockk
import io.mockk.slot
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.clients.consumer.OffsetAndMetadata
import org.apache.kafka.clients.producer.Callback
import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.clients.producer.RecordMetadata
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.header.Headers
import org.apache.kafka.common.header.internals.RecordHeaders
import org.junit.jupiter.api.Assertions.assertArrayEquals
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertNull
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertThrows
import java.time.Duration
import java.time.Instant
import java.util.concurrent.CompletableFuture

class RoninEventSerializerTest {
    private val fixedInstant: Instant = Instant.ofEpochSecond(1660000000)

    private data class Stuff(val id: String)

    @Test
    fun `validate serialization`() {
        val originalEvent = RoninEvent(
            id = "1",
            time = fixedInstant,
            specVersion = "4.2",
            dataSchema = "le-schema",
            dataContentType = "stuff",
            source = "tests",
            type = "dummy",
            data = Stuff("3"),
            subject = "subject"
        )

        val serializer: RoninEventSerializer<Stuff> = RoninEventSerializer()
        val serializedHeaders = RecordHeaders()
        val serializedBytes = serializer.serialize("topic", serializedHeaders, originalEvent)

        assertEquals("1", serializedHeaders.getString("ce_id"))
        assertEquals("tests", serializedHeaders.getString("ce_source"))
        assertEquals("4.2", serializedHeaders.getString("ce_specversion"))
        assertEquals("dummy", serializedHeaders.getString("ce_type"))
        assertEquals("stuff", serializedHeaders.getString("content-type"))
        assertEquals("le-schema", serializedHeaders.getString("ce_dataschema"))
        assertEquals("2022-08-08T23:06:40Z", serializedHeaders.getString("ce_time"))
        assertEquals("subject", serializedHeaders.getString("ce_subject"))

        assertEquals("{\"id\":\"3\"}", serializedBytes?.decodeToString())
    }

    @Test
    fun `serialize with no headers throws exception`() {
        val serializer: RoninEventSerializer<Stuff> = RoninEventSerializer()
        assertThrows<Exception> {
            serializer.serialize(
                "topic",
                RoninEvent(
                    id = "1",
                    time = fixedInstant,
                    specVersion = "4.2",
                    dataSchema = "le-schema",
                    dataContentType = "stuff",
                    source = "tests",
                    type = "dummy",
                    data = Stuff("3"),
                    subject = "subject"
                )
            )
        }
    }

    @Test
    fun `serialize with null headers throws exception`() {
        val serializer: RoninEventSerializer<Stuff> = RoninEventSerializer()
        assertThrows<Exception> {
            serializer.serialize(
                "topic", null,
                RoninEvent(
                    id = "1",
                    time = fixedInstant,
                    specVersion = "4.2",
                    dataSchema = "le-schema",
                    dataContentType = "stuff",
                    source = "tests",
                    type = "dummy",
                    data = Stuff("3"),
                    subject = "subject"
                )
            )
        }
    }

    @Test
    fun `serialize with null event returns null, no headers`() {
        val serializer: RoninEventSerializer<Stuff> = RoninEventSerializer()
        assertNull(serializer.serialize("topic", RecordHeaders(), null))
    }

    @Test
    fun `compare serialization to RoninProducer `() {
        val originalEvent = RoninEvent(
            id = "1",
            time = fixedInstant,
            specVersion = "4.2",
            dataSchema = "le-schema",
            dataContentType = "stuff",
            source = "tests",
            type = "dummy",
            data = Stuff("3"),
            subject = "subject"
        )

        val producedRecord = captureRecordFromRoninProducer(originalEvent)

        val serializer: RoninEventSerializer<Stuff> = RoninEventSerializer()
        val serializedHeaders = RecordHeaders()
        val serializedBytes = serializer.serialize("topic", serializedHeaders, originalEvent)

        with(producedRecord) {
            assertEquals("topic", topic())

            // Assert that the RoninConsumer put subject into key, Serializer put it into ce_subject
            assertEquals("subject", serializedHeaders.getString("ce_subject"))
            assertEquals("subject", key())

            // Assert the rest of the headers match what the consumer has
            assertEquals(headers().getString("ce_id"), serializedHeaders.getString("ce_id"))
            assertEquals(headers().getString("ce_source"), serializedHeaders.getString("ce_source"))
            assertEquals(headers().getString("ce_specversion"), serializedHeaders.getString("ce_specversion"))
            assertEquals(headers().getString("ce_type"), serializedHeaders.getString("ce_type"))
            assertEquals(headers().getString("content-type"), serializedHeaders.getString("content-type"))
            assertEquals(headers().getString("ce_dataschema"), serializedHeaders.getString("ce_dataschema"))
            assertEquals(headers().getString("ce_time"), serializedHeaders.getString("ce_time"))

            // Assert that the bytes in data are the same
            assertArrayEquals(value(), serializedBytes)
        }
    }

    @Test
    fun `test serialized is consumable by by RoninConsumer`() {
        val originalEvent = RoninEvent(
            id = "1",
            time = fixedInstant,
            specVersion = "4.2",
            dataSchema = "le-schema",
            dataContentType = "stuff",
            source = "tests",
            type = "stuff",
            data = Stuff("3"),
            subject = "subject"
        )

        val serializer: RoninEventSerializer<Stuff> = RoninEventSerializer()
        val serializedHeaders = RecordHeaders()
        val serializedBytes = serializer.serialize("topic", serializedHeaders, originalEvent)

        val kafkaConsumer = mockk<KafkaConsumer<String, ByteArray>> {
            every { subscribe(listOf("topic.1", "topic.2")) } returns Unit
            every { commitSync(any<Map<TopicPartition, OffsetAndMetadata>>()) } returns Unit
            every { wakeup() } returns Unit
            every { close() } returns Unit
        }
        val roninConsumer = RoninConsumer(
            listOf("topic.1", "topic.2"),
            mapOf("stuff" to Stuff::class),
            kafkaConsumer = kafkaConsumer,
            kafkaProperties = RoninConsumerKafkaProperties()
        )

        every { kafkaConsumer.poll(any<Duration>()) } returns MockUtils.records(
            MockUtils.record("subject", serializedBytes!!, serializedHeaders),
        )

        val processed = mutableListOf<RoninEvent<*>>()
        roninConsumer.process { e ->
            processed.add(e)
            roninConsumer.stop()
            RoninEventResult.ACK
        }

        assertEquals(originalEvent, processed[0])
    }

    private fun captureRecordFromRoninProducer(event: RoninEvent<Stuff>): ProducerRecord<String, ByteArray> {
        val kafkaProducer = mockk<KafkaProducer<String, ByteArray>>()
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

        roninProducer.send(event)
        return recordSlot.captured
    }

    private fun Headers.getString(key: String) = lastHeader(key).value().decodeToString()
}
