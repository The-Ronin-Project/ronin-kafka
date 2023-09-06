package com.projectronin.kafka.serde

import com.projectronin.kafka.config.RoninConfig.Companion.RONIN_DESERIALIZATION_TOPICS_CONFIG
import com.projectronin.kafka.config.RoninConfig.Companion.RONIN_DESERIALIZATION_TYPES_CONFIG
import com.projectronin.kafka.data.KafkaHeaders
import com.projectronin.kafka.data.StringHeader
import com.projectronin.kafka.exceptions.DeserializationException
import com.projectronin.kafka.exceptions.EventHeaderMissing
import com.projectronin.kafka.exceptions.UnknownEventType
import org.apache.kafka.common.header.internals.RecordHeaders
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertNotNull
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertThrows
import java.time.Instant

class RoninEventDeserializerTest {
    private data class Stuff(val id: String)
    private val fixedInstant: Instant = Instant.ofEpochSecond(1660000000)

    @Test
    fun `deserialize no headers error`() {
        val deserializer = RoninEventDeserializer<Stuff>()
        deserializer.configure(mutableMapOf(RONIN_DESERIALIZATION_TYPES_CONFIG to "stuff:com.projectronin.kafka.serde.RoninEventDeserializerTest\$Stuff"), false)
        assertThrows<Exception> {
            deserializer.deserialize("topic", "MattersNot".toByteArray())
        }
    }

    @Test
    fun `deserialize with complete headers`() {
        val deserializer = RoninEventDeserializer<Stuff>()
        deserializer.configure(mutableMapOf(RONIN_DESERIALIZATION_TYPES_CONFIG to "stuff:com.projectronin.kafka.serde.RoninEventDeserializerTest\$Stuff"), false)
        val headers = RecordHeaders(
            mutableListOf(
                StringHeader(KafkaHeaders.id, "1"),
                StringHeader(KafkaHeaders.source, "test"),
                StringHeader(KafkaHeaders.specVersion, "4.2"),
                StringHeader(KafkaHeaders.type, "stuff"),
                StringHeader(KafkaHeaders.contentType, "content"),
                StringHeader(KafkaHeaders.dataSchema, "schema"),
                StringHeader(KafkaHeaders.time, "2022-08-08T23:06:40Z"),
                StringHeader(KafkaHeaders.subject, "stuff.3"),
            )
        )
        val event = deserializer.deserialize("topic", headers, "{\"id\":\"3\"}".encodeToByteArray())

        assertNotNull(event)
        assertEquals("1", event.id)
        assertEquals("test", event.source)
        assertEquals("4.2", event.specVersion)
        assertEquals("stuff", event.type)
        assertEquals("content", event.dataContentType)
        assertEquals("schema", event.dataSchema)
        assertEquals(fixedInstant, event.time)
        assertEquals("stuff.3", event.getSubject())
        assertEquals(Stuff("3"), event.data)
    }

    @Test
    fun `deserializes valid message with topic mapping`() {
        val deserializer = RoninEventDeserializer<Stuff>()
        deserializer.configure(mutableMapOf(RONIN_DESERIALIZATION_TOPICS_CONFIG to "some_topic:com.projectronin.kafka.serde.RoninEventDeserializerTest\$Stuff"), false)
        val headers = RecordHeaders(
            mutableListOf(
                StringHeader(KafkaHeaders.id, "1"),
                StringHeader(KafkaHeaders.source, "test"),
                StringHeader(KafkaHeaders.specVersion, "4.2"),
                StringHeader(KafkaHeaders.type, "stuff"),
                StringHeader(KafkaHeaders.contentType, "content"),
                StringHeader(KafkaHeaders.dataSchema, "schema"),
                StringHeader(KafkaHeaders.time, "2022-08-08T23:06:40Z"),
                StringHeader(KafkaHeaders.subject, "stuff.3"),
            )
        )
        val event = deserializer.deserialize("some_topic", headers, "{\"id\":\"3\"}".encodeToByteArray())

        assertNotNull(event)
        assertEquals("1", event.id)
        assertEquals("test", event.source)
        assertEquals("4.2", event.specVersion)
        assertEquals("stuff", event.type)
        assertEquals("content", event.dataContentType)
        assertEquals("schema", event.dataSchema)
        assertEquals(fixedInstant, event.time)
        assertEquals("stuff.3", event.getSubject())
        assertEquals(Stuff("3"), event.data)
    }

    @Test
    fun `topic and type mappings work together`() {
        val deserializer = RoninEventDeserializer<Stuff>()
        deserializer.configure(mutableMapOf(RONIN_DESERIALIZATION_TOPICS_CONFIG to "some_topic:com.projectronin.kafka.serde.RoninEventDeserializerTest\$Stuff", RONIN_DESERIALIZATION_TYPES_CONFIG to "a_type:com.projectronin.kafka.serde.RoninEventDeserializerTest\$Stuff"), false)

        val eventByTopic = deserializer.deserialize(
            "some_topic",
            RecordHeaders(
                mutableListOf(
                    StringHeader(KafkaHeaders.id, "1"),
                    StringHeader(KafkaHeaders.source, "test"),
                    StringHeader(KafkaHeaders.specVersion, "4.2"),
                    StringHeader(KafkaHeaders.type, "stuff"),
                    StringHeader(KafkaHeaders.contentType, "content"),
                    StringHeader(KafkaHeaders.dataSchema, "schema"),
                    StringHeader(KafkaHeaders.time, "2022-08-08T23:06:40Z"),
                    StringHeader(KafkaHeaders.subject, "stuff.3"),
                )
            ),
            "{\"id\":\"3\"}".encodeToByteArray()
        )

        val eventByType = deserializer.deserialize(
            "other_topic",
            RecordHeaders(
                mutableListOf(
                    StringHeader(KafkaHeaders.id, "2"),
                    StringHeader(KafkaHeaders.source, "test"),
                    StringHeader(KafkaHeaders.specVersion, "4.2"),
                    StringHeader(KafkaHeaders.type, "a_type"),
                    StringHeader(KafkaHeaders.contentType, "content"),
                    StringHeader(KafkaHeaders.dataSchema, "schema"),
                    StringHeader(KafkaHeaders.time, "2022-08-08T23:06:40Z"),
                    StringHeader(KafkaHeaders.subject, "stuff.3"),
                )
            ),
            "{\"id\":\"3\"}".encodeToByteArray()
        )

        assertNotNull(eventByTopic)
        assertEquals("1", eventByTopic.id)

        assertNotNull(eventByType)
        assertEquals("2", eventByType.id)
    }

    @Test
    fun `deserialize missing required headers error`() {
        val deserializer = RoninEventDeserializer<Stuff>()
        deserializer.configure(mutableMapOf(RONIN_DESERIALIZATION_TYPES_CONFIG to "stuff:com.projectronin.kafka.serde.RoninEventDeserializerTest\$Stuff"), false)
        assertThrows<EventHeaderMissing> {
            val headers = RecordHeaders(
                mutableListOf(
                    StringHeader(KafkaHeaders.id, "1"),
                    StringHeader(KafkaHeaders.source, "test"),
                    StringHeader(KafkaHeaders.specVersion, "4.2"),
                    StringHeader(KafkaHeaders.contentType, "content"),
                    StringHeader(KafkaHeaders.dataSchema, "schema"),
                    StringHeader(KafkaHeaders.time, "2022-08-08T23:06:40Z"),
                    StringHeader(KafkaHeaders.subject, "stuff.3"),
                )
            )
            deserializer.deserialize("topic", headers, "MattersNot".toByteArray())
        }
    }

    @Test
    fun `deserialize missing type in map error`() {
        val deserializer = RoninEventDeserializer<Stuff>()
        deserializer.configure(mutableMapOf(RONIN_DESERIALIZATION_TYPES_CONFIG to "foo:com.projectronin.kafka.serde.RoninEventDeserializerTest\$Stuff"), false)
        assertThrows<UnknownEventType> {
            val headers = RecordHeaders(
                mutableListOf(
                    StringHeader(KafkaHeaders.id, "1"),
                    StringHeader(KafkaHeaders.source, "test"),
                    StringHeader(KafkaHeaders.specVersion, "4.2"),
                    StringHeader(KafkaHeaders.type, "stuff"),
                    StringHeader(KafkaHeaders.contentType, "content"),
                    StringHeader(KafkaHeaders.dataSchema, "schema"),
                    StringHeader(KafkaHeaders.time, "2022-08-08T23:06:40Z"),
                    StringHeader(KafkaHeaders.subject, "stuff.3"),
                )
            )
            deserializer.deserialize("topic", headers, "MattersNot".toByteArray())
        }
    }

    @Test
    fun `deserialize bad data error`() {
        val deserializer = RoninEventDeserializer<Stuff>()
        deserializer.configure(mutableMapOf(RONIN_DESERIALIZATION_TYPES_CONFIG to "stuff:com.projectronin.kafka.serde.RoninEventDeserializerTest\$Stuff"), false)
        assertThrows<DeserializationException> {
            val headers = RecordHeaders(
                mutableListOf(
                    StringHeader(KafkaHeaders.id, "1"),
                    StringHeader(KafkaHeaders.source, "test"),
                    StringHeader(KafkaHeaders.specVersion, "4.2"),
                    StringHeader(KafkaHeaders.type, "stuff"),
                    StringHeader(KafkaHeaders.contentType, "content"),
                    StringHeader(KafkaHeaders.dataSchema, "schema"),
                    StringHeader(KafkaHeaders.time, "2022-08-08T23:06:40Z"),
                    StringHeader(KafkaHeaders.subject, "stuff.3"),
                )
            )
            deserializer.deserialize("topic", headers, "MattersNot".toByteArray())
        }
    }
}
