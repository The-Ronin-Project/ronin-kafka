package com.projectronin.kafka.data

import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertNull
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertDoesNotThrow
import java.time.Instant
import java.util.UUID

class RoninEventTests {
    data class Data(val id: Int)

    @Test
    fun `minimum required fields test`() {
        val data = Data(1)
        val event = RoninEvent(
            dataSchema = "https://projectronin.com/data-schema",
            source = "tests",
            type = "data.created",
            data = data
        )

        assertDoesNotThrow { UUID.fromString(event.id) }
        assertTrue(Instant.now().isAfter(event.time))
        assertEquals("1.0", event.specVersion)
        assertEquals("https://projectronin.com/data-schema", event.dataSchema)
        assertEquals("data.created", event.type)
        assertEquals("tests", event.source)
        assertEquals("application/json", event.dataContentType)
        assertNull(event.getSubject())
        assertNull(event.tenantId)
        assertNull(event.patientId)
        assertNull(event.resourceType)
        assertNull(event.resourceId)
        assertEquals(data, event.data)
    }

    @Test
    fun `full fields test`() {
        val data = Data(1)
        val time = Instant.now()
        val event = RoninEvent(
            id = "id12",
            time = time,
            specVersion = "7.5",
            dataContentType = "application/json",
            subject = "subject",
            tenantId = "tenantId",
            patientId = "patientId",
            resourceType = "resourceType",
            resourceId = "resourceId",
            dataSchema = "https://projectronin.com/data-schema",
            source = "tests",
            type = "data.created",
            data = data
        )

        assertEquals("id12", event.id)
        assertEquals(time, event.time)
        assertEquals("7.5", event.specVersion)
        assertEquals("https://projectronin.com/data-schema", event.dataSchema)
        assertEquals("data.created", event.type)
        assertEquals("tests", event.source)
        assertEquals("application/json", event.dataContentType)
        assertEquals("subject", event.getSubject())
        assertEquals("tenantId", event.tenantId)
        assertEquals("patientId", event.patientId)
        assertEquals("resourceType", event.resourceType)
        assertEquals("resourceId", event.resourceId)
        assertEquals(data, event.data)
    }

    @Test
    fun `set subject test`() {
        val data = Data(12)
        val event = RoninEvent(
            dataSchema = "https://projectronin.com/data-schema",
            source = "tests",
            type = "data.created",
            data = data,
            subject = "data.create/12"
        )

        assertEquals("data.create/12", event.getSubject())
        assertEquals("data.create", event.resourceType)
        assertEquals("12", event.resourceId)
    }

    @Test
    fun `set subject improper format test`() {
        val data = Data(12)
        val event = RoninEvent(
            dataSchema = "https://projectronin.com/data-schema",
            source = "tests",
            type = "data.created",
            data = data,
            subject = "badsubject"
        )

        assertEquals("badsubject", event.getSubject())
        assertNull(event.resourceType)
        assertNull(event.resourceId)
    }

    @Test
    fun `set resource fields test`() {
        val data = Data(12)
        val event = RoninEvent(
            dataSchema = "https://projectronin.com/data-schema",
            source = "tests",
            type = "data.created",
            data = data,
            resourceType = "data.create",
            resourceId = "12"
        )

        assertEquals("data.create/12", event.getSubject())
        assertEquals("data.create", event.resourceType)
        assertEquals("12", event.resourceId)
    }

    @Test
    fun `set resource type field test`() {
        val data = Data(12)
        val event = RoninEvent(
            dataSchema = "https://projectronin.com/data-schema",
            source = "tests",
            type = "data.created",
            data = data,
            resourceType = "data.create"
        )

        assertNull(event.getSubject())
        assertEquals("data.create", event.resourceType)
        assertNull(null, event.resourceId)
    }

    @Test
    fun `set resource id field test`() {
        val data = Data(12)
        val event = RoninEvent(
            dataSchema = "https://projectronin.com/data-schema",
            source = "tests",
            type = "data.created",
            data = data,
            resourceId = "12"
        )

        assertNull(event.getSubject())
        assertNull(null, event.resourceType)
        assertEquals("12", event.resourceId)
    }

    @Test
    fun `set from headers test`() {
        val data = Data(1)
        val time = Instant.now()

        @Suppress("DEPRECATION")
        val event = RoninEvent(
            headers = mapOf(
                KafkaHeaders.id to "id12",
                KafkaHeaders.time to time.toString(),
                KafkaHeaders.specVersion to "7.5",
                KafkaHeaders.contentType to "application/json",
                KafkaHeaders.subject to "resourceType/resourceId",
                KafkaHeaders.tenantId to "tenantId",
                KafkaHeaders.patientId to "patientId",
                KafkaHeaders.dataSchema to "https://projectronin.com/data-schema",
                KafkaHeaders.source to "tests",
                KafkaHeaders.resourceVersion to "5",
                KafkaHeaders.type to "data.created"
            ),
            data = data
        )

        assertEquals("id12", event.id)
        assertEquals(time, event.time)
        assertEquals("7.5", event.specVersion)
        assertEquals("https://projectronin.com/data-schema", event.dataSchema)
        assertEquals("data.created", event.type)
        assertEquals("tests", event.source)
        assertEquals("application/json", event.dataContentType)
        assertEquals("resourceType/resourceId", event.getSubject())
        assertEquals("tenantId", event.tenantId)
        assertEquals("patientId", event.patientId)
        assertEquals("resourceType", event.resourceType)
        assertEquals("resourceId", event.resourceId)
        assertEquals(5, event.resourceVersion)
        assertEquals(data, event.data)
    }
}
