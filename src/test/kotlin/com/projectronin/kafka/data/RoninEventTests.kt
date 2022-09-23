package com.projectronin.kafka.data

import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertDoesNotThrow
import java.util.UUID

class RoninEventTests {
    data class Data(override val id: Int) : RoninEvent.Data<Int>

    private val event = RoninEvent(
        dataSchema = "https://projectronin.com/data-schema",
        source = "tests",
        type = "data.created",
        subject = "data.created/1",
        data = Data(1)
    )

    @Test
    fun `sets subject`() {
        assertEquals("data.created/1", event.subject)
    }

    @Test
    fun `defaults id`() {
        assertDoesNotThrow { UUID.fromString(event.id) }
    }

    @Test
    fun `defaults specVersion`() {
        assertEquals("1.0", event.specVersion)
    }

    @Test
    fun `defaults dataContentType`() {
        assertEquals("application/json", event.dataContentType)
    }
}
