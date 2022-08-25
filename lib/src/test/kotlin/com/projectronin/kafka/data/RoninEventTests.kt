package com.projectronin.kafka.data

import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test
import java.util.regex.Pattern

class RoninEventTests {
    data class Data(override val id: Int) : RoninEvent.Data<Int>

    private val event = RoninEvent(
        source = "tests",
        type = "data.created",
        dataSchema = "https://projectronin.com/data-schema",
        data = Data(1)
    )

    @Test
    fun `sets subject`() {
        assertEquals("data.created/1", event.subject)
    }

    @Test
    fun `defaults id`() {
        assertTrue(Pattern.matches("([a-f0-9]{8}(-[a-f0-9]{4}){4}[a-f0-9]{8})", event.id))
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