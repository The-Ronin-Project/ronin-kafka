package com.projectronin.kafka.data

import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test

class StringHeaderTests {
    @Test
    fun `provides the header key`() {
        val header = StringHeader("blah", "yep")
        assertEquals("blah", header.key())
    }

    @Test
    fun `provides the header value`() {
        val header = StringHeader("blah", "yep")
        assertTrue("yep".toByteArray(Charsets.UTF_8).contentEquals(header.value()))
    }
}
