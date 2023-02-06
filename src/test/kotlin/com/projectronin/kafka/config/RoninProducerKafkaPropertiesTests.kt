package com.projectronin.kafka.config

import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertFalse
import org.junit.jupiter.api.Assertions.assertNotEquals
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test

class RoninProducerKafkaPropertiesTests {
    @Test
    fun `provides defaults`() {
        val properties = RoninProducerKafkaProperties().properties
        assertTrue(properties.containsKey("key.serializer.encoding"))
        assertTrue(properties.containsKey("key.serializer"))
        assertTrue(properties.containsKey("value.serializer.encoding"))
        assertTrue(properties.containsKey("value.serializer"))
        assertTrue(properties.containsKey("acks"))
        assertTrue(properties.containsKey("enable.idempotence"))
        assertTrue(properties.containsKey("retries"))
        assertTrue(properties.containsKey("max.in.flight.requests.per.connection"))
        assertTrue(properties.containsKey("buffer.memory"))
        assertTrue(properties.containsKey("max.block.ms"))
        assertTrue(properties.containsKey("linger.ms"))
        assertTrue(properties.containsKey("batch.size"))
        assertTrue(properties.containsKey("compression.type"))
    }

    @Test
    fun `overrides defaults`() {
        val defaults = RoninProducerKafkaProperties().properties
        val overridden = RoninProducerKafkaProperties("linger.ms" to 42).properties

        assertEquals(42, overridden["linger.ms"])
        assertNotEquals(defaults["linger.ms"], overridden["linger.ms"])
    }

    @Test
    fun `appends new property`() {
        val defaults = RoninProducerKafkaProperties().properties
        val appended = RoninProducerKafkaProperties("the.answer" to 42L).properties

        assertFalse(defaults.containsKey("the.answer"))
        assertTrue(appended.containsKey("the.answer"))
        assertEquals(42L, appended["the.answer"])
    }
}
