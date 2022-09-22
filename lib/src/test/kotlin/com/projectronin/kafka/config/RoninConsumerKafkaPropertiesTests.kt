package com.projectronin.kafka.config

import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertFalse
import org.junit.jupiter.api.Assertions.assertNotEquals
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test

class RoninConsumerKafkaPropertiesTests {
    @Test
    fun `provides defaults`() {
        val properties = RoninConsumerKafkaProperties().properties
        assertTrue(properties.containsKey("key.deserializer.encoding"))
        assertTrue(properties.containsKey("key.deserializer"))
        assertTrue(properties.containsKey("value.deserializer.encoding"))
        assertTrue(properties.containsKey("value.deserializer"))
        assertTrue(properties.containsKey("enable.auto.commit"))
        assertTrue(properties.containsKey("auto.offset.reset"))
        assertTrue(properties.containsKey("fetch.max.wait.ms"))
        assertTrue(properties.containsKey("fetch.min.bytes"))
        assertTrue(properties.containsKey("session.timeout.ms"))
        assertTrue(properties.containsKey("heartbeat.interval.ms"))
    }

    @Test
    fun `overrides defaults`() {
        val defaults = RoninConsumerKafkaProperties().properties
        val overridden = RoninConsumerKafkaProperties("fetch.min.bytes" to 42).properties

        assertEquals(42, overridden["fetch.min.bytes"])
        assertNotEquals(defaults["fetch.min.bytes"], overridden["fetch.min.bytes"])
    }

    @Test
    fun `appends new property`() {
        val defaults = RoninConsumerKafkaProperties().properties
        val appended = RoninConsumerKafkaProperties("the.answer" to 42L).properties

        assertFalse(defaults.containsKey("the.answer"))
        assertTrue(appended.containsKey("the.answer"))
        assertEquals(42L, appended["the.answer"])
    }
}
