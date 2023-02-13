package com.projectronin.kafka.serde

import org.junit.jupiter.api.Assertions.assertInstanceOf
import org.junit.jupiter.api.Test

class RoninEventSerdeTest {
    private data class Stuff(val id: String)

    @Test
    fun testSerde() {
        val serde = RoninEventSerde<Stuff>()

        assertInstanceOf(RoninEventSerializer::class.java, serde.serializer())
        assertInstanceOf(RoninEventDeserializer::class.java, serde.deserializer())
    }
}
