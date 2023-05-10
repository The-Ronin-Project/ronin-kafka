package com.projectronin.kafka.serde.wrapper

import org.junit.jupiter.api.Assertions.assertInstanceOf
import org.junit.jupiter.api.Test

class WrapperSerdeTest {
    private data class Stuff(val id: String)

    @Test
    fun testSerde() {
        val serde = WrapperSerde<Stuff>()

        assertInstanceOf(WrapperSerializer::class.java, serde.serializer())
        assertInstanceOf(WrapperDeserializer::class.java, serde.deserializer())
    }
}
