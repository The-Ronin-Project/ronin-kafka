package com.projectronin.kafka.serde.wrapper

import org.junit.jupiter.api.Assertions.assertInstanceOf
import org.junit.jupiter.api.Test

class SerdeTest {
    private data class Stuff(val id: String)

    @Test
    fun testSerde() {
        val serde = Serde<Stuff>()

        assertInstanceOf(Serializer::class.java, serde.serializer())
        assertInstanceOf(Deserializer::class.java, serde.deserializer())
    }
}
