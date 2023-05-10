package com.projectronin.kafka.serde.wrapper

import com.projectronin.kafka.config.RoninConfig.Companion.RONIN_DESERIALIZATION_TYPES_CONFIG
import com.projectronin.kafka.data.RoninWrapper
import com.projectronin.kafka.data.StringHeader
import org.apache.kafka.common.header.internals.RecordHeaders
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertNotNull
import org.junit.jupiter.api.Test

class WrapperDeserializerTest {
    private data class Stuff(val id: String)
    @Test
    fun `deserialize with complete headers`() {
        val deserializer = WrapperDeserializer<Stuff>()
        deserializer.configure(
            mutableMapOf(
                RONIN_DESERIALIZATION_TYPES_CONFIG to "stuff:com.projectronin.kafka.serde.wrapper.WrapperDeserializerTest\$Stuff"
            ),
            false
        )
        val headers = RecordHeaders(
            mutableListOf(
                StringHeader(RoninWrapper.Headers.wrapperVersion, "1"),
                StringHeader(RoninWrapper.Headers.sourceService, "assets"),
                StringHeader(RoninWrapper.Headers.tenantId, "apposnd"),
                StringHeader(RoninWrapper.Headers.dataType, "stuff"),
            )
        )
        val wrapper = deserializer.deserialize("topic", headers, "{\"id\":\"3\"}".encodeToByteArray())

        assertNotNull(wrapper)
        assertEquals("1", wrapper.wrapperVersion)
        assertEquals("assets", wrapper.sourceService)
        assertEquals("apposnd", wrapper.tenantId)
        assertEquals("stuff", wrapper.dataType)
        assertEquals(Stuff("3"), wrapper.data)
    }
}
