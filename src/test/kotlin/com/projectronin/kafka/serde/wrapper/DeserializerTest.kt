package com.projectronin.kafka.serde.wrapper

import com.projectronin.kafka.data.RoninWrapper
import com.projectronin.kafka.data.StringHeader
import org.apache.kafka.common.header.internals.RecordHeaders
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertNotNull
import org.junit.jupiter.api.Test

class DeserializerTest {
    private data class Stuff(val id: String)
    @Test
    fun `deserialize with complete headers`() {
        val deserializer = Deserializer<Stuff>()
        deserializer.configure(
            mutableMapOf(
                Deserializer.RONIN_WRAPPER_DESERIALIZATION_TYPES_CONFIG to
                    "stuff:com.projectronin.kafka.serde.wrapper.DeserializerTest\$Stuff"
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
