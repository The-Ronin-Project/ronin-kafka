package com.projectronin.kafka.serde.wrapper

import com.projectronin.kafka.data.RoninWrapper
import org.apache.kafka.common.errors.SerializationException
import org.apache.kafka.common.header.Headers
import org.apache.kafka.common.header.internals.RecordHeaders
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertNull
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertThrows

class SerializerTest {
    private data class Stuff(val id: String)

    @Test
    fun `validate serialization`() {
        val originalEvent = RoninWrapper(
            wrapperVersion = "1", sourceService = "assets", tenantId = "apposnd", dataType = "stuff", data = Stuff("3")
        )

        val serializer: Serializer<Stuff> = Serializer()
        val serializedHeaders = RecordHeaders()
        val serializedBytes = serializer.serialize("topic", serializedHeaders, originalEvent)

        assertEquals("1", serializedHeaders.getString("ronin_wrapper_version"))
        assertEquals("assets", serializedHeaders.getString("ronin_source_service"))
        assertEquals("apposnd", serializedHeaders.getString("ronin_tenant_id"))
        assertEquals("stuff", serializedHeaders.getString("ronin_data_type"))

        assertEquals("{\"id\":\"3\"}", serializedBytes?.decodeToString())
    }

    @Test
    fun `null serializes to null `() {
        val serializer: Serializer<Stuff> = Serializer()
        assertNull(serializer.serialize("topic", RecordHeaders(), null))
    }

    @Test
    fun `no headers`() {
        val originalEvent = RoninWrapper(
            wrapperVersion = "1", sourceService = "assets", tenantId = "apposnd", dataType = "stuff", data = Stuff("3")
        )
        val serializer: Serializer<Stuff> = Serializer()
        assertThrows<SerializationException> { serializer.serialize("topic", null, originalEvent) }
    }

    private fun Headers.getString(key: String) = lastHeader(key).value().decodeToString()
}
