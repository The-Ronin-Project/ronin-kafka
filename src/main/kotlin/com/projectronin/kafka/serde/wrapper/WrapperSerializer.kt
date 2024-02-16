package com.projectronin.kafka.serde.wrapper

import com.fasterxml.jackson.databind.ObjectMapper
import com.projectronin.kafka.config.MapperFactory
import com.projectronin.kafka.data.RoninWrapper
import com.projectronin.kafka.data.RoninWrapper.Headers
import com.projectronin.kafka.data.StringHeader
import org.apache.kafka.common.errors.SerializationException
import org.apache.kafka.common.serialization.Serializer

@Deprecated("Library has been replaced by ronin-common kafka")
class WrapperSerializer<T> : Serializer<RoninWrapper<T>> {
    private val mapper: ObjectMapper = MapperFactory.mapper

    override fun serialize(topic: String?, data: RoninWrapper<T>?): ByteArray {
        throw SerializationException(
            "Serialize method without headers is not a " +
                "valid means to deserialize a RoninWrapper"
        )
    }

    override fun serialize(
        topic: String?,
        headers: org.apache.kafka.common.header.Headers?,
        wrapper: RoninWrapper<T>?
    ): ByteArray? {
        if (headers == null)
            throw SerializationException(
                "Headers are required to deserialize a RoninWrapper, " +
                    "but the headers were not initialized."
            )
        if (wrapper == null)
            return null

        headers.add(StringHeader(Headers.wrapperVersion, wrapper.wrapperVersion))
        headers.add(StringHeader(Headers.sourceService, wrapper.sourceService))
        headers.add(StringHeader(Headers.tenantId, wrapper.tenantId))
        headers.add(StringHeader(Headers.dataType, wrapper.dataType))

        return mapper.writeValueAsBytes(wrapper.data)
    }
}
