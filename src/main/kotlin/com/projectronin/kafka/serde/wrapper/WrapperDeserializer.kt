package com.projectronin.kafka.serde.wrapper

import com.fasterxml.jackson.databind.ObjectMapper
import com.projectronin.kafka.config.MapperFactory
import com.projectronin.kafka.config.RoninConfig.Companion.RONIN_DESERIALIZATION_TYPES_CONFIG
import com.projectronin.kafka.data.RoninWrapper
import com.projectronin.kafka.exceptions.DeserializationException
import com.projectronin.kafka.exceptions.EventHeaderMissing
import com.projectronin.kafka.exceptions.UnknownEventType
import org.apache.kafka.common.header.Headers
import kotlin.reflect.KClass

@Deprecated("Library has been replaced by ronin-common kafka")
class WrapperDeserializer<T> : org.apache.kafka.common.serialization.Deserializer<RoninWrapper<T>> {
    private lateinit var typeMap: Map<String, KClass<*>>
    private val mapper: ObjectMapper = MapperFactory.mapper

    override fun configure(configs: MutableMap<String, *>?, isKey: Boolean) {
        super.configure(configs, isKey)

        val types = configs?.get(RONIN_DESERIALIZATION_TYPES_CONFIG) as String

        typeMap = types.split(",")
            .associate {
                val (left, right) = it.split(":")
                left to Class.forName(right).kotlin
            }
    }

    override fun deserialize(topic: String?, bytes: ByteArray?): RoninWrapper<T> {
        throw DeserializationException("Deserialize method without headers is not supported by this deserializer", Nothing::class)
    }

    override fun deserialize(topic: String?, headers: Headers, bytes: ByteArray?): RoninWrapper<T> {
        val roninHeaders = headers
            .filter { it.value() != null && it.value().isNotEmpty() }
            .associate { it.key() to it.value().decodeToString() }

        roninHeaders
            .keys
            .let {
                val missing = RoninWrapper.Headers.required - it
                if (missing.isNotEmpty()) {
                    throw EventHeaderMissing(missing, topic)
                }
            }

        val type = roninHeaders[RoninWrapper.Headers.dataType]!!
        val valueClass = typeMap[type] ?: throw UnknownEventType(type, topic)

        try {
            @Suppress("UNCHECKED_CAST")
            val data = mapper.readValue(bytes, valueClass.java) as T
            return RoninWrapper(roninHeaders, data)
        } catch (e: Exception) {
            throw DeserializationException(type, valueClass)
        }
    }
}
