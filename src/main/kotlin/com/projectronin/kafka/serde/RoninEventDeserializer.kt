package com.projectronin.kafka.serde

import com.fasterxml.jackson.databind.ObjectMapper
import com.projectronin.kafka.config.MapperFactory
import com.projectronin.kafka.data.KafkaHeaders
import com.projectronin.kafka.data.RoninEvent
import com.projectronin.kafka.exceptions.DeserializationException
import com.projectronin.kafka.exceptions.EventHeaderMissing
import com.projectronin.kafka.exceptions.UnknownEventType
import org.apache.kafka.common.header.Headers
import org.apache.kafka.common.serialization.Deserializer
import kotlin.reflect.KClass

class RoninEventDeserializer<T> : Deserializer<RoninEvent<T>> {
    private lateinit var typeMap: Map<String, KClass<*>>
    private val mapper: ObjectMapper = MapperFactory.mapper

    override fun configure(configs: MutableMap<String, *>?, isKey: Boolean) {
        super.configure(configs, isKey)

        val types = configs?.get(RONIN_EVENT_DESERIALIZATION_TYPES_CONFIG) as String

        typeMap = types.split(",")
            .associate {
                val (left, right) = it.split(":")
                left to Class.forName(right).kotlin
            }
    }

    override fun deserialize(topic: String?, bytes: ByteArray?): RoninEvent<T> {
        TODO("Not yet implemented")
    }

    override fun deserialize(topic: String?, headers: Headers, bytes: ByteArray?): RoninEvent<T> {
        val roninHeaders = headers
            .filter { it.value() != null && it.value().isNotEmpty() }
            .associate { it.key() to it.value().decodeToString() }

        roninHeaders
            .keys
            .let { KafkaHeaders.required - it }
            .let {
                if (it.isNotEmpty()) {
                    throw EventHeaderMissing(topic, it)
                }
            }

        val type = roninHeaders[KafkaHeaders.type]!!
        val valueClass = typeMap[type] ?: throw UnknownEventType(topic, roninHeaders[KafkaHeaders.id].toString(), type)

        try {
            @Suppress("UNCHECKED_CAST")
            val data = mapper.readValue(bytes, valueClass.java) as T
            return RoninEvent(roninHeaders, data)
        } catch (e: Exception) {
            throw DeserializationException(type, valueClass)
        }
    }

    companion object {
        const val RONIN_EVENT_DESERIALIZATION_TYPES_CONFIG = "json.deserializer.types"
    }
}
