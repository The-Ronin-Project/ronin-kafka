package com.projectronin.kafka.serde

import com.fasterxml.jackson.databind.ObjectMapper
import com.projectronin.kafka.config.MapperFactory
import com.projectronin.kafka.config.RoninConfig.Companion.RONIN_DESERIALIZATION_TOPICS_CONFIG
import com.projectronin.kafka.config.RoninConfig.Companion.RONIN_DESERIALIZATION_TYPES_CONFIG
import com.projectronin.kafka.data.KafkaHeaders
import com.projectronin.kafka.data.RoninEvent
import com.projectronin.kafka.exceptions.DeserializationException
import com.projectronin.kafka.exceptions.EventHeaderMissing
import com.projectronin.kafka.exceptions.UnknownEventType
import org.apache.kafka.common.header.Headers
import org.apache.kafka.common.serialization.Deserializer
import kotlin.reflect.KClass

class RoninEventDeserializer<T> : Deserializer<RoninEvent<T>> {
    private lateinit var topicMap: Map<String, KClass<*>>
    private lateinit var typeMap: Map<String, KClass<*>>
    private val mapper: ObjectMapper = MapperFactory.mapper

    override fun configure(configs: MutableMap<String, *>, isKey: Boolean) {
        super.configure(configs, isKey)

        val topics = configs[RONIN_DESERIALIZATION_TOPICS_CONFIG] as String?
        val types = configs[RONIN_DESERIALIZATION_TYPES_CONFIG] as String?

        topicMap = makeTypeMap(topics)
        typeMap = makeTypeMap(types)
    }

    override fun deserialize(topic: String?, bytes: ByteArray?): RoninEvent<T> {
        throw Exception("Deserialize method without headers is not supported by this deserializer")
    }

    override fun deserialize(topic: String, headers: Headers, bytes: ByteArray): RoninEvent<T> {
        val roninHeaders = headers
            .filter { it.value() != null && it.value().isNotEmpty() }
            .associate { it.key() to it.value().decodeToString() }

        roninHeaders
            .keys
            .let {
                val missing = KafkaHeaders.required - it
                if (missing.isNotEmpty()) {
                    throw EventHeaderMissing(missing, topic)
                }
            }

        val type = roninHeaders[KafkaHeaders.type]!!
        val valueClass = topicMap[topic] ?: typeMap[type] ?: throw UnknownEventType(roninHeaders[KafkaHeaders.id].toString(), type, topic)

        try {
            @Suppress("UNCHECKED_CAST")
            val data = mapper.readValue(bytes, valueClass.java) as T
            return RoninEvent(roninHeaders, data)
        } catch (e: Exception) {
            throw DeserializationException(type, valueClass)
        }
    }

    private fun makeTypeMap(config: String?): Map<String, KClass<out Any>> =
        config?.split(",")?.associate {
            val (left, right) = it.split(":")
            left.trim() to Class.forName(right.trim()).kotlin
        } ?: emptyMap()
}
