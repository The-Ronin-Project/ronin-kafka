package com.projectronin.kafka.strawman.consumer

import com.fasterxml.jackson.databind.ObjectMapper
import com.projectronin.kafka.data.KafkaHeaders
import com.projectronin.kafka.data.RoninEvent
import com.projectronin.kafka.strawman.producer.RoninEventHeaders
import org.apache.kafka.common.header.Headers
import org.apache.kafka.common.serialization.Deserializer
import kotlin.reflect.KClass

interface JsonDeserializerTypeResolver<T : Any> {
    fun resolveType(topic: String, type: String): KClass<T>?
}

interface DeserializationErrorHandler<T> {
    enum class ErrorType { MISSING_EVENT_TYPE, DESERIALIZTION_FAILURE, EMPTY_BODY, TYPE_RESOLUTION_ERROR }
    // TODO: what should thsi return? in theory an error handler could figure out some way to turn the data into a T...
    // so maybe it should return `T?` instead of void ?
    fun handleError(erorrType: ErrorType, topic: String, headers: Headers, data: ByteArray?, cause: Exception?)
}

class LoggingDeserializationErrorHandler<T : Any?> : DeserializationErrorHandler<T> {
    override fun handleError(
        erorrType: DeserializationErrorHandler.ErrorType,
        topic: String,
        headers: Headers,
        data: ByteArray?,
        cause: Exception?
    ) {
        println("presumably do some real logging here")
    }
}

class SingleTypeResolver<T : Any>(private val type: KClass<T>) : JsonDeserializerTypeResolver<T> {
    override fun resolveType(topic: String, type: String): KClass<T> = this.type
}

class TopicTypeResolver(
    private val typesByTopic: Map<String, KClass<Any>>
) : JsonDeserializerTypeResolver<Any> {
    override fun resolveType(topic: String, type: String): KClass<Any>? = typesByTopic[topic]
}

class RoninJsonDeserializer<T : Any>(
    private val objectMapper: ObjectMapper,
    private val typeResolver: JsonDeserializerTypeResolver<T>,
    private val deserializationErrorHandler: DeserializationErrorHandler<T>,
) : Deserializer<T> {
    override fun configure(configs: Map<String?, *>?, isKey: Boolean) {
        // TODO: build up the TypeMapper, decide what object mapper to use, etc from config if provided
    }

    override fun deserialize(topic: String, data: ByteArray?) = throw IllegalStateException("headers must be provided")

    override fun deserialize(topic: String, headers: Headers, data: ByteArray?): T? {
        if (data == null) {
            deserializationErrorHandler.handleError(DeserializationErrorHandler.ErrorType.EMPTY_BODY, topic, headers, data, null)
            return null
        }

        val typeHeaderValue = headers.find { it.key() == KafkaHeaders.type }?.value()?.decodeToString()
        if (typeHeaderValue == null) {
            deserializationErrorHandler.handleError(DeserializationErrorHandler.ErrorType.MISSING_EVENT_TYPE, topic, headers, data, null)
            return null
        }

        val javaType = typeResolver.resolveType(topic, typeHeaderValue)
        if (javaType == null) {
            deserializationErrorHandler.handleError(DeserializationErrorHandler.ErrorType.TYPE_RESOLUTION_ERROR, topic, headers, data, null)
            return null
        }

        return try {
            objectMapper.readValue(data, javaType.java)
        } catch (e: Exception) {
            deserializationErrorHandler.handleError(DeserializationErrorHandler.ErrorType.TYPE_RESOLUTION_ERROR, topic, headers, data, e)
            null
        }
    }
}

class RoninEventJsonDeserializer<T : Any>(
    private val bodyDeserializer: RoninJsonDeserializer<T>
) : Deserializer<RoninEvent<T?>> {
    override fun deserialize(topic: String?, data: ByteArray?): RoninEvent<T?> = throw IllegalStateException("headers must be provided")

    override fun deserialize(topic: String, headers: Headers, data: ByteArray?): RoninEvent<T?> {
        val body = bodyDeserializer.deserialize(topic, headers, data)
        return RoninEventHeaders.fromHeaders(headers).toEvent(body)
    }
}
