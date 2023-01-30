package com.projectronin.kafka.strawman.consumer

import com.fasterxml.jackson.databind.ObjectMapper
import com.projectronin.kafka.config.MapperFactory
import com.projectronin.kafka.config.RoninConsumerKafkaProperties
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.common.serialization.Deserializer
import org.apache.kafka.common.serialization.StringDeserializer
import kotlin.reflect.KClass

class RoninKafkaJsonDeserializerFactory(
    private val objectMapper: ObjectMapper
) {
    fun <T : Any> createDeserializer(type: KClass<T>, errorHandler: DeserializationErrorHandler<T> = LoggingDeserializationErrorHandler()): Deserializer<T> {
        return RoninKafkaJsonDeserializer<T>(objectMapper, SingleTypeResolver(type), errorHandler)
    }

    fun createDeserializerByTopic(typesByTopic: Map<String, KClass<Any>>, errorHandler: DeserializationErrorHandler<Any> = LoggingDeserializationErrorHandler()): Deserializer<*> {
        return RoninKafkaJsonDeserializer(objectMapper, TopicTypeResolver(typesByTopic), errorHandler)
    }

    fun createDeserializerByTopicAndType(typesByTopicAndType: Map<String, Map<String, KClass<Any>>>, errorHandler: DeserializationErrorHandler<Any> = LoggingDeserializationErrorHandler()): Deserializer<*> =
        TODO()

    fun createDeserializerByType(typesByEventType: Map<String, KClass<Any>>): Deserializer<*> =
        TODO("not sure anyone should actually ever do this")
}

class RoninKafkaConsumerFactory(
    private val config: RoninConsumerKafkaProperties,
    private val deserializerFactory: RoninKafkaJsonDeserializerFactory = RoninKafkaJsonDeserializerFactory(MapperFactory.mapper)
) {
    fun <T : Any> createKafkaConsumer(type: KClass<T>, deserializationErrorHandler: DeserializationErrorHandler<T> = LoggingDeserializationErrorHandler()): KafkaConsumer<String, T> {
        return KafkaConsumer<String, T>(config.properties, StringDeserializer(), deserializerFactory.createDeserializer(type))
    }
}
