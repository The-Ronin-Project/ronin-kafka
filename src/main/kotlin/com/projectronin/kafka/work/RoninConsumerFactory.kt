package com.projectronin.kafka.work

import com.projectronin.kafka.config.MapperFactory
import com.projectronin.kafka.config.RoninConsumerKafkaProperties
import com.projectronin.kafka.work.components.NoOpConsumerExceptionHandler
import com.projectronin.kafka.work.components.RetryEventHandler
import com.projectronin.kafka.work.components.RoninConsumerEventParser
import com.projectronin.kafka.work.interfaces.EventHandler
import com.projectronin.kafka.work.interfaces.EventParser
import com.projectronin.kafka.work.interfaces.RoninConsumerExceptionHandler
import com.projectronin.kafka.work.metrics.MeteredConsumerExceptionHandler
import com.projectronin.kafka.work.metrics.MeteredEventHandler
import com.projectronin.kafka.work.metrics.MeteredEventParser
import com.projectronin.kafka.work.metrics.MeteredKafkaConsumer
import io.micrometer.core.instrument.MeterRegistry
import mu.KotlinLogging
import org.apache.kafka.clients.consumer.Consumer
import org.apache.kafka.clients.consumer.KafkaConsumer
import kotlin.reflect.KClass

// note: this could be a builder as well
object RoninConsumerFactory {
    private val logger = KotlinLogging.logger {}

    fun createRoninConsumer(
        topics: List<String>,
        typeMap: Map<String, KClass<*>>,
        kafkaProperties: RoninConsumerKafkaProperties,
        eventHandler: EventHandler,
        exceptionHandler: RoninConsumerExceptionHandler = NoOpConsumerExceptionHandler,
        meterRegistry: MeterRegistry? = null
    ): RoninConsumer {

        val kafkaConsumer = createKafkaConsumer(topics, kafkaProperties, meterRegistry)

        val trueExceptionHandler =
            if (meterRegistry != null) MeteredConsumerExceptionHandler(exceptionHandler, meterRegistry)
            else exceptionHandler

        val transientRetries: Int =
            kafkaProperties.properties.getValue("ronin.handler.transient.retries").toString().toInt()
        val retryEventHandler: EventHandler = RetryEventHandler(
            eventHandler = eventHandler,
            transientRetries = transientRetries,
            exceptionHandler = trueExceptionHandler
        )

        val eventParser = createEventParser(typeMap, trueExceptionHandler)
        if (meterRegistry != null) {
            return RoninConsumer(
                kafkaConsumer,
                MeteredEventParser(eventParser, meterRegistry),
                MeteredEventHandler(retryEventHandler, meterRegistry)
            )
        }
        return RoninConsumer(kafkaConsumer, eventParser, retryEventHandler)
    }

    private fun createKafkaConsumer(
        topics: List<String>,
        kafkaProperties:
            RoninConsumerKafkaProperties,
        meterRegistry: MeterRegistry?
    ): Consumer<String, ByteArray> {
        val kafkaConsumer = KafkaConsumer<String, ByteArray>(kafkaProperties.properties).apply {
            logger.info { "subscribing to topics `$topics`" }
            this.subscribe(topics)
        }
        if (meterRegistry != null) {
            return MeteredKafkaConsumer(kafkaConsumer, meterRegistry)
        }
        return kafkaConsumer
    }

    private fun createEventParser(
        typeMap: Map<String, KClass<*>>,
        consumerExceptionHandler: RoninConsumerExceptionHandler
    ): EventParser {
        return RoninConsumerEventParser(
            typeMap = typeMap,
            mapper = MapperFactory.mapper,
            consumerExceptionHandler = consumerExceptionHandler
        )
    }
}
