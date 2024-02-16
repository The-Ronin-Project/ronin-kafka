package com.projectronin.kafka.handlers

import mu.KotlinLogging
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.streams.errors.ProductionExceptionHandler
import java.lang.Exception

@Deprecated("Library has been replaced by ronin-common kafka")
class LogAndContinueProductionExceptionHandler : ProductionExceptionHandler {
    private val logger = KotlinLogging.logger {}

    override fun configure(configs: MutableMap<String, *>?) {}

    override fun handle(
        record: ProducerRecord<ByteArray, ByteArray>?,
        exception: Exception?
    ): ProductionExceptionHandler.ProductionExceptionHandlerResponse {
        logger.error("There was an exception (${exception?.message}) writing the message to Kafka from the Stream.")
        return ProductionExceptionHandler.ProductionExceptionHandlerResponse.CONTINUE
    }
}
