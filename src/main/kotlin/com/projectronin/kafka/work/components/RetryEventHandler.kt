package com.projectronin.kafka.work.components

import com.projectronin.kafka.data.RoninEvent
import com.projectronin.kafka.data.RoninEventResult
import com.projectronin.kafka.exceptions.TransientRetriesExhausted
import com.projectronin.kafka.work.interfaces.EventHandler
import com.projectronin.kafka.work.interfaces.RoninConsumerExceptionHandler
import mu.KotlinLogging

class RetryEventHandler(
    private val eventHandler: EventHandler,
    private val transientRetries: Int,
    private val exceptionHandler: RoninConsumerExceptionHandler = NoOpConsumerExceptionHandler,
) : EventHandler {

    private val logger = KotlinLogging.logger {}

    override fun handle(event: RoninEvent<*>, topic: String): RoninEventResult {
        try {
            for (attemptNumber in 0..transientRetries) {
                logger.debug { "processing event id: `${event.id}` subject: `${event.subject}` attempt $attemptNumber" }
                val handlerResult = eventHandler.handle(event, topic)

                when (handlerResult) {
                    RoninEventResult.ACK -> logger.debug { "handler acknowledged" }
                    RoninEventResult.TRANSIENT_FAILURE -> logger.info { "Transient failure reported on attempt $attemptNumber" }
                    RoninEventResult.PERMANENT_FAILURE -> logger.error { "Permanent failure reported on attempt $attemptNumber" }
                }

                if (handlerResult != RoninEventResult.TRANSIENT_FAILURE) {
                    return handlerResult
                }
            }

            logger.warn { "max transient retries reached for event $event" }
            reportException(event, topic, TransientRetriesExhausted(1 + transientRetries))
            return RoninEventResult.TRANSIENT_FAILURE
        } catch (t: Throwable) {
            // unknown/unhandled exception - just log it and signal permanent failure
            logger.error(t) { "Unhandled exception processing $event: ${t.message}" }
            try {
                reportException(event, topic, t)
            } catch (reportException: Throwable) {
                logger.error(t) { "Unhandled exception while reporting exception!!, ${t.message}" }
            }
            return RoninEventResult.PERMANENT_FAILURE
        }
    }

    private fun reportException(event: RoninEvent<*>, topic: String, t: Throwable) {
        exceptionHandler.handleEventProcessingException(event, topic, t)
    }
}
