package com.projectronin.kafka.exceptions

import com.projectronin.kafka.data.RoninEvent
import org.apache.kafka.clients.consumer.ConsumerRecord

/**
 * Allows consumers of [com.projectronin.kafka.RoninConsumer] to receive callbacks on exceptions
 */
interface ConsumerExceptionHandler {
    fun recordHandlingException(record: ConsumerRecord<String, *>, t: Throwable)
    fun eventProcessingException(events: List<RoninEvent<*>>, t: Throwable)
    fun pollException(t: Throwable)
    fun deserializationException(t: Throwable)
}
