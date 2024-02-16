package com.projectronin.kafka.exceptions

import com.projectronin.kafka.data.RoninEvent
import org.apache.kafka.clients.consumer.ConsumerRecord

/**
 * Allows consumers of [com.projectronin.kafka.RoninConsumer] to receive callbacks on exceptions
 */
@Deprecated("Library has been replaced by ronin-common kafka")
interface ConsumerExceptionHandler {
    fun recordHandlingException(record: ConsumerRecord<String, ByteArray>, t: Throwable)
    fun eventProcessingException(events: List<RoninEvent<*>>, t: Throwable)
}
