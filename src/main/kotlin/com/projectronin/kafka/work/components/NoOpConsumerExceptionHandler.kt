package com.projectronin.kafka.work.components

import com.projectronin.kafka.data.RoninEvent
import com.projectronin.kafka.work.interfaces.RoninConsumerExceptionHandler
import org.apache.kafka.clients.consumer.ConsumerRecord

object NoOpConsumerExceptionHandler : RoninConsumerExceptionHandler {

    override fun handleRecordTransformationException(record: ConsumerRecord<String, ByteArray>, t: Throwable) {
        // literally does nothing as default
    }

    override fun handleEventProcessingException(event: RoninEvent<*>, topic: String, t: Throwable) {
        // literally does nothing as default
    }
}
