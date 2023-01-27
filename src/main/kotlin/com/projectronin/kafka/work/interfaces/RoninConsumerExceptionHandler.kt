package com.projectronin.kafka.work.interfaces

import com.projectronin.kafka.data.RoninEvent
import org.apache.kafka.clients.consumer.ConsumerRecord

interface RoninConsumerExceptionHandler {

    fun handleRecordTransformationException(record: ConsumerRecord<String, ByteArray>, t: Throwable)
    fun handleEventProcessingException(event: RoninEvent<*>, topic: String, t: Throwable)
}
