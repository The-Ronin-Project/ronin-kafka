package com.projectronin.kafka.work.interfaces

import com.projectronin.kafka.data.RoninEvent
import org.apache.kafka.clients.consumer.ConsumerRecord

interface EventParser {

    fun parseEvent(record: ConsumerRecord<String, ByteArray>): RoninEvent<*>?
}
