package com.projectronin.kafka

import com.projectronin.kafka.data.KafkaHeaders
import com.projectronin.kafka.data.RoninEvent
import io.mockk.every
import io.mockk.mockk
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.clients.consumer.ConsumerRecords

object MockUtils {
    class Header(private val key: String, private val value: String) : org.apache.kafka.common.header.Header {
        override fun key() = key
        override fun value() = value.toByteArray()
    }

    fun record(type: String, key: String, value: RoninEvent<*>): ConsumerRecord<String, RoninEvent<*>> {
        return mockk {
            every { topic() } returns "topic"
            every { partition() } returns 1
            every { offset() } returns 42
            every { key() } returns key
            every { value() } returns value
            every { headers() } returns mockk {
                val h = mutableListOf(
                    Header(KafkaHeaders.id, value.id),
                    Header(KafkaHeaders.time, value.time.toString()),
                    Header(KafkaHeaders.specVersion, value.specVersion),
                    Header(KafkaHeaders.dataSchema, value.dataSchema),
                    Header(KafkaHeaders.contentType, value.dataContentType),
                    Header(KafkaHeaders.source, value.source),
                    Header(KafkaHeaders.type, value.type),
                )
                every { iterator() } returns h.iterator()
            }
            every { timestamp() } returns 1659999750
        }
    }

    fun records(vararg records: ConsumerRecord<String, RoninEvent<*>>): ConsumerRecords<String, RoninEvent<*>> = mockk {
        mutableListOf(*records).let {
            every { iterator() } returns it.iterator()
            every { count() } returns records.size
        }
    }
}
