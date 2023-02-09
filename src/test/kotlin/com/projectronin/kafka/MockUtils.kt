package com.projectronin.kafka

import com.projectronin.kafka.data.KafkaHeaders
import io.mockk.every
import io.mockk.mockk
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.clients.consumer.ConsumerRecords

object MockUtils {
    class Header(private val key: String, private val value: String) : org.apache.kafka.common.header.Header {
        override fun key() = key
        override fun value() = value.toByteArray()
    }

    fun record(type: String, key: String, value: String, subject: String? = null): ConsumerRecord<String, ByteArray> {
        return mockk {
            every { topic() } returns "topic"
            every { partition() } returns 1
            every { offset() } returns 42
            every { key() } returns key
            every { value() } returns value.toByteArray()
            every { headers() } returns mockk {
                val h = mutableListOf(
                    Header(KafkaHeaders.id, "1"),
                    Header(KafkaHeaders.time, "2022-08-08T23:06:40Z"),
                    Header(KafkaHeaders.specVersion, "3"),
                    Header(KafkaHeaders.dataSchema, "4"),
                    Header(KafkaHeaders.contentType, "5"),
                    Header(KafkaHeaders.source, "6"),
                    Header(KafkaHeaders.type, type),
                )
                if (subject != null)
                    h.add(Header(KafkaHeaders.subject, subject))

                every { iterator() } returns h.iterator()
            }
            every { timestamp() } returns 1659999750
        }
    }

    fun records(vararg records: ConsumerRecord<String, ByteArray>): ConsumerRecords<String, ByteArray> = mockk {
        mutableListOf(*records).let {
            every { iterator() } returns it.iterator()
            every { count() } returns records.size
        }
    }
}
