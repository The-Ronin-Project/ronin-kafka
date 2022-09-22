package com.projectronin.kafka

import com.projectronin.kafka.data.KafkaHeaders
import com.projectronin.kafka.data.RoninEvent
import com.projectronin.kafka.data.RoninEventResult
import io.mockk.every
import io.mockk.mockk
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.clients.consumer.OffsetAndMetadata
import org.apache.kafka.common.TopicPartition
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test
import java.time.Duration

class RoninConsumerStatusTests {
    data class Stuff(override val id: String) : RoninEvent.Data<String>
    class Header(private val key: String, private val value: String) : org.apache.kafka.common.header.Header {
        override fun key() = key
        override fun value() = value.toByteArray()
    }

    private val kafkaConsumer = mockk<KafkaConsumer<String, String>> {
        every { subscribe(listOf("topic.1", "topic.2")) } returns Unit
        every { commitSync(any<Map<TopicPartition, OffsetAndMetadata>>()) } returns Unit
    }
    private val roninConsumer = RoninConsumer(
        listOf("topic.1", "topic.2"),
        mapOf("stuff" to Stuff::class),
        kafkaConsumer = kafkaConsumer
    )

    // TODO: this can be removed when #6 is merged down
    private fun mockRecord(type: String, key: String, value: String): ConsumerRecord<String, String> {
        return mockk {
            every { topic() } returns "topic"
            every { partition() } returns 1
            every { offset() } returns 42
            every { key() } returns key
            every { value() } returns value
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
                every { iterator() } returns h.iterator()
            }
        }
    }

    @Test
    fun initialized() {
        assertEquals(RoninConsumer.Status.INITIALIZED, roninConsumer.status())
    }

    @Test
    fun `processing and stopped`() {
        every { kafkaConsumer.poll(any<Duration>()) } returns mockk {
            val records = mutableListOf(
                mockRecord("stuff", "key1.1", "{\"id\": \"one\"}"),
                mockRecord("stuff", "key1.2", "{\"id\": \"two\"}"),
                mockRecord("stuff", "last", "{\"id\": \"three\"}"),
            )
            every { iterator() } returns records.iterator()
        }

        var inProcessstatus: RoninConsumer.Status = RoninConsumer.Status.INITIALIZED
        roninConsumer.process {
            inProcessstatus = roninConsumer.status()
            roninConsumer.stop()
            RoninEventResult.ACK
        }
        assertEquals(RoninConsumer.Status.PROCESSING, inProcessstatus)
        assertEquals(RoninConsumer.Status.STOPPED, roninConsumer.status())
    }
}
