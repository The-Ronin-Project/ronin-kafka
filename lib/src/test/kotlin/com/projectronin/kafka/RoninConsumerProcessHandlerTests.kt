package com.projectronin.kafka

import com.projectronin.kafka.data.KafkaHeaders
import com.projectronin.kafka.data.RoninEvent
import com.projectronin.kafka.data.RoninEventResult
import com.projectronin.kafka.exceptions.ConsumerExceptionHandler
import com.projectronin.kafka.exceptions.TransientRetriesExhausted
import io.mockk.every
import io.mockk.mockk
import io.mockk.verify
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.clients.consumer.OffsetAndMetadata
import org.apache.kafka.common.TopicPartition
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test
import java.time.Duration

/**
 * These are just testing the return values of teh handler function and that they're correctly dealt with
 */
class RoninConsumerProcessHandlerTests {
    data class Stuff(override val id: String) : RoninEvent.Data<String>
    class Header(private val key: String, private val value: String) : org.apache.kafka.common.header.Header {
        override fun key() = key
        override fun value() = value.toByteArray()
    }

    private val kafkaConsumer = mockk<KafkaConsumer<String, String>> {
        every { subscribe(listOf("topic.1", "topic.2")) } returns Unit
        every { commitSync(any<Map<TopicPartition, OffsetAndMetadata>>()) } returns Unit
    }
    private val exceptionHandler: ConsumerExceptionHandler = mockk {}
    private val roninConsumer = RoninConsumer(
        listOf("topic.1", "topic.2"),
        mapOf("stuff" to Stuff::class),
        kafkaConsumer = kafkaConsumer,
        exceptionHandler = exceptionHandler
    )

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
    fun `ACK commits with no exceptions`() {
        every { kafkaConsumer.poll(any<Duration>()) } returns mockk {
            val records = mutableListOf(
                mockRecord("stuff", "key1.1", "{\"id\": \"one\"}"),
                mockRecord("stuff", "last", "{\"id\": \"two\"}"),
            )
            every { iterator() } returns records.iterator()
        }

        roninConsumer.process {
            if (it.subject == "last") {
                roninConsumer.stop()
            }
            RoninEventResult.ACK
        }

        verify(exactly = 2) { kafkaConsumer.commitSync(mapOf(TopicPartition("topic", 1) to OffsetAndMetadata(43))) }
        verify(exactly = 0) { exceptionHandler.recordHandlingException(any(), any()) }
        verify(exactly = 0) { exceptionHandler.eventProcessingException(any(), any()) }
    }

    @Test
    fun `single TRANSIENT_FAILURE retries twice, commits, and no exceptions`() {
        every { kafkaConsumer.poll(any<Duration>()) } returns mockk {
            val records = mutableListOf(
                mockRecord("stuff", "key1.1", "{\"id\": \"one\"}"),
                mockRecord("stuff", "last", "{\"id\": \"two\"}"),
            )
            every { iterator() } returns records.iterator()
        }

        var counter = 0
        roninConsumer.process {
            if (it.subject == "last") {
                roninConsumer.stop()
            }
            counter++
            // return TRANSIENT_FAILURE for the first call of `key1.1`, ACK for everything else
            if (it.subject == "last" || counter > 1) {
                RoninEventResult.ACK
            } else {
                RoninEventResult.TRANSIENT_FAILURE
            }
        }
        assertEquals(3, counter)

        verify(exactly = 2) { kafkaConsumer.commitSync(mapOf(TopicPartition("topic", 1) to OffsetAndMetadata(43))) }
        verify(exactly = 0) { exceptionHandler.recordHandlingException(any(), any()) }
        verify(exactly = 0) { exceptionHandler.eventProcessingException(any(), any()) }
    }

    @Test
    fun `TRANSIENT_FAILURE exhausts retries, calls exception handler, commits`() {
        every { kafkaConsumer.poll(any<Duration>()) } returns mockk {
            val records = mutableListOf(
                mockRecord("stuff", "key1.1", "{\"id\": \"one\"}"),
            )
            every { iterator() } returns records.iterator()
        }
        every { exceptionHandler.eventProcessingException(any(), any()) } returns Unit

        var counter = 0
        roninConsumer.process {
            roninConsumer.stop()
            counter++
            RoninEventResult.TRANSIENT_FAILURE
        }
        assertEquals(4, counter)

        verify(exactly = 1) { kafkaConsumer.commitSync(mapOf(TopicPartition("topic", 1) to OffsetAndMetadata(43))) }
        verify(exactly = 0) { exceptionHandler.recordHandlingException(any(), any()) }
        verify(exactly = 1) { exceptionHandler.eventProcessingException(any(), any<TransientRetriesExhausted>()) }
    }

    @Test
    fun `PERMANENT_FAILURE calls exception handler, exits`() {
        every { kafkaConsumer.poll(any<Duration>()) } returns mockk {
            val records = mutableListOf(
                mockRecord("stuff", "key1.1", "{\"id\": \"one\"}"),
                mockRecord("stuff", "last", "{\"id\": \"two\"}"),
            )
            every { iterator() } returns records.iterator()
        }
        every { exceptionHandler.eventProcessingException(any(), any()) } returns Unit

        var counter = 0
        roninConsumer.process {
            counter++
            if (it.subject == "last") {
                RoninEventResult.ACK
            } else {
                RoninEventResult.PERMANENT_FAILURE
            }
        }
        assertEquals(1, counter)
        // TODO: add status check

        verify(exactly = 0) { kafkaConsumer.commitSync(any<Map<TopicPartition, OffsetAndMetadata>>()) }
        verify(exactly = 0) { exceptionHandler.recordHandlingException(any(), any()) }
        verify(exactly = 0) { exceptionHandler.eventProcessingException(any(), any()) }
    }

    @Test
    fun `unhandled exception calls exception handler, exits`() {
        every { kafkaConsumer.poll(any<Duration>()) } returns mockk {
            val records = mutableListOf(
                mockRecord("stuff", "key1.1", "{\"id\": \"one\"}"),
                mockRecord("stuff", "last", "{\"id\": \"two\"}"),
            )
            every { iterator() } returns records.iterator()
        }
        every { exceptionHandler.eventProcessingException(any(), any()) } returns Unit

        var counter = 0
        roninConsumer.process {
            counter++
            if (it.subject == "last") {
                RoninEventResult.ACK
            } else {
                throw RuntimeException("kaboom")
            }
        }
        assertEquals(1, counter)
        // TODO: add status check

        verify(exactly = 0) { kafkaConsumer.commitSync(any<Map<TopicPartition, OffsetAndMetadata>>()) }
        verify(exactly = 0) { exceptionHandler.recordHandlingException(any(), any()) }
        verify(exactly = 1) { exceptionHandler.eventProcessingException(any(), any()) }
    }
}
