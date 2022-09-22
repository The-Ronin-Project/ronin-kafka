package com.projectronin.kafka

import com.fasterxml.jackson.databind.JsonMappingException
import com.projectronin.kafka.data.KafkaHeaders
import com.projectronin.kafka.data.RoninEvent
import com.projectronin.kafka.data.RoninEventResult
import com.projectronin.kafka.exceptions.ConsumerExceptionHandler
import io.mockk.every
import io.mockk.mockk
import io.mockk.verify
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.clients.consumer.OffsetAndMetadata
import org.apache.kafka.common.TopicPartition
import org.hamcrest.MatcherAssert.assertThat
import org.hamcrest.Matchers.contains
import org.hamcrest.Matchers.instanceOf
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test
import java.time.Duration
import java.time.Instant

class RoninConsumerProcessTests {
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
    private val fixedInstant = Instant.ofEpochSecond(1660000000)

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
    fun `receives events`() {
        every { kafkaConsumer.poll(any<Duration>()) } returns mockk {
            val records = mutableListOf(
                mockRecord("stuff", "key1.1", "{\"id\": \"one\"}"),
                mockRecord("stuff", "key1.2", "{\"id\": \"two\"}"),
                mockRecord("stuff", "last", "{\"id\": \"three\"}"),
            )
            every { iterator() } returns records.iterator()
        }

        val processed = mutableListOf<RoninEvent<*>>()
        roninConsumer.process { e ->
            processed.add(e)
            if (e.subject == "last") {
                roninConsumer.stop()
            }
            RoninEventResult.ACK
        }

        assertThat(
            processed,
            contains(
                RoninEvent("1", fixedInstant, "3", "4", "5", "6", "stuff", "key1.1", Stuff("one")),
                RoninEvent("1", fixedInstant, "3", "4", "5", "6", "stuff", "key1.2", Stuff("two")),
                RoninEvent("1", fixedInstant, "3", "4", "5", "6", "stuff", "last", Stuff("three")),
            )
        )
        verify(exactly = 3) { kafkaConsumer.commitSync(mapOf(TopicPartition("topic", 1) to OffsetAndMetadata(43))) }
    }

    @Test
    fun `unknown type is skipped`() {
        every { kafkaConsumer.poll(any<Duration>()) } returns mockk {
            val records = mutableListOf(
                mockRecord("nope", "key1.1", "{\"id\": \"one\"}"),
                mockRecord("stuff", "last", "{\"id\": \"three\"}"),
            )
            every { iterator() } returns records.iterator()
        }

        val processed = mutableListOf<RoninEvent<*>>()
        roninConsumer.process { e ->
            processed.add(e)
            if (e.subject == "last") {
                roninConsumer.stop()
            }
            RoninEventResult.ACK
        }

        assertThat(
            processed,
            contains(
                RoninEvent("1", fixedInstant, "3", "4", "5", "6", "stuff", "last", Stuff("three")),
            )
        )
        verify(exactly = 2) { kafkaConsumer.commitSync(any<Map<TopicPartition, OffsetAndMetadata>>()) }
    }

    @Test
    fun `record with missing headers is skipped`() {
        every { kafkaConsumer.poll(any<Duration>()) } returns mockk {
            val records = mutableListOf(
                mockk {
                    every { topic() } returns "topic.1"
                    every { partition() } returns 1
                    every { key() } returns "key.1"
                    every { value() } returns "{\"id\": \"one\"}"
                    every { headers() } returns mockk {
                        val h = mutableListOf(
                            Header(KafkaHeaders.id, "1"),
                        )
                        every { iterator() } returns h.iterator()
                    }
                    every { offset() } returns 42
                },
                mockRecord("stuff", "last", "{\"id\": \"three\"}"),
            )
            every { iterator() } returns records.iterator()
        }

        val processed = mutableListOf<RoninEvent<*>>()
        roninConsumer.process { e ->
            processed.add(e)
            if (e.subject == "last") {
                roninConsumer.stop()
            }
            RoninEventResult.ACK
        }

        assertThat(
            processed,
            contains(
                RoninEvent("1", fixedInstant, "3", "4", "5", "6", "stuff", "last", Stuff("three")),
            )
        )
        verify(exactly = 2) { kafkaConsumer.commitSync(any<Map<TopicPartition, OffsetAndMetadata>>()) }
    }

    @Test
    fun `deserialization failure without error handler`() {
        every { kafkaConsumer.poll(any<Duration>()) } returns mockk {
            val records = mutableListOf(
                mockRecord("stuff", "key1.2", "{\"nope\": 3}"),
                mockRecord("stuff", "last", "{\"id\": \"three\"}"),
            )
            every { iterator() } returns records.iterator()
        }

        val processed = mutableListOf<RoninEvent<*>>()
        roninConsumer.process { e ->
            processed.add(e)
            if (e.subject == "last") {
                roninConsumer.stop()
            }
            RoninEventResult.ACK
        }

        assertThat(
            processed,
            contains(
                RoninEvent("1", fixedInstant, "3", "4", "5", "6", "stuff", "last", Stuff("three")),
            )
        )
        verify(exactly = 2) { kafkaConsumer.commitSync(any<Map<TopicPartition, OffsetAndMetadata>>()) }
    }

    @Test
    fun `deserialization failure with error handler`() {
        val exceptionRecords = mutableListOf<ConsumerRecord<String, String>>()
        val exceptions = mutableListOf<Throwable>()
        val roninConsumer = RoninConsumer(
            listOf("topic.1", "topic.2"),
            mapOf("stuff" to Stuff::class),
            kafkaConsumer = kafkaConsumer,
            exceptionHandler = object : ConsumerExceptionHandler {
                override fun recordHandlingException(record: ConsumerRecord<String, String>, t: Throwable) {
                    exceptionRecords.add(record)
                    exceptions.add(t)
                }

                override fun eventProcessingException(events: List<RoninEvent<*>>, t: Throwable) {
                    TODO("Not yet implemented")
                }
            }
        )
        val exceptionRecord = mockRecord("stuff", "key1.2", "{\"nope\": 3}")
        every { kafkaConsumer.poll(any<Duration>()) } returns mockk {
            val records = mutableListOf(
                exceptionRecord,
                mockRecord("stuff", "last", "{\"id\": \"three\"}"),
            )
            every { iterator() } returns records.iterator()
        }

        val processed = mutableListOf<RoninEvent<*>>()
        roninConsumer.process { e ->
            processed.add(e)
            if (e.subject == "last") {
                roninConsumer.stop()
            }
            RoninEventResult.ACK
        }

        assertThat(
            processed,
            contains(
                RoninEvent("1", fixedInstant, "3", "4", "5", "6", "stuff", "last", Stuff("three")),
            )
        )
        verify(exactly = 2) { kafkaConsumer.commitSync(any<Map<TopicPartition, OffsetAndMetadata>>()) }
        assertThat(exceptionRecords, contains(exceptionRecord))
        assertThat(exceptions, contains(instanceOf(JsonMappingException::class.java)))
    }

    @Test
    fun `processing error without error handler`() {
        every { kafkaConsumer.poll(any<Duration>()) } returns mockk {
            val records = mutableListOf(
                mockRecord("stuff", "key1.2", "{\"id\": \"two\"}"),
                mockRecord("stuff", "last", "{\"id\": \"three\"}"),
            )
            every { iterator() } returns records.iterator()
        }

        val processed = mutableListOf<RoninEvent<*>>()
        roninConsumer.process { e ->
            processed.add(e)
            when (e.subject) {
                "key1.2" -> throw RuntimeException("kaboom")
                "last" -> roninConsumer.stop()
            }
            RoninEventResult.ACK
        }

        assertThat(
            processed,
            contains(
                RoninEvent("1", fixedInstant, "3", "4", "5", "6", "stuff", "key1.2", Stuff("two")),
            )
        )
        verify(exactly = 0) { kafkaConsumer.commitSync(any<Map<TopicPartition, OffsetAndMetadata>>()) }
    }

    @Test
    fun `processing error with error handler`() {
        val exceptionEvents = mutableListOf<RoninEvent<*>>()
        val exceptions = mutableListOf<Throwable>()
        val roninConsumer = RoninConsumer(
            listOf("topic.1", "topic.2"),
            mapOf("stuff" to Stuff::class),
            kafkaConsumer = kafkaConsumer,
            exceptionHandler = object : ConsumerExceptionHandler {
                override fun recordHandlingException(record: ConsumerRecord<String, String>, t: Throwable) {
                    TODO("Not yet implemented")
                }

                override fun eventProcessingException(events: List<RoninEvent<*>>, t: Throwable) {
                    exceptionEvents.addAll(events)
                    exceptions.add(t)
                }
            }
        )

        every { kafkaConsumer.poll(any<Duration>()) } returns mockk {
            val records = mutableListOf(
                mockRecord("stuff", "key1.2", "{\"id\": \"two\"}"),
                mockRecord("stuff", "last", "{\"id\": \"three\"}"),
            )
            every { iterator() } returns records.iterator()
        }

        val processed = mutableListOf<RoninEvent<*>>()
        val exception = RuntimeException("kaboom")
        roninConsumer.process { e ->
            processed.add(e)
            when (e.subject) {
                "key1.2" -> throw exception
                "last" -> roninConsumer.stop()
            }
            RoninEventResult.ACK
        }

        val exceptionEvent =
            RoninEvent("1", fixedInstant, "3", "4", "5", "6", "stuff", "key1.2", Stuff("two"))
        assertThat(
            processed,
            contains(
                exceptionEvent,
            )
        )
        assertThat(exceptionEvents, contains(exceptionEvent))
        assertThat(exceptions, contains(exception))
        verify(exactly = 0) { kafkaConsumer.commitSync(any<Map<TopicPartition, OffsetAndMetadata>>()) }
    }

    @Test
    fun `processing error with throwing error handler`() {
        val roninConsumer = RoninConsumer(
            listOf("topic.1", "topic.2"),
            mapOf("stuff" to Stuff::class),
            kafkaConsumer = kafkaConsumer,
            exceptionHandler = object : ConsumerExceptionHandler {
                override fun recordHandlingException(record: ConsumerRecord<String, String>, t: Throwable) {
                    TODO("Not yet implemented")
                }

                override fun eventProcessingException(events: List<RoninEvent<*>>, t: Throwable) {
                    throw RuntimeException("kaboom kaboom")
                }
            }
        )

        every { kafkaConsumer.poll(any<Duration>()) } returns mockk {
            val records = mutableListOf(
                mockRecord("stuff", "key1.2", "{\"id\": \"two\"}"),
            )
            every { iterator() } returns records.iterator()
        }

        roninConsumer.process { throw RuntimeException("source kaboom") }

        verify(exactly = 0) { kafkaConsumer.commitSync(any<Map<TopicPartition, OffsetAndMetadata>>()) }
        assertEquals(RoninConsumer.Status.STOPPED, roninConsumer.status())
    }
}
