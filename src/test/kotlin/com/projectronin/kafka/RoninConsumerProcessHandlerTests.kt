package com.projectronin.kafka

import com.projectronin.kafka.config.RoninConsumerKafkaProperties
import com.projectronin.kafka.data.RoninEvent
import com.projectronin.kafka.data.RoninEventResult
import com.projectronin.kafka.exceptions.ConsumerExceptionHandler
import com.projectronin.kafka.exceptions.TransientRetriesExhausted
import io.micrometer.core.instrument.simple.SimpleMeterRegistry
import io.mockk.every
import io.mockk.mockk
import io.mockk.verify
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

    private val kafkaConsumer = mockk<KafkaConsumer<String, ByteArray>> {
        every { subscribe(listOf("topic.1", "topic.2")) } returns Unit
        every { commitSync(any<Map<TopicPartition, OffsetAndMetadata>>()) } returns Unit
    }
    private val exceptionHandler: ConsumerExceptionHandler = mockk {}
    private val metrics = SimpleMeterRegistry()
    private val roninConsumer = RoninConsumer(
        listOf("topic.1", "topic.2"),
        mapOf("stuff" to Stuff::class),
        kafkaConsumer = kafkaConsumer,
        exceptionHandler = exceptionHandler,
        meterRegistry = metrics,
        kafkaProperties = RoninConsumerKafkaProperties()
    )

    @Test
    fun `ACK commits with no exceptions`() {
        every { kafkaConsumer.poll(any<Duration>()) } returns MockUtils.records(
            MockUtils.record("stuff", "key1.1", "{\"id\": \"one\"}"),
            MockUtils.record("stuff", "last", "{\"id\": \"two\"}"),
        )

        roninConsumer.process {
            if (it.subject == "last") {
                roninConsumer.stop()
            }
            RoninEventResult.ACK
        }

        verify(exactly = 2) { kafkaConsumer.commitSync(mapOf(TopicPartition("topic", 1) to OffsetAndMetadata(43))) }
        verify(exactly = 0) { exceptionHandler.recordHandlingException(any(), any()) }
        verify(exactly = 0) { exceptionHandler.eventProcessingException(any(), any()) }
        assertEquals(2.0, metrics[RoninConsumer.Metrics.HANDLER_ACK].counter().count())
    }

    @Test
    fun `single TRANSIENT_FAILURE retries twice, commits, and no exceptions`() {
        every { kafkaConsumer.poll(any<Duration>()) } returns MockUtils.records(
            MockUtils.record("stuff", "key1.1", "{\"id\": \"one\"}"),
            MockUtils.record("stuff", "last", "{\"id\": \"two\"}"),
        )

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
        assertEquals(2.0, metrics[RoninConsumer.Metrics.HANDLER_ACK].counter().count())
        assertEquals(1.0, metrics[RoninConsumer.Metrics.HANDLER_TRANSIENT_FAILURE].counter().count())
    }

    @Test
    fun `TRANSIENT_FAILURE exhausts retries, calls exception handler, commits`() {
        every { kafkaConsumer.poll(any<Duration>()) } returns MockUtils.records(
            MockUtils.record("stuff", "key1.1", "{\"id\": \"one\"}"),
        )
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
        assertEquals(4.0, metrics[RoninConsumer.Metrics.HANDLER_TRANSIENT_FAILURE].counter().count())
        assertEquals(1.0, metrics[RoninConsumer.Metrics.HANDLER_TRANSIENT_FAILURE_EXHAUSTED].counter().count())
    }

    @Test
    fun `PERMANENT_FAILURE calls exception handler, exits`() {
        every { kafkaConsumer.poll(any<Duration>()) } returns MockUtils.records(
            MockUtils.record("stuff", "key1.1", "{\"id\": \"one\"}"),
            MockUtils.record("stuff", "last", "{\"id\": \"two\"}"),
        )
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
        assertEquals(1.0, metrics[RoninConsumer.Metrics.HANDLER_PERMANENT_FAILURE].counter().count())
    }

    @Test
    fun `unhandled exception calls exception handler, exits`() {
        every { kafkaConsumer.poll(any<Duration>()) } returns MockUtils.records(
            MockUtils.record("stuff", "key1.1", "{\"id\": \"one\"}"),
            MockUtils.record("stuff", "last", "{\"id\": \"two\"}"),
        )
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
        assertEquals(1.0, metrics[RoninConsumer.Metrics.HANDLER_UNHANDLED_EXCEPTION].counter().count())
    }
}
