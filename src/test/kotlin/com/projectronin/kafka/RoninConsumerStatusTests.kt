package com.projectronin.kafka

import com.projectronin.kafka.config.RoninConsumerKafkaProperties
import com.projectronin.kafka.data.RoninEventResult
import io.mockk.Runs
import io.mockk.every
import io.mockk.just
import io.mockk.mockk
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.clients.consumer.OffsetAndMetadata
import org.apache.kafka.common.TopicPartition
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertDoesNotThrow
import java.time.Duration

class RoninConsumerStatusTests {
    private data class Stuff(val id: String)

    private val kafkaConsumer = mockk<KafkaConsumer<String, ByteArray>> {
        every { subscribe(listOf("topic.1", "topic.2")) } returns Unit
        every { commitSync(any<Map<TopicPartition, OffsetAndMetadata>>()) } returns Unit
    }
    private val roninConsumer = RoninConsumer(
        listOf("topic.1", "topic.2"),
        mapOf("stuff" to Stuff::class),
        kafkaConsumer = kafkaConsumer,
        kafkaProperties = RoninConsumerKafkaProperties()
    )

    @Test
    fun initialized() {
        assertEquals(RoninConsumer.Status.INITIALIZED, roninConsumer.status())
    }

    @Test
    fun unsubscribe() {
        every { kafkaConsumer.unsubscribe() } just Runs
        assertDoesNotThrow { roninConsumer.unsubscribe() }
    }

    @Test
    fun `processing and stopped`() {
        every { kafkaConsumer.poll(any<Duration>()) } returns MockUtils.records(
            MockUtils.record("stuff", "key1.1", "{\"id\": \"one\"}", null),
            MockUtils.record("stuff", "key1.2", "{\"id\": \"two\"}"),
            MockUtils.record("stuff", "last", "{\"id\": \"three\"}"),
        )

        var inProcessStatus: RoninConsumer.Status = RoninConsumer.Status.INITIALIZED
        roninConsumer.process {
            inProcessStatus = roninConsumer.status()
            roninConsumer.stop()
            RoninEventResult.ACK
        }
        assertEquals(RoninConsumer.Status.PROCESSING, inProcessStatus)
        assertEquals(RoninConsumer.Status.STOPPED, roninConsumer.status())
    }
}
