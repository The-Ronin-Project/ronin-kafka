package com.projectronin.kafka

import com.projectronin.kafka.data.RoninEvent
import com.projectronin.kafka.data.RoninEventResult
import io.mockk.every
import io.mockk.mockk
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.clients.consumer.OffsetAndMetadata
import org.apache.kafka.common.TopicPartition
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test
import java.time.Duration

class RoninConsumerStatusTests {
    data class Stuff(override val id: String) : RoninEvent.Data<String>

    private val kafkaConsumer = mockk<KafkaConsumer<String, ByteArray>> {
        every { subscribe(listOf("topic.1", "topic.2")) } returns Unit
        every { commitSync(any<Map<TopicPartition, OffsetAndMetadata>>()) } returns Unit
    }
    private val roninConsumer = RoninConsumer(
        listOf("topic.1", "topic.2"),
        mapOf("stuff" to Stuff::class),
        kafkaConsumer = kafkaConsumer
    )

    @Test
    fun initialized() {
        assertEquals(RoninConsumer.Status.INITIALIZED, roninConsumer.status())
    }

    @Test
    fun `processing and stopped`() {
        every { kafkaConsumer.poll(any<Duration>()) } returns MockUtils.records(
            MockUtils.record("stuff", "key1.1", "{\"id\": \"one\"}"),
            MockUtils.record("stuff", "key1.2", "{\"id\": \"two\"}"),
            MockUtils.record("stuff", "last", "{\"id\": \"three\"}"),
        )

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
