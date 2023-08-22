package com.projectronin.kafka.extensions

import kotlinx.coroutines.ExperimentalCoroutinesApi
import kotlinx.coroutines.test.runTest
import org.apache.kafka.clients.producer.MockProducer
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.common.errors.TimeoutException
import org.apache.kafka.common.serialization.Serializer
import org.apache.kafka.common.serialization.StringSerializer
import org.junit.jupiter.api.Assertions
import org.junit.jupiter.api.Test

class ProducerExtTest {

    @OptIn(ExperimentalCoroutinesApi::class)
    @Test
    fun successfulAsync() = runTest {
        val mockProducer = MockProducer(
            true,
            StringSerializer(),
            Serializer<String> { _, _ -> "_".toByteArray() }
        )

        val metadata = mockProducer.asyncSend(ProducerRecord("topic", "key", "message"))
        Assertions.assertNotNull(metadata)

        val producerRecord = mockProducer.history().get(0)
        Assertions.assertEquals("key", producerRecord.key())
        Assertions.assertEquals("message", producerRecord.value())
    }

    @OptIn(ExperimentalCoroutinesApi::class)
    @Test
    fun failedAsync() = runTest {
        val mockProducer = MockProducer(
            true,
            StringSerializer(),
            Serializer<String> { _, _ -> "_".toByteArray() }
        )
        val thrown = TimeoutException()
        mockProducer.sendException = thrown

        val exception = try {
            mockProducer.asyncSend(ProducerRecord("topic", "key", "message"))
        } catch (e: TimeoutException) {
            e
        }

        Assertions.assertEquals(thrown, exception)
    }
}
