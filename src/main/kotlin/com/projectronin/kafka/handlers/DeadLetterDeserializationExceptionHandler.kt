package com.projectronin.kafka.handlers

import com.projectronin.kafka.config.RoninConfig.Companion.DEAD_LETTER_TOPIC_CONFIG
import mu.KotlinLogging
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.clients.producer.RecordMetadata
import org.apache.kafka.streams.errors.DeserializationExceptionHandler
import org.apache.kafka.streams.processor.ProcessorContext

class DeadLetterDeserializationExceptionHandler : DeserializationExceptionHandler {
    private val logger = KotlinLogging.logger {}
    private var dlq: String? = null
    private var producer: KafkaProducer<ByteArray, ByteArray>? = null

    override fun configure(configs: MutableMap<String, *>?) {
        dlq = configs?.get(DEAD_LETTER_TOPIC_CONFIG) as String?
        producer = DeadLetterProducer.producer(configs)
    }

    override fun handle(
        context: ProcessorContext?,
        record: ConsumerRecord<ByteArray, ByteArray>?,
        exception: Exception?
    ): DeserializationExceptionHandler.DeserializationHandlerResponse {
        if (dlq == null) {
            logger.error("Stream configured to use Dead Letter Queue for deserialization exceptions, but no topic set.")
            return DeserializationExceptionHandler.DeserializationHandlerResponse.FAIL
        }

        producer?.send(
            ProducerRecord(
                dlq,
                null,
                record?.key(),
                record?.value(),
                record?.headers()
            )
        ) { recordMetadata: RecordMetadata?, ex: java.lang.Exception? ->
            recordMetadata?.let {
                logger.error(
                    "Exception Deserializing Message $context. " +
                        "Message forwarded to DLQ $dlq at $recordMetadata"
                )
            }
            ex?.let {
                logger.error(
                    "Exception Deserializing Message $context. " +
                        "Attempts to write message to DLQ $dlq failed with exception ${ex.message}"
                )
            }
        }
        return DeserializationExceptionHandler.DeserializationHandlerResponse.CONTINUE
    }
}
