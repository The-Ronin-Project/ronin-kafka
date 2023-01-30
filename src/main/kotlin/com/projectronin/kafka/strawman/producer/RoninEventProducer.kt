package com.projectronin.kafka.strawman.producer

import com.projectronin.kafka.data.KafkaHeaders
import com.projectronin.kafka.data.RoninEvent
import com.projectronin.kafka.data.StringHeader
import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.clients.producer.RecordMetadata
import java.time.format.DateTimeFormatter
import java.util.concurrent.Future

/**
 * This is an example of a medium amount of an opinionated option. Everything is still fairly manual, like specifying
 * the key, building the RoninEvent, etc, but it's limited to a single type and topic and the caller doesn't have to deal
 * with serializer (but in exchange, the producer provided to the constructor _must_ know about serialization).
 *
 * Locking this option down to a single version feels pretty silly. No reason this can't just take whatever object as
 * long as the serializer knows how to deal with it.
 */
class RoninEventProducer<T : Any>(
    private val topic: String,
    // TODO: document that this producer _must_ know how to serialize T
    private val producer: KafkaProducer<String, T>,
) {
    fun send(key: String, event: RoninEvent<T>): Future<RecordMetadata> {
        val record = ProducerRecord<String, T>(
            topic, null, null, key, event.data, determineHeaders(event)
        )
        return producer.send(record)
    }

    fun sendSync(key: String, event: RoninEvent<T>): RecordMetadata {
        // KafkaProducer can choose to buffer messages, so we could wait forever if we just did future.get() without flushing
        // the downside is that flush() waits on _all_ pending messages to write, not just this one
        var future = send(key, event)
        flush()
        return future.get()
    }

    fun flush() = producer.flush()

    private fun determineHeaders(event: RoninEvent<T>) = listOf(
        StringHeader(KafkaHeaders.id, event.id),
        StringHeader(KafkaHeaders.source, event.source),
        StringHeader(KafkaHeaders.specVersion, event.specVersion),
        StringHeader(KafkaHeaders.type, event.type),
        StringHeader(KafkaHeaders.contentType, event.dataContentType),
        StringHeader(KafkaHeaders.dataSchema, event.dataSchema),
        StringHeader(KafkaHeaders.time, DateTimeFormatter.ISO_INSTANT.format(event.time)),
    )
}
