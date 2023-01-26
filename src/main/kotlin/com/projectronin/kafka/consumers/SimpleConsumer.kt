package com.projectronin.kafka.consumers

import EventBodyMalformedException
import RoninEventDecoder
import com.projectronin.kafka.data.RoninEvent
import com.projectronin.kafka.exceptions.EventHeaderMissing
import org.apache.kafka.clients.consumer.Consumer
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.clients.consumer.OffsetAndMetadata
import org.apache.kafka.common.TopicPartition
import java.time.Duration
import kotlin.reflect.KClass

class RoninKafkaConsumerSubscriptions

interface RoninEventHandlerCallback

interface RoninEventHandler<T> {
    fun handleEvent(event: RoninEvent<T>, callback: RoninEventHandlerCallback)
}

enum class SimpleConsumerErrorType {
    MISSING_REQUIRED_HEADER,
    BODY_MALFORMED,
    UNKNOWN
}

data class SimpleConsumerError(
    val type: SimpleConsumerErrorType,
    val description: String,
    val error: Exception
)

data class SimpleConsumerRecord<T : Any>(
    val event: RoninEvent<T>?,
    val record: ConsumerRecord<String, ByteArray>,
    val error: SimpleConsumerError?,
)

class SimpleConsumerRecords<T : Any>(private val records: List<SimpleConsumerRecord<T>>) : Iterable<SimpleConsumerRecord<T>> {
    override fun iterator(): Iterator<SimpleConsumerRecord<T>> = records.iterator()

    fun hasErrors() = records.any { false }
}

/**
 * Wrapper around [KafkaConsumer] that understands the [RoninEvent] envelope. This can be used to poll for specific
 * events from specific topics instead of [KafkaConsumer] giving back raw [ConsumerRecord]s.
 *
 * Note that this intentionally has a few limitations:
 * * It only supports a single topic
 * * It only supports [RoninEvent] messages
 */
class SimpleConsumer<T : Any>(
    topic: String,
    private val topicDataType: KClass<T>,
    private val consumer: Consumer<String, ByteArray>,
    private val decoder: RoninEventDecoder = RoninEventDecoder()
) {
    private val handlersByTopic: Map<String, RoninEventHandler<*>> = mutableMapOf()

    init {
        consumer.subscribe(listOf(topic))
    }

    fun poll(timeout: Duration): SimpleConsumerRecords<T> {
        val records = consumer.poll(timeout).map(this::convert)
        return SimpleConsumerRecords(records)
    }

    /**
     * Commits offsets returned on the last poll. See [KafkaConsumer.commitSync].
     *
     * Note that this also includes offsets for messages that resulted in errors, so in general you will only want to
     * call this if the entire batch returned from [poll] was processed successfully.
     */
    fun commitSync() {
        consumer.commitSync()
    }

    /**
     * Commits the offset for the provided record.
     *
     * You should only call this when the provided message and all messages before it (at least on the same partition)
     * have been successfully processed.
     *
     * See [KafkaConsumer.commitSync] for possible exceptions this throws.
     */
    fun commitSync(record: SimpleConsumerRecord<T>) {
        // The committed offset should always be the offset of the next message that your application
        // will read. This is where we’ll start reading next time we start.
        val offsetMap = mapOf(
            TopicPartition(
                record.record.topic(),
                record.record.partition()
            ) to OffsetAndMetadata(record.record.offset() + 1)
        )
        consumer.commitSync(offsetMap)
    }

    private fun convert(record: ConsumerRecord<String, ByteArray>): SimpleConsumerRecord<T> =
        try {
            SimpleConsumerRecord<T>(
                event = decoder.parseRecord(record, topicDataType.java),
                record = record,
                error = null
            )
        } catch (ex: Exception) {
            val type = when (ex) {
                is EventHeaderMissing -> SimpleConsumerErrorType.MISSING_REQUIRED_HEADER
                is EventBodyMalformedException -> SimpleConsumerErrorType.BODY_MALFORMED
                else -> SimpleConsumerErrorType.UNKNOWN
            }

            SimpleConsumerRecord<T>(
                event = null,
                record = record,
                error = SimpleConsumerError(type, "failed converting Kafka record to RoninEvent: ${ex.message}", ex)
            )
        }
}

/**
 * Handles when an error occurs while trying to interpret a Kafka record (e.g. invalid headers, JSON deserialization failed, etc)
 */
interface InvalidEventErrorHandler {
    fun handleError(error: SimpleConsumerError)
}

class UnrecoverableRecordProcessingException : RuntimeException()

interface SimpleRecordHandler<T : Any> {
    /**
     * Processes the provided record.
     *
     * @throws UnrecoverableRecordProcessingException if processing fails in a way that is known to be unrecoverable. This is essentially
     * intended as a marker that retries should not be attempted.
     */
    fun handleRecord(record: SimpleConsumerRecord<T>)
}

interface SimpleRecordProcessor<T : Any> {
    fun processRecord(handler: SimpleRecordProcessor<T>, record: SimpleConsumerRecord<T>)
}

/**
 * Polls a topic for events, converts them to [RoninEvent]s, and processes them.
 */
class SimpleExecutor<T : Any>(
    private val consumer: SimpleConsumer<T>,
    private val processor: SimpleRecordProcessor<T>,
) {
    fun executeUntilError(pollTimeout: Duration = Duration.ofMillis(500)) {
        while (true) {
            processRecords(consumer.poll(pollTimeout))
        }
    }

    private fun processRecords(records: SimpleConsumerRecords<T>) {
        records.forEach()
    }
}
