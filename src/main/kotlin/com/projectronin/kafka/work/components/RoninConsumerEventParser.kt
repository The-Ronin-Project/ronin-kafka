package com.projectronin.kafka.work.components

import com.fasterxml.jackson.databind.ObjectMapper
import com.projectronin.kafka.config.MapperFactory
import com.projectronin.kafka.data.KafkaHeaders
import com.projectronin.kafka.data.RoninEvent
import com.projectronin.kafka.exceptions.EventHeaderMissing
import com.projectronin.kafka.exceptions.UnknownEventType
import com.projectronin.kafka.work.getHeaderMap
import com.projectronin.kafka.work.getHeaderTypeValue
import com.projectronin.kafka.work.interfaces.EventParser
import com.projectronin.kafka.work.interfaces.RoninConsumerExceptionHandler
import mu.KotlinLogging
import org.apache.kafka.clients.consumer.ConsumerRecord
import java.time.Instant
import kotlin.reflect.KClass

class RoninConsumerEventParser(
    private val typeMap: Map<String, KClass<*>>,
    private val mapper: ObjectMapper = MapperFactory.mapper,
    private val consumerExceptionHandler: RoninConsumerExceptionHandler = NoOpConsumerExceptionHandler
) : EventParser {
    private val logger = KotlinLogging.logger {}

    /**
     * Translate a [ConsumerRecord] to a [RoninEvent]. Includes validation of Kafka record headers (as they are
     * currently required for the creation of a [RoninEvent]) as well as recording the time between now and when
     * the record was enqueued (there's some slight differences in the timestamp on the record depending on the
     * record's timestampType).
     *
     * @param record The ConsumerRecord<String, ByteArray> as received from Kafka
     * @return the [RoninEvent] equivalent of the provided [ConsumerRecord]
     * @throws UnknownEventType when the record's ce_type header value isn't found in [typeMap]
     */
    override fun parseEvent(record: ConsumerRecord<String, ByteArray>): RoninEvent<*>? {

        val headerMap: Map<String, String> = record.getHeaderMap()
        val type = record.getHeaderTypeValue()

        try {
            validateHeaders(headerMap)
            val valueClass = typeMap[type] ?: throw UnknownEventType(record.key(), type)
            return toRoninEvent(headerMap, record.key(), record.value(), valueClass)
        } catch (e: Exception) {
            recordHandlingException(record, e)
            return null
        }
    }

    fun recordHandlingException(record: ConsumerRecord<String, ByteArray>, t: Throwable) {
        // determine logMessage to record based on the exception.
        val logMessage = when (t) {
            is EventHeaderMissing -> {
                "Error parsing Record Key `${record.key()}`: Missing required headers: ${t.message}"
            }
            is UnknownEventType -> {
                "Error parsing Record Key `${record.key()}`: Unknown Type encountered: ${t.message}"
            }
            else -> {
                "Error parsing Record Key `${record.key()}`: Error: ${t.message}"
            }
        }
        logger.error(t) { logMessage }
        consumerExceptionHandler.handleRecordTransformationException(record, t)
    }

    /**
     * Combines record headers, key, and value into an instance of a [RoninEvent]. Includes de-serializing the record
     * payload into an instance of a class per the [typeMap].
     * @param headers The kafka record headers converted to a map of strings
     * @param key The kafka record key. Translates to [RoninEvent.subject]
     * @param value The kafka record payload. Should JSON serialized to a string
     * @param valueClass The [KClass] to deserialize the value into
     * @return an instance of [RoninEvent]
     */
    private fun toRoninEvent(
        headers: Map<String, String>,
        key: String,
        value: ByteArray,
        valueClass: KClass<out Any>
    ): RoninEvent<*> =
        RoninEvent(
            id = headers.getValue(KafkaHeaders.id),
            time = Instant.parse(headers.getValue(KafkaHeaders.time)),
            specVersion = headers.getValue(KafkaHeaders.specVersion),
            dataSchema = headers.getValue(KafkaHeaders.dataSchema),
            dataContentType = headers.getValue(KafkaHeaders.contentType),
            source = headers.getValue(KafkaHeaders.source),
            type = headers.getValue(KafkaHeaders.type),
            subject = key,
            data = mapper.readValue(value, valueClass.java),
        )

    /**
     * Validate the headers contain all Ronin Standard Event fields as needed.
     * See Also: [Ronin Event Standard](https://projectronin.atlassian.net/wiki/spaces/ENG/pages/1748041738/Ronin+Event+Standard)
     *
     * @param headers key/value map of strings containing the record headers
     * @throws EventHeaderMissing If any of the required headers are missing or blank
     */
    private fun validateHeaders(headers: Map<String, String>) {
        headers
            .keys
            .let { KafkaHeaders.required - it }
            .let {
                if (it.isNotEmpty()) {
                    throw EventHeaderMissing(it)
                }
            }
    }
}
