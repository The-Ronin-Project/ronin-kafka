package com.projectronin.kafka

import com.fasterxml.jackson.databind.ObjectMapper
import com.projectronin.kafka.config.KafkaProperties
import com.projectronin.kafka.config.MapperFactory
import com.projectronin.kafka.data.RoninEvent
import com.projectronin.kafka.data.StringHeader
import mu.KLogger
import mu.KotlinLogging
import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.clients.producer.RecordMetadata
import java.time.format.DateTimeFormatter
import java.util.concurrent.Future

/**
 * Ronin base kafka producer
 * @property topic the kafka topic to produce to
 * @property source the name of the application producing the RoninEvents
 * @property dataSchema the schema for validating the RoninEvent.Data payloads
 * @property specVersion Ronin Event Standard spec version. Currently MUST be 1.0
 * @property dataContentType Content type that the RoninEvent. Data will be serialized as. Currently only `application/json`
 * @property mapper Jackson object mapper to use for serialization
 * @property kafkaProperties Kafka configuration properties. Will override defaults
 * @constructor Creates a kafka producer for RoninEvents
 */
open class RoninProducer(
    val topic: String,
    val source: String,
    val dataSchema: String,
    val specVersion: String = "1.0",
    val dataContentType: String = "application/json",
    val mapper: ObjectMapper = MapperFactory.mapper,
    val kafkaProperties: KafkaProperties = KafkaProperties(),
    val kafkaProducer: KafkaProducer<String, String> = KafkaProducer<String, String>(kafkaProperties.properties)
) {
    private val instantFormatter = DateTimeFormatter.ISO_INSTANT
    private val logger: KLogger = KotlinLogging.logger { }

    /**
     * Send an [event] to the configured kafka topic
     * @return Future containing the kafka RecordMetadata result
     */
    fun <T : RoninEvent.Data<*>> send(event: RoninEvent<T>): Future<RecordMetadata> {
        val record = ProducerRecord(
            topic,
            null, // partition
            event.subject, // key
            mapper.writeValueAsString(event.data), // value
            recordHeaders(event)
        )
        logger.debug { "payload: ${record.value()}" }

        return kafkaProducer
            .send(record) { metadata, e ->
                when (e) {
                    null -> logger.debug {
                        "successfully sent event id: `${event.id}` subject: `${event.subject}` metadata: `$metadata`"
                    }
                    else -> {
                        logger.warn(e) {
                            "Exception sending event id: `${event.id}` subject: `${event.subject}` metadata: `$metadata`"
                        }
                    }
                }
            }
    }

    /**
     * Send [data] with the given [type] and [subject] to the configured kafka topic
     * @return Future containing the kafka RecordMetadata result
     */
    fun <ID> send(type: String, subject: String, data: RoninEvent.Data<ID>): Future<RecordMetadata> =
        send(
            RoninEvent(
                dataSchema = dataSchema,
                source = source,
                specVersion = specVersion,
                dataContentType = dataContentType,
                type = type,
                subject = subject,
                data = data,
            )
        )

    /**
     * Flushes the producer queue of all unsent events
     */
    fun flush() = kafkaProducer.flush()

    /**
     * translate [event] into a list of headers for a kafka record
     * @return list of StringHeader
     */
    private fun <T : RoninEvent.Data<*>> recordHeaders(event: RoninEvent<T>): List<StringHeader> =
        listOf(
            StringHeader("ce_id", event.id),
            StringHeader("ce_source", event.source),
            StringHeader("ce_specversion", event.specVersion),
            StringHeader("ce_type", event.type),
            StringHeader("content-type", event.dataContentType),
            StringHeader("ce_dataschema", event.dataSchema),
            StringHeader("ce_time", instantFormatter.format(event.time)),
        )
}
