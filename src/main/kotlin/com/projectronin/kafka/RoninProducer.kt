package com.projectronin.kafka

import com.fasterxml.jackson.databind.ObjectMapper
import com.projectronin.kafka.config.MapperFactory
import com.projectronin.kafka.config.RoninProducerKafkaProperties
import com.projectronin.kafka.data.KafkaHeaders
import com.projectronin.kafka.data.RoninEvent
import com.projectronin.kafka.data.StringHeader
import io.micrometer.core.instrument.MeterRegistry
import mu.KLogger
import mu.KotlinLogging
import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.clients.producer.RecordMetadata
import java.time.format.DateTimeFormatter
import java.util.concurrent.Future
import java.util.concurrent.TimeUnit

/**
 * Ronin base kafka producer
 *
 * See Also: [Ronin Event Standard](https://projectronin.atlassian.net/wiki/spaces/ENG/pages/1748041738/Ronin+Event+Standard)
 *
 * @property topic the kafka topic to produce to
 * @property source the name of the application producing the RoninEvents
 * @property dataSchema the schema for validating the RoninEvent.Data payloads
 * @property specVersion Ronin Event Standard spec version. Currently MUST be 1.0
 * @property dataContentType Content type that the RoninEvent. Data will be serialized as. Currently only `application/json`
 * @property mapper Jackson object mapper to use for serialization
 * @property kafkaProperties Kafka configuration properties. See [RoninProducerKafkaProperties] for defaults
 * @property kafkaProducer [KafkaProducer] instance to use for sending kafka records
 * @constructor Creates a kafka producer for RoninEvents
 */
open class RoninProducer(
    val topic: String,
    val source: String,
    val dataSchema: String,
    val specVersion: String = "1.0",
    val dataContentType: String = "application/json",
    val mapper: ObjectMapper = MapperFactory.mapper,
    val kafkaProperties: RoninProducerKafkaProperties = RoninProducerKafkaProperties(),
    val kafkaProducer: KafkaProducer<String, ByteArray> = KafkaProducer<String, ByteArray>(kafkaProperties.properties),
    val meterRegistry: MeterRegistry? = null
) {
    private val instantFormatter = DateTimeFormatter.ISO_INSTANT
    private val logger: KLogger = KotlinLogging.logger { }

    object Metrics {
        // KafkaProducer.send is async, this measures the time between when send is called and the callback is invoked
        const val SEND_TIMER = "roninkafka.producer.send"
        const val FLUSH_TIMER = "roninkafka.producer.flush"
    }

    /**
     * Send an [event] to the configured kafka topic
     * @return Future containing the kafka RecordMetadata result
     */
    fun <T : RoninEvent.Data<*>> send(event: RoninEvent<T>): Future<RecordMetadata> {
        val record = ProducerRecord(
            topic,
            null, // partition
            event.subject, // key
            mapper.writeValueAsBytes(event.data), // value
            recordHeaders(event)
        )
        logger.debug { "payload: ${record.value()}" }

        val start = System.currentTimeMillis()
        return kafkaProducer
            .send(record) { metadata, e ->
                val success: Boolean =
                    when (e) {
                        null -> {
                            logger.debug {
                                "successfully sent event id: `${event.id}` subject: `${event.subject}` metadata: `$metadata`"
                            }
                            true
                        }

                        else -> {
                            logger.error(e) {
                                "Exception sending event id: `${event.id}` subject: `${event.subject}` metadata: `$metadata`"
                            }
                            false
                        }
                    }

                meterRegistry
                    ?.timer(Metrics.SEND_TIMER, "success", success.toString(), "topic", topic, "ce_type", event.type)
                    ?.record(System.currentTimeMillis() - start, TimeUnit.MILLISECONDS)
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
    fun flush() {
        val start = System.currentTimeMillis()
        kafkaProducer.flush()
        meterRegistry
            ?.timer(Metrics.FLUSH_TIMER)
            ?.record(System.currentTimeMillis() - start, TimeUnit.MILLISECONDS)
    }

    /**
     * translate [event] into a list of headers for a kafka record
     * @return list of StringHeader
     */
    private fun <T : RoninEvent.Data<*>> recordHeaders(event: RoninEvent<T>): List<StringHeader> =
        listOf(
            StringHeader(KafkaHeaders.id, event.id),
            StringHeader(KafkaHeaders.source, event.source),
            StringHeader(KafkaHeaders.specVersion, event.specVersion),
            StringHeader(KafkaHeaders.type, event.type),
            StringHeader(KafkaHeaders.contentType, event.dataContentType),
            StringHeader(KafkaHeaders.dataSchema, event.dataSchema),
            StringHeader(KafkaHeaders.time, instantFormatter.format(event.time)),
        )
}
