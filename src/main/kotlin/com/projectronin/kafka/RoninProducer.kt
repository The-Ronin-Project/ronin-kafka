package com.projectronin.kafka

import com.fasterxml.jackson.databind.ObjectMapper
import com.projectronin.kafka.config.MapperFactory
import com.projectronin.kafka.config.RoninProducerKafkaProperties
import com.projectronin.kafka.data.RoninEvent
import com.projectronin.kafka.serde.addRoninEventHeaders
import io.micrometer.core.instrument.MeterRegistry
import mu.KLogger
import mu.KotlinLogging
import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.clients.producer.RecordMetadata
import org.apache.kafka.common.header.internals.RecordHeaders
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
 * @property dataSchema the schema for validating the payloads
 * @property kafkaProperties Kafka configuration properties. See [RoninProducerKafkaProperties] for defaults
 * @property specVersion Ronin Event Standard spec version. Currently MUST be 1.0
 * @property dataContentType Content type that the RoninEvent. Data will be serialized as. Currently only `application/json`
 * @property mapper Jackson object mapper to use for serialization
 * @property kafkaProducer [KafkaProducer] instance to use for sending kafka records
 * @constructor Creates a kafka producer for RoninEvents
 */
open class RoninProducer(
    val topic: String,
    val source: String,
    val dataSchema: String,
    val kafkaProperties: RoninProducerKafkaProperties,
    val specVersion: String = "1.0",
    val dataContentType: String = "application/json",
    val mapper: ObjectMapper = MapperFactory.mapper,
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

    init {
        logger.info { "sending on topic $topic" }
    }

    /**
     * Send an [event] to the configured kafka topic
     * @return Future containing the kafka RecordMetadata result
     */
    fun <T> send(event: RoninEvent<T>, key: String? = null): Future<RecordMetadata> {
        val messageKey = key ?: event.getSubject()
        val record = ProducerRecord(
            topic,
            null, // partition
            messageKey, // key
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
                                "successfully sent event id: `${event.id}` " +
                                    "subject: `${event.getSubject()}` metadata: `$metadata`"
                            }
                            true
                        }

                        else -> {
                            logger.error(e) {
                                "Exception sending event id: `${event.id}` " +
                                    "subject: `${event.getSubject()}` metadata: `$metadata`"
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
    fun <T> send(type: String, subject: String?, data: T): Future<RecordMetadata> =
        send(
            RoninEvent(
                specVersion = specVersion,
                dataSchema = dataSchema,
                dataContentType = dataContentType,
                source = source,
                type = type,
                data = data,
                subject = subject,
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
    private fun <T> recordHeaders(event: RoninEvent<T>): RecordHeaders {
        val headers = RecordHeaders()
        headers.addRoninEventHeaders(event)
        return headers
    }
}
