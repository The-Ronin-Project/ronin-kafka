package com.projectronin.kafka

import com.fasterxml.jackson.databind.ObjectMapper
import com.projectronin.kafka.config.MapperFactory
import com.projectronin.kafka.config.RoninConsumerKafkaProperties
import com.projectronin.kafka.data.KafkaHeaders
import com.projectronin.kafka.data.RoninEvent
import com.projectronin.kafka.data.RoninEventResult
import com.projectronin.kafka.exceptions.ConsumerExceptionHandler
import com.projectronin.kafka.exceptions.EventHeaderMissing
import com.projectronin.kafka.exceptions.TransientRetriesExhausted
import com.projectronin.kafka.exceptions.UnknownEventType
import io.micrometer.core.instrument.DistributionSummary
import io.micrometer.core.instrument.MeterRegistry
import mu.KotlinLogging
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.clients.consumer.OffsetAndMetadata
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.errors.WakeupException
import org.apache.kafka.common.header.Headers
import java.time.Duration
import java.time.Instant
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicReference
import kotlin.reflect.KClass

/**
 * Ronin base kafka consumer.
 *
 * Used to consume and process [RoninEvent]s from kafka
 *
 * See Also: [Ronin Event Standard](https://projectronin.atlassian.net/wiki/spaces/ENG/pages/1748041738/Ronin+Event+Standard)
 *
 * @property topics List of kafka topics to subscribe to
 * @property typeMap Mapping of [RoninEvent.type] to [KClass] where the [KClass] is of any type.
 *                   Used to deserialize events.
 * @property kafkaProperties kafka configuration settings. See [RoninConsumerKafkaProperties] for defaults
 * @property mapper ObjectMapper for deserializing events
 * @property kafkaConsumer [KafkaConsumer] instance to use for receiving kafka records
 * @property exceptionHandler Optional error handler for processing failed [RoninEvent]s
 * @constructor Creates a kafka consumer for RoninEvents
 */
@Deprecated("Library has been replaced by ronin-common kafka")
class RoninConsumer(
    val topics: List<String>,
    val typeMap: Map<String, KClass<*>>,
    val kafkaProperties: RoninConsumerKafkaProperties,
    private val mapper: ObjectMapper = MapperFactory.mapper,
    private val kafkaConsumer: KafkaConsumer<String, ByteArray> = KafkaConsumer(kafkaProperties.properties),
    private val exceptionHandler: ConsumerExceptionHandler? = null,
    val meterRegistry: MeterRegistry? = null
) {
    private val logger = KotlinLogging.logger {}
    private val transientRetries: Int =
        kafkaProperties.properties.getValue("ronin.handler.transient.retries").toString().toInt()
    private var status = AtomicReference(Status.INITIALIZED)

    enum class Status { INITIALIZED, PROCESSING, STOPPED }

    private val pollTimer = meterRegistry?.timer(Metrics.POLL_TIMER)
    private val pollDistribution =
        meterRegistry?.let {
            DistributionSummary
                .builder(Metrics.POLL_MESSAGE_DISTRIBUTION)
                .publishPercentileHistogram()
                .register(meterRegistry)
        }

    init {
        logger.info { "subscribing to topics `$topics`" }
        kafkaConsumer.subscribe(topics)
    }

    object Metrics {
        const val POLL_TIMER = "roninkafka.consumer.poll"
        const val POLL_MESSAGE_DISTRIBUTION = "roninkafka.consumer.poll.messages"
        const val MESSAGE_QUEUE_TIMER = "roninkafka.consumer.message.queue.timer"
        const val MESSAGE_HANDLER_TIMER = "roninkafka.consumer.message.handler.timer"

        const val EXCEPTION_UNKNOWN_TYPE = "roninkafka.consumer.exceptions.unknowntype"
        const val EXCEPTION_MISSING_HEADER = "roninkafka.consumer.exceptions.missingheader"
        const val EXCEPTION_DESERIALIZATION = "roninkafka.consumer.exceptions.deserialization"

        const val HANDLER_ACK = "roninkafka.consumer.handler.ack"
        const val HANDLER_TRANSIENT_FAILURE = "roninkafka.consumer.handler.transient_failure"
        const val HANDLER_PERMANENT_FAILURE = "roninkafka.consumer.handler.permanent_failure"
        const val HANDLER_TRANSIENT_FAILURE_EXHAUSTED = "roninkafka.consumer.handler.transient_failure_exhausted"
        const val HANDLER_UNHANDLED_EXCEPTION = "roninkafka.consumer.handler.unhandled_exception"
    }

    /**
     * Loop until [stop] is called to continuously fetch RoninEvents off of Kafka and pass each of them to a
     * processor function one at a time. Kafka offsets are committed after each successfully processed event.
     *
     * To stop processing messages and have this function return, call [stop]
     *
     * Exception scenarios:
     *
     *  * Kafka record missing required headers: logged, counted, and record skipped
     *  * [RoninEvent.type] not included the [typeMap]: logged, counted, and record skipped
     *  * Deserialization exception: logged and counted. If an [exceptionHandler] is defined, the kafka record and exception
     *                               are passed to the [errorHandler]
     *  * Unhandled processing exception: logged and counted. If an [exceptionHandler] is defined, the kafka record and
     *                                    exception are passed to the [errorHandler]
     *
     * @param timeout Maximum amount of time to block while polling kafka. Default: 100ms
     * @param handler The function that each [RoninEvent] should be passed to for processing. If the function returns
     *                  normally, processing is considered successful and the offset is committed. If an exception
     *                  is thrown
     */
    fun process(timeout: Duration = Duration.ofMillis(100), handler: (RoninEvent<*>) -> RoninEventResult) {
        if (!status.compareAndSet(Status.INITIALIZED, Status.PROCESSING)) {
            throw IllegalStateException("process() can only be called once per instance")
        }

        try {
            while (status() == Status.PROCESSING) {
                poll(timeout)
                    .forEach { (record, event) ->
                        if (event == null || handleEvent(record, event, handler)) {
                            commitRecordOffset(record)
                        }
                    }
            }
        } catch (ex: WakeupException) {
            if (status.get() != Status.STOPPED) {
                throw ex
            } else {
                /* We expect a WakeupException when stop() is called and we wake up KafkaConsumer, so it's safe to ignore it */
            }
        } finally {
            kafkaConsumer.close()
        }

        logger.info { "Exiting processing" }
    }

    /**
     * Poll Kafka for events. If events exist, this function will handle them and return immediately. Otherwise, it will
     * wait for the [timeout].
     * Kafka offsets are committed after each successfully processed event.
     *
     * Exception scenarios:
     *
     *  * Kafka record missing required headers: logged, counted, and record skipped
     *  * [RoninEvent.type] not included the [typeMap]: logged, counted, and record skipped
     *  * Deserialization exception: logged and counted. If an [exceptionHandler] is defined, the kafka record and exception
     *                               are passed to the [errorHandler]
     *  * Unhandled processing exception: logged and counted. If an [exceptionHandler] is defined, the kafka record and
     *                                    exception are passed to the [errorHandler]
     *
     * @param timeout Maximum amount of time to block while polling kafka. Default: 100ms
     * @param handler The function that each [RoninEvent] should be passed to for processing. If the function returns
     *                  normally, processing is considered successful and the offset is committed. If an exception
     *                  is thrown
     */
    fun pollOnce(timeout: Duration = Duration.ofMillis(100), handler: (RoninEvent<*>) -> RoninEventResult) {
        if (status.get() == Status.STOPPED) {
            throw IllegalStateException("pollOnce() cannot be called after the instance is stopped")
        }

        poll(timeout)
            .forEach { (record, event) ->
                if (event == null || handleEvent(record, event, handler)) {
                    commitRecordOffset(record)
                }
            }
    }

    /**
     * Retrieve the current status of the [RoninConsumer]
     * @return [RoninConsumer.Status]
     */
    fun status(): Status = status.get()

    /**
     * Discontinue polling done by [process] functions and have them return
     */
    fun stop() {
        status.set(Status.STOPPED)
        kafkaConsumer.wakeup()
    }

    fun unsubscribe() {
        kafkaConsumer.unsubscribe()
    }

    private fun poll(timeout: Duration): Sequence<Pair<ConsumerRecord<String, ByteArray>, RoninEvent<*>?>> {
        val start = System.currentTimeMillis()
        return kafkaConsumer
            .poll(timeout)
            .also {
                // TODO: Should these tag with a list of topics? list of types?
                pollDistribution?.record(it.count().toDouble())
                pollTimer?.record(System.currentTimeMillis() - start, TimeUnit.MILLISECONDS)
            }
            .asSequence()
            .takeWhile { status() != Status.STOPPED }
            .map { record -> Pair(record, parseEvent(record)) }
    }

    /**
     * Commit the offset for the provided record
     * @param record The ConsumerRecord that should be committed as processed for this consumer group
     */
    private fun commitRecordOffset(record: ConsumerRecord<*, *>) {
        // The committed offset should always be the offset of the next message that your application
        // will read. This is where we’ll start reading next time we start.
        val offsetMap = mapOf(
            TopicPartition(
                record.topic(),
                record.partition()
            ) to OffsetAndMetadata(record.offset() + 1)
        )
        logger.debug { "committing offset $offsetMap" }
        kafkaConsumer.commitSync(offsetMap)
    }

    /**
     * Pass the given [event] to the consumer supplied [handler] lambda and retry if necessary
     * @param event the event to provide to the handler
     * @param handler the lambda to pass the event to and evaluate the result of
     * @return Boolean indicating whether the offset for the event should be committed
     */
    private fun handleEvent(
        record: ConsumerRecord<*, *>,
        event: RoninEvent<*>,
        handler: (RoninEvent<*>) -> RoninEventResult
    ): Boolean {
        try {
            (0..transientRetries).forEach { attempt ->
                logger.debug { "processing event id: `${event.id}` subject: `${event.getSubject()}` attempt $attempt" }

                val start = System.currentTimeMillis()
                val result = handler(event)
                meterRegistry
                    ?.timer(Metrics.MESSAGE_HANDLER_TIMER, "topic", record.topic(), KafkaHeaders.type, event.type)
                    ?.record(System.currentTimeMillis() - start, TimeUnit.MILLISECONDS)
                when (result) {
                    RoninEventResult.ACK -> {
                        logger.debug { "handler acknowledged" }
                        meterRegistry
                            ?.counter(Metrics.HANDLER_ACK, "topic", record.topic(), KafkaHeaders.type, event.type)
                            ?.increment()
                        return true
                    }

                    RoninEventResult.TRANSIENT_FAILURE -> {
                        logger.info { "Transient failure reported on attempt $attempt" }
                        meterRegistry
                            ?.counter(
                                Metrics.HANDLER_TRANSIENT_FAILURE,
                                "topic",
                                record.topic(),
                                KafkaHeaders.type,
                                event.type
                            )
                            ?.increment()
                    }

                    RoninEventResult.PERMANENT_FAILURE -> {
                        logger.warn { "Permanent failure reported on attempt $attempt" }
                        meterRegistry
                            ?.counter(
                                Metrics.HANDLER_PERMANENT_FAILURE,
                                "topic",
                                record.topic(),
                                KafkaHeaders.type,
                                event.type
                            )
                            ?.increment()
                        stop()
                        return false
                    }
                }
            }

            logger.warn { "max transient retries reached for event $event" }
            meterRegistry
                ?.counter(
                    Metrics.HANDLER_TRANSIENT_FAILURE_EXHAUSTED,
                    "topic",
                    record.topic(),
                    KafkaHeaders.type,
                    event.type
                )
                ?.increment()
            reportException {
                eventProcessingException(listOf(event), TransientRetriesExhausted(1 + transientRetries))
            }
            return true
        } catch (e: Exception) {
            logger.error(e) { "unhandled exception processing $event, exiting processing" }
            meterRegistry
                ?.counter(Metrics.HANDLER_UNHANDLED_EXCEPTION, "topic", record.topic(), KafkaHeaders.type, event.type)
                ?.increment()
            reportException { eventProcessingException(listOf(event), e) }
            stop()
            return false
        }
    }

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
    private fun parseEvent(record: ConsumerRecord<String, ByteArray>): RoninEvent<*>? {
        val headers = parseHeaders(record.headers())
        val type = headers[KafkaHeaders.type] ?: "null"

        meterRegistry
            ?.timer(Metrics.MESSAGE_QUEUE_TIMER, "topic", record.topic(), KafkaHeaders.type, type)
            ?.record(System.currentTimeMillis() - record.timestamp(), TimeUnit.MILLISECONDS)

        try {
            validateHeaders(headers)
            val valueClass = typeMap[type] ?: throw UnknownEventType(record.key(), type)
            return toRoninEvent(headers, record.key(), record.value(), valueClass)
        } catch (e: Exception) {
            // determine metric and logMessage to record based on the exception.
            val (exceptionMetricName, logMessage) = when (e) {
                is EventHeaderMissing -> {
                    Pair(
                        Metrics.EXCEPTION_MISSING_HEADER,
                        "Error parsing Record Key `${record.key()}`: Missing required headers: ${e.message}"
                    )
                }
                is UnknownEventType -> {
                    Pair(
                        Metrics.EXCEPTION_UNKNOWN_TYPE,
                        "Error parsing Record Key `${record.key()}`: Unknown Type encountered: ${e.message}"
                    )
                }
                else -> {
                    Pair(
                        Metrics.EXCEPTION_DESERIALIZATION,
                        "Error parsing Record Key `${record.key()}`: Error: ${e.message}"
                    )
                }
            }

            logger.error(e) { logMessage }
            meterRegistry
                ?.counter(exceptionMetricName, "topic", record.topic(), KafkaHeaders.type, type)
                ?.increment()
            reportException { recordHandlingException(record, e) }
            return null
        }
    }

    /**
     * Parse kafka [ConsumerRecord] headers out into an easier to use map
     * @return a key/value map of strings representing the headers
     */
    private fun parseHeaders(headers: Headers): Map<String, String> =
        headers
            .filter { it.value() != null && it.value().isNotEmpty() }
            .associate { it.key() to it.value().decodeToString() }

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

    /**
     * Combines record headers, key, and value into an instance of a [RoninEvent]. Includes de-serializing the record
     * payload into an instance of a class per the [typeMap].
     * @param headers The kafka record headers converted to a map of strings
     * @param key The kafka record key. Translates to [subject]
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
            data = mapper.readValue(value, valueClass.java),
            subject = headers.getOrDefault(KafkaHeaders.subject, key), // TODO - Remove in the future as the need for this backwards compatability goes away.
        )

    /**
     * An exception occurred... if an [exceptionHandler] was provided, call the provided block to report the
     * exception via the [exceptionHandler] callbacks. And do so in a protected way. If exceptions are unhandled
     * here, stop processing
     * @param block lambda for passing the appropriate exception details ot the appropriate callback
     */
    private fun reportException(block: ConsumerExceptionHandler.() -> Unit) {
        try {
            exceptionHandler?.let { block(it) }
        } catch (e: Exception) {
            logger.error(e) { "Unhandled exception during exception scenario, exiting processing" }
            stop()
        }
    }
}
