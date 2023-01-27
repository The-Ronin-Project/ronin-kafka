package com.projectronin.kafka.work

import com.projectronin.kafka.RoninConsumer
import com.projectronin.kafka.data.RoninEvent
import com.projectronin.kafka.data.RoninEventResult
import com.projectronin.kafka.exceptions.UnknownEventType
import com.projectronin.kafka.work.interfaces.EventHandler
import com.projectronin.kafka.work.interfaces.EventParser
import mu.KotlinLogging
import org.apache.kafka.clients.consumer.Consumer
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.clients.consumer.OffsetAndMetadata
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.errors.WakeupException
import java.time.Duration
import java.util.concurrent.atomic.AtomicReference

class RoninConsumer(
    val kafkaConsumer: Consumer<String, ByteArray>,
    private val eventParser: EventParser,
    private val eventHandler: EventHandler
) {
    private val logger = KotlinLogging.logger {}
    private var status = AtomicReference(Status.INITIALIZED)

    enum class Status { INITIALIZED, PROCESSING, STOPPED }

    fun executeSinglePollProcess(timeout: Duration = DEFAULT_POLL_TIMEOUT) {
        when (status()) {
            Status.STOPPED -> throw IllegalStateException("Cannot execute poll.  The Consumer has been stopped.")
            Status.INITIALIZED -> status.set(Status.PROCESSING)
            Status.PROCESSING -> { /* do nothing */ }
        }

        try {
            kafkaConsumer
                .poll(timeout)
                .asSequence() // get a lazy sequence to facilitate exiting via the filter
                .filter { status() == Status.PROCESSING } // exit immediately if we are stopped
                .forEach { record ->
                    parseEvent(record)
                        ?.let { handleEvent(it, record.topic()) }
                        .let { result ->
                            // Commit if we couldn't parse (null) OR handleEvent returned true
                            if (result != false) {
                                commitRecordOffset(record)
                            }
                        }
                }
        } catch (e: WakeupException) {
            // if got a wake-up exception, and we are stopped, then just close everything down.
            // otherwise re-throw the exception
            when (status()) {
                Status.STOPPED -> kafkaConsumer.close(DEFAULT_CLOSE_TIMEOUT_MS)
                else -> throw e
            }
        }
    }

    /**
     * starts a continuously running poll process.
     * NOTE: This method might not live here in the long run and subject to removal!!!
     */
    fun executeForeverPollProcess(timeout: Duration = DEFAULT_POLL_TIMEOUT) {
        // continue polling forever.  Call 'stop()' to halt
        while (status() != Status.STOPPED) {
            executeSinglePollProcess(timeout = timeout)
        }
        logger.info { "Exiting processing" }
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

    // note: this was originally added fo: https://projectronin.atlassian.net/browse/INT-1240
    fun unsubscribe() {
        kafkaConsumer.unsubscribe()
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
        event: RoninEvent<*>,
        topic: String
    ): Boolean {
        val returnValue = eventHandler.handle(event, topic)
        if (returnValue == RoninEventResult.PERMANENT_FAILURE) {
            stop()
            return false
        }
        return true
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
        return eventParser.parseEvent(record)
    }

    companion object {
        // default timeout on close event (normal kafka default is 30s, don't want to wait that long)
        val DEFAULT_CLOSE_TIMEOUT_MS = Duration.ofMillis(10 * 1000)
        // default timeout on kafka poll event
        val DEFAULT_POLL_TIMEOUT: Duration = Duration.ofMillis(100)
    }
}
