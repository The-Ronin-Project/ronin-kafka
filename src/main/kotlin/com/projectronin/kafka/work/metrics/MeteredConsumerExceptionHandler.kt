package com.projectronin.kafka.work.metrics

import com.projectronin.kafka.data.KafkaHeaders
import com.projectronin.kafka.data.RoninEvent
import com.projectronin.kafka.exceptions.EventHeaderMissing
import com.projectronin.kafka.exceptions.TransientRetriesExhausted
import com.projectronin.kafka.exceptions.UnknownEventType
import com.projectronin.kafka.work.getHeaderTypeValue
import com.projectronin.kafka.work.interfaces.RoninConsumerExceptionHandler
import com.projectronin.kafka.work.metrics.ConsumerMetrics.EXCEPTION_DESERIALIZATION
import com.projectronin.kafka.work.metrics.ConsumerMetrics.EXCEPTION_MISSING_HEADER
import com.projectronin.kafka.work.metrics.ConsumerMetrics.EXCEPTION_UNKNOWN_TYPE
import com.projectronin.kafka.work.metrics.ConsumerMetrics.HANDLER_TRANSIENT_FAILURE_EXHAUSTED
import com.projectronin.kafka.work.metrics.ConsumerMetrics.HANDLER_UNHANDLED_EXCEPTION
import com.projectronin.kafka.work.metrics.ConsumerMetrics.TOPIC_TAG
import io.micrometer.core.instrument.MeterRegistry
import org.apache.kafka.clients.consumer.ConsumerRecord

class MeteredConsumerExceptionHandler(
    val exceptonHandler: RoninConsumerExceptionHandler,
    private val meterRegistry: MeterRegistry
) : RoninConsumerExceptionHandler {
    override fun handleEventProcessingException(event: RoninEvent<*>, topic: String, t: Throwable) {
        val exceptionMetricName = when (t) {
            is TransientRetriesExhausted -> HANDLER_TRANSIENT_FAILURE_EXHAUSTED
            else -> HANDLER_UNHANDLED_EXCEPTION
        }
        meterRegistry
            .counter(
                exceptionMetricName,
                TOPIC_TAG,
                topic,
                KafkaHeaders.type,
                event.type
            )
            .increment()
    }

    override fun handleRecordTransformationException(record: ConsumerRecord<String, ByteArray>, t: Throwable) {

        exceptonHandler.handleRecordTransformationException(record, t)

        val type = record.getHeaderTypeValue()
        // determine metric to record based on the exception.
        val exceptionMetricName = when (t) {
            is EventHeaderMissing -> EXCEPTION_MISSING_HEADER
            is UnknownEventType -> EXCEPTION_UNKNOWN_TYPE
            else -> EXCEPTION_DESERIALIZATION
        }

        meterRegistry
            .counter(exceptionMetricName, TOPIC_TAG, record.topic(), KafkaHeaders.type, type)
            .increment()
    }
}
