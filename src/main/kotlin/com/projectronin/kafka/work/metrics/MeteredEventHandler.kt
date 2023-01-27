package com.projectronin.kafka.work.metrics

import com.projectronin.kafka.data.KafkaHeaders
import com.projectronin.kafka.data.RoninEvent
import com.projectronin.kafka.data.RoninEventResult
import com.projectronin.kafka.work.interfaces.EventHandler
import com.projectronin.kafka.work.metrics.ConsumerMetrics.HANDLER_ACK
import com.projectronin.kafka.work.metrics.ConsumerMetrics.HANDLER_PERMANENT_FAILURE
import com.projectronin.kafka.work.metrics.ConsumerMetrics.HANDLER_TRANSIENT_FAILURE
import com.projectronin.kafka.work.metrics.ConsumerMetrics.MESSAGE_HANDLER_TIMER
import com.projectronin.kafka.work.metrics.ConsumerMetrics.TOPIC_TAG
import io.micrometer.core.instrument.MeterRegistry
import java.util.concurrent.TimeUnit

class MeteredEventHandler(
    val eventHandler: EventHandler,
    private val meterRegistry: MeterRegistry
) : EventHandler {

    override fun handle(event: RoninEvent<*>, topic: String): RoninEventResult {
        val start = System.currentTimeMillis()

        val result = eventHandler.handle(event, topic)

        meterRegistry
            .timer(MESSAGE_HANDLER_TIMER, TOPIC_TAG, topic, KafkaHeaders.type, event.type)
            .record(System.currentTimeMillis() - start, TimeUnit.MILLISECONDS)

        val resultMetricName = when (result) {
            RoninEventResult.ACK -> HANDLER_ACK
            RoninEventResult.TRANSIENT_FAILURE -> HANDLER_TRANSIENT_FAILURE
            RoninEventResult.PERMANENT_FAILURE -> HANDLER_PERMANENT_FAILURE
        }
        meterRegistry.counter(resultMetricName, TOPIC_TAG, topic, KafkaHeaders.type, event.type).increment()
        return result
    }
}
