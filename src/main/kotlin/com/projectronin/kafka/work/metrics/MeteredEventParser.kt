package com.projectronin.kafka.work.metrics

import com.projectronin.kafka.data.KafkaHeaders
import com.projectronin.kafka.data.RoninEvent
import com.projectronin.kafka.work.getHeaderMap
import com.projectronin.kafka.work.interfaces.EventParser
import com.projectronin.kafka.work.metrics.ConsumerMetrics.MESSAGE_QUEUE_TIMER
import com.projectronin.kafka.work.metrics.ConsumerMetrics.POLL_MESSAGE_DISTRIBUTION
import com.projectronin.kafka.work.metrics.ConsumerMetrics.POLL_TIMER
import com.projectronin.kafka.work.metrics.ConsumerMetrics.TOPIC_TAG
import io.micrometer.core.instrument.DistributionSummary
import io.micrometer.core.instrument.MeterRegistry
import org.apache.kafka.clients.consumer.ConsumerRecord
import java.util.concurrent.TimeUnit

class MeteredEventParser(
    val eventParser: EventParser,
    private val meterRegistry: MeterRegistry
) : EventParser {
    private val pollTimer = meterRegistry.timer(POLL_TIMER)
    private val pollDistribution =
        meterRegistry.let {
            DistributionSummary
                .builder(POLL_MESSAGE_DISTRIBUTION)
                .publishPercentileHistogram()
                .register(meterRegistry)
        }

    override fun parseEvent(record: ConsumerRecord<String, ByteArray>): RoninEvent<*>? {
        val headerMap: Map<String, String> = record.getHeaderMap()
        val type = headerMap[KafkaHeaders.type] ?: "null"

        meterRegistry
            .timer(MESSAGE_QUEUE_TIMER, TOPIC_TAG, record.topic(), KafkaHeaders.type, type)
            .record(System.currentTimeMillis() - record.timestamp(), TimeUnit.MILLISECONDS)

        return eventParser.parseEvent(record)
    }
}
