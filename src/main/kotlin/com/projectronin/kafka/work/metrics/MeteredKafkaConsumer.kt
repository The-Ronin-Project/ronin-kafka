package com.projectronin.kafka.work.metrics

import com.projectronin.kafka.work.metrics.ConsumerMetrics.POLL_MESSAGE_DISTRIBUTION
import com.projectronin.kafka.work.metrics.ConsumerMetrics.POLL_TIMER
import io.micrometer.core.instrument.DistributionSummary
import io.micrometer.core.instrument.MeterRegistry
import org.apache.kafka.clients.consumer.Consumer
import org.apache.kafka.clients.consumer.ConsumerRecords
import java.time.Duration
import java.util.concurrent.TimeUnit

class MeteredKafkaConsumer(
    val kafkaConsumer: Consumer<String, ByteArray>,
    private val meterRegistry: MeterRegistry,
) : Consumer<String, ByteArray> by kafkaConsumer {

    private val pollTimer = meterRegistry.timer(POLL_TIMER)
    private val pollDistribution =
        meterRegistry.let {
            DistributionSummary
                .builder(POLL_MESSAGE_DISTRIBUTION)
                .publishPercentileHistogram()
                .register(meterRegistry)
        }

    override fun poll(timeout: Duration?): ConsumerRecords<String, ByteArray> {
        val start = System.currentTimeMillis()
        return kafkaConsumer
            .poll(timeout)
            .also {
                // TODO: Should these tag with a list of topics? list of types?
                pollDistribution.record(it.count().toDouble())
                pollTimer.record(System.currentTimeMillis() - start, TimeUnit.MILLISECONDS)
            }
    }

    // NOTE: lots of other 'cool stuff' available via kafkaConsumer.metrics()
}
