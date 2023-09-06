package com.projectronin.kafka.examples

import com.projectronin.kafka.RoninConsumer
import com.projectronin.kafka.data.RoninEventResult
import mu.KLogger
import mu.KotlinLogging
import org.springframework.scheduling.annotation.Async
import org.springframework.stereotype.Component
import javax.annotation.PreDestroy

@Component
class ConsumerProcess(private val consumer: RoninConsumer) {

    private val logger: KLogger = KotlinLogging.logger { }

    @PreDestroy
    fun stop() {
        logger.info { "shutting down processor..." }
        consumer.stop()
    }

    @Async
    fun run() {
        logger.info { "before consuming" }
        consumer
            .process {
                logger.info { "received ${it.getSubject()} [${it.id}]" }
                RoninEventResult.ACK
            }
        logger.info { "done consuming" }
    }
}
