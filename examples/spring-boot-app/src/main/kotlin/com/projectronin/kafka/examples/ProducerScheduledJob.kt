package com.projectronin.kafka.examples

import com.projectronin.kafka.RoninProducer
import com.projectronin.kafka.examples.data.Van
import mu.KLogger
import mu.KotlinLogging
import org.springframework.scheduling.annotation.Scheduled
import org.springframework.stereotype.Component
import java.time.LocalDateTime
import java.time.format.DateTimeFormatter

@Component
class ProducerScheduledJob(private val producer: RoninProducer<Van>) {
    private val logger: KLogger = KotlinLogging.logger { }
    private val formatter = DateTimeFormatter.ofPattern("E-A")

    @Scheduled(fixedRate = 5000)
    fun send() {
        val van = Van(
            formatter.format(LocalDateTime.now()),
            "mercedes",
            "sprinter",
            Van.Style.DELIVERY
        )
        val subject = "van/${van.id}"
        producer.send("van.cruising", subject, van)
        logger.info { "Sent: $subject " }
    }
}
