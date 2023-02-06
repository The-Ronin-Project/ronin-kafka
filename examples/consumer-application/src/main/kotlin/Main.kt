import com.projectronin.kafka.RoninConsumer
import com.projectronin.kafka.config.RoninConsumerKafkaProperties
import com.projectronin.kafka.data.RoninEvent
import com.projectronin.kafka.data.RoninEventResult
import com.projectronin.kafka.examples.data.Van
import com.projectronin.kafka.examples.data.Wing
import com.projectronin.kafka.exceptions.ConsumerExceptionHandler
import mu.KLogger
import mu.KotlinLogging
import org.apache.kafka.clients.consumer.ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG
import org.apache.kafka.clients.consumer.ConsumerConfig.GROUP_ID_CONFIG
import org.apache.kafka.clients.consumer.ConsumerRecord
import sun.misc.Signal

fun main(args: Array<String>) {
    val bootstrapServer = if (args.isNotEmpty()) args[0] else "localhost:31090"

    val logger: KLogger = KotlinLogging.logger { }
    val typeMap = mapOf(
        "van.cruising" to Van::class, "wing.flown" to Wing::class
    )

    val roninConsumer = RoninConsumer(
        topics = listOf("local.us.ronin-kafka.rides.v1"),
        typeMap,
        kafkaProperties = RoninConsumerKafkaProperties(
            BOOTSTRAP_SERVERS_CONFIG to bootstrapServer,
            GROUP_ID_CONFIG to "processing-consumer-application",
        ),
        exceptionHandler = object : ConsumerExceptionHandler {
            override fun recordHandlingException(record: ConsumerRecord<String, *>, t: Throwable) {
                logger.error(t) { "Failed to parse kafka record into a RoninEvent! - $record" }
                // do something useful with the record
            }

            override fun eventProcessingException(events: List<RoninEvent<*>>, t: Throwable) {
                logger.error(t) { "Unhandled exception while processing events!" }
                // do something useful with the event(s). Dead letter queue?
            }

            override fun pollException(t: Throwable) {
                TODO("Not yet implemented")
            }

            override fun deserializationException(t: Throwable) {
                TODO("Not yet implemented")
            }
        }
    )

    Signal.handle(Signal("INT")) {
        logger.info { "shutting down processor..." }
        roninConsumer.stop()
    }

    logger.info { "before consuming" }
    roninConsumer.process {
        logger.info { "got ${it.subject} [${it.id}]" }
        RoninEventResult.ACK
    }
    logger.info { "done consuming" }
}
