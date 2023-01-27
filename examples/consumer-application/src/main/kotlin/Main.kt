import com.projectronin.kafka.config.RoninConsumerKafkaProperties
import com.projectronin.kafka.data.RoninEvent
import com.projectronin.kafka.data.RoninEventResult
import com.projectronin.kafka.examples.data.Van
import com.projectronin.kafka.examples.data.Wing
import com.projectronin.kafka.work.RoninConsumerFactory
import com.projectronin.kafka.work.interfaces.RoninConsumerExceptionHandler
import mu.KLogger
import mu.KotlinLogging
import org.apache.kafka.clients.consumer.ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG
import org.apache.kafka.clients.consumer.ConsumerConfig.GROUP_ID_CONFIG
import org.apache.kafka.clients.consumer.ConsumerRecord
import sun.misc.Signal

fun main() {
    val logger: KLogger = KotlinLogging.logger { }

    val roninConsumer = RoninConsumerFactory.createRoninConsumer(
        topics = listOf("local.us.ronin-kafka.rides.v1"),
        typeMap = mapOf(
            "van.cruising" to Van::class,
            "wing.flown" to Wing::class
        ),
        kafkaProperties = RoninConsumerKafkaProperties(
            BOOTSTRAP_SERVERS_CONFIG to "localhost:9092",
            GROUP_ID_CONFIG to "processing-consumer-application",
        ),
        // moved the eventHandler into the roninConsumer, rather than having to pass
        //   in on every individual poll request.  (can always move back if desired)
        eventHandler = { event, topic ->
            logger.warn { "got ${event.subject} [${event.id}] for topic: $topic" }
            RoninEventResult.ACK
        },
        exceptionHandler = object : RoninConsumerExceptionHandler {
            override fun handleRecordTransformationException(record: ConsumerRecord<String, ByteArray>, t: Throwable) {
                logger.error(t) { "Failed to parse kafka record into a RoninEvent! - $record" }
                // do something useful with the record
            }
            override fun handleEventProcessingException(event: RoninEvent<*>, topic: String, t: Throwable) {
                logger.error(t) { "Unhandled exception while processing events!" }
                // do something useful with the event(s). Dead letter queue?
            }
        }
    )

    Signal.handle(Signal("INT")) {
        logger.info { "shutting down processor..." }
        roninConsumer.stop()
    }

    logger.info { "before consuming" }
    // note: the event handler was defined earlier.
    roninConsumer.executeForeverPollProcess()
    logger.info { "done consuming" }
}
