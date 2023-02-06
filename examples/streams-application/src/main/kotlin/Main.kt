import com.projectronin.kafka.RoninEventHandler
import com.projectronin.kafka.data.RoninEvent
import com.projectronin.kafka.examples.data.Van
import com.projectronin.kafka.examples.data.Wing
import mu.KLogger
import mu.KotlinLogging

val logger: KLogger = KotlinLogging.logger { }

fun main(args: Array<String>) {
    val bootstrapServer = if (args.isNotEmpty()) args[0] else "localhost:31090"

    val eventHandler = RoninEventHandler.Builder()
        .asApplication("processing-stream-application")
        .fromKafka(bootstrapServer)
        .fromTopics(listOf("local.us.ronin-kafka.rides.v1"))
        .withType("van.cruising", Van::class)
        .withType("wing.flown", Wing::class)
        .withHandler(::logMessage)
        .withDeserializationExceptionHandler(RoninEventHandler.SkipItDeserializationExceptionHandler::class)
        .build()

    eventHandler.start()
}

private fun logMessage(k: String?, m: RoninEvent<*>) {
    logger.info { "got $k [${m.javaClass.name}]" }
}
