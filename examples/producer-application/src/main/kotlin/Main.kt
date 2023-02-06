
import com.projectronin.kafka.RoninProducer
import com.projectronin.kafka.config.RoninProducerKafkaProperties
import com.projectronin.kafka.data.RoninEvent
import com.projectronin.kafka.examples.data.Van
import com.projectronin.kafka.examples.data.Wing
import org.apache.kafka.clients.producer.ProducerConfig.BOOTSTRAP_SERVERS_CONFIG
import java.time.LocalDateTime
import java.time.format.DateTimeFormatter
import kotlin.random.Random

fun Wing.toEvent(type: String): RoninEvent<Any> =
    RoninEvent(
        source = "ronin-kafka-examples",
        dataSchema = "https://not-a-schema",
        data = this,
        type = type,
        subject = "wing/$id",
    )

fun main(args: Array<String>) {
    val bootstrapServer = if (args.isNotEmpty()) args[0] else "localhost:31090"

    val roninProducer =
        RoninProducer<Any>(
            topic = "local.us.ronin-kafka.rides.v1",
            source = "producer-application",
            dataSchema = "https://schema",
            kafkaProperties = RoninProducerKafkaProperties(BOOTSTRAP_SERVERS_CONFIG to bootstrapServer)
        )

    val formatter = DateTimeFormatter.ofPattern("E-A")
    val r = Random(System.currentTimeMillis())

    val van = Van(
        formatter.format(LocalDateTime.now()),
        "volkswagen",
        "vanagon",
        Van.Style.CAMPER
    )
    val wing = Wing(r.nextInt(1000, 9999), "zero3", area = 19, aspectRatio = 4.5f)

    roninProducer.send("van.cruising", "van/${van.id}", van)
    roninProducer.send("wing.flown", "wing/${wing.id}", wing)

    roninProducer.send(
        RoninEvent(
            source = "producer-application",
            dataSchema = "https://not-a-schema",
            data = Van("the beast", "chevy", "astrovan", Van.Style.CREEPER),
            type = "van.cruising",
            subject = "van/the-beast",
        )
    )
    roninProducer.send(Wing(r.nextInt(1000, 9999), "vivo", area = 19, aspectRatio = 5.48f).toEvent("wing.flown"))

    // flush out any queued, but unsent events before we exit
    roninProducer.flush()
}
