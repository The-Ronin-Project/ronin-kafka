
import com.projectronin.kafka.RoninProducer
import com.projectronin.kafka.config.RoninProducerKafkaProperties
import com.projectronin.kafka.data.RoninEvent
import com.projectronin.kafka.examples.data.Van
import com.projectronin.kafka.examples.data.Wing
import org.apache.kafka.clients.producer.ProducerConfig.BOOTSTRAP_SERVERS_CONFIG
import java.time.LocalDateTime
import java.time.format.DateTimeFormatter
import kotlin.random.Random

fun Wing.toEvent(type: String): RoninEvent<Wing> =
    RoninEvent(
        dataSchema = "https://not-a-schema",
        source = "ronin-kafka-examples",
        type = type,
        data = this,
        subject = "wing/$id",
    )

fun main() {
    val roninProducer =
        RoninProducer(
            topic = "local.us.ronin-kafka.rides.v1",
            source = "producer-application",
            dataSchema = "https://schema",
            kafkaProperties = RoninProducerKafkaProperties(BOOTSTRAP_SERVERS_CONFIG to "localhost:9092")
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
            dataSchema = "https://not-a-schema",
            source = "producer-application",
            type = "van.cruising",
            data = Van("the beast", "chevy", "astrovan", Van.Style.CREEPER),
            subject = "van/the-beast",
        )
    )
    roninProducer.send(Wing(r.nextInt(1000, 9999), "vivo", area = 19, aspectRatio = 5.48f).toEvent("wing.flown"))

    // flush out any queued, but unsent events before we exit
    roninProducer.flush()
}
