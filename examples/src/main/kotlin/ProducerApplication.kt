import com.projectronin.kafka.RoninProducer
import com.projectronin.kafka.config.KafkaProperties
import com.projectronin.kafka.data.RoninEvent
import java.time.LocalDateTime
import java.time.format.DateTimeFormatter
import kotlin.random.Random

fun Wing.toEvent(type: String): RoninEvent<Wing> =
    RoninEvent(
        source = "ronin-kafka-examples",
        dataSchema = "https://not-a-schema",
        data = this,
        type = type,
        subject = "wing/$id",
    )

fun main(args: Array<String>) {
    val roninProducer =
        RoninProducer(
            topic = "topic.1",
            source = "my-app",
            dataSchema = "https://schema",
            kafkaProperties = KafkaProperties("bootstrap.servers" to "localhost:9092")
        )

    val formatter = DateTimeFormatter.ofPattern("E-A")
    val r = Random(System.currentTimeMillis())

    val van = Van(
        formatter.format(LocalDateTime.now()),
        "volkswagen",
        "vanagon",
        Van.Style.CAMPER
    )
    val wing = Wing(r.nextInt(), "zero3", area = 19, aspectRatio = 4.5f)

    roninProducer.send("van.cruising", "van/${van.id}", van)
    roninProducer.send("wing.flown", "wing/${wing.id}", wing)

    roninProducer.send(
        RoninEvent(
            source = "ronin-kafka-examples",
            dataSchema = "https://not-a-schema",
            data = Van("the beast", "chevy", "astrovan", Van.Style.CREEPER), type = "van.cruising",
            subject = "van/the beast",
        )
    )
    roninProducer.send(Wing(r.nextInt(), "zero3", area = 19, aspectRatio = 4.5f).toEvent("wing.flown"))

    // flush out any queued, but unsent events before we exit
    roninProducer.flush()
}
