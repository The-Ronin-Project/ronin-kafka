import com.projectronin.kafka.data.RoninEvent
import java.time.Instant

data class Van(
    override val id: String,
    val make: String,
    val model: String,
    val style: Style,
    val created: Instant = Instant.now(),
) : RoninEvent.Data<String> {
    enum class Style {
        CAMPER, CREEPER, DELIVERY
    }
}
