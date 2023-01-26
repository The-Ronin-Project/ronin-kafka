import com.fasterxml.jackson.databind.ObjectMapper
import com.projectronin.kafka.config.MapperFactory
import com.projectronin.kafka.data.KafkaHeaders
import com.projectronin.kafka.data.RoninEvent
import com.projectronin.kafka.exceptions.EventHeaderMissing
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.header.Headers
import org.apache.kafka.common.serialization.Deserializer
import org.apache.kafka.common.serialization.Serde
import org.apache.kafka.common.serialization.Serializer
import java.time.Instant

class EventBodyMalformedException(key: String, cause: Throwable?) :
    RuntimeException(cause = cause, message = "The body of the message either couldn't be deserialized or was an unexpected type: $key")

class RoninEventDecoder(
    private val mapper: ObjectMapper = MapperFactory.mapper
) {
    /**
     * Translate a [ConsumerRecord] to a [RoninEvent].
     *
     * @param record The ConsumerRecord<String, ByteArray> as received from Kafka that will be converted to a [RoninEvent]
     * @return the [RoninEvent] equivalent of the provided [ConsumerRecord]
     * @throws EventHeaderMissing if the message header is missing any required field
     * @throws
     */
    fun <T> parseRecord(record: ConsumerRecord<String, ByteArray>, eventDataClass: Class<T>): RoninEvent<T> {
        val headers = parseHeaders(record.headers())
        validateHeaders(headers)
        return toRoninEvent(headers, record.key(), record.value(), eventDataClass)
    }

    inline fun <reified T> parseRecord(record: ConsumerRecord<String, ByteArray>): RoninEvent<T> {
        return parseRecord(record, T::class.java)
    }

    /**
     * Parse kafka [ConsumerRecord] headers out into an easier to use map
     * @return a key/value map of strings representing the headers
     */
    private fun parseHeaders(headers: Headers): Map<String, String> =
        headers
            .filter { it.value() != null && it.value().isNotEmpty() }
            .associate { it.key() to it.value().decodeToString() }

    /**
     * Validate the headers contain all Ronin Standard Event fields as needed.
     * See Also: [Ronin Event Standard](https://projectronin.atlassian.net/wiki/spaces/ENG/pages/1748041738/Ronin+Event+Standard)
     *
     * @param headers key/value map of strings containing the record headers
     * @throws EventHeaderMissing If any of the required headers are missing or blank
     */
    private fun validateHeaders(headers: Map<String, String>) {
        headers
            .keys
            .let { KafkaHeaders.required - it }
            .let {
                if (it.isNotEmpty()) {
                    throw EventHeaderMissing(it)
                }
            }
    }

    /**
     * Deserializes the record payload and shoves it and the headers into a RoninEvent.
     */
    private fun <T> toRoninEvent(
        headers: Map<String, String>,
        key: String,
        value: ByteArray,
        valueClass: Class<T>
    ): RoninEvent<T> =
        RoninEvent(
            id = headers.getValue(KafkaHeaders.id),
            time = Instant.parse(headers.getValue(KafkaHeaders.time)),
            specVersion = headers.getValue(KafkaHeaders.specVersion),
            dataSchema = headers.getValue(KafkaHeaders.dataSchema),
            dataContentType = headers.getValue(KafkaHeaders.contentType),
            source = headers.getValue(KafkaHeaders.source),
            type = headers.getValue(KafkaHeaders.type),
            subject = key,
            data = try {
                mapper.readValue(value, valueClass)
            } catch (e: Exception) {
                throw EventBodyMalformedException(key, e)
            },
        )
}

class RoninEventSerde : Serde<RoninEvent<*>> {
    override fun serializer(): Serializer<RoninEvent<*>> {
        TODO("Not yet implemented")
    }

    override fun deserializer(): Deserializer<RoninEvent<*>> {
        TODO("Not yet implemented")
    }
}
