package com.projectronin.kafka.exceptions

import com.projectronin.kafka.data.RoninEventResult
import kotlin.reflect.KClass

/**
 * Occurs with [com.projectronin.kafka.RoninConsumer]s when one of the [Ronin Event Standard] fields is missing from the Kafka record headers.
 *
 * See Also: [Ronin Event Standard](https://projectronin.atlassian.net/wiki/spaces/ENG/pages/1748041738/Ronin+Event+Standard)
 */
class EventHeaderMissing(missingHeaders: List<String>, val topic: String? = null) : RuntimeException(
    "Unable to process event. The following headers are required: ${missingHeaders.joinToString(", ")}"
)

/**
 * Occurs when a Kafka message is received with a `ce_type` header that the [com.projectronin.kafka.RoninConsumer]'s
 * [com.projectronin.kafka.RoninConsumer.typeMap] does not have an entry for.
 */
class UnknownEventType(val key: String, val type: String?, val topic: String? = null) : RuntimeException("No processor found for event type `$type`")

/**
 * Occurs when a process handler returns [RoninEventResult.TRANSIENT_FAILURE] more than
 * `ronin.handler.transient.retries` times
 */
class TransientRetriesExhausted(attempts: Int) :
    RuntimeException("Skipping event after $attempts transient failures")

class DeserializationException(val type: String, valueClass: KClass<*>) :
    RuntimeException("There was an exception attempting to deserialize the message for the type $type into the class $valueClass")
