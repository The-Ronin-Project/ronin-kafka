package com.projectronin.kafka.work.metrics

object ConsumerMetrics {
    const val POLL_TIMER = "roninkafka.consumer.poll"
    const val POLL_MESSAGE_DISTRIBUTION = "roninkafka.consumer.poll.messages"
    const val MESSAGE_QUEUE_TIMER = "roninkafka.consumer.message.queue.timer"
    const val MESSAGE_HANDLER_TIMER = "roninkafka.consumer.message.handler.timer"

    const val EXCEPTION_UNKNOWN_TYPE = "roninkafka.consumer.exceptions.unknowntype"
    const val EXCEPTION_MISSING_HEADER = "roninkafka.consumer.exceptions.missingheader"
    const val EXCEPTION_DESERIALIZATION = "roninkafka.consumer.exceptions.deserialization"

    const val HANDLER_ACK = "roninkafka.consumer.handler.ack"
    const val HANDLER_TRANSIENT_FAILURE = "roninkafka.consumer.handler.transient_failure"
    const val HANDLER_PERMANENT_FAILURE = "roninkafka.consumer.handler.permanent_failure"
    const val HANDLER_TRANSIENT_FAILURE_EXHAUSTED = "roninkafka.consumer.handler.transient_failure_exhausted"
    const val HANDLER_UNHANDLED_EXCEPTION = "roninkafka.consumer.handler.unhandled_exception"

    const val TOPIC_TAG = "topic"
}
