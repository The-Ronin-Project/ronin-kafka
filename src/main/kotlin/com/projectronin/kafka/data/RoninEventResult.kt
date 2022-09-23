package com.projectronin.kafka.data

enum class RoninEventResult {
    /**
     * The [RoninEvent] was processed successfully and should be acknowledged as processed
     * (AKA commit it's kafka offset).
     */
    ACK,

    /**
     * There was a transient issue processing the [RoninEvent] and it should be re-sent.
     */
    TRANSIENT_FAILURE,

    /**
     * The [RoninEvent] cannot be processed, exit processing
     */
    PERMANENT_FAILURE,
}
