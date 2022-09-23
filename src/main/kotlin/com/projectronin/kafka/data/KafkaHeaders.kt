package com.projectronin.kafka.data

object KafkaHeaders {
    const val id = "ce_id"
    const val time = "ce_time"
    const val specVersion = "ce_specversion"
    const val dataSchema = "ce_dataschema"
    const val contentType = "content-type"
    const val source = "ce_source"
    const val type = "ce_type"

    val required = listOf(
        id,
        time,
        specVersion,
        dataSchema,
        contentType,
        source,
        type
    )
}
