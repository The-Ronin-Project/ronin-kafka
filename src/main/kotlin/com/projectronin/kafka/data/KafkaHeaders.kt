package com.projectronin.kafka.data

object KafkaHeaders {
    const val id = "ce_id"
    const val time = "ce_time"
    const val specVersion = "ce_specversion"
    const val dataSchema = "ce_dataschema"
    const val contentType = "content-type"
    const val source = "ce_source"
    const val type = "ce_type"
    const val subject = "ce_subject"
    const val tenantId = "ronin_tenant_id"
    const val patientId = "ronin_patient_id"
    const val resourceVersion = "ronin_resourceversion"

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
