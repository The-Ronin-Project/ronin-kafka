package com.projectronin.kafka.data

data class RoninWrapper<T>(
    val wrapperVersion: String = "1",
    val sourceService: String,
    val tenantId: String,
    val dataType: String,
    val data: T
) {
    constructor(headers: Map<String, String>, data: T) : this(
        wrapperVersion = headers[Headers.wrapperVersion]!!,
        sourceService = headers[Headers.sourceService]!!,
        tenantId = headers[Headers.tenantId]!!,
        dataType = headers[Headers.dataType]!!,
        data = data
    )

    class Headers private constructor() {
        companion object {
            const val wrapperVersion = "ronin_wrapper_version"
            const val sourceService = "ronin_source_service"
            const val tenantId = "ronin_tenant_id"
            const val dataType = "ronin_data_type"

            val required = listOf(
                wrapperVersion,
                sourceService,
                tenantId,
                dataType
            )
        }
    }
}
