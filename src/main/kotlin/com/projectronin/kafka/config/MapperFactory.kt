package com.projectronin.kafka.config

import com.fasterxml.jackson.databind.DeserializationFeature
import com.fasterxml.jackson.databind.SerializationFeature
import com.fasterxml.jackson.datatype.jdk8.Jdk8Module
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule
import com.fasterxml.jackson.module.kotlin.jsonMapper
import com.fasterxml.jackson.module.kotlin.kotlinModule
import com.fasterxml.jackson.module.paramnames.ParameterNamesModule

@Deprecated("Library has been replaced by ronin-common kafka")
object MapperFactory {
    val mapper by lazy {
        jsonMapper {
            addModule(kotlinModule())
            addModule(ParameterNamesModule())
            addModule(JavaTimeModule())
            addModule(Jdk8Module())

            configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false)
            // see [com.fasterxml.jackson.databind.util.StdDateFormat]
            // For serialization defaults to using an ISO-8601 compliant format
            // (format String "yyyy-MM-dd'T'HH:mm:ss.SSSZ") and for deserialization, both ISO-8601 and RFC-1123.
            configure(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS, false)
        }
    }
}
