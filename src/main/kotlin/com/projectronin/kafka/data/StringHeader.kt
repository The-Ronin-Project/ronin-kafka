package com.projectronin.kafka.data

/**
 * helper class for translating RoninEvent properties to kafka headers
 */
data class StringHeader(val key: String, val value: String) : org.apache.kafka.common.header.Header {
    override fun key() = key
    override fun value() = value.toByteArray(Charsets.UTF_8)
}
