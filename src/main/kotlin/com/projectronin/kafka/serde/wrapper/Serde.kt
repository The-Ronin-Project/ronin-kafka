package com.projectronin.kafka.serde.wrapper

import com.projectronin.kafka.data.RoninWrapper
import org.apache.kafka.common.serialization.Serde

class Serde<T> : Serde<RoninWrapper<T>> {
    private val serializer = com.projectronin.kafka.serde.wrapper.Serializer<T>()
    private val deserializer = com.projectronin.kafka.serde.wrapper.Deserializer<T>()

    override fun serializer(): org.apache.kafka.common.serialization.Serializer<RoninWrapper<T>> {
        return serializer
    }

    override fun deserializer(): org.apache.kafka.common.serialization.Deserializer<RoninWrapper<T>> {
        return deserializer
    }

    override fun configure(configs: MutableMap<String, *>?, isKey: Boolean) {
        serializer.configure(configs, isKey)
        deserializer.configure(configs, isKey)
    }

    override fun close() {
        serializer.close()
        deserializer.close()
    }
}
