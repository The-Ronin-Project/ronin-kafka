package com.projectronin.kafka.serde

import com.projectronin.kafka.data.RoninEvent
import org.apache.kafka.common.serialization.Deserializer
import org.apache.kafka.common.serialization.Serde
import org.apache.kafka.common.serialization.Serializer

class RoninEventSerde<T> : Serde<RoninEvent<T>> {
    private val serializer = RoninEventSerializer<T>()
    private val deserializer = RoninEventDeserializer<T>()

    override fun serializer(): Serializer<RoninEvent<T>> {
        return serializer
    }

    override fun deserializer(): Deserializer<RoninEvent<T>> {
        return deserializer
    }

    override fun configure(configs: MutableMap<String, *>, isKey: Boolean) {
        serializer.configure(configs, isKey)
        deserializer.configure(configs, isKey)
    }

    override fun close() {
        serializer.close()
        deserializer.close()
    }
}
