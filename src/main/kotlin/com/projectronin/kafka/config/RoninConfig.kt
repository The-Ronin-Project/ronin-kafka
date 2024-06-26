package com.projectronin.kafka.config

class RoninConfig private constructor() {
    companion object {
        const val RONIN_DESERIALIZATION_TYPES_CONFIG = "ronin.json.deserializer.types"
        const val RONIN_DESERIALIZATION_TOPICS_CONFIG = "ronin.json.deserializer.topics"
        const val DEAD_LETTER_TOPIC_CONFIG = "ronin.dead.letter.topic"
    }
}
