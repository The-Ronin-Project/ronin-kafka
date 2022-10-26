package com.projectronin.kafka.examples.data

data class Wing(
    val id: Int,
    val model: String? = null,
    val area: Int,
    val aspectRatio: Float
)
