package com.projectronin.kafka.examples.data

import com.projectronin.kafka.data.RoninEvent

data class Wing(
    override val id: Int,
    val model: String? = null,
    val area: Int,
    val aspectRatio: Float
) : RoninEvent.Data<Int>
