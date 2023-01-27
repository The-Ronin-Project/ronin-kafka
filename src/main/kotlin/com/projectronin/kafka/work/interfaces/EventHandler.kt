package com.projectronin.kafka.work.interfaces

import com.projectronin.kafka.data.RoninEvent
import com.projectronin.kafka.data.RoninEventResult

fun interface EventHandler {

    fun handle(event: RoninEvent<*>, topic: String): RoninEventResult
}
