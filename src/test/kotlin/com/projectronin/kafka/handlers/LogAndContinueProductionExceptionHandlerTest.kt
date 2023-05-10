package com.projectronin.kafka.handlers

import org.apache.kafka.streams.errors.ProductionExceptionHandler
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test

class LogAndContinueProductionExceptionHandlerTest {
    val handler = LogAndContinueProductionExceptionHandler()
    @Test
    fun `configure doesn't do diddly`() {
        handler.configure(HashMap<String, Any?>())
        // No exceptions were thrown!
    }

    @Test
    fun `handle logs the error and continues`() {
        val response = handler.handle(null, Exception("some exception"))
        assertEquals(ProductionExceptionHandler.ProductionExceptionHandlerResponse.CONTINUE, response)
    }
}
