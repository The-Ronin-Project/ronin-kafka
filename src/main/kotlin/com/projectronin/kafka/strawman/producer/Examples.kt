package com.projectronin.kafka.strawman.producer

import com.projectronin.kafka.config.RoninProducerKafkaProperties
import java.lang.IllegalStateException

data class Patient(val id: String, val name: String)
data class PatientDelete(val id: String)

data class PatientEventV1(
    val create: Patient? = null,
    val update: Patient? = null,
    val delete: PatientDelete? = null,
) {
    fun patientId(): String {
        // pretend this isn't janky
        if (create != null) {
            return create.id
        }
        if (update != null) {
            return update.id
        }
        if (delete != null) {
            return delete.id
        }
        throw IllegalStateException()
    }

    fun type(): String {
        if (create != null) {
            return "create"
        }
        if (update != null) {
            return "update"
        }
        if (delete != null) {
            return "delete"
        }
        throw IllegalStateException()
    }
}

fun magicProducerExample() {
    // This would be a bean in your spring config somewhere
    // and likely wrapped up in a ronin-kafka-spring module eventually to get autowired
    val producerFactory = RoninKafkaProducerFactory(
        RoninProducerKafkaProperties("bootstrap.servers" to "localhost:9092"), RoninKafkaSerializerFactory()
    )

    // This would be a bean in your service's spring config
    val patientProducer = producerFactory.createRoninProducer(
        topic = "emr.patient.v1",
        type = PatientEventV1::class,
        keyExtractor = KeyExtractor { p -> p.patientId() },
        typeExtractor = HeaderExtractor { p -> p.type() },
        sourceExtractor = StaticValueExtractor("some-service"),
        schemaExtractor = StaticValueExtractor("patient/schema/v1/2023-01-26-sha"),
        subjectExtractor = HeaderExtractor { p -> "RoninPatient/${p.patientId()}" }
    )

    // Then where ever in your code, you can inject the patient producer and just throw patient events at it
    // without having to worry about the headers
    val event = PatientEventV1(create = Patient("123", "Corbin"))
    patientProducer.send(event)
}
