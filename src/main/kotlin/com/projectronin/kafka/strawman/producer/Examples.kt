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
        // these 3 would likely always need to be specified, short of an uncomfortable amount of magic
        keyExtractor = KeyExtractor { p -> p.patientId() },
        typeExtractor = HeaderExtractor { p -> p.type() },
        subjectExtractor = HeaderExtractor { p -> "RoninPatient/${p.patientId()}" },
        // But these I think would eventually become magic. I think the source would just integrate with Spring Boot
        // or whatever and automatically become the Spring Boot service name. Or maybe it'd be the docker image url,
        // or who knows what -- but this should totally be able to just be a StandardSource() instance as a default.
        sourceExtractor = StaticValueExtractor("some-service"),
        // This I think would require some changes on the json schema gradle plugin side, but I'd like to see this be
        // introspectable from the class/instances. Like a SchemaVersionExtractor() instance that can look at a
        // PatientEventV1 instance and grab a static SCHEMA_VERSION field on it with reflection or something. Just in
        // general, the generated models are the authority of what this version is, so I don't want it to ever be
        // hard coded or config (unless that config _also_ controls what version of the schema is used, so the two are
        // always the same)
        schemaExtractor = StaticValueExtractor("patient/schema/v1/2023-01-26-sha"),
    )

    // Then where ever in your code, you can inject the patient producer and just throw patient events at it
    // without having to worry about the headers
    val event = PatientEventV1(create = Patient("123", "Corbin"))
    patientProducer.send(event)
}
