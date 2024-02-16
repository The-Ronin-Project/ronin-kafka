# Module ronin-kafka [Deprecated - No Longer Maintained]

`ronin-kafka` is a messaging library for sending and receiving `RoninEvent` objects via kafka. A
`RoninEvent` object is a data class that provides
the [Ronin Event Standard](https://projectronin.atlassian.net/l/cp/Ts2DgMuT)
metadata attributes and a `data` element of any type for storing an event payload.

# Status

Currently a work-in-progress, but usable for non-production systems at this point.

The TODO list:

1. Document usage and example projects
    1. ~~`README.md` usage~~
    2. ~~Probably need a spring-boot example for both producer and consumer? Currently just have simple kotlin apps~~
2. Setup GHA workflows for
    1. ~~unit tests~~
    2. ~~codecov~~
    3. ~~deployment to maven~~
3. ~~Review logging messages and levels~~
4. `RoninProducer`
    1. Add some exception handling
    2. ~~Metrics~~
    3. Add support for custom partitioner
5. `RoninConsumer`
    1. ~~Metrics~~
    2. Add support for consumer re-balance events?
6. Test out access patterns and usability with product engineering
7. Confirm/tweak default Kafka configurations
8. Tie into Kafka MBeans for stock kafka producer and consumer client metrics

# Local dev

To setup a local development environment for modifying `ronin-kafka`...

NOTE: Only Step 1 is necessary if you just want a local cluster to test other kafka-using services

1. run a local kafka cluster: `docker compose -f examples/docker-compose-kafka.yaml up -d`
2. build: `./gradlew clean build`
3. run the example consumer application: `./gradlew examples:consumer-application:run`
4. run the example producer application: `./gradlew examples:producer-application:run`
5. Optionally access Kafka UI at `http://localhost:8888/` for an easy way to explore topics/messages/consumers

Additionally, you can run the spring-boot example application like so:

`SPRING_PROFILES_ACTIVE=local ./gradlew examples:spring-boot-app:bootRun`

The `local` profile points at `application-local.yml` and connects to your kafka on `localhost:9092`. If you'd like
to connect to the dev kafka cluster, you can use `SPRING_PROFIELS_ACTIVE=cloud,secret`. `application-cloud.yml` has
the cloud connection details. You'll have to manually create `application-secret.yml` with the following for the
username and password:

```yaml
kafka:
  sasl:
    username: <USERNAME>
    password: <PASSWORD>
```

# Usage

## RoninProducer

`RoninProducer` is a simple wrapper around standard `KafkaProducer` functionality meant to simplify configuration
and standardization of messaging `RoninEvent`s over Kafka message queues at Project Ronin.

### Configuration

The `RoninProducerKafkaProperties` class provides a starting place for configuring a
Kafka producer. When creating a `RoninProducer`, you can override or append any additional Kafka configuration that is
needed. If you're not very familiar with kafka and configuring `KafkaProducer`s, you probably don't need to override
anything. At a minimum, you will need to provide the `bootstrap.servers` configuration.

[Standard Kafka producer configuration reference](https://docs.confluent.io/platform/current/installation/configuration/producer-configs.html).

A minimal `RoninProducer` creation would look something like this:

```kotlin
val roninProducer =
    RoninProducer(
        topic = "local.us.your-system.your-event-type.v1",
        source = "your-producer-application",
        dataSchema = "https://link-to-your-json-schema",
        kafkaProperties = RoninProducerKafkaProperties("bootstrap.servers" to "localhost:9092")
    )
```

Ronin standard topic naming conventions can be found [here](https://projectronin.atlassian.net/l/cp/GcKLBQu3)

For details on the Ronin Event Standard and fields used such as `source` and `dataSchema`
, [go here](https://projectronin.atlassian.net/l/cp/Ts2DgMuT)

### `RoninProducer` parameters

When creating a `RoninProducer`, the following parameters are available:

* `topic`: Required. The kafka topic to send events on
* `source`: Required. The name of the application producing events
* `dataSchema`: Required. The schema that validates all events sent via this producer
* `kafkaProperties`: Required. An instance of `RoninProducerKafkaProperties` with all configurations
* `specVersion`: Optional. Defaults to `1.0`. The default should always be used
* `dataContentType`: Optional. Defaults to `application/json` and should be left on the default
* `mapper`: Optional. Defaults to a Jackson Object Mapper as seen in the `MapperFactory`
* `kafkaProducer`: Optional. Defaults to a `KafkaProducer<String, ByteArray>` configured via `kafkaProperties`
* `meterRegistry`: Optional. Defaults to `null`. Provide a micrometer `MeterRegistry` if you'd like metrics from the
  producer

See the [Ronin Event Standard] for details on `source`, `dataSchema`, `SpecVersion`, `dataContentType` and other event
fields.

### Sending `RoninEvent`s

There are two options available for sending an event via `RoninProducer`...

If the auto-generated `RoninEvent` attributes are acceptable, you can just pass in some metadata and an instance
of a class:

```kotlin
roninProducer.send(
    "wing.flown",
    "wing/42",
    object {
        val id: Int = 42
    }
)
```

Alternatively, if you need full control over the `RoninEvent` object that gets sent, you can construct it yourself
and pass it in:

```kotlin
roninProducer.send(
    RoninEvent(
        id = "dec03a73-71ff-463d-b271-0ace067dc35d",
        time = Instant.ofEpochSecond(1660000000),
        specVersion = "1.0",
        contentType = "application/json",
        source = "producer-application",
        dataSchema = "https://not-a-schema",
        type = "wing.flown",
        subject = "wing/42",
        data = object {
            val id: Int = 42
        }
    )
)
```

## RoninConsumer

`RoninConsumer` is a simple wrapper around standard `KafkaConsumer` functionality meant to simplify configuration
and standardization of messaging `RoninEvent`s over Kafka message queues at Project Ronin.

### Configuration

The `RoninConsumerKafkaProperties` class provides a starting place for configuring a
Kafka consumer. When creating a `RoninConsumer`, you can override or append any additional Kafka configuration that is
needed. If you're not very familiar with kafka and configuring `KafkaConsumer`s, you probably don't need to override
anything. At a minimum, you will need to provide the `bootstrap.servers` and `group.id` configuration.

[Standard Kafka consumer configuration reference](https://docs.confluent.io/platform/current/installation/configuration/consumer-configs.html)

#### Additional Ronin specific configuration

* `ronin.handler.transient.retries`: Indicates how many times an event should be re-sent to the consuming application's
  handler lambda if it returns `RoninEventResult.TRANSIENT_FAULRE` Default: 3.

A minimal `RoninConsumer` creation would look something like this:

```kotlin
 val roninConsumer =
    RoninConsumer(
        topics = listOf("local.us.your-system.your-event-type.v1"),
        typeMap = mapOf<String, KClass<out Any>>(
            "type.1" to Type1Class::class,
            "type.2" to Type2Class::class
        ),
        kafkaProperties = RoninConsumerKafkaProperties(
            "bootstrap.servers" to "localhost:9092",
            "group.id" to "your-system-consumer-group",
        )
    )
```

### `RoninConsumer` parameters

When creating a `RoninConsumer`, the following parameters are available:

* `topics`: Required. A list of topics to consume from
* `typeMap`: Required. A map from the Ronin Event Standard's `type` attribute to a Kotlin class for deserializing into
* `kafkaProperties`: Required. An instance of `RoninConsumerKafkaProperties` with all configurations
* `mapper`: Optional. Defaults to a Jackson Object Mapper as seen in the `MapperFactory`
* `kafkaConsumer`: Optional. Defaults to a `KafkaConsumer<String, ByteArray>` configured via `kafkaProperties`
* `exceptionHandler`: Optional. Defaults to null. If you want to be notified of exceptions, pass this in for callbacks.
* `meterRegistry`: Optional. Defaults to `null`. Provide a micrometer `MeterRegistry` if you'd like metrics from the
  consumer

### Receiving `RoninEvent`s

To receive `RoninEvent`s off of the kafka topic, create a `RoninConsumer` and invoke the `process` function. This is
a blocking function that will loop continuously until `stop()` is called or an unhandled exception is encountered.

```kotlin
 roninConsumer
    .process {
        logger.info { "received event ${it.subject} [${it.id}]" }
        RoninEventResult.ACK
    }
```

Your process lambda should return a `RoninEventResult` indicating one of the following:

* `ACK`: The event was processed successfully and should be acknowledged as processed. This will commit the kafka offset
  for the record
* `TRANSIENT_FAILURE`: A transient failure was encountered while processing and the event should be re-sent. Events are
  resent immediately. The `RoninConsumer` will re-send the event a maximum of `ronin.handler.transient.retries` times (
  default: 3).
  After that, the exception handler will be called and the event will be skipped.
* `PERMANENT_FAILURE`: A permanent failure was encountered while processing the event and the consumer should exit.

### Exception handling

The `RoninConsumer`'s `exceptionHandler` will be called for two distinct event issues:

* `recordHandlingException`: This callback will be called if there is any issue with translating the raw kafka
  record into a `RoninEvent` object. This includes missing of required headers as well as deserialization issues.
* `eventProcessingException`: This callback will be called if there are exhausted transient retries or unhandled
  exceptions
  by the consumer's handler function

## Serde

If you'd like to consume the serialization/deserialization functionality of `RoninEvent` directly, you can via either a
`Serde` (`RoninEventSerde`) or via the direct classes. Either way you go about it, the important thing to be aware of
is that deserialization must be configured to know what types are expected. This is done based on either the type header
on the event, or per topic (or a mix). For example:

```properties
# On a type basis -- these types may be on either the same topic or separate
ronin.json.deserializer.types=some.type:com.projectronin.foo.SomeClass,other.type:com.projectronin.foo.OtherClass

# Or on a topic basis -- this requires that all events on a topic are always the same type.
# For example, some.topic must _always_ have events on it that are deserializable to a com.projectronin.foo.Foo instance
ronin.json.deserializer.topics=some.topic:com.projectronin.foo.Foo,other.topic:com.projectronin.foo.Bar
```
