package com.projectronin.kafka.strawman.producer

import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.clients.producer.RecordMetadata
import java.util.concurrent.Future

/**
 * This is an example of a _very_ opinionated, _very_ magic producer. It's a bit of a pain to construct (though less so
 * with RoninKafkaProducerFactory), but once constructed, you just throw your actual event at it without having to worry
 * about keys, schemas, etc.
 *
 * The appeal of this to me is that the key, schema, etc are in a way higher level concerns in the same way that when
 * you're interacting with a SQL database, your UserRepository or UserDAO or whatever doesn't know what version the
 * user table is migrated to, it doesn't know what indexes exist, etc. It just thinks in pure User objects and the rest
 * of that is handled by liquibase and whatnot.
 *
 * I'm torn on whether this should be locked down to a single topic or not. I can't imagine in practice we'll be writing
 * the same type to more than 1 topic often, but... you never know.
 *
 * I'm even more torn on whether or not it should be locked down to a single type. In theory, as long as all of the
 * dependencies support a certain type, it could support many types.
 *
 * To an extent these two questions come down to whether we expect that people will often want to have a single producer
 * for their entire application, or if they'll want a producer per topic/type.
 */
class RoninProducer<T : Any>(
    private val topic: String,
    private val producer: KafkaProducer<String, T>,
    private val keyExtractor: KeyExtractor<T>,
    private val headersExtractor: HeadersExtractor<T>
) : AutoCloseable {
    fun send(event: T): Future<RecordMetadata> {
        val record = ProducerRecord<String, T>(
            topic,
            null,
            // TODO: is it reasonable to assume the key is always in the event (or that the user wants it to be null)??
            keyExtractor.determineKey(event), event, headersExtractor.determineHeaders(event).toHeaders()
        )
        return producer.send(record)
    }

    // TODO: how crazy would it be to let the user also just send a raw (but typed!) ProducerRecord?
    // That's an easy way to bypass key and header and whatnot quirks if they need to do something custom-ish without
    // dropping all the way to RoninProducer. But... on the other side of that coin, the factory should probably be capable
    // of just giving you a raw KafkaProducer<String, T> so this isn't needed.
    // fun send(ProducerRecord<String, T>) { ... }

    // TOOD: how bad of idea is this? Like how high volume of service can you get away with this before this is a
    // really, really stupid idea?
    fun sendSync(event: T): RecordMetadata {
        // KafkaProducer can choose to buffer messages, so we could wait forever if we just did future.get() without flushing
        // the downside is that flush() waits on _all_ pending messages to write, not just this one
        var future = send(event)
        flush()
        return future.get()
    }

    fun flush() = producer.flush()

    override fun close() = producer.close()
}
