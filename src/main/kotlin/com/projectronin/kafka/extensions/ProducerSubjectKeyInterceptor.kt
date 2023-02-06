package com.projectronin.kafka.extensions

import com.projectronin.kafka.data.RoninEvent
import org.apache.kafka.clients.producer.ProducerInterceptor
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.clients.producer.RecordMetadata

class ProducerSubjectKeyInterceptor : ProducerInterceptor<String, RoninEvent<*>> {
    override fun configure(configs: MutableMap<String, *>?) {
        // Implement as needed
    }

    override fun onAcknowledgement(metadata: RecordMetadata?, exception: Exception?) {
        // Implement as needed
    }

    override fun close() {
        // Implement as needed
    }

    override fun onSend(record: ProducerRecord<String, RoninEvent<*>>): ProducerRecord<String, RoninEvent<*>> {
        if (record.value() == null) {
            return record
        }

        if (record.key() != null) {
            return record
        }

        return ProducerRecord<String, RoninEvent<*>>(
            record.topic(),
            record.partition(),
            record.timestamp(),
            record.value().subject,
            record.value(),
            record.headers()
        )
    }
}
