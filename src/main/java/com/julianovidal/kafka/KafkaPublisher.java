package com.julianovidal.kafka;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import java.util.concurrent.Future;

public class KafkaPublisher<T> {

    private final String topicName;
    private final KafkaProducer<String, String> producer;

    KafkaPublisher(final String topicName, final KafkaProducer<String, String> producer) {
        this.topicName = topicName;
        this.producer = producer;
    }

    public void publish(T data) {
        final ProducerRecord<String, String> producerRecord = new ProducerRecord<>(topicName, data.toString());
        final Future<RecordMetadata> future = producer.send(producerRecord);
        producer.flush();
    }
}
