package com.julianovidal.kafka;

import com.julianovidal.kafka.listener.KafkaSubscriptionListener;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Arrays;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;

public class KafkaConnector {

    private final Map<String, KafkaConsumer> registeredConsumers;
    private volatile Object lock = new Object();
    private KafkaProducer<String, String> kafkaProducer;

    public KafkaConnector() {
        registeredConsumers = new ConcurrentHashMap<>();
    }

    private Properties loadConsumerProperties() {
        final Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "consumer-01");
        return props;
    }

    private Properties loadProducerProperties() {
        final Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        return props;
    }

    private void registerConsumer(final String topicName, final KafkaConsumer kafkaConsumer) {
        registeredConsumers.put(topicName, kafkaConsumer);
    }

    public <T> void subscribe(final String topicName, final KafkaSubscriptionListener<T> subscriptionListener) {
        if (!registeredConsumers.containsKey(topicName)) {
            final Properties consumerProperties = loadConsumerProperties();
            KafkaConsumer<String, String> kafkaConsumer = new KafkaConsumer<>(consumerProperties);
            kafkaConsumer.subscribe(Arrays.asList(topicName));
            registerConsumer(topicName, kafkaConsumer);
        }

        KafkaConsumer<String, String> kafkaConsumer = registeredConsumers.get(topicName);
        Executors.newCachedThreadPool().submit(new KafkaSubscription(topicName, kafkaConsumer, subscriptionListener));
    }


    public <T> KafkaPublisher<T> createPublisher(Class<? super T> type, String topicName) {
        synchronized (lock) {
            if (kafkaProducer == null) {
                final Properties producerProperties = loadProducerProperties();
                kafkaProducer = new KafkaProducer<>(producerProperties);
            }
        }

        return new KafkaPublisher<>(topicName, kafkaProducer);
    }

}
