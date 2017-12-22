package com.julianovidal.kafka;

import com.julianovidal.kafka.listener.KafkaSubscriptionListener;

public final class KafkaSubscriptionFactory {

    private static final KafkaConnector KAFKA_CONNECTOR = new KafkaConnector();

    private KafkaSubscriptionFactory() {}

    public static void subscribe(final String topicName, KafkaSubscriptionListener listener) {
        KAFKA_CONNECTOR.subscribe(topicName, listener);
    }
}
