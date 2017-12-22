package com.julianovidal.kafka;

import com.julianovidal.kafka.listener.KafkaSubscriptionListener;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.TimeUnit;

public class KafkaSubscription<T> implements Runnable {

    private static final Logger LOGGER = LoggerFactory.getLogger(KafkaSubscription.class);

    private final String topicName;
    private final KafkaConsumer<String, String> consumer;
    private final KafkaSubscriptionListener<T> subscriptionListener;

    KafkaSubscription(final String topicName, final KafkaConsumer<String, String> consumer, final KafkaSubscriptionListener<T> subscriptionListener) {
        this.topicName = topicName;
        this.consumer = consumer;
        this.subscriptionListener = subscriptionListener;
    }

    @Override
    public void run() {
        do {

            final ConsumerRecords<String, String> records = consumer.poll(500);
            if (!records.isEmpty()) {
                records.records(topicName).iterator().forEachRemaining(record -> {
                    subscriptionListener.onMessage((T)record.value());

                });
                consumer.commitAsync();
            }

            try {
                TimeUnit.MILLISECONDS.sleep(500);
            } catch (InterruptedException e) {
                LOGGER.error(e.getMessage(), e);
            }

        } while(true);
    }

}
