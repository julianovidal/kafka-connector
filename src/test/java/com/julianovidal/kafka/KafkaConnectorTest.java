package com.julianovidal.kafka;

import com.julianovidal.kafka.listener.KafkaSubscriptionListener;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.TimeUnit;

public class KafkaConnectorTest {

    private static final Logger LOGGER = LoggerFactory.getLogger(KafkaConnectorTest.class);

    @Test
    public void subscriptionTest() {

        KafkaSubscriptionFactory.subscribe("test", (KafkaSubscriptionListener<String>) message -> LOGGER.info(message.toString()));

        final KafkaConnector kafkaConnector = new KafkaConnector();
        KafkaPublisher<String> kafkaPublisher = kafkaConnector.createPublisher(String.class, "test");

        for (int i = 0; i < 10; i++) {
            LOGGER.info("Producing value " + i);

            kafkaPublisher.publish(String.valueOf(i));

            try {
                TimeUnit.SECONDS.sleep(1);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }

        try {
            Thread.sleep(100000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

}
