package com.julianovidal.kafka.listener;

public interface KafkaSubscriptionListener<T> {

    void onMessage(T message);
}
