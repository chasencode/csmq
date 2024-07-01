package io.github.chasencode.csmq.core;

import io.github.chasencode.csmq.model.CSMessage;

import java.util.concurrent.atomic.AtomicInteger;

/**
 * message consumer
 * @Program: csmq
 * @Description: message consumer
 * @Author: Chasen
 * @Create: 2024-06-25 21:04
 **/
public class CSConsumer<T> {

    private String id;
    CSBroker broker;
    String topic;
    CSMq mq;

    static AtomicInteger idgen = new AtomicInteger(0);

    public CSConsumer(CSBroker broker) {
        this.broker = broker;
        this.id = "CID" + idgen.getAndIncrement();
    }

    public void subscribe(String topic) {
        this.topic = topic;
        mq = broker.find(topic);
        if (mq == null) {
            throw new RuntimeException("Topic not found: " + topic);
        }
    }

    public CSMessage<T> poll(long timeout) {
        return mq.poll(timeout);
    }

    public void listen(CSListener<T> listener) {
        mq.addListener(listener);
    }
}
