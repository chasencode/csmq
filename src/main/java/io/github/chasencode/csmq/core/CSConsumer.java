package io.github.chasencode.csmq.core;

/**
 * message consumer
 * @Program: csmq
 * @Description: message consumer
 * @Author: Chasen
 * @Create: 2024-06-25 21:04
 **/
public class CSConsumer<T> {


    CSBroker broker;
    String topic;
    CSMq mq;

    public CSConsumer(CSBroker broker) {
        this.broker = broker;
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
