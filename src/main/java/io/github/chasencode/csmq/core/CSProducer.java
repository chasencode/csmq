package io.github.chasencode.csmq.core;

import lombok.AllArgsConstructor;

/**
 * @Program: csmq
 * @Description: 生产者
 * @Author: Chasen
 * @Create: 2024-06-25 20:51
 **/
@AllArgsConstructor
public class CSProducer {

    CSBroker broker;

    public boolean send(String topic,CSMessage message) {
        CSMq mq = broker.find(topic);
        if (mq == null) {
            throw new RuntimeException("Topic not found: " + topic);
        }
        return mq.send(message);
    }
}
