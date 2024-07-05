package io.github.chasencode.csmq.client;

import io.github.chasencode.csmq.model.CSMessage;
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

    public boolean send(String topic, CSMessage message) {
       return broker.send(topic,message);
    }
}
