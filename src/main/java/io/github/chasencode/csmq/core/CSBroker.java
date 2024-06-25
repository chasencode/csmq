package io.github.chasencode.csmq.core;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * broker for topics
 * @Program: csmq
 * @Description:
 * @Author: Chasen
 * @Create: 2024-06-25 20:52
 **/
public class CSBroker {
    Map<String, CSMq> mpMapping = new ConcurrentHashMap<>(64);

    public CSMq find(String topic) {
        return mpMapping.get(topic);
    }

    public CSMq createTopic(String topic) {
        return mpMapping.putIfAbsent(topic, new CSMq(topic));
    }

    public CSProducer createProducer() {
        return new CSProducer(this);
    }

    public CSConsumer<?> createConsumer(String topic) {
        final CSConsumer<?> csConsumer = new CSConsumer(this);
        csConsumer.subscribe(topic);
        return csConsumer;
    }
}
