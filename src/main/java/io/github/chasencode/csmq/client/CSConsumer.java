package io.github.chasencode.csmq.client;

import io.github.chasencode.csmq.demo.Order;
import io.github.chasencode.csmq.model.CSMessage;
import lombok.Getter;

import java.util.HashMap;
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

    static AtomicInteger idgen = new AtomicInteger(0);

    public CSConsumer(CSBroker broker) {
        this.broker = broker;
        this.id = "CID" + idgen.getAndIncrement();
    }

    public void sub(String topic) {
       broker.sub(topic, id);
    }


    public void unsub(String topic) {
        broker.unsub(topic, id);
    }


    public CSMessage<T> recv(String top) {
        return broker.recv(top, id);
    }

    public boolean ack(String topic, int offset) {
        return broker.ack(topic, id, offset);
    }


    public boolean ack(String topic, CSMessage<?> message) {
        if (message.getHeaders() == null) {
            message.setHeaders(new HashMap<>());
        }
        final int  offset = Integer.parseInt(message.getHeaders().get("X-offset"));
        return ack(topic, offset);
    }

    public void listen(String topic,CSListener<T> listener) {
        this.listener = listener;
        broker.addConsumer(topic,this);
    }
    @Getter
    private CSListener listener;

}
