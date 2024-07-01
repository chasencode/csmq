package io.github.chasencode.csmq.demo;

import io.github.chasencode.csmq.core.CSBroker;
import io.github.chasencode.csmq.core.CSConsumer;
import io.github.chasencode.csmq.model.CSMessage;
import io.github.chasencode.csmq.core.CSProducer;

import java.io.IOException;

/**
 * @Program: csmq
 * @Description:
 * @Author: Chasen
 * @Create: 2024-06-25 21:10
 **/
public class CSMqDemo {

    public static void main(String[] args) throws IOException {
        long ids = 0;
        String topic = "cs.order";
        CSBroker broker = new CSBroker();
        broker.createTopic(topic);

        final CSProducer csProducer = broker.createProducer();
        final CSConsumer<?> csConsumer = broker.createConsumer(topic);
        csConsumer.listen(message -> {
            System.out.println(" onMessage =>" + message);
        });



//        csConsumer.subscribe(topic);
//        for (int i = 0; i < 10; i++) {
//            Order order = new Order((long) ids, "time" + ids, 100 * ids);
//            csProducer.send(topic, new CSMessage((long) ids++, order, null));
//        }
//        for (int i = 0; i < 10; i++) {
//            final CSMessage<Order> poll = (CSMessage<Order>) csConsumer.poll(1000);
//            System.out.println(poll);
//        }

        while (true) {
            char read = (char) System.in.read();
            if (read == 'q' || read == 'e') {
                break;
            }
            if (read == 'p') {
                Order order = new Order(ids, "time" + ids, 100 * ids);
                csProducer.send(topic, new CSMessage((long) ids++, order, null));
                System.out.println("send ok=" + order);
            }
            if (read == 'c') {
                final CSMessage<Order> poll = (CSMessage<Order>) csConsumer.poll(1000);
                System.out.println("poll ok=" + poll);
            }
            if (read == 'a') {
                for (int i = 0; i < 10; i++) {
                    Order order = new Order((long) ids, "time" + ids, 100 * ids);
                    final boolean send = csProducer.send(topic, new CSMessage((long) ids++, order, null));
                    System.out.println("这里2" + send);
                }
            }
        }
    }
}
