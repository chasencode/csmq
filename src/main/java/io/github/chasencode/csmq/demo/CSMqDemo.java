package io.github.chasencode.csmq.demo;

import com.alibaba.fastjson.JSON;
import io.github.chasencode.csmq.client.CSBroker;
import io.github.chasencode.csmq.client.CSConsumer;
import io.github.chasencode.csmq.model.CSMessage;
import io.github.chasencode.csmq.client.CSProducer;

import java.io.IOException;
import java.util.HashMap;

/**
 * @Program: csmq
 * @Description:
 * @Author: Chasen
 * @Create: 2024-06-25 21:10
 **/
public class CSMqDemo {

    public static void main(String[] args) throws IOException {
        long ids = 0;
        String topic = "cn.chasen.test";
        CSBroker broker = CSBroker.getDefault();

        final CSProducer csProducer = broker.createProducer();
//        final CSConsumer<?> csConsumer = broker.createConsumer(topic);
//        csConsumer.listen(message -> {
//            System.out.println(" onMessage =>" + message);
//        });

        final CSConsumer<?> consumer1 = broker.createConsumer(topic);

        consumer1.listen(topic, message -> {
            System.out.println(" onMessage =>" + message);
        });

        for (int i = 0; i < 10; i++) {
            Order order = new Order((long) ids, "time" + ids, 100 * ids);
            csProducer.send(topic, new CSMessage(ids++,  JSON.toJSONString(order), new HashMap<>()));
        }
//        for (int i = 0; i < 10; i++) {
//            final CSMessage<String> message = (CSMessage<String>) consumer1.recv(topic);
//            System.out.println(message);
//            consumer1.ack(topic, message);
//        }

//        while (true) {
//            char read = (char) System.in.read();
//            if (read == 'q' || read == 'e') {
//                consumer1.unsub(topic);
//                break;
//            }
//            if (read == 'p') {
//                Order order = new Order(ids, "time" + ids, 100 * ids);
//                csProducer.send(topic, new CSMessage((long) ids++, order, null));
//                System.out.println("send ok=" + order);
//            }
//            if (read == 'c') {
//                final CSMessage<Order> message = (CSMessage<Order>) consumer1.recv(topic);
//                consumer1.ack(topic, message);
//                System.out.println("poll ok=" + message);
//            }
//            if (read == 'a') {
//                for (int i = 0; i < 10; i++) {
//                    Order order = new Order((long) ids, "time" + ids, 100 * ids);
//                    final boolean send = csProducer.send(topic, new CSMessage((long) ids++, JSON.toJSONString(order), null));
//                    System.out.println("这里2" + send);
//                }
//            }
//        }
    }
}
