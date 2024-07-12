package io.github.chasencode.csmq.server;

import com.alibaba.fastjson.JSON;
import io.github.chasencode.csmq.model.CSMessage;
import io.github.chasencode.csmq.store.Indexer;
import io.github.chasencode.csmq.store.Store;

import java.util.*;

/**
 * queues
 *
 * @Program: csmq
 * @Description:
 * @Author: Chasen
 * @Create: 2024-07-01 22:01
 **/
public class MessageQueue {

    private static final String TEST_TOPIC = "cn.chasen.test";

    public static final Map<String, MessageQueue> queues = new HashMap<>();

    static {
        System.out.println("这里初始化queues");
        queues.putIfAbsent(TEST_TOPIC, new MessageQueue(TEST_TOPIC));
//        queues.putIfAbsent("a", new MessageQueue("a"));
    }

    private Map<String, MessageSubscription> subscriptions = new HashMap<>();

    private String topic;

    private Store store = null;

    private CSMessage<?>[] queue = new CSMessage<?>[1024 * 1000];

    public MessageQueue(String topic) {
        this.topic = topic;
        store = new Store(topic);
        store.init();
    }

    public static List<CSMessage<?>> batch(String topic, String consumerId, Integer size) {
        final MessageQueue messageQueue = queues.get(topic);
        if (messageQueue == null) {
            throw new RuntimeException("topic not found");
        }
        if (messageQueue.subscriptions.containsKey(consumerId)) {
            int offset = messageQueue.subscriptions.get(consumerId).getOffset();
            int next_offset = 0;
            if (offset > -1) {
                final Indexer.Entry entry = Indexer.getEntry(topic, offset);
                next_offset = offset + entry.getLength();
            }
            List<CSMessage<?>> result = new LinkedList<>();
            // 看35分钟左右解释
//            final CSMessage<?> message = messageQueue.receive(ind + 1);
            CSMessage<?> message = messageQueue.receive(next_offset);
            while(message != null) {
                result.add(message);
                if (result.size() >= size) {
                    break;
                }
                message = messageQueue.receive(++offset);
            }
            System.out.println("===>> batch: topic/cid/size = "
                    + topic + "/" + consumerId + "/" + result.size());
            System.out.println("===>>last message = " + message);
            return result;
        }
        throw new RuntimeException("subscriptions not found for topic/consumerId =" + topic + "/" + consumerId);
    }

    public int send(CSMessage<String> message) {
        if (message.getHeaders() == null) {
            message.setHeaders(new HashMap<>());
        }
        int offset = store.pos();

        message.getHeaders().put("X-offset", String.valueOf(offset));
        store.write(message);

        return  offset;
    }

    public CSMessage<?> receive(int offset) {
        return store.read(offset);
    }

    public void subscribe(MessageSubscription subscription) {
        final String consumerId = subscription.getConsumerId();
        subscriptions.putIfAbsent(consumerId, subscription);
    }


    private void unsubscribe(MessageSubscription subscription) {
        final String consumerId = subscription.getConsumerId();
        subscriptions.remove(consumerId);
    }


    public static void sub(MessageSubscription subscription) {
        final MessageQueue messageQueue = queues.get(subscription.getTopic());
        System.out.println(" ===>> sub:" + subscription);
        if (messageQueue == null) {
            throw new RuntimeException("topic not found");
        }
        messageQueue.subscribe(subscription);
    }


    public static void unsub(MessageSubscription subscription) {
        final MessageQueue messageQueue = queues.get(subscription.getTopic());
        System.out.println(" ===>> unsub:" + subscription);
        if (messageQueue == null) {
            return;
        }
        messageQueue.unsubscribe(subscription);
    }

    public static int send(String topic, CSMessage<String> message) {
        final MessageQueue messageQueue = queues.get(topic);
        System.out.println("===>> server send: topic/message = " + topic + "/" + JSON.toJSONString(message));
        if (messageQueue == null) {
            throw new RuntimeException("topic not found");
        }
        return messageQueue.send(message);
    }

    public static CSMessage<?> recv(String topic, String consumerId, int offset) {
        final MessageQueue messageQueue = queues.get(topic);
        if (messageQueue == null) {
            throw new RuntimeException("topic not found");
        }
        if (messageQueue.subscriptions.containsKey(consumerId)) {
            return messageQueue.receive(offset);
        }
        throw new RuntimeException("subscriptions not found for topic/consumerId =" + topic + "/" + consumerId);

    }

    // 使用此方法，需要手动调用ack, 更新订阅关系里的offset
    public static CSMessage<?> recv(String topic, String consumerId) {
        final MessageQueue messageQueue = queues.get(topic);
        if (messageQueue == null) {
            throw new RuntimeException("topic not found");
        }
        if (messageQueue.subscriptions.containsKey(consumerId)) {
            int offset = messageQueue.subscriptions.get(consumerId).getOffset();
            int next_offset = 0;
            if (offset > -1) {
                final Indexer.Entry entry = Indexer.getEntry(topic, offset);
                next_offset = offset + entry.getLength();
            }
            final CSMessage<?> message = messageQueue.receive(next_offset);
            System.out.println("===>> recv: topic/cid/ind = "
                    + topic + "/" + consumerId + "/" + next_offset);
            System.out.println("===>> message = " + message);
            return message;
        }
        throw new RuntimeException("subscriptions not found for topic/consumerId =" + topic + "/" + consumerId);

    }

    public static int ack(String topic, String consumerId, int offset) {
        final MessageQueue messageQueue = queues.get(topic);
        if (messageQueue == null) {
            throw new RuntimeException("topic not found");
        }
        if (messageQueue.subscriptions.containsKey(consumerId)) {
            final MessageSubscription subscription = messageQueue.subscriptions.get(consumerId);
            if (offset > subscription.getOffset() && offset < Store.LEN) {
                System.out.println(" ===>> ack: topic/cid/offset = "
                        + topic + "/" + consumerId + "/" + offset);
                subscription.setOffset(offset);
                return offset;
            }
            return -1;
        }
        throw new RuntimeException("subscriptions not found for topic/consumerId =" + topic + "/" + consumerId);

    }
}
