package io.github.chasencode.csmq.server;

import io.github.chasencode.csmq.model.CSMessage;

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
        queues.putIfAbsent(TEST_TOPIC, new MessageQueue(TEST_TOPIC));
        queues.putIfAbsent("a", new MessageQueue("a"));
    }

    private Map<String, MessageSubscription> subscriptions = new HashMap<>();

    private String topic;

    private CSMessage<?>[] queue = new CSMessage<?>[1024 * 1000];

    private int index = 0;

    public MessageQueue(String topic) {
        this.topic = topic;
    }

    public static List<CSMessage<?>> batch(String topic, String consumerId, Integer size) {
        final MessageQueue messageQueue = queues.get(topic);
        if (messageQueue == null) {
            throw new RuntimeException("topic not found");
        }
        if (messageQueue.subscriptions.containsKey(consumerId)) {
            int ind = messageQueue.subscriptions.get(consumerId).getOffset();
            int offset = ind + 1;
            List<CSMessage<?>> result = new LinkedList<>();
            // 看35分钟左右解释
//            final CSMessage<?> message = messageQueue.receive(ind + 1);
            final CSMessage<?> message = messageQueue.receive(offset);
            while(message != null) {
                result.add(message);
                if (result.size() >= size) {
                    break;
                }
                messageQueue.receive(++offset);
            }
            System.out.println("===>> recv: topic/cid/size = "
                    + topic + "/" + consumerId + "/" + result.size());
            System.out.println("===>>last message = " + message);
        }
        throw new RuntimeException("subscriptions not found for topic/consumerId =" + topic + "/" + consumerId);
    }

    public int send(CSMessage<?> message) {
        if (index >= queue.length) {
            return -1;
        }
        message.getHeaders().put("X-offset", String.valueOf(index));
        queue[index++] = message;
        return index;
    }

    public CSMessage<?> receive(int ind) {
        if (ind <= index) {
            return queue[ind];
        }
        return null;
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

    public static int send(String topic, CSMessage<?> message) {
        final MessageQueue messageQueue = queues.get(topic);
        System.out.println("===>> server send: topic/message = " + topic + "/" + messageQueue);
        if (messageQueue == null) {
            throw new RuntimeException("topic not found");
        }
        return messageQueue.send(message);
    }

    public static CSMessage<?> recv(String topic, String consumerId, int ind) {
        final MessageQueue messageQueue = queues.get(topic);
        if (messageQueue == null) {
            throw new RuntimeException("topic not found");
        }
        if (messageQueue.subscriptions.containsKey(consumerId)) {
            return messageQueue.receive(ind);
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
            int ind = messageQueue.subscriptions.get(consumerId).getOffset();
            // 看35分钟左右解释
            final CSMessage<?> message = messageQueue.receive(ind + 1);
            System.out.println("===>> recv: topic/cid/ind = "
                    + topic + "/" + consumerId + "/" + ind);
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
            if (offset > subscription.getOffset() && offset <= messageQueue.index) {
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
