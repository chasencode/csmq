package io.github.chasencode.csmq.core;

import lombok.SneakyThrows;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.TimeUnit;

/**
 * mq for topic
 * @Program: csmq
 * @Description:
 * @Author: Chasen
 * @Create: 2024-06-25 20:53
 **/
public class CSMq {

    private String topic;

    private final LinkedBlockingDeque<CSMessage> queue = new LinkedBlockingDeque<>();

    private final List<CSListener> listeners = new ArrayList();

    public CSMq(String topic) {
        this.topic = topic;
    }

    public boolean send(CSMessage message) {
//        System.out.println("消息进队列");
        final boolean offer = queue.offer(message);
        listeners.forEach(listeners -> listeners.onMessage(message));
//        System.out.println("这里1");
        return offer;
    }
    // 拉模式获取消息
    @SneakyThrows
    public <T> CSMessage poll(long timeout) {
        return queue.poll(timeout, TimeUnit.MILLISECONDS);
    }

    public <T> void addListener(CSListener<T> listener) {
        listeners.add(listener);
    }
}
