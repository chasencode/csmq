package io.github.chasencode.csmq.client;

import cn.kimmking.utils.HttpUtils;
import cn.kimmking.utils.ThreadUtils;
import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.TypeReference;
import io.github.chasencode.csmq.model.CSMessage;
import io.github.chasencode.csmq.model.Result;
import lombok.Getter;
import org.springframework.util.LinkedMultiValueMap;
import org.springframework.util.MultiValueMap;

import java.util.ArrayList;
import java.util.List;

import static com.alibaba.fastjson.JSON.toJSONString;

/**
 * broker for topics
 *
 * @Program: csmq
 * @Description:
 * @Author: Chasen
 * @Create: 2024-06-25 20:52
 **/

public class CSBroker {

    @Getter
    public static CSBroker Default = new CSBroker();

    public static final String brokerUrl = "http://127.0.0.1:8765/csmq";


    static {
        init();
    }

    public static void init() {
        ThreadUtils.getDefault().init(1);
        ThreadUtils.getDefault().schedule(() -> {
            final MultiValueMap<String, CSConsumer<?>> consumers = getDefault().getConsumers();
            consumers.forEach((topic, consumerList) -> {
                consumerList.forEach(consumer -> {
                    final CSMessage<?> recv = consumer.recv(topic);
                    if (recv != null) {
                        return;
                    }
                    try {
                        consumer.getListener().onMessage(recv);
                        consumer.ack(topic, recv);
                    } catch (Exception e) {
                        // @todo 重试
                        return;
                    }
                });
            });
        },100, 100);
    }

    public CSProducer createProducer() {
        return new CSProducer(this);
    }

    public CSConsumer<?> createConsumer(String topic) {
        final CSConsumer<?> csConsumer = new CSConsumer(this);
        csConsumer.sub(topic);
        return csConsumer;
    }

    public boolean send(String topic, CSMessage message) {
        System.out.println("==>> broker send topic/message: " + topic + "/" + JSON.toJSONString(message));
        Result<String> result = HttpUtils.httpPost(toJSONString(message),
                brokerUrl + "/send?t=" + topic, new TypeReference<Result<String>>() {
                });
        System.out.println("==>> send result: " + result);
        return result.getCode() == 1;

    }

    public void sub(String topic, String consumerId) {
        System.out.println("==>> sub topic/consumerId: " + topic + "/" + consumerId);
        Result<String> result = HttpUtils.httpGet(brokerUrl + "/sub?t=" + topic + "&cid=" + consumerId,
                new TypeReference<Result<String>>() {
                });
        System.out.println("==>> sub result: " + result);
    }

    public <T> CSMessage<T> recv(String topic, String id) {
        System.out.println("==>> recv topic/id: " + topic + "/" + id);
        Result<CSMessage<String>> result = HttpUtils.httpGet(brokerUrl + "/recv?t=" + topic + "&cid=" + id,
                new TypeReference<>() {
                });
        System.out.println("==>> recv result: " + result);
        return (CSMessage<T>)  result.getData();
    }


    public void unsub(String topic, String consumerId) {
        System.out.println("==>> unsub topic/consumerId: " + topic + "/" + consumerId);
        Result<String> result = HttpUtils.httpGet(brokerUrl + "/unsub?t=" + topic + "&cid=" + consumerId,
                new TypeReference<Result<String>>() {
                });
        System.out.println("==>> unsub result: " + result);
    }

    public boolean ack(String topic, String consumerId, int offset) {
        System.out.println("==>> ack topic/consumerId/offset: " + topic + "/" + consumerId + "/" + offset);
        Result<String> result = HttpUtils.httpGet(brokerUrl + "/ack?t=" + topic + "&cid=" + consumerId + "&offset=" + offset,
                new TypeReference<Result<String>>() {
                });
        System.out.println(" ===>> ack result: " + result);
        return result.getCode() == 1;
    }

    @Getter
    private final MultiValueMap<String, CSConsumer<?>> consumers = new LinkedMultiValueMap();


    public <T> void addConsumer(String topic, CSConsumer<?> tcsConsumer) {
        consumers.add(topic,tcsConsumer);
    }
}
