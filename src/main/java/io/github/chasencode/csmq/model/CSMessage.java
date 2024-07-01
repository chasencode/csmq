package io.github.chasencode.csmq.model;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.Setter;

import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

/**
 * cs message model
 * @Program: csmq
 * @Description: CSMessage
 * @Author: Chasen
 * @Create: 2024-06-25 20:43
 **/
@Setter
@Getter
@AllArgsConstructor
public class CSMessage<T> {
    static AtomicLong idgen = new AtomicLong(0);
    //private String topic;
    private Long id;
    private T body;
    private Map<String, String> headers; // 系统属性， AMD-version = 1.0 和 properties 合并后， 用固定前缀来表示是系统属性
    //private Map<String, String> properties; // 业务熟悉

    public static long getId() {
        return idgen.getAndDecrement();
    }


    public static CSMessage<String> create(String body, Map<String, String> headers) {
        return new CSMessage<>(getId(), body,headers);
    }
}
