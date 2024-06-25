package io.github.chasencode.csmq.core;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.Setter;

import java.util.Map;

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
    //private String topic;
    private Long id;
    private T body;
    private Map<String, String> headers; // 系统属性， AMD-version = 1.0 和 properties 合并后， 用固定前缀来表示是系统属性
    //private Map<String, String> properties; // 业务熟悉



}
