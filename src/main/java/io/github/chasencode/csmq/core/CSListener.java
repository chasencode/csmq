package io.github.chasencode.csmq.core;

import io.github.chasencode.csmq.model.CSMessage;

/**
 * @Program: csmq
 * @Description:
 * @Author: Chasen
 * @Create: 2024-06-25 21:24
 **/
public interface CSListener<T> {
    void onMessage(CSMessage<T> message);
}
