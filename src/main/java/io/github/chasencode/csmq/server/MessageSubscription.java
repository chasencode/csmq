package io.github.chasencode.csmq.server;

import lombok.AllArgsConstructor;
import lombok.Data;

/**
 * @Program: csmq
 * @Description:
 * @Author: Chasen
 * @Create: 2024-07-01 22:07
 **/
@Data
@AllArgsConstructor
public class MessageSubscription {

    private String topic;
    private String consumerId;
    private int offset = -1;

}
