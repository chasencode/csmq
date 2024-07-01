package io.github.chasencode.csmq.server;

import io.github.chasencode.csmq.model.CSMessage;
import io.github.chasencode.csmq.model.Result;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;

/**
 * @Program: csmq
 * @Description:
 * @Author: Chasen
 * @Create: 2024-07-01 21:57
 **/
@Controller
@RequestMapping("/csmq")
public class MQServer {


    // send
    @RequestMapping("/send")
    public Result<String> send(@RequestParam("t") String topic,
                               @RequestParam("cid") String consumerId,
                               @RequestBody CSMessage<String> message) {
        ;
        return Result.ok("" + MessageQueue.send(topic, consumerId, message));
    }

    // receive
    @RequestMapping("/receive")
    public Result<CSMessage<?>> receive(@RequestParam("t") String topic,
                                             @RequestParam("cid") String consumerId,
                                             @RequestBody CSMessage<String> message) {
        return Result.msg(MessageQueue.recv(topic, consumerId));
    }

    // ack
    @RequestMapping("/ack")
    public Result<String> ack(@RequestParam("t") String topic,
                              @RequestParam("cid") String consumerId,
                              @RequestParam("offset") Integer offset) {
        return Result.ok();
    }

    // subscribe
    @RequestMapping("/sub")
    public Result subscribe(@RequestParam("t") String topic, @RequestParam("cid") String consumerId) {
        MessageQueue.sub(new MessageSubscription(topic, consumerId, -1));
        return Result.ok();
    }

    // unsubscribe
    @RequestMapping("/unsub")
    public Result unsubscribe(@RequestParam("t") String topic, @RequestParam("cid") String consumerId) {
        MessageQueue.unsub(new MessageSubscription(topic, consumerId, -1));
        return Result.ok();
    }
}
