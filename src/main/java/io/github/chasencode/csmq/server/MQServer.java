package io.github.chasencode.csmq.server;

import io.github.chasencode.csmq.model.CSMessage;
import io.github.chasencode.csmq.model.Result;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.*;

import java.util.List;

/**
 * @Program: csmq
 * @Description:
 * @Author: Chasen
 * @Create: 2024-07-01 21:57
 **/
@RestController
@RequestMapping("/csmq")
public class MQServer {


    // send
    @PostMapping("/send")
    public Result<String> send(@RequestBody CSMessage<String> message, @RequestParam("t") String topic) {
        ;
        return Result.ok("" + MessageQueue.send(topic, message));
    }

    // receive
    @RequestMapping("/recv")
    public Result<CSMessage<?>> receive(@RequestParam("t") String topic,
                                             @RequestParam("cid") String consumerId) {
        return Result.msg(MessageQueue.recv(topic, consumerId));
    }


    // receive
    @RequestMapping("/batch")
    public Result<List<CSMessage<?>>> batch(@RequestParam("t") String topic,
                                            @RequestParam("cid") String consumerId,
                                            @RequestParam(name = "size", defaultValue = "1000") Integer size) {
        return Result.msg(MessageQueue.batch(topic, consumerId, size));
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
    public Result sub(@RequestParam("t") String topic, @RequestParam("cid") String consumerId) {
        MessageQueue.sub(new MessageSubscription(topic, consumerId, -1));
        return Result.ok();
    }

    // unsubscribe
    @RequestMapping("/unsub")
    public Result unsub(@RequestParam("t") String topic, @RequestParam("cid") String consumerId) {
        MessageQueue.unsub(new MessageSubscription(topic, consumerId, -1));
        return Result.ok();
    }
}
