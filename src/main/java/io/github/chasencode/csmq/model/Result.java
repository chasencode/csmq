package io.github.chasencode.csmq.model;

import lombok.AllArgsConstructor;
import lombok.Data;

import java.util.HashMap;
import java.util.List;

/**
 * Result for MQServer
 * @Program: csmq
 * @Description:
 * @Author: Chasen
 * @Create: 2024-07-01 22:12
 **/
@Data
@AllArgsConstructor
public class Result<T> {
    private int code; // 1 == success, 0 == failure

    private T data;

    public static Result<String> ok() {
        return new Result<String>(1, "ok");
    }

    public static Result<String> ok(String msg) {
        return new Result<String>(1, msg);
    }

    public static Result<CSMessage<String>> msg(String msg) {
        return new Result<>(1, CSMessage.create(msg, new HashMap<>()));
    }


    public static Result<CSMessage<?>> msg(CSMessage<?> msg) {
        return new Result<>(1, msg);
    }


    public static Result<List<CSMessage<?>>> msg(List<CSMessage<?>> msg) {
        return new Result<>(1, msg);
    }
}
