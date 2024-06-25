package io.github.chasencode.csmq.demo;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * @Program: csmq
 * @Description:
 * @Author: Chasen
 * @Create: 2024-06-25 21:11
 **/
@Data
@AllArgsConstructor
@NoArgsConstructor
public class Order {
    private Long orderId;
    private String item;
    private double price;
}
