package io.github.chasencode.csmq.store;

import lombok.AllArgsConstructor;
import lombok.Data;
import org.springframework.util.LinkedMultiValueMap;
import org.springframework.util.MultiValueMap;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * @Program: csmq
 * @Description: entry indexer
 * @Author: Chasen
 * @Create: 2024-07-09 22:18
 **/
public class Indexer {

    static MultiValueMap<String, Entry> indexes = new LinkedMultiValueMap<>();
    static Map<Integer, Entry> mappings = new HashMap<>();

    public static void addEntry(String topic, int offset, int length) {
        Entry value = new Entry(offset, length);
        indexes.add(topic, value);
        mappings.put(offset, value);
    }

    public static List<Entry> getEntries(String topic) {
        return indexes.get(topic);
    }

    public static Entry getEntry(String topic, int offset) {
        return mappings.get(offset);
    }



    @Data
    @AllArgsConstructor
    public static class Entry {
        /**
         * 文件内偏移量
         */
        int offset;
        /**
         *  当前消息的长度大小
         */
        int length;
    }

}
