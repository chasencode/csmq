package io.github.chasencode.csmq.store;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.TypeReference;
import io.github.chasencode.csmq.model.CSMessage;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.HashMap;
import java.util.Scanner;

/**
 * @Program: csmq
 * @Description:
 * @Author: Chasen
 * @Create: 2024-07-09 21:57
 **/
public class StoreDemo {

    public static void main(String[] args) throws IOException {

        String content = """
                this is a good file
                this is a new line for store
                """;
        System.out.println("len = " + content.getBytes().length);
        int length = content.getBytes(StandardCharsets.UTF_8).length;
        File file = new File("test.dat");
        if (!file.exists()) file.createNewFile();

        Path path = Paths.get(file.getAbsolutePath());
        try (FileChannel channel = (FileChannel) Files.newByteChannel(path,
                StandardOpenOption.WRITE, StandardOpenOption.READ)) {
            final MappedByteBuffer mappedByteBuffer =
                    channel.map(FileChannel.MapMode.READ_WRITE, 0, 1024);

            for (int i = 0; i < 10; i++) {
                System.out.println(i + "->" + mappedByteBuffer.position());
                CSMessage<String> csMessage = CSMessage.create(i + ":" + content, new HashMap<>());

                String msg = JSON.toJSONString(csMessage);
                System.out.println("id=" + csMessage.getId());
                Indexer.addEntry("test", mappedByteBuffer.position(), msg.getBytes(StandardCharsets.UTF_8).length);
                mappedByteBuffer.put(Charset.forName("UTF-8").encode(JSON.toJSONString(csMessage)));
            }

            length += 2;
            final ByteBuffer readOnlyBuffer = mappedByteBuffer.asReadOnlyBuffer();
            Scanner sc = new Scanner(System.in);
            while (sc.hasNext()) {
                String line = sc.nextLine();
                System.out.println("zzzzz " + line);
                if (line.equals("q")) break;
                System.out.println(" IN =" + line);
                int id = Integer.parseInt(line);
                Indexer.Entry entry = Indexer.getEntry("test", id);
                readOnlyBuffer.position(entry.getOffset());
                int len = entry.getLength();
                byte[] bytes = new byte[len];
                readOnlyBuffer.get(bytes, 0, len);
                String s = new String(bytes, StandardCharsets.UTF_8);
                System.out.println("  read only ==>> " + s);
                CSMessage<String> message = JSON.parseObject(s, new TypeReference<CSMessage<String>>() {
                });
                System.out.println("message.body =" + message.getBody());
            }
        }
    }
}
