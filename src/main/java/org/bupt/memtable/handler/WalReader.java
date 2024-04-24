package org.bupt.memtable.handler;

import org.bupt.memtable.Memtable;
import org.bupt.param.KV;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

import static org.bupt.util.UvarintUtil.readUvarintFromStream;

public class WalReader {
    private String path ;

    private FileInputStream file;

    private BufferedReader reader;

    public WalReader(String path) throws FileNotFoundException {
        this.path = path;
        this.file = new FileInputStream(path);
        this.reader = new BufferedReader(new InputStreamReader(this.file, StandardCharsets.UTF_8) );
    }

    public boolean RestoreToMemtable(Memtable memtable) throws IOException {
        try{
            byte[] body = new byte[file.available()];
            file.read(body);
            List<KV> kvs = readAll(new ByteArrayInputStream(body));
            for(KV kv : kvs){
                memtable.put(kv.key, kv.value);
            }
            return true;
        }
        catch (IOException e){
            e.printStackTrace();
            return false;
        }finally {
            file.getChannel().position(0);
        }
    }
    // 将文件中读到的原始内容解析成一系列kv对数据
    private List<KV> readAll(InputStream reader) throws IOException {
        List<KV> kvs = new ArrayList<>();
        try (DataInputStream dataInput = new DataInputStream(reader)) {
            while (true) {
                // 从reader中读取首个uint64作为key长度
                long keyLen = readUvarintFromStream(dataInput);
                if (keyLen == 0) {
                    break;  // 假设0作为key长度表示文件内容读取完毕
                }

                // 从reader中读取下一个uint64作为val长度
                long valLen = readUvarintFromStream(dataInput);

                // 从reader中读取对应于key长度的字节数作为key
                byte[] keyBuf = new byte[(int) keyLen];
                dataInput.readFully(keyBuf);

                // 从reader中读取对应于val长度的字节数作为val
                byte[] valBuf = new byte[(int) valLen];
                dataInput.readFully(valBuf);

                kvs.add(new KV(keyBuf, valBuf));
            }
        }
        return kvs;
    }
    public void close() throws IOException {
        if (reader != null) {
            reader.close();
        }
        if (file != null) {
            file.close();
        }
    }
}
