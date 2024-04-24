package org.bupt.memtable.handler;

import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

import static org.bupt.util.UvarintUtil.putUvarint;

public class WalWriter {
    private String path;

    private FileOutputStream file;

    private byte[] buffer;

    public WalWriter(String path)  {
        this.path = path;
        this.buffer = new byte[128];
        try{
            if(!Files.exists(Paths.get(path))){
                Files.createFile(Path.of(path));
            }
            this.file = new FileOutputStream(path,true);
        }catch (IOException e){
            this.file = null;
            e.printStackTrace();
        }
    }

    public boolean write(byte[] key , byte[] value){
        try{
            int offset = 0;
            offset = putUvarint(key.length, buffer, offset);
            offset = putUvarint(value.length, buffer, offset);
            file.write(buffer, 0, offset);
            file.write(key);
            file.write(value);
            return true;
        }catch (Exception e){
            return false;
        }
    }

    public void close() {
        try {
            this.file.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
