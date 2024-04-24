package org.bupt.sstable;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStream;

import static org.bupt.util.UvarintUtil.putUvarint;

public class Block {
    public Config cfg;
    public byte[] buffer;

    public int entitesCnt;

    public byte[] preKey;

    public ByteArrayOutputStream record;

    public Block(Config cfg){
        this.cfg = cfg;
        this.record = new ByteArrayOutputStream();
        this.preKey = new byte[0];
        this.buffer = new byte[64];
    }

    public int size(){
        return record.size();
    }
    public void clear(){
        record.reset();
        entitesCnt = 0;
        preKey = new byte[0];
        buffer = new byte[64];
    }
    public int Flush(OutputStream file){
        try {
            int size = record.size();
            record.writeTo(file);
            return size;
        } catch (IOException e) {
            e.printStackTrace();
            return -1;
        }finally {
            clear();
        }
    }
    public boolean Append(byte[] key , byte[] value){
        try{
            int shared = SharedPrefixLen(preKey , key);
            int offset = 0;
            offset = putUvarint(shared, buffer, offset);
            offset = putUvarint(key.length - shared, buffer, offset);
            offset = putUvarint(value.length, buffer, offset);
            record.write(buffer,0,offset);
            record.write(key,shared,key.length-shared);
            record.write(value);
            return true;
        } catch (IOException e) {
            e.printStackTrace();
            return false;
        } finally {
            preKey = key;
            entitesCnt++;
        }
    }
    public static int SharedPrefixLen(byte[] a , byte[] b ){
        int i=0;
        for(; i<a.length&&i<b.length;i++){
            if(a[i]!=b[i]){
                break;
            }
        }
        return i;
    }
}
