package org.bupt.sstable.handler;

import lombok.extern.log4j.Log4j2;
import org.bupt.param.KV;
import org.bupt.sstable.Config;
import org.bupt.param.Index;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;


import static org.bupt.util.UvarintUtil.*;

@Log4j2
public class SSTReader {
    Config cfg;

    int indexOffset;
    public int filterOffset;
    int indexSize;
    int filterSize;
    RandomAccessFile reader;

    private String path;

    public SSTReader(Config cfg , String file){
        this.cfg = cfg;
        String path = String.join(File.separator, cfg.dir , file);

        try{
            this.path = path;
            this.reader = new RandomAccessFile(path,"r");
            log.info("create a read file:"+path);
        }catch (Exception e){
            this.reader = null;
            e.printStackTrace();
        }
    }
    public void ReadFoot(){
        try{
            int seekIndex = (int) (this.reader.length() -cfg.SSTFooterSize);
            this.reader.seek(seekIndex);
            this.filterOffset = (int) readUvarintFromFile(this.reader);
            this.filterSize = (int) readUvarintFromFile(this.reader);
            this.indexOffset = (int) readUvarintFromFile(this.reader);
            this.indexSize = (int) readUvarintFromFile(this.reader);
        }catch (Exception e){
            e.printStackTrace();
        }
    }
    public byte[] ReadBlock(int offset , int size){
        try{
            this.reader.seek(offset);
            byte[] data = new byte[size];
            this.reader.read(data);
            return data;
        }catch (Exception e){
            e.printStackTrace();
            return null;
        }
    }
    public List<KV> ReadData(){
        if(this.indexOffset == 0 || this.indexSize == 0||this.filterSize==0||this.filterOffset==0){
            ReadFoot();
        }
        try{
            return ReadBlockData(ReadBlock(0 , this.filterOffset));
        }catch (Exception e){
            e.printStackTrace();
            return null;
        }
    }
    public List<KV> ReadBlockData(byte[] data){
        if(this.indexOffset == 0 || this.indexSize == 0||this.filterSize==0||this.filterOffset==0){
            ReadFoot();
        }
        try{
            ByteBuffer reader = ByteBuffer.wrap(data);
            byte[] preKey = new byte[0];
            List<KV> kvs = new ArrayList<>();
            while(reader.hasRemaining()){
                KV kv = readKV(reader , preKey);
                preKey = kv.key;
                kvs.add(kv);
            }
            return kvs;
        }catch (Exception e){
            e.printStackTrace();
            return null;
        }
    }
    public List<Index> ReadIndex(){
        if(this.indexOffset == 0 || this.indexSize == 0){
            ReadFoot();
        }
        try{
            return readIndex(ReadBlock(this.indexOffset , this.indexSize));
        }catch (Exception e){
            e.printStackTrace();
            return null;
        }
    }
    public List<Index> readIndex(byte[] data)throws IOException{
        List<Index> indexList = new ArrayList<>();
        ByteBuffer reader =ByteBuffer.wrap(data);
        byte[] preKey = new byte[0];
        while(reader.hasRemaining()){
            KV kv = readKV(reader , preKey);
            preKey = kv.key;
            ByteBuffer valueReader = ByteBuffer.wrap(kv.value);
            int preBlockOffset = (int)readUvarintFromBuffer(valueReader);
            int preBlockSize = (int)readUvarintFromBuffer(valueReader);
            indexList.add(new Index(kv.key,preBlockOffset,preBlockSize));
        }
        return indexList;
    }

    public Map<Integer , byte[]> ReadFilter(){
        if(this.filterOffset == 0 || this.filterSize == 0){
            ReadFoot();
        }
        try{
            return readFilter(ReadBlock(this.filterOffset , this.filterSize));
        }catch (Exception e){
            e.printStackTrace();
            return null;
        }
    }
    public Map<Integer , byte[]> readFilter(byte[] data) throws IOException {
        Map<Integer , byte[]> blockToFilter = new HashMap<>();
        ByteBuffer reader = ByteBuffer.wrap(data);
        byte[] preKey = new byte[0];
        while(reader.hasRemaining()){
            KV kv = readKV(reader,preKey);
            preKey = kv.key;
            blockToFilter.put((int)readUvarintFromBuffer(ByteBuffer.wrap(kv.key)),kv.value);
        }
        return blockToFilter;
    }

    private KV readKV(ByteBuffer byteBuffer , byte[] preKey){
        int shared=0, keyLen=0 , valLen=0;
        try{
            shared = (int) readUvarintFromBuffer(byteBuffer);
            keyLen = (int)readUvarintFromBuffer(byteBuffer);
            valLen = (int)readUvarintFromBuffer(byteBuffer);
            byte[] keylen = new byte[keyLen];
            byteBuffer.get(keylen);
            byte[] val = new byte[valLen];
            byteBuffer.get(val);
            byte[] key = new byte[shared + keyLen];
            System.arraycopy(preKey,0,key,0,shared);
            System.arraycopy(keylen,0,key,shared,keyLen);
            return new KV(key,val);
        }catch (Exception e){
            e.printStackTrace();
            return null;
        }
    }

    public void close(){
        try{
            this.reader.close();
            log.info("close the read file"+this.path);

        }catch (Exception e){
            e.printStackTrace();
        }
    }
    public int size(){
        return indexOffset+indexSize;
    }
    public int filterSize(){
        if(this.filterOffset == 0 || this.filterSize == 0){
            ReadFoot();
        }
        return filterOffset+filterSize;
    }
    public int dataSize(){
        if(this.filterOffset == 0 || this.filterSize == 0){
            ReadFoot();
        }
        return filterOffset;
    }
}
