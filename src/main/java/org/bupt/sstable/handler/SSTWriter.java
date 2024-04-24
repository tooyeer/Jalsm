package org.bupt.sstable.handler;

import lombok.Getter;
import lombok.extern.log4j.Log4j2;
import org.bupt.param.Index;
import org.bupt.sstable.Block;
import org.bupt.sstable.Config;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.bupt.util.UvarintUtil.putUvarint;

@Log4j2
public class SSTWriter {
    Config cfg ;
    FileOutputStream writer ;
    ByteArrayOutputStream dataBuffer;
    ByteArrayOutputStream indexBuffer;
    ByteArrayOutputStream filterBuffer;
    @Getter
    Map<Integer ,byte[]> blockToFilter;

    List<Index> indexs;
    Block dataBlock;
    Block indexBlock;
    Block filterBlock;

    byte[] preKey;
    byte[] buffer;
    int preBlockOffset;
    int preBlockSize;


    private String path;
    public SSTWriter(String file , Config cfg){
        this.cfg = cfg;
        this.dataBlock = new Block(cfg);
        this.filterBlock = new Block(cfg);
        this.indexBlock = new Block(cfg);
        this.dataBuffer = new ByteArrayOutputStream();
        this.indexBuffer = new ByteArrayOutputStream();
        this.filterBuffer = new ByteArrayOutputStream();
        this.preKey = new byte[0];
        this.blockToFilter = new HashMap<>();
        this.indexs = new ArrayList<>();
        this.path= String.join(File.separator, cfg.dir , file);
        this.buffer = new byte[64];
    }

    public boolean Append(byte[] key , byte[] value){
        if(dataBlock.entitesCnt==0){
            InsertIndex(key);
        }
        if(this.indexs.size()-this.blockToFilter.size()!=1){
            throw new RuntimeException("indexs size is not equal to blockToFilter size");
        }
        if(!dataBlock.Append(key , value)){
            return false;
        }
        cfg.filter.add(key);
        this.preKey = key;
        if(dataBlock.size()>=cfg.SSTDataBlockSize){
            refreshBlock();
        }

        return true;
    }
    private void refreshBlock(){
        if(this.cfg.filter.keyLen()==0){
            return ;
        }
        this.preBlockOffset = dataBuffer.size();
        byte[] bitMap = cfg.filter.hash();
        blockToFilter.put(preBlockOffset , bitMap);
        int offset = putUvarint(preBlockOffset , this.buffer , 0);
        byte[] newbuffer = new byte[offset];
        System.arraycopy(this.buffer , 0 , newbuffer , 0 , offset);

        filterBlock.Append(newbuffer , bitMap);
        cfg.filter.reset();
        preBlockSize = dataBlock.Flush(dataBuffer);
    }
    private void InsertIndex(byte[] key){
        byte[] indexKey = GetSeparatorBetween(preKey , key);
        int offset = 0;
        byte[] buffer = new byte[128];
        offset = putUvarint(preBlockOffset, buffer, offset);
        offset = putUvarint(preBlockSize , buffer , offset);
        byte[] newbuffer = new byte[offset];
        System.arraycopy(buffer , 0 , newbuffer , 0 , offset);
        indexBlock.Append(indexKey , newbuffer);
        indexs.add(new Index(indexKey , preBlockOffset , preBlockSize));
        return;
    }
    private byte[] GetSeparatorBetween(byte[] a , byte[] b){
        if(a.length==0){
            byte[] newBytes = new byte[b.length];
            System.arraycopy(b,0,newBytes,0,b.length);
            newBytes[newBytes.length-1]--;
            return newBytes;
        }
        return a;
    }

    public void Finish()throws IOException{
        refreshBlock();
        InsertIndex(preKey);
        filterBlock.Flush(filterBuffer);
        indexBlock.Flush(indexBuffer);
        if(this.indexs.size()-this.blockToFilter.size()!=1){
            System.out.println("indexs size is not equal to blockToFilter size");
        }
        byte[] footer = new byte[cfg.SSTFooterSize];
        int offset = 0;
        int filterOffset = dataBuffer.size();
        int filterSize = filterBuffer.size();
        int indexOffset = filterOffset + filterSize;
        int indexSize = indexBuffer.size();
        if(this.indexs.size()-this.blockToFilter.size()!=1){
            new RuntimeException("indexs size is not equal to blockToFilter size");
        }
        offset = putUvarint(filterOffset, footer, offset);
        offset = putUvarint(filterSize, footer, offset);
        offset = putUvarint(indexOffset, footer, offset);
        offset = putUvarint(indexSize, footer, offset);
        ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
        outputStream.write(dataBuffer.toByteArray());
        outputStream.write(filterBuffer.toByteArray());
        outputStream.write(indexBuffer.toByteArray());
        outputStream.write(footer);
        try{
            if(!Files.exists(Paths.get(this.path))) {
                Files.createFile(Path.of(this.path));
            }
            this.writer = new FileOutputStream(this.path);
        }catch (Exception e){
            throw new RuntimeException(e);
        }
        this.writer.write(outputStream.toByteArray());
        outputStream.close();
        return;
    }
    public int size(){
        return dataBuffer.size() + filterBuffer.size() + indexBuffer.size();
    }
    public List<Index> getIndex(){
        return indexs;
    }

    public void close(){
        dataBuffer.reset();
        dataBlock.clear();
        indexBuffer.reset();
        indexBlock.clear();
        filterBuffer.reset();
        filterBlock.clear();
        try {
            writer.close();
            log.info("close the write file"+this.path);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}
