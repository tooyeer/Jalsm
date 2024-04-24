package org.bupt.sstable;

import org.bupt.param.Index;
import org.bupt.param.KV;
import org.bupt.sstable.handler.SSTReader;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

public class Node {
    private Config cfg ;
    private String file;
    private SSTReader reader;
    private int level;
    private int size;
    private int seq;
    private List<Index> indexList;
    private Map<Integer,byte[]> blockToFilter;
    byte[] beginKey;
    byte[] endKey;
    public Node(Config cfg , String file , int level , int size, int seq ,SSTReader reader, List<Index> indexList , Map<Integer,byte[]> blockToFilter){
        this.cfg = cfg;
        this.file = file;
        this.level = level;
        this.size = size;
        this.seq = seq;
        this.indexList = indexList;
        this.reader = reader;
        this.blockToFilter = blockToFilter;
        this.beginKey = indexList.get(0).key;
        this.endKey = indexList.get(indexList.size()-1).key;
        this.reader.ReadFoot();
        if(this.reader.size() != size){
            throw new RuntimeException("node size not match");
        }
    }
    public List<KV> getAll(){
        return reader.ReadData();
    }
    public byte[] start(){
        return beginKey;
    }
    public byte[] end(){
        return endKey;
    }
    public int size(){
        return size;
    }
    public void close(){
        reader.close();
    }
    public int getLevel(){
        return level;
    }
    public int getSeq(){
        return seq;
    }
    public void destroy() throws IOException {
        this.reader.close();
        String path = String.join(File.separator, cfg.dir , file);
        Path f = Path.of(path);
        Files.delete(f);
    }
    public byte[] get(byte[] key){
        int index = binarySearchIndex(this.indexList , key);
        if(index<0||index>this.indexList.size()){
            return null;
        }
        int blockOffset = this.indexList.get(index).preBlockOffset;
        int blockSize = this.indexList.get(index).preBlockSize;
        byte[] bitmap = this.blockToFilter.get(blockOffset);
        if(!cfg.filter.exist(bitmap , key)){
            return null;
        }
        byte[] block = reader.ReadBlock(blockOffset , blockSize);
        List<KV> kvs = reader.ReadBlockData(block);
        for(KV kv : kvs){
            if(Arrays.compare(kv.key , key)==0){
                return kv.value;
            }
        }
        return null;
    }
    public int binarySearchIndex(List<Index> list , byte[] key){
        int l = 0;
        int r = list.size()-1;
        while(l<r&&r<list.size()&&l>=0){
            int mid = (l+r)>>1;
            int cmp = Arrays.compare(list.get(mid).key , key);
            if(cmp < 0){
                l = mid + 1;
            }else if(cmp==0){
                return mid;
            }else{
                r = mid;
            }
        }
        return l;
    }
}
