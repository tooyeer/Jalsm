package org.bupt.memtable.impl;

import org.bupt.memtable.Memtable;
import org.bupt.param.KV;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Random;

public class SkipList implements Memtable {
    private class SkipListNode{
        byte[] key ;
        byte[] value;
        SkipListNode[] forwards;
        public SkipListNode(byte[] key, byte[] value) {
            this.key = key;
            this.value = value;
            this.forwards = new SkipListNode[MAX_LEVEL];
        }
    }
    private final int MAX_LEVEL = 16;
    private SkipListNode head;
    private int size;
    private int entitesCnt;
    public SkipList() {
        head = new SkipListNode(null, null);
        size = 0;
        entitesCnt = 0;
    }
    @Override
    public void put(byte[] key, byte[] value) {
        SkipListNode update[] = new SkipListNode[MAX_LEVEL];
        SkipListNode x = head;
        for(int i = MAX_LEVEL - 1; i >= 0; i--) {
            while ((x.forwards[i] != null) && Arrays.compare(x.forwards[i].key, key) < 0) {
                x = x.forwards[i];
            }
            update[i] = x;
        }
        x = x.forwards[0];
        if(x!=null&&Arrays.compare(x.key , key) ==0){
            x.value = value;
            size = size - x.value.length + value.length;
        }else{
            entitesCnt++;
            size += key.length + value.length;
            int level = randomLevel();
            SkipListNode newNode = new SkipListNode(key, value);
            for(int i = 0; i < level; i++) {
                newNode.forwards[i] = update[i].forwards[i];
                update[i].forwards[i] = newNode;
            }
        }
    }
    private int randomLevel() {
        int level = 1;
        while (new Random().nextInt() % 2 == 0) {
            level++;
        }
        return Math.min(level, MAX_LEVEL);
    }
    @Override
    public byte[] get(byte[] key) {
        SkipListNode x = head;
        for(int i = MAX_LEVEL - 1; i >= 0; i--) {
            while ((x.forwards[i] != null) && Arrays.compare(x.forwards[i].key, key) < 0) {
                x = x.forwards[i];
            }
        }
        x = x.forwards[0];
        if(x!=null&&Arrays.compare(x.key , key) ==0){
            return x.value;
        }
        return null;
    }

    @Override
    public List<KV> All() {
        List<KV> res = new ArrayList<>();
        SkipListNode x = head.forwards[0];
        while (x != null) {
            KV kv = new KV(x.key, x.value);
            res.add(kv);
            x = x.forwards[0];
        }
        return res;
    }

    @Override
    public int size() {
        return size;
    }

    @Override
    public int EntriesCnt() {
        return entitesCnt;
    }
}
