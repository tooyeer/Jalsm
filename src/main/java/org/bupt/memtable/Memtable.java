package org.bupt.memtable;

import org.bupt.param.KV;

import java.util.List;

public interface Memtable {
    void put(byte[] key , byte[] value);

    // get the value of a key
    byte[] get(byte[] key);

    // get all key-value pairs in the memtable
    List<KV> All();

    // get the size of the memtable
    int size();

    // get the number of entries in the memtable
    int EntriesCnt();
}
