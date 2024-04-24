package org.bupt.sstable;

public interface Filter {
    // add a key to the filter
    void add(byte[] key );

    // check if the filter contains a key
    boolean isContains(byte[] key);

    // 生成过滤器对应的bitMap
    byte[] hash();

    boolean exist(byte[] bitmap , byte[] key);

    // reset the filter
    void reset();


    // 存在多少个key
    int keyLen();
}
