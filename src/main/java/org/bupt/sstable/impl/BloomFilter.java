package org.bupt.sstable.impl;

import org.bupt.sstable.Filter;

import java.util.BitSet;
import java.util.Objects;

public class BloomFilter implements Filter {
    private final BitSet bitSet;
    private final int bitSetSize;
    private final int hashCount;
    private int keyCount;
        // 对byte数组进行MurmurHash
        public  int hash(byte[] data, int seed) {
            int length = data.length;
            int hash = seed;

            for (int i = 0; i < length; i++) {
                hash = hash ^ (data[i] & 0xFF);
                hash = (hash * 0x5bd1e995) >>> 24;
            }
            return hash;
        }

    public BloomFilter(int bitSetSize, int hashCount) {
        this.bitSetSize = bitSetSize;
        this.hashCount = hashCount;
        this.bitSet = new BitSet(bitSetSize);
        this.keyCount = 0;
    }

    @Override
    public void add(byte[] key) {
        Objects.requireNonNull(key);
        if (keyCount >= bitSetSize) {
            throw new IllegalStateException("Bloom filter is full");
        }
        for (int i = 0; i < hashCount; i++) {
            int hash = hash(key, i);
            bitSet.set(hash);
        }
        keyCount++;
    }

    @Override
    public boolean isContains(byte[] key) {
        Objects.requireNonNull(key);
        for (int i = 0; i < hashCount; i++) {
            int hash = hash(key, i);
            if (!bitSet.get(hash)) {
                return false;
            }
        }
        return true;
    }

    @Override
    public byte[] hash() {
        return bitSet.toByteArray();
    }

    @Override
    public  boolean exist(byte[] bitmap, byte[] key) {
        Objects.requireNonNull(bitmap);
        Objects.requireNonNull(key);
        BitSet set  = BitSet.valueOf(bitmap);
        for (int i = 0; i < hashCount; i++) {
            int hash = hash(key, i);
            if (!set.get(hash)) {
                return false;
            }
        }
        return true;
    }

    @Override
    public void reset() {
        bitSet.clear();
        keyCount = 0;
    }

    @Override
    public int keyLen() {
        return keyCount;
    }
}
