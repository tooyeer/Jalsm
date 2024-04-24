package org.bupt.sstable.impl;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class BloomFilterTest {
    private BloomFilter bloomFilter;

    @BeforeEach
    void setUp() {
        bloomFilter = new BloomFilter(1024, 5);
    }

    @Test
    void shouldAddKeySuccessfully() {
        byte[] key = new byte[]{109, 97, 105, 110, 95, 42, 49};
        byte[] bitmap = new byte[]{-128, 0, 4, 4, 0, 72, 0, 0, 4, 0, 37, 4, 0, 64, 0, 0, 0, 4, 0, 72, 0, 80, 64, -128, 0, 0, 12, 0, 32, 0, 80, 66};
        assertTrue(bloomFilter.exist(bitmap , key));
    }

    @Test
    void shouldNotContainNonExistentKey() {
        byte[] key = "test".getBytes();
        byte[] nonExistentKey = "nonExistent".getBytes();
        bloomFilter.add(key);
        assertFalse(bloomFilter.isContains(nonExistentKey));
    }

    @Test
    void shouldThrowExceptionWhenFilterIsFull() {
        byte[] key = "test".getBytes();
        bloomFilter = new BloomFilter(1, 2);
        bloomFilter.add(key);
        assertThrows(IllegalStateException.class, () -> bloomFilter.add(key));
    }

    @Test
    void shouldResetSuccessfully() {
        byte[] key = "test".getBytes();
        bloomFilter.add(key);
        bloomFilter.reset();
        assertFalse(bloomFilter.isContains(key));
    }

    @Test
    void shouldReturnCorrectKeyLength() {
        byte[] key1 = "test1".getBytes();
        byte[] key2 = "test2".getBytes();
        bloomFilter.add(key1);
        bloomFilter.add(key2);
        assertEquals(2, bloomFilter.keyLen());
    }
}
