package org.bupt.memtable;

public class MemTableCompactItem {
    public String walFile;
    public Memtable memtable;
    public MemTableCompactItem(String walFile, Memtable memtable) {
        this.walFile = walFile;
        this.memtable = memtable;
    }
}
