package org.bupt.param;

public class Index {
        public byte[] key;
        public int preBlockOffset;
        public int preBlockSize;
        public Index(byte[] key , int preBlockOffset , int preBlockSize){
            this.key = key;
            this.preBlockOffset = preBlockOffset;
            this.preBlockSize = preBlockSize;
        }
}
