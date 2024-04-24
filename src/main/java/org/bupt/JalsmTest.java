package org.bupt;

import org.bupt.param.Index;
import org.bupt.param.KV;
import org.bupt.sstable.handler.SSTReader;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.bupt.sstable.Config;

import java.io.*;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Random;

import static org.bupt.util.UvarintUtil.readUvarintFromFile;
import static org.junit.jupiter.api.Assertions.*;

class JalsmTest {
    private Jalsm jalsm;

    @BeforeEach
    void setUp() {
        jalsm = Jalsm.getInstance();
    }

    @Test
    void getInstance_returnsSingletonInstance() {
        Jalsm instance1 = Jalsm.getInstance();
        Jalsm instance2 = Jalsm.getInstance();
        assertSame(instance1, instance2);
    }

    @Test
    void putAndGet_returnsCorrectValue() {
        byte[] key = "main_*0".getBytes();
        byte[] value = "value_*0*414314213".getBytes();
        jalsm.PUT(key, value);
        assertArrayEquals(value, jalsm.GET(key));
    }

    @Test
    void get_returnsNullForNonExistentKey() {
        byte[] key = "nonExistentKey".getBytes();
        assertNull(jalsm.GET(key));
    }

    @Test
    void putAndGet_handlesMultipleKeys() {
        byte[] key1 = "key1".getBytes();
        byte[] value1 = "value1".getBytes();
        byte[] key2 = "key2".getBytes();
        byte[] value2 = "value2".getBytes();
        jalsm.PUT(key1, value1);
        jalsm.PUT(key2, value2);
        assertArrayEquals(value1, jalsm.GET(key1));
        assertArrayEquals(value2, jalsm.GET(key2));
    }

    @Test
    void putAndGet_overwritesExistingKey() {
        byte[] key = "key".getBytes();
        byte[] value1 = "value1".getBytes();
        byte[] value2 = "value2".getBytes();
        jalsm.PUT(key, value1);
        jalsm.PUT(key, value2);
        assertArrayEquals(value2, jalsm.GET(key));
    }

    @Test
    void put_throwsExceptionForNullKey() {
        byte[] key = null;
        byte[] value = "value".getBytes();
        assertThrows(NullPointerException.class, () -> jalsm.PUT(key, value));
    }

    @Test
    void put_throwsExceptionForNullValue() {
        byte[] key = "key".getBytes();
        byte[] value = null;
        assertThrows(NullPointerException.class, () -> jalsm.PUT(key, value));
    }

    @Test
    void get_FileInf() throws IOException {
        Config  cfg = new Config();
//        String file = "2_1.sst";
//        String path = String.join(File.separator, cfg.dir , file);
//        FileInputStream inputStream = new FileInputStream(path);
//        int available= inputStream.available();
//        RandomAccessFile accessFile = new RandomAccessFile(path,"r");
//        int length = (int) accessFile.length();
//        int seekIndex = (int) (accessFile.length() -cfg.SSTFooterSize);
//        accessFile.seek(seekIndex);
//       int filterOffset = (int) readUvarint(accessFile);
//        int filterSize = (int) readUvarint(accessFile);
//        int indexOffset = (int) readUvarint(accessFile);
//        int indexSize = (int) readUvarint(accessFile);
        SSTReader reader = new SSTReader(cfg, "1_8.sst");
        reader.ReadFoot();
        Map<Integer , byte[]> map = reader.ReadFilter();
        List<Index> indexList = reader.ReadIndex();
        List<KV> kvs = reader.ReadBlockData(reader.ReadBlock(122876,1027));
        List<KV> kvList = reader.ReadData();
    }

    @Test
    void test_jalsm_PutAndGet(){
        int testCnt = 300000;
        Random random  =new Random();
        String ThreadName = "main_*";
        long beginTime = System.currentTimeMillis() , writeEndTime , readEndTime;
        int count = 0;
        for(int i=0;i<testCnt;i++){
            byte[] key = (ThreadName + i).getBytes();
            byte[] value = ("value_*"+ i +"*_*"+ThreadName).getBytes();
            jalsm.PUT(key,value);
        }
        writeEndTime = System.currentTimeMillis();
        for(int i=0;i<testCnt;i++){
            byte[] key = (ThreadName + i).getBytes();
            byte[] getValue =  jalsm.GET(key);
            byte[] value = ("value_*"+ i +"*_*"+ThreadName).getBytes();
            if(value!=null&& Arrays.compare(value,getValue)==0){
                count++;
            }
        }
        readEndTime = System.currentTimeMillis();
        System.out.println("PUT count:"+testCnt);
        System.out.println("PUT time cost:"+(writeEndTime-beginTime)+"ms");
        System.out.println("GET count:"+count);
        System.out.println("GET time cost:"+(readEndTime-writeEndTime)+"ms");
        jalsm.CLOSE();
    }

    @Test
    public void test_jalsm_clodstart(){
        int testCnt = 300000;

        int count = 0;
        String threadName = Thread.currentThread().getName();
        long beginTime = System.currentTimeMillis()  , readEndTime;
        Jalsm jalsm1 = Jalsm.getInstance();
        for(int i=0;i<testCnt;i++){
            byte[] key = (threadName + i).getBytes();
            byte[] getValue =  jalsm1.GET(key);
            byte[] value = ("value_*"+ i +"*_*"+threadName).getBytes();
            if(value!=null&& Arrays.compare(value,getValue)==0){
                count++;
            }
        }
        readEndTime = System.currentTimeMillis();
        System.out.println("GET count:"+count);
        System.out.println("GET time cost:"+(readEndTime-beginTime)+"ms");
        jalsm.CLOSE();
    }

    public static long readUvarint( RandomAccessFile buffer) throws IOException {
        long value = 0;
        int shift = 0;

        while (true) {
            byte b =buffer.readByte();
            // 读取当前字节的低7位
            long byteValue = (b & 0x7FL);

            // 将读取的7位值左移相应的位数，并合并到value中
            value |= (byteValue << shift);

            // 检查当前字节是否是UVarint的最后一个字节
            if ((b & 0x80) == 0) {
                break;
            }

            shift += 7; // 为下一个字节的7位数据左移7位
        }
        return value;
    }

}
