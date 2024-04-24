package org.bupt.sstable;

import org.bupt.memtable.Memtable;
import org.bupt.memtable.impl.SkipList;
import org.bupt.sstable.impl.BloomFilter;

import java.io.File;
import java.io.InputStream;
import java.util.Properties;

public class Config {
    // 存储路径
    public String dir;
    // 最大层数
    public int maxLevel;
    // SST文件大小
    public long SSTSize;
    // 每层SST文件数量
    public int SSTNumperLevel;
    // SST数据块大小
    public int SSTDataBlockSize;
    // SST Footer大小
    public int SSTFooterSize;
    // 过滤器
    public Filter filter;
    // Memtable
    public Memtable memtable;
    public String filterType;
    public String memtableType;
    // 私有静态实例变量
    private static volatile Config instance;
    public Memtable CreateMemtable(){
        if(memtableType.equals("SkipList")){
            return new SkipList();
        }
        return new SkipList();
    }
    public Filter CreateFilter(){
        if(filterType.equals("BloomFilter")){
            return new BloomFilter(1024,5);
        }
        return new BloomFilter(1024,5);
    }
    public Config(){
        Properties prop = new Properties();
        try (InputStream in = Config.class.getClassLoader().getResourceAsStream("config.properties")) {
            prop.load(in);// Load the properties
            this.dir = prop.getProperty("dir");
            this.maxLevel = Integer.parseInt(prop.getProperty("maxLevel"));
            this.SSTSize = Long.parseLong(prop.getProperty("SSTSize"));
            this.SSTNumperLevel = Integer.parseInt(prop.getProperty("SSTNumperLevel"));
            this.SSTDataBlockSize = Integer.parseInt(prop.getProperty("SSTDataBlockSize"));
            this.SSTFooterSize = Integer.parseInt(prop.getProperty("SSTFooterSize"));
            this.filterType = prop.getProperty("filterType");
            this.memtableType = prop.getProperty("memtableType");
            this.filter = CreateFilter();
            this.memtable = CreateMemtable();
            checkConfig();
        }catch (Exception e){
            e.printStackTrace();
        }
    }
    private void checkConfig(){
        File file = new File(this.dir);
        String wal = String.join(File.separator,this.dir,"walfile");
        File walfile = new File(wal);
        if(!file.exists()){
            file.mkdirs();
            walfile.mkdirs();
        }else if(!walfile.exists()){
            walfile.mkdirs();
        }
    }
    public static Config getConfig(){
        if(instance == null){
            synchronized (Config.class){
                if(instance == null){
                    instance = new Config();
                }
            }
        }
        return instance;
    }


}
