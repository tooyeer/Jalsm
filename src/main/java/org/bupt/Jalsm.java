package org.bupt;

import org.bupt.memtable.handler.*;
import org.bupt.param.Index;
import org.bupt.param.KV;
import org.bupt.sstable.Config;
import org.bupt.memtable.*;
import org.bupt.sstable.Node;
import org.bupt.sstable.handler.*;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.stream.Collectors;

public class Jalsm {
    Config cfg;

    ReentrantReadWriteLock dataLock;

    ReentrantReadWriteLock[] levelLocks;

    Memtable memtable;

    List<MemTableCompactItem> rOnlyMemTables;

    int memTableIndex;

    List<AtomicInteger> levelToSeq;

    WalWriter walWriter;

    ExecutorService executors;

    List<List<Node>> nodes;

    private static volatile Jalsm instance;
    public static Jalsm getInstance() {
        if (instance == null) {
            synchronized (Jalsm.class) {
                if (instance == null) {
                    instance = new Jalsm();
                }
            }
        }
        return instance;
    }

    public Jalsm() {
        this.cfg = Config.getConfig();
        this.dataLock = new ReentrantReadWriteLock();
        this.levelLocks = new ReentrantReadWriteLock[cfg.maxLevel];
        this.rOnlyMemTables = new ArrayList<>();
        for(int i = 0; i < cfg.maxLevel; i++) {
            this.levelLocks[i] = new ReentrantReadWriteLock();
        }
        this.levelToSeq = new ArrayList<>(cfg.maxLevel);
        for(int i = 0; i < cfg.maxLevel; i++) {
            this.levelToSeq.add(new AtomicInteger(0));
        }
        this.executors = Executors.newFixedThreadPool(1);
        this.nodes = new ArrayList<>();
        for(int i=0;i<cfg.maxLevel;i++){
            this.nodes.add(new ArrayList<>());
        }
        try {
            reloadFromSSTable();
            reLoadFromWalFile();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
    public void newMemTable(){
        this.memtable = cfg.CreateMemtable();
        this.walWriter = new WalWriter(walfile());
    }
    private void reLoadFromWalFile() throws IOException {
        File dir = new File(String.join(File.separator,cfg.dir , "walfile"));
        if(!dir.exists()){
            dir.mkdirs();
            newMemTable();
            return ;
        }
        if(dir.isDirectory()){
            String[] files = dir.list();
            List<String> walFiles = Arrays.stream(files).filter(f->f.endsWith(".wal")).collect(Collectors.toList());
            if(walFiles.isEmpty()){
                newMemTable();
            }else{
                walFiles.sort((a,b)->{
                    int aIndex = Integer.parseInt(a.substring(0,a.length()-4));
                    int bIndex = Integer.parseInt(b.substring(0,b.length()-4));
                    return aIndex - bIndex;
                });
                for(int i=0;i<walFiles.size();i++){
                    String walFilePath = String.join(File.separator,cfg.dir,"walfile",walFiles.get(i));
                    WalReader reader = new WalReader(walFilePath);
                    Memtable newMemtable = cfg.CreateMemtable();
                    reader.RestoreToMemtable(newMemtable);
                    if(i==walFiles.size()-1){
                        this.memtable = newMemtable;
                        this.memTableIndex = Integer.parseInt(walFiles.get(i).substring(0,walFiles.get(i).length()-4));
                        this.walWriter = new WalWriter(walFilePath);
                    }else{
                        MemTableCompactItem oldItem = new MemTableCompactItem(walFilePath,newMemtable);
                        rOnlyMemTables.add(oldItem);
                        doMemCompate(oldItem);
                    }
                    reader.close();
                }
            }
        }
    }
    public void reloadFromSSTable(){
        File dir = new File(this.cfg.dir);
        if(dir.exists()&&dir.isDirectory()){
            String[] files = dir.list();
            List<String> sstFiles = Arrays.stream(files).filter(f->f.endsWith(".sst")).toList();
            for(String file : sstFiles){
                String[] parts = file.split("_");
                int level = Integer.parseInt(parts[0]);
                int seq = Integer.parseInt(parts[1].substring(0,parts[1].length()-4));
                this.levelLocks[level].readLock().lock();
                SSTReader reader = new SSTReader(cfg,file);
                reader.ReadFoot();
                List<Index> indexList = reader.ReadIndex();
                Map<Integer,byte[]> blockToFilter = reader.ReadFilter();
                this.levelLocks[level].readLock().unlock();
                Node node = new Node(cfg,file,level,reader.size(),seq,reader,indexList,blockToFilter);
                insertNodeWithReader(node);
            }
        }
    }
    private void insertNodeWithReader(Node node){
        levelToSeq.get(node.getLevel()).set(node.getSeq());
        if(node.getLevel()==0){
            this.levelLocks[0].writeLock().lock();
            this.nodes.getFirst().add(node);
            this.levelLocks[0].writeLock().unlock();
            return;
        }

        for(int i=0;i<this.nodes.get(node.getLevel()).size();i++){
            if(Arrays.compare(node.end() , this.nodes.get(node.getLevel()).get(i).start())<0){
                this.levelLocks[node.getLevel()].writeLock().lock();
                List<Node> newNodes = new ArrayList<>(this.nodes.get(node.getLevel()).size()+1);
                newNodes.addAll(this.nodes.get(node.getLevel()).subList(0,i));
                newNodes.add(node);
                newNodes.addAll(this.nodes.get(node.getLevel()).subList(i,this.nodes.get(node.getLevel()).size()));
                this.nodes.set(node.getLevel(),newNodes);
                this.levelLocks[node.getLevel()].writeLock().unlock();
                return;
            }
        }
        this.levelLocks[node.getLevel()].writeLock().lock();
        this.nodes.get(node.getLevel()).add(node);
        this.levelLocks[node.getLevel()].writeLock().unlock();
    }
    public void PUT(byte[] key, byte[] value) {
        Objects.requireNonNull(key);
        Objects.requireNonNull(value);
        dataLock.writeLock().lock();
        walWriter.write(key,value);
        memtable.put(key, value);

        if(this.memtable.size()*5/4>this.cfg.SSTSize){
            refreshMemTableWhileFUll();
            return ;
        }
        dataLock.writeLock().unlock();
    }
    private void refreshMemTableWhileFUll(){
        MemTableCompactItem oldItem = new MemTableCompactItem(walfile() , memtable);
        rOnlyMemTables.add(oldItem);
        walWriter.close();
        memTableIndex++;
        newMemTable();
        dataLock.writeLock().unlock();
        doMemCompate(oldItem);
    }
    public byte[] GET(byte[] key){
        dataLock.readLock().lock();
            Objects.requireNonNull(key);
            byte[] value = memtable.get(key);
            if(value!=null){
                dataLock.readLock().unlock();
                return value;
            }
            for(int i=0;i<this.rOnlyMemTables.size();i++){
                value = rOnlyMemTables.get(i).memtable.get(key);
                if(value!=null){
                    dataLock.readLock().unlock();
                    return value;
                }
            }
            dataLock.readLock().unlock();

            for(int i=0;i<this.nodes.size();i++){
                try{
                    this.levelLocks[i].readLock().lock();
                    List<Node> nodeList = this.nodes.get(i);
                    for(Node node : nodeList){
                        if(Arrays.compare(key,node.start())>=0&&Arrays.compare(key , node.end())<=0){
                            value = node.get(key);
                            if(value!=null){
                                this.levelLocks[i].readLock().unlock();
                                return value;
                            }
                        }
                    }
                    this.levelLocks[i].readLock().unlock();
                }catch (Exception e) {
                    e.printStackTrace();
                }
            }
        return null;
    }
    public void CLOSE(){
        try{
            executors.shutdown();
            while(!executors.isTerminated()){
                executors.awaitTermination(1, TimeUnit.SECONDS);
                Thread.sleep(1000);
            }
            for(int i=0;i<this.nodes.size();i++){
                List<Node> nodeList = this.nodes.get(i);
                for(Node node : nodeList){
                    node.close();
                }
            }
        }catch (Exception e){
            e.printStackTrace();
        }
    }
    private void doMemCompate(MemTableCompactItem oldItem){
        executors.submit(()->{
            try{
                dataLock.writeLock().lock();
                flushMemTable(oldItem.memtable);
                for(int i= 0; i < rOnlyMemTables.size(); i++) {
                    if(rOnlyMemTables.get(i) == oldItem) {
                        List<MemTableCompactItem> newTable = rOnlyMemTables.stream().skip(i).toList();
                        rOnlyMemTables.clear();
                        rOnlyMemTables.addAll(newTable);
                        break;
                    }
                }
                Path file = Paths.get(oldItem.walFile);
                Files.delete(file);
                dataLock.writeLock().unlock();
            }catch (Exception e){
                System.out.println(Thread.currentThread().getName());
                e.printStackTrace();
            }
        });
    }
    private void flushMemTable(Memtable memtable){
        int seq = this.levelToSeq.getFirst().get()+1;
        SSTWriter writer = new SSTWriter(sstFile(0,seq),cfg);
        try{
            List<KV> kvs = memtable.All();
            kvs.forEach(kv->{
                writer.Append(kv.key,kv.value);
            });
            writer.Finish();
            Node node = new Node(cfg,sstFile(0,seq),0, writer.size(), seq,new SSTReader(this.cfg , sstFile(0,seq)),writer.getIndex(),writer.getBlockToFilter());
            insertNodeWithReader(node);
            trySSTableCompate(0);
        } catch (IOException e) {
            throw new RuntimeException(e);
        } finally {
            writer.close();
        }
    }
    private void trySSTableCompate(int level){
        if(level>=cfg.maxLevel-1){
            return ;
        }
        long size = 0;
        this.levelLocks[level].readLock().lock();
        for(Node node : nodes.get(level)){
            size+=node.size();
        }
        this.levelLocks[level].readLock().unlock();
        if(size>=cfg.SSTSize*Math.pow(10,level)*cfg.SSTNumperLevel){
            doSSTableCompate(level);
        }
    }
    private void doSSTableCompate(int level) {
        executors.submit(() -> {

            List<Node> pickedNodes = pickNode(level);
            List<KV> pickKV = pickKVs(pickedNodes);

            SSTWriter writer = new SSTWriter(sstFile(level + 1, levelToSeq.get(level + 1).get() + 1), cfg);
            long maxLevelSize = (long) (cfg.SSTSize * Math.pow(2,level+1)* cfg.SSTNumperLevel);
            try{
                int seq = this.levelToSeq.get(level+1).get()+1;
                for(int i=0;i<pickKV.size();i++){
                    if(writer.size()>maxLevelSize){
                        writer.Finish();
                        Node Node = new Node(this.cfg,sstFile(level+1,seq),level+1,writer.size(),seq,new SSTReader(this.cfg,sstFile(level+1,seq)),writer.getIndex(),writer.getBlockToFilter());
                        writer.close();
                        insertNodeWithReader(Node);
                        writer = new SSTWriter(sstFile(level+1,levelToSeq.get(level+1).get()+1),cfg);
                        seq = this.levelToSeq.get(level+1).get()+1;
                    }
                    writer.Append(pickKV.get(i).key,pickKV.get(i).value);
                    if(i==pickKV.size()-1){
                        writer.Finish();
                        Node Node = new Node(this.cfg,sstFile(level+1,seq),level+1,writer.size(),seq,new SSTReader(this.cfg,sstFile(level+1,seq)),writer.getIndex(),writer.getBlockToFilter());
                        writer.close();
                        insertNodeWithReader(Node);
                        seq = this.levelToSeq.get(level+1).get()+1;
                    }
                }
                removePickedNodes(pickedNodes);
                trySSTableCompate(level+1);
            }catch (IOException e){
                e.printStackTrace();
                throw new RuntimeException(e);
            }
        });
    }
    private List<Node> pickNode(int level){
        List<Node> pickedNodes = new ArrayList<>();
        byte[] start = this.nodes.get(level).getFirst().start();
        byte[] end = this.nodes.get(level).getFirst().end();
        int mid = this.nodes.get(level).size()>>1;
        if(Arrays.compare(this.nodes.get(level).get(mid).start(),start)<0){
            start = this.nodes.get(level).get(mid).start();
        }
        if(Arrays.compare(this.nodes.get(level).get(mid).end(),end)>0){
            end = this.nodes.get(level).get(mid).end();
        }
        for(int i=level+1;i>=level;i--){
            for(Node node : this.nodes.get(i)){
                if(Arrays.compare(node.start(),end)<0&&Arrays.compare(node.end(),start)>0){
                    pickedNodes.add(node);
                }
            }
        }
        return pickedNodes;
    }


    private List<KV> pickKVs(List<Node> nodes){
        Memtable table = this.cfg.CreateMemtable();
        for(Node node :nodes){
            node.getAll().forEach(kv->{
                if(kv!=null){
                    table.put(kv.key,kv.value);
                }
            });
        }
        return table.All();
    }

    private void removePickedNodes(List<Node> nodes) throws IOException {
        for(Node node :nodes){
            for(int i=0;i<this.nodes.get(node.getLevel()).size();i++){
                if(this.nodes.get(node.getLevel()).get(i)==node){
                    this.levelLocks[node.getLevel()].writeLock().lock();
                    node.close();
                    this.nodes.get(node.getLevel()).get(i).destroy();
                    this.nodes.get(node.getLevel()).remove(i);
                    this.levelLocks[node.getLevel()].writeLock().unlock();
                    break;
                }
            }
        }
    }

    private String walfile(){
        return String.join(File.separator,cfg.dir , "walfile", String.format("%d.wal", this.memTableIndex));
    }
    private String sstFile(int level , int seq){
        return String.format("%d_%d.sst",level,seq);
    }
}
