package org.bupt;

import io.netty.bootstrap.Bootstrap;
import io.netty.buffer.ByteBuf;
import io.netty.channel.*;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioSocketChannel;

import java.io.IOException;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.bupt.util.UvarintUtil.putUvarint;
import static org.bupt.util.UvarintUtil.readUvarintFromByteBuf;

public class JalsmClient {
    private  Channel channel = null;

    private final static AtomicInteger seqId = new AtomicInteger(0);

    private final static Map<Integer, CompletableFuture<byte[]>> responseMap = new ConcurrentHashMap<>();


    public JalsmClient() throws InterruptedException {
        this.channel = new Bootstrap()
                .group(new NioEventLoopGroup())
                .channel(NioSocketChannel.class)
                .handler(new ChannelInitializer<Channel>() {
                    @Override
                    protected void initChannel(Channel ch) {
                        ChannelPipeline pipeline = ch.pipeline();
                        pipeline.addLast(new SimpleChannelInboundHandler<Object>() {
                            @Override
                            protected void channelRead0(ChannelHandlerContext ctx, Object msg) {
                                ByteBuf input = (ByteBuf) msg;
                                try {
                                    int id =(int) readUvarintFromByteBuf(input);
                                    CompletableFuture<byte[]> future = responseMap.remove(id);
                                    byte[] result = new byte[input.readableBytes()];
                                    future.complete(result);
                                    System.out.println("client get result:"+new String(result));
                                } catch (IOException e) {
                                    throw new RuntimeException(e);
                                }
                            }
                            @Override
                            public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
                                ctx.close();
                            }
                        });
                    }
                })
                .connect("localhost", 12421)
                .sync()
                .channel();
    }
    public CompletableFuture<byte[]> sendMessageAndGetResponse(int type , byte[] key , byte[] value) throws Exception {
        int id = seqId.getAndIncrement();
        CompletableFuture<byte[]> responseFuture = new CompletableFuture<>();
        responseMap.put(id , responseFuture);
        // 发送消息到服务器
        ByteBuf outBuffer = channel.alloc().buffer();
        byte[] buffer = new byte[32];
        int offset = 0;
        offset = putUvarint(id,buffer,offset);
        offset = putUvarint(type,buffer,offset);
        if(type == 1 ){
            //put
            offset = putUvarint(key.length,buffer,offset);
            offset = putUvarint(value.length,buffer,offset);
            byte[] newBuffer = new byte[offset];
            System.arraycopy(buffer,0,newBuffer,0,offset);
            outBuffer.writeBytes(newBuffer);
            outBuffer.writeBytes(key);
            outBuffer.writeBytes(value);
            System.out.println("client put key:"+new String(key) + " value:"+new String(value));
            channel.writeAndFlush(outBuffer);
        }else if(type == 2){
            //get
            offset = putUvarint(key.length,buffer,offset);
            byte[] newBuffer = new byte[offset];
            System.arraycopy(buffer,0,newBuffer,0,offset);
            outBuffer.writeBytes(newBuffer);
            outBuffer.writeBytes(key);
            channel.writeAndFlush(outBuffer);
        }else if(type == 3){
            //close
            channel.writeAndFlush(outBuffer);
            channel.close();
        }
        return responseFuture;
    }
}
