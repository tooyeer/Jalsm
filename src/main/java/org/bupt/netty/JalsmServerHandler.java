package org.bupt.netty;

import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.buffer.ByteBuf;
import org.bupt.Jalsm;

import javax.tools.JavaFileManager;
import java.io.IOException;

import static org.bupt.util.UvarintUtil.putUvarint;
import static org.bupt.util.UvarintUtil.readUvarintFromByteBuf;

public class JalsmServerHandler extends ChannelInboundHandlerAdapter  {
    private final Jalsm jalsm = Jalsm.getInstance();
    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) throws IOException {
        // 假设接收到的消息是ByteBuf类型
        ByteBuf inBuffer = (ByteBuf) msg;
        ByteBuf output = ctx.alloc().buffer();
        int id = (int)readUvarintFromByteBuf(inBuffer);
        int type = (int) readUvarintFromByteBuf(inBuffer);
        byte[] buffer = new byte[30];
        int offset = putUvarint(id,buffer,0);
        output.writeBytes(buffer , 0 , offset);
        if(type==1){
            //put
            int keyLength = (int) readUvarintFromByteBuf(inBuffer);
            int valueLength = (int) readUvarintFromByteBuf(inBuffer);
            byte[] key = new byte[keyLength];
            inBuffer.readBytes(key);
            byte[] value = new byte[valueLength];
            inBuffer.readBytes(value);
            System.out.println("Server put key:"+new String(key)+" value:"+new String(value));
            jalsm.PUT(key,value);
            output.writeBytes(new byte[]{1});
            ctx.writeAndFlush(output);
        }else if(type==2){
            //get
            int keyLength = (int) readUvarintFromByteBuf(inBuffer);
            byte[] key = new byte[keyLength];
            inBuffer.readBytes(key);
            byte[] value = jalsm.GET(key);
            if(value==null){
                output.writeBytes(new byte[]{0});
            }else{
                output.writeBytes(new byte[]{1});
                offset = putUvarint(value.length,buffer,0);
                output.writeBytes(buffer,0,offset);
                output.writeBytes(value);
            }
            ctx.writeAndFlush(output);
        }else if(type==3){
            //close
            jalsm.CLOSE();
            ctx.close();
        }
    }
}
