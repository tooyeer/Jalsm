package org.bupt;

import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.epoll.Epoll;
import io.netty.channel.epoll.EpollEventLoopGroup;
import io.netty.channel.epoll.EpollServerSocketChannel;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.handler.logging.LogLevel;
import io.netty.handler.logging.LoggingHandler;
import org.bupt.netty.JalsmServerHandler;
import org.junit.jupiter.api.Test;

public class JalsmServer {
    public static void run() {
        // 创建EpollEventLoopGroup的实例，用于处理I/O操作
        EventLoopGroup bossGroup = new NioEventLoopGroup(1);
        EventLoopGroup workerGroup = new NioEventLoopGroup(1);

        try {
            ServerBootstrap b = new ServerBootstrap();
            b.group(bossGroup, workerGroup)
                    .channel(NioServerSocketChannel.class) // 使用Epoll的ServerSocketChannel
                    .handler(new LoggingHandler(LogLevel.INFO)) // 记录访问日志
                    .childHandler(new ChannelInitializer<SocketChannel>() {
                        @Override
                        protected void initChannel(SocketChannel ch) {
                            // 添加自己的ChannelHandler
                            ch.pipeline().addLast(new JalsmServerHandler());
                        }
                    });
            ChannelFuture f = b.bind(12421).sync(); // 绑定端口并等待绑定成功
            f.channel().closeFuture().sync(); // 等待服务器关闭
        } catch (InterruptedException e) {
            e.printStackTrace();
        } finally {
            workerGroup.shutdownGracefully();
            bossGroup.shutdownGracefully();
        }
    }

    @Test
    public void test() {
        run();
    }
}
