package org.bupt;

import java.util.Arrays;
import java.util.Objects;
import java.util.Random;
import java.util.concurrent.*;

import io.netty.channel.ChannelFuture;
import lombok.extern.log4j.Log4j2;

@Log4j2
public class Main {

    public static void main(String[] args) throws InterruptedException {

        JalsmServer.run();
        return ;
    }
}