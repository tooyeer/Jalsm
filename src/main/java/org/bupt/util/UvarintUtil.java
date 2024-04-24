package org.bupt.util;

import io.netty.buffer.ByteBuf;

import java.io.IOException;
import java.io.InputStream;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;

public class UvarintUtil {
    public static long readUvarintFromStream(InputStream in) throws IOException {
        long value = 0;
        int shift = 0;
        byte b;
        do {
            b = (byte) in.read();
            if (b == -1) {
                return 0;
            }
            // 将输入字节的低7位添加到结果中。
            value |= (long) (b & 0x7F) << shift;
            shift += 7;
        } while ((b & 0x80) != 0); // 检查是否是最后一个字节。

        return value;
    }
    public static long readUvarintFromFile(RandomAccessFile in) throws IOException {
        long value = 0;
        int shift = 0;

        while (true) {
            byte b =in.readByte();
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
    public static long readUvarintFromBuffer(ByteBuffer in) throws IOException {
        long value = 0;
        int shift = 0;
        while (true) {
            byte b =in.get();
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
    public static long readUvarintFromByteBuf(ByteBuf in) throws IOException {
        long value = 0;
        int shift = 0;

        while (true) {
            byte b =in.getByte(in.readerIndex());
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

    public static int putUvarint(long value, byte[] buffer , int offset) {
        while (true) {
            // 将值分成7位一组，每次取低7位进行编码
            byte b = (byte) (value & 0x7FL);
            value >>>= 7;

            // 如果还有更多的7位组要编码，则设置当前字节的最高位
            if (value != 0) {
                b |= 0x80;
            }

            buffer[offset++] = b;

            // 如果已经编码了最后一个7位组，则退出循环
            if (value == 0) {
                break;
            }
        }
        return offset;
    }
}
