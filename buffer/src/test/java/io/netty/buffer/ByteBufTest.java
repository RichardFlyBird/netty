/*
 * Copyright 2012 The Netty Project
 *
 * The Netty Project licenses this file to you under the Apache License,
 * version 2.0 (the "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at:
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations
 * under the License.
 */
package io.netty.buffer;

import org.junit.Test;

import java.nio.ByteBuffer;

import static org.junit.Assert.assertEquals;

/**
 * An abstract test class for channel buffers
 */
public class ByteBufTest {

    @Test
    public void UnPoolByteBufTest() {
        /**
         * 《申请java堆内存》
         */
        // netty 封装的ByteBuf
        ByteBuf bytebuf = Unpooled.buffer(1024);
        bytebuf.writeChar(3);
        bytebuf.writeInt(10);
        assertEquals(3, bytebuf.readChar());
        assertEquals(10, bytebuf.readInt());

        // JDK 原生的ByteBuffer
        ByteBuffer byteBuffer = ByteBuffer.allocate(1024);
        byteBuffer.putChar((char) 3);
        byteBuffer.putInt(10);
        // 切换成读模式
        byteBuffer.flip();
        assertEquals((char) 3, byteBuffer.getChar());
        assertEquals(10, byteBuffer.getInt());

        /**
         * 《申请c堆内存》
         */
        // netty 封装的ByteBuf
        ByteBuf directBuffer = Unpooled.directBuffer(1024);
        directBuffer.writeChar(3);
    }

}
