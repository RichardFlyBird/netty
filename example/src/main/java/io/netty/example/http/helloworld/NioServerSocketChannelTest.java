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
package io.netty.example.http.helloworld;

import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelOption;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.handler.logging.LogLevel;
import io.netty.handler.logging.LoggingHandler;
import io.netty.handler.ssl.SslContext;
import io.netty.handler.ssl.SslContextBuilder;
import io.netty.handler.ssl.util.SelfSignedCertificate;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.*;
import java.util.Iterator;

public class NioServerSocketChannelTest {

    private static final int PORT = 9090;

    public static void main(String[] args) throws IOException {
        // 创建 ServerSocketChannel 并绑定端口
        ServerSocketChannel serverChannel = ServerSocketChannel.open();
        serverChannel.bind(new InetSocketAddress(PORT));
        serverChannel.configureBlocking(false);

        // 创建 Selector
        Selector selector = Selector.open();
        serverChannel.register(selector, SelectionKey.OP_ACCEPT);

        System.out.println("NIO Server started on port " + PORT);

        while (true) {
            // 阻塞等待就绪事件
            selector.select();

            Iterator<SelectionKey> keyIterator = selector.selectedKeys().iterator();
            while (keyIterator.hasNext()) {
                SelectionKey key = keyIterator.next();
                keyIterator.remove();

                try {
                    if (key.isAcceptable()) {
                        // 接受客户端连接
                        ServerSocketChannel server = (ServerSocketChannel) key.channel();
                        SocketChannel clientChannel = server.accept();
                        clientChannel.configureBlocking(false);
                        clientChannel.register(selector, SelectionKey.OP_READ);
                        System.out.println("Connected to: " + clientChannel.getRemoteAddress());

                    } else if (key.isReadable()) {
                        // 读取客户端数据
                        SocketChannel clientChannel = (SocketChannel) key.channel();
                        ByteBuffer buffer = ByteBuffer.allocate(1024);
                        int bytesRead = clientChannel.read(buffer);

                        if (bytesRead == -1) {
                            System.out.println("Connection closed by client: " + clientChannel.getRemoteAddress());
                            clientChannel.close();
                            continue;
                        }

                        buffer.flip();
                        String received = new String(buffer.array(), 0, buffer.limit());
                        System.out.println("Received: " + received);

                        // Echo 回客户端
                        buffer.rewind();
                        clientChannel.write(buffer);
                    }
                } catch (IOException e) {
                    key.cancel();
                    key.channel().close();
                    e.printStackTrace();
                }
            }
        }
    }
}
