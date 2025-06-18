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

import java.io.*;
import java.net.ServerSocket;
import java.net.Socket;

public class BlockingIOServerTest {

    private static final int PORT = 9090;

    public static void main(String[] args) {
        try {
            ServerSocket serverSocket = new ServerSocket(PORT);
            System.out.println("Blocking IO Server started on port " + PORT);

            while (true) {
                // 阻塞，直到有客户端连接
                Socket clientSocket = serverSocket.accept();
                System.out.println("Accepted connection from " + clientSocket.getRemoteSocketAddress());

                // 启动新线程处理客户端
                new Thread(new ClientHandler(clientSocket)).start();
            }

        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    static class ClientHandler implements Runnable {
        private final Socket socket;

        public ClientHandler(Socket socket) {
            this.socket = socket;
        }

        @Override
        public void run() {
            try {
                BufferedReader reader = new BufferedReader(new InputStreamReader(socket.getInputStream()));
                BufferedWriter writer = new BufferedWriter(new OutputStreamWriter(socket.getOutputStream()));
                String line;
                while ((line = reader.readLine()) != null) {
                    System.out.println("Received: " + line);
                    writer.write("Echo: " + line + "\n");
                    writer.flush();
                }
            } catch (IOException e) {
                System.out.println("Connection closed: " + socket.getRemoteSocketAddress());
            } finally {
                try {
                    socket.close();
                } catch (IOException ignore) {
                }
            }
        }
    }

}
