/*
 * Copyright 2015 The Netty Project
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

import io.netty.util.concurrent.FastThreadLocal;
import io.netty.util.concurrent.FastThreadLocalThread;
import io.netty.util.internal.SystemPropertyUtil;
import org.junit.Test;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.LockSupport;

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.junit.Assert.*;

public class PooledByteBufAllocatorTest2 {

    @Test
    public void testAllocateHeap() {
        // 从 DEFAULT 内存池中中申请一段内存: 默认申请 direct 内存。其中 DEFAULT 是pooled内存池
        /**
         * 分两步研究:
         *     1. DEFAULT 内存池 是如何创建的 (内存池可以是 direct，也可以是heap的)
         *     2. 如何从上一步的内存池中 申请一小段区域 来使用。
         */
        ByteBuf buf = PooledByteBufAllocator.DEFAULT.buffer();
    }

}
