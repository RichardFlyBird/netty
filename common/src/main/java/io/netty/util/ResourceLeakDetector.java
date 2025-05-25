/*
 * Copyright 2013 The Netty Project
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

package io.netty.util;

import io.netty.util.internal.MathUtil;
import io.netty.util.internal.PlatformDependent;
import io.netty.util.internal.SystemPropertyUtil;
import io.netty.util.internal.logging.InternalLogger;
import io.netty.util.internal.logging.InternalLoggerFactory;

import java.lang.ref.PhantomReference;
import java.lang.ref.ReferenceQueue;
import java.util.ArrayDeque;
import java.util.Deque;
import java.util.EnumSet;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicBoolean;

import static io.netty.util.internal.StringUtil.EMPTY_STRING;
import static io.netty.util.internal.StringUtil.NEWLINE;
import static io.netty.util.internal.StringUtil.simpleClassName;

public class ResourceLeakDetector<T> {

    private static final String PROP_LEVEL_OLD = "io.netty.leakDetectionLevel";
    private static final String PROP_LEVEL = "io.netty.leakDetection.level";
    private static final Level DEFAULT_LEVEL = Level.SIMPLE;

    private static final String PROP_MAX_RECORDS = "io.netty.leakDetection.maxRecords";
    private static final int DEFAULT_MAX_RECORDS = 4;
    private static final int MAX_RECORDS;

    /**
     * Represents the level of resource leak detection.
     */
    public enum Level {
        /**
         * Disables resource leak detection.
         */
        DISABLED,
        /**
         * Enables simplistic sampling resource leak detection which reports there is a leak or not,
         * at the cost of small overhead (default).
         */
        SIMPLE,
        /**
         * Enables advanced sampling resource leak detection which reports where the leaked object was accessed
         * recently at the cost of high overhead.
         */
        ADVANCED,
        /**
         * Enables paranoid resource leak detection which reports where the leaked object was accessed recently,
         * at the cost of the highest possible overhead (for testing purposes only).
         */
        PARANOID
    }

    private static Level level;

    private static final InternalLogger logger = InternalLoggerFactory.getInstance(ResourceLeakDetector.class);

    static {
        final boolean disabled;
        if (SystemPropertyUtil.get("io.netty.noResourceLeakDetection") != null) {
            disabled = SystemPropertyUtil.getBoolean("io.netty.noResourceLeakDetection", false);
            logger.debug("-Dio.netty.noResourceLeakDetection: {}", disabled);
            logger.warn(
                    "-Dio.netty.noResourceLeakDetection is deprecated. Use '-D{}={}' instead.",
                    PROP_LEVEL, DEFAULT_LEVEL.name().toLowerCase());
        } else {
            disabled = false;
        }

        Level defaultLevel = disabled? Level.DISABLED : DEFAULT_LEVEL;

        // First read old property name
        String levelStr = SystemPropertyUtil.get(PROP_LEVEL_OLD, defaultLevel.name()).trim().toUpperCase();

        // If new property name is present, use it
        levelStr = SystemPropertyUtil.get(PROP_LEVEL, levelStr).trim().toUpperCase();
        Level level = DEFAULT_LEVEL;
        for (Level l: EnumSet.allOf(Level.class)) {
            if (levelStr.equals(l.name()) || levelStr.equals(String.valueOf(l.ordinal()))) {
                level = l;
            }
        }

        MAX_RECORDS = SystemPropertyUtil.getInt(PROP_MAX_RECORDS, DEFAULT_MAX_RECORDS);

        ResourceLeakDetector.level = level;
        if (logger.isDebugEnabled()) {
            logger.debug("-D{}: {}", PROP_LEVEL, level.name().toLowerCase());
            logger.debug("-D{}: {}", PROP_MAX_RECORDS, MAX_RECORDS);
        }
    }

    // Should be power of two.
    static final int DEFAULT_SAMPLING_INTERVAL = 128;

    /**
     * @deprecated Use {@link #setLevel(Level)} instead.
     */
    @Deprecated
    public static void setEnabled(boolean enabled) {
        setLevel(enabled? Level.SIMPLE : Level.DISABLED);
    }

    /**
     * Returns {@code true} if resource leak detection is enabled.
     */
    public static boolean isEnabled() {
        return getLevel().ordinal() > Level.DISABLED.ordinal();
    }

    /**
     * Sets the resource leak detection level.
     */
    public static void setLevel(Level level) {
        if (level == null) {
            throw new NullPointerException("level");
        }
        ResourceLeakDetector.level = level;
    }

    /**
     * Returns the current resource leak detection level.
     */
    public static Level getLevel() {
        return level;
    }

    /** the linked list of active resources */
    private final DefaultResourceLeak head = new DefaultResourceLeak(null);
    private final DefaultResourceLeak tail = new DefaultResourceLeak(null);

    private final ReferenceQueue<Object> refQueue = new ReferenceQueue<Object>();
    private final ConcurrentMap<String, Boolean> reportedLeaks = PlatformDependent.newConcurrentHashMap();

    private final String resourceType;
    private final int samplingInterval;
    private final int mask;
    private final long maxActive;
    private long active;
    private final AtomicBoolean loggedTooManyActive = new AtomicBoolean();

    private long leakCheckCnt;

    /**
     * @deprecated use {@link ResourceLeakDetectorFactory#newResourceLeakDetector(Class, int, long)}.
     */
    @Deprecated
    public ResourceLeakDetector(Class<?> resourceType) {
        this(simpleClassName(resourceType));
    }

    /**
     * @deprecated use {@link ResourceLeakDetectorFactory#newResourceLeakDetector(Class, int, long)}.
     */
    @Deprecated
    public ResourceLeakDetector(String resourceType) {
        this(resourceType, DEFAULT_SAMPLING_INTERVAL, Long.MAX_VALUE);
    }

    /**
     * This should not be used directly by users of {@link ResourceLeakDetector}.
     * Please use {@link ResourceLeakDetectorFactory#newResourceLeakDetector(Class)}
     * or {@link ResourceLeakDetectorFactory#newResourceLeakDetector(Class, int, long)}
     */
    @SuppressWarnings("deprecation")
    public ResourceLeakDetector(Class<?> resourceType, int samplingInterval, long maxActive) {
        this(simpleClassName(resourceType), samplingInterval, maxActive);
    }

    /**
     * @deprecated use {@link ResourceLeakDetectorFactory#newResourceLeakDetector(Class, int, long)}.
     */
    @Deprecated
    public ResourceLeakDetector(String resourceType, int samplingInterval, long maxActive) {
        if (resourceType == null) {
            throw new NullPointerException("resourceType");
        }
        if (samplingInterval <= 0) {
            throw new IllegalArgumentException("samplingInterval: " + samplingInterval + " (expected: 1+)");
        }
        if (maxActive <= 0) {
            throw new IllegalArgumentException("maxActive: " + maxActive + " (expected: 1+)");
        }

        this.resourceType = resourceType;
        this.samplingInterval = MathUtil.findNextPositivePowerOfTwo(samplingInterval);
        // samplingInterval is a power of two so we calculate a mask that we can use to
        // check if we need to do any leak detection or not.
        mask = this.samplingInterval - 1;
        this.maxActive = maxActive;

        head.next = tail;
        tail.prev = head;
    }

    /**
     * Creates a new {@link ResourceLeak} which is expected to be closed via {@link ResourceLeak#close()} when the
     * related resource is deallocated.
     *
     * @return the {@link ResourceLeak} or {@code null}
     */
    public final ResourceLeak open(T obj) {
        Level level = ResourceLeakDetector.level;
        if (level == Level.DISABLED) {
            return null;
        }

        if (level.ordinal() < Level.PARANOID.ordinal()) {
            // 抽样是否 生成内存泄露检测的对象: mask =128 - 1, 尾数都是1, 与 leakCheckCnt相与即可得到求余的结果，作为随机数
            if ((leakCheckCnt ++ & mask) == 0) {
                reportLeak(level);
                return new DefaultResourceLeak(obj);
            } else {
                return null;
            }
        } else {
            reportLeak(level);
            return new DefaultResourceLeak(obj);
        }
    }

    private void reportLeak(Level level) {
        // 日志不可用，则gg了，无法打印内存泄露的信息，则直接return
        if (!logger.isErrorEnabled()) {
            for (;;) {
                @SuppressWarnings("unchecked")
                DefaultResourceLeak ref = (DefaultResourceLeak) refQueue.poll();
                if (ref == null) {
                    break;
                }
                ref.close();
            }
            return;
        }

        // Report too many instances.
        int samplingInterval = level == Level.PARANOID? 1 : this.samplingInterval;
        if (active * samplingInterval > maxActive && loggedTooManyActive.compareAndSet(false, true)) {
            reportInstancesLeak(resourceType);
        }

        // Detect and report previous leaks.
        /**
         * 每次new 新的DefaultResourceLeak对象  监视新的 buf对象之前，for循环 queue中所有需要处理的监视对象 (虚引用对象)，看下是否引用的对象内存被释放了
         *      类比:
         *          1. redis 读时 删除过期数据
         *          2. linux分配虚拟内存，只有当虚拟内存被access时，才会触发 page fault，进入linux中断函数分配真实的物理page
         */
        for (;;) {
            @SuppressWarnings("unchecked")
            DefaultResourceLeak ref = (DefaultResourceLeak) refQueue.poll();
            if (ref == null) {
                break;
            }

            ref.clear(); // 这一步没必要，可以删除。因为只有gc之后，ref.referent=null之后，ref 对象才会被放入refQueue.poll()。所以在合理没必要再次置空，除非jvm有bug

            // 若之前已经正常release buf内存了，则之前就会调用 ref.close()方法
            // 所以正常情况(即buf已经被release过了)的话: 这里再调用ref.close()等于false，则直接continue了
            if (!ref.close()) {
                continue;
            }

            // 若代码走到这里，则说明 freed 标志位不为true，即发生 内存泄露，需要上报了
            // ref.toString() 会把属性 lastRecords上记录的堆栈 转化成字符串打印到log中
            String records = ref.toString();
            if (reportedLeaks.putIfAbsent(records, Boolean.TRUE) == null) {
                if (records.isEmpty()) {
                    reportUntracedLeak(resourceType);
                } else {
                    reportTracedLeak(resourceType, records);
                }
            }
        }
    }

    /**
     * This method is called when a traced leak is detected. It can be overridden for tracking how many times leaks
     * have been detected.
     */
    protected void reportTracedLeak(String resourceType, String records) {
        logger.error(
                "LEAK: {}.release() was not called before it's garbage-collected. " +
                "See http://netty.io/wiki/reference-counted-objects.html for more information.{}",
                resourceType, records);
    }

    /**
     * This method is called when an untraced leak is detected. It can be overridden for tracking how many times leaks
     * have been detected.
     */
    protected void reportUntracedLeak(String resourceType) {
        logger.error("LEAK: {}.release() was not called before it's garbage-collected. " +
                "Enable advanced leak reporting to find out where the leak occurred. " +
                "To enable advanced leak reporting, " +
                "specify the JVM option '-D{}={}' or call {}.setLevel() " +
                "See http://netty.io/wiki/reference-counted-objects.html for more information.",
                resourceType, PROP_LEVEL, Level.ADVANCED.name().toLowerCase(), simpleClassName(this));
    }

    /**
     * This method is called when instance leaks are detected. It can be overridden for tracking how many times leaks
     * have been detected.
     */
    protected void reportInstancesLeak(String resourceType) {
        logger.error("LEAK: You are creating too many " + resourceType + " instances.  " +
                resourceType + " is a shared resource that must be reused across the JVM," +
                "so that only a few instances are created.");
    }

    private final class DefaultResourceLeak extends PhantomReference<Object> implements ResourceLeak {
        // 记录内存的创建堆栈，方便排错。只有 level=ADVANCED / PARANOID才会记录内存的创建堆栈。其他如 SIMPLE 不会记录。
        private final String creationRecord;
        private final Deque<String> lastRecords = new ArrayDeque<String>();
        private final AtomicBoolean freed;
        private DefaultResourceLeak prev;
        private DefaultResourceLeak next;
        private int removedRecords;

        DefaultResourceLeak(Object referent) {
            super(referent, referent != null? refQueue : null);

            if (referent != null) {
                Level level = getLevel();
                // 只有 ADVANCED/PARANOID才会记录内存的创建堆栈。其他 SIMPLE 不会
                if (level.ordinal() >= Level.ADVANCED.ordinal()) {
                    creationRecord = newRecord(null, 3);
                } else {
                    creationRecord = null;
                }

                // TODO: Use CAS to update the list.
                /** head 是 static final ResourceLeakDetector<ByteBuf> leakDetector 对象中的属性
                 *      1. 全局leakDetector是GC root，永远不会被GC，所以head也永远不会被GC
                 *      2. 把 新的 DefaultResourceLeak 对象挂载head链表上，即 新的 DefaultResourceLeak 对象被强引用了，也永远不会被回收(为了监视 referent而存在，若没了，就没法监视了)，除非被人手动移除链表。
                 */
                synchronized (head) {
                    prev = head;
                    next = head.next;
                    head.next.prev = this;
                    head.next = this;
                    active ++;
                }
                freed = new AtomicBoolean();
            } else {
                // 若为 空引用对象，则什么都不做，相当于free过了，所以设置freed=true即可，
                creationRecord = null;
                freed = new AtomicBoolean(true);
            }
        }

        @Override
        public void record() {
            record0(null, 3);
        }

        @Override
        public void record(Object hint) {
            record0(hint, 3);
        }

        private void record0(Object hint, int recordsToSkip) {
            if (creationRecord != null) {
                String value = newRecord(hint, recordsToSkip);

                synchronized (lastRecords) {
                    int size = lastRecords.size();
                    if (size == 0 || !lastRecords.getLast().equals(value)) {
                        lastRecords.add(value);
                    }
                    if (size > MAX_RECORDS) {
                        lastRecords.removeFirst();
                        ++removedRecords;
                    }
                }
            }
        }

        @Override
        public boolean close() {
            if (freed.compareAndSet(false, true)) {
                synchronized (head) {
                    active --;
                    prev.next = next;
                    next.prev = prev;
                    prev = null;
                    next = null;
                }
                return true;
            }
            return false;
        }

        @Override
        public String toString() {
            if (creationRecord == null) {
                return EMPTY_STRING;
            }

            final Object[] array;
            final int removedRecords;
            synchronized (lastRecords) {
                array = lastRecords.toArray();
                removedRecords = this.removedRecords;
            }

            StringBuilder buf = new StringBuilder(16384).append(NEWLINE);
            if (removedRecords > 0) {
                buf.append("WARNING: ")
                .append(removedRecords)
                .append(" leak records were discarded because the leak record count is limited to ")
                .append(MAX_RECORDS)
                .append(". Use system property ")
                .append(PROP_MAX_RECORDS)
                .append(" to increase the limit.")
                .append(NEWLINE);
            }
            buf.append("Recent access records: ")
            .append(array.length)
            .append(NEWLINE);

            if (array.length > 0) {
                for (int i = array.length - 1; i >= 0; i --) {
                    buf.append('#')
                       .append(i + 1)
                       .append(':')
                       .append(NEWLINE)
                       .append(array[i]);
                }
            }

            buf.append("Created at:")
               .append(NEWLINE)
               .append(creationRecord);

            buf.setLength(buf.length() - NEWLINE.length());
            return buf.toString();
        }
    }

    private static final String[] STACK_TRACE_ELEMENT_EXCLUSIONS = {
            "io.netty.util.ReferenceCountUtil.touch(",
            "io.netty.buffer.AdvancedLeakAwareByteBuf.touch(",
            "io.netty.buffer.AbstractByteBufAllocator.toLeakAwareBuffer(",
            "io.netty.buffer.AdvancedLeakAwareByteBuf.recordLeakNonRefCountingOperation("
    };

    static String newRecord(Object hint, int recordsToSkip) {
        StringBuilder buf = new StringBuilder(4096);

        // Append the hint first if available.
        if (hint != null) {
            buf.append("\tHint: ");
            // Prefer a hint string to a simple string form.
            if (hint instanceof ResourceLeakHint) {
                buf.append(((ResourceLeakHint) hint).toHintString());
            } else {
                buf.append(hint);
            }
            buf.append(NEWLINE);
        }

        // Append the stack trace.
        // 通过 Throwable()可以获得运行时: 当前代码行的调用堆栈，记录下来。
        // 即从哪里分配的当前对象，后期检测到内存泄露需要报告的时候，直接把这个堆栈打印出来，方便用户排查问题
        StackTraceElement[] array = new Throwable().getStackTrace();
        for (StackTraceElement e: array) {
            if (recordsToSkip > 0) {
                recordsToSkip --;
            } else {
                String estr = e.toString();

                // Strip the noisy stack trace elements.
                boolean excluded = false;
                for (String exclusion: STACK_TRACE_ELEMENT_EXCLUSIONS) {
                    if (estr.startsWith(exclusion)) {
                        excluded = true;
                        break;
                    }
                }

                if (!excluded) {
                    buf.append('\t');
                    buf.append(estr);
                    buf.append(NEWLINE);
                }
            }
        }

        return buf.toString();
    }
}
