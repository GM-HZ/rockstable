package cn.gm.light.rtable.utils;

import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicLong;

/**
 * @author 明溪
 * @version 1.0
 * @project rocksTable
 * @description TODO
 * @date 2025/3/8 19:34:55
 */

public class RtThreadFactory implements ThreadFactory {
    private static final AtomicLong GLOBAL_ID = new AtomicLong(0);
    private final ThreadGroup threadGroup;
    private final String threadNamePrefix;
    private final boolean daemon;
    private final int priority;
    private final AtomicLong threadId = new AtomicLong(0); // 每个工厂独立计数

    // 默认构造函数（守护线程，优先级 NORM，使用当前线程组）
    public RtThreadFactory(String poolName) {
        this(Thread.currentThread().getThreadGroup(),
                poolName + "-thread",
                true, // 默认守护线程
                Thread.NORM_PRIORITY);
    }

    // 带完整参数的构造函数
    public RtThreadFactory(ThreadGroup threadGroup,
                           String threadNamePrefix,
                           boolean daemon,
                           int priority) {
        this.threadGroup = threadGroup;
        this.threadNamePrefix = threadNamePrefix;
        this.daemon = daemon;
        this.priority = priority;
    }

    @Override
    public Thread newThread(Runnable r) {
        long id = threadId.incrementAndGet();
        Thread thread = new Thread(threadGroup, r,
                threadNamePrefix + "-" + id,
                0); // 0 表示默认堆栈大小
        thread.setDaemon(daemon);
        thread.setPriority(priority);
        thread.setUncaughtExceptionHandler((t, e) -> {
            System.err.printf("Thread [%s] in pool [%s] crashed: %n",
                    t.getName(), threadNamePrefix);
            e.printStackTrace();
        });
        return thread;
    }

    // 工厂方法：为线程池生成默认配置的工厂
    public static ThreadFactory forThreadPool(String poolName) {
        return new RtThreadFactory(poolName);
    }
}

