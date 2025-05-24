package org.apache.phoenix.job;

import org.apache.phoenix.monitoring.HTableThreadPoolHistograms;
import org.apache.phoenix.monitoring.HTableThreadPoolMetricsManager;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

public class HTableThreadPoolWithUtilizationStats extends ThreadPoolExecutor {

    private final String threadPoolName;
    private final Supplier<HTableThreadPoolHistograms> hTableThreadPoolHistogramsSupplier;

    public HTableThreadPoolWithUtilizationStats(int corePoolSize, int maximumPoolSize,
                                                long keepAliveTime, TimeUnit unit,
                                                BlockingQueue<Runnable> workQueue,
                                                ThreadFactory threadFactory,
                                                String threadPoolName,
                                                Supplier<HTableThreadPoolHistograms> supplier) {
        super(corePoolSize, maximumPoolSize, keepAliveTime, unit, workQueue, threadFactory);
        this.threadPoolName = threadPoolName;
        this.hTableThreadPoolHistogramsSupplier = supplier;
    }

    @Override
    public void execute(Runnable command) {
        if (hTableThreadPoolHistogramsSupplier != null) {
            HTableThreadPoolMetricsManager.getInstance().updateQueueSize(threadPoolName,
                this.getQueue().size(), hTableThreadPoolHistogramsSupplier);
        }
        super.execute(command);
    }

    @Override
    protected void beforeExecute(Thread t, Runnable r) {
        if (hTableThreadPoolHistogramsSupplier != null) {
            HTableThreadPoolMetricsManager.getInstance().updateActiveThreads(threadPoolName,
                    this.getActiveCount(), hTableThreadPoolHistogramsSupplier);
        }
        super.beforeExecute(t, r);
    }
}
