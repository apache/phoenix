package org.apache.phoenix.job;

import org.apache.phoenix.monitoring.ThreadPoolHistograms;
import org.apache.phoenix.monitoring.ThreadPoolMetricsManager;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

public class ThreadPoolWithUtilizationStats extends ThreadPoolExecutor {

    final private ThreadPoolHistograms threadPoolHistograms;

    public ThreadPoolWithUtilizationStats(int corePoolSize, int maximumPoolSize,
                                          long keepAliveTime, TimeUnit unit,
                                          BlockingQueue<Runnable> workQueue,
                                          ThreadFactory threadFactory,
                                          ThreadPoolHistograms threadPoolHistograms) {
        super(corePoolSize, maximumPoolSize, keepAliveTime, unit, workQueue, threadFactory);
        this.threadPoolHistograms = threadPoolHistograms;
        if (threadPoolHistograms != null) {
            ThreadPoolMetricsManager.collectUtilizationHistograms(this, threadPoolHistograms);
        }
    }

    @Override
    public void execute(Runnable command) {
        if (threadPoolHistograms != null) {
            threadPoolHistograms.updateQueuedSize(this.getQueue().size());
        }
        super.execute(command);
    }

    @Override
    protected void beforeExecute(Thread t, Runnable r) {
        if (threadPoolHistograms != null) {
            threadPoolHistograms.updateActiveThreads(this.getActiveCount());
        }
        super.beforeExecute(t, r);
    }
}
