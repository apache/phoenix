/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.phoenix.job;

import static org.apache.phoenix.monitoring.PhoenixMetrics.CountMetric.REJECTED_TASK_COUNT;
import static org.apache.phoenix.monitoring.PhoenixMetrics.CountMetric.TASK_COUNT;
import static org.apache.phoenix.monitoring.PhoenixMetrics.SizeMetric.TASK_END_TO_END_TIME;
import static org.apache.phoenix.monitoring.PhoenixMetrics.SizeMetric.TASK_EXECUTION_TIME;
import static org.apache.phoenix.monitoring.PhoenixMetrics.SizeMetric.TASK_QUEUE_WAIT_TIME;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Callable;
import java.util.concurrent.Executors;
import java.util.concurrent.FutureTask;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.RejectedExecutionHandler;
import java.util.concurrent.RunnableFuture;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import com.google.common.util.concurrent.ThreadFactoryBuilder;
/**
 * 
 * Thread pool executor that executes scans in parallel
 *
 * 
 * @since 0.1
 */
@SuppressWarnings("rawtypes")
public class JobManager<T> extends AbstractRoundRobinQueue<T> {
	
    private static final AtomicLong PHOENIX_POOL_INDEX = new AtomicLong(1);
	
    public JobManager(int maxSize) {
        super(maxSize, true); // true -> new producers move to front of queue; this reduces latency.
    }

	@Override
    protected Object extractProducer(T o) {
        if( o instanceof  JobFutureTask){
            return ((JobFutureTask)o).getJobId();
        }
        return o;
    }        

    public static interface JobRunnable<T> extends Runnable {
        public Object getJobId();
    }

    public static ThreadPoolExecutor createThreadPoolExec(int keepAliveMs, int size, int queueSize, boolean useInstrumentedThreadPool) {
        BlockingQueue<Runnable> queue;
        if (queueSize == 0) {
            queue = new SynchronousQueue<Runnable>(); // Specialized for 0 length.
        } else {
            queue = new JobManager<Runnable>(queueSize);
        }
        String name = "phoenix-" + PHOENIX_POOL_INDEX.getAndIncrement();
        ThreadFactory threadFactory = new ThreadFactoryBuilder()
                .setNameFormat(name + "-thread-%s")
                .setDaemon(true)
                .setThreadFactory(
                        new ContextClassLoaderThreadFactory(JobManager.class.getClassLoader()))
                .build();
        ThreadPoolExecutor exec;
        if (useInstrumentedThreadPool) {
            // For thread pool, set core threads = max threads -- we don't ever want to exceed core threads, but want to go up to core threads *before* using the queue.
            exec = new InstrumentedThreadPoolExecutor(name, size, size, keepAliveMs, TimeUnit.MILLISECONDS, queue, threadFactory) {
                @Override
                protected <T> RunnableFuture<T> newTaskFor(Callable<T> call) {
                    return new InstrumentedJobFutureTask<T>(call);
                }
        
                @Override
                protected <T> RunnableFuture<T> newTaskFor(Runnable runnable, T value) {
                    return new InstrumentedJobFutureTask<T>(runnable, value);
                }
            };
        } else {
            // For thread pool, set core threads = max threads -- we don't ever want to exceed core threads, but want to go up to core threads *before* using the queue.
            exec = new ThreadPoolExecutor(size, size, keepAliveMs, TimeUnit.MILLISECONDS, queue, threadFactory) {
                @Override
                protected <T> RunnableFuture<T> newTaskFor(Callable<T> call) {
                    // Override this so we can create a JobFutureTask so we can extract out the parentJobId (otherwise, in the default FutureTask, it is private). 
                    return new JobFutureTask<T>(call);
                }
        
                @Override
                protected <T> RunnableFuture<T> newTaskFor(Runnable runnable, T value) {
                    return new JobFutureTask<T>(runnable, value);
                }
            };
        }
        exec.allowCoreThreadTimeOut(true); // ... and allow core threads to time out.  This just keeps things clean when idle, and is nice for ftests modes, etc., where we'd especially like these not to linger.
        return exec;
    }

    /**
     * Subclasses FutureTask for the sole purpose of providing {@link #getCallable()}, which is used to extract the producer in the {@link JobBasedRoundRobinQueue}
     */
    static class JobFutureTask<T> extends FutureTask<T> {
        private final Object jobId;
        
        public JobFutureTask(Runnable r, T t) {
            super(r, t);
            if(r instanceof JobRunnable){
              	this.jobId = ((JobRunnable)r).getJobId();
            } else {
            	this.jobId = this;
            }
        }
        
        public JobFutureTask(Callable<T> c) {
            super(c);
            // FIXME: this fails when executor used by hbase
            if (c instanceof JobCallable) {
                this.jobId = ((JobCallable<T>) c).getJobId();
            } else {
                this.jobId = this;
            }
        }
        
        public Object getJobId() {
            return jobId;
        }
    }
    
    /**
     * Instrumented version of {@link JobFutureTask} that measures time spent by a task at various stages in the queue
     * and when executed.
     */
    private static class InstrumentedJobFutureTask<T> extends JobFutureTask<T> {

        /*
         * Time at which the task was submitted to the executor.
         */
        private final long taskSubmissionTime;

        // Time at which the task is about to be executed. 
        private long taskExecutionStartTime;

        public InstrumentedJobFutureTask(Runnable r, T t) {
            super(r, t);
            this.taskSubmissionTime = System.currentTimeMillis();
        }

        public InstrumentedJobFutureTask(Callable<T> c) {
            super(c);
            this.taskSubmissionTime = System.currentTimeMillis();
        }
        
        @Override
        public void run() {
            this.taskExecutionStartTime = System.currentTimeMillis();
            super.run();
        }
        
        public long getTaskSubmissionTime() {
            return taskSubmissionTime;
        }
        
        public long getTaskExecutionStartTime() {
            return taskExecutionStartTime;
        }

    }
    
    /**
     * Delegating callable implementation that preserves the parentJobId and sets up thread tracker stuff before delegating to the actual command. 
     */
    public static interface JobCallable<T> extends Callable<T> {
        public Object getJobId();
    }


    /**
     * Extension of the default thread factory returned by {@code Executors.defaultThreadFactory}
     * that sets the context classloader on newly-created threads to be a specific classloader (and
     * not the context classloader of the calling thread).
     * <p/>
     * See {@link org.apache.phoenix.util.PhoenixContextExecutor} for the rationale on changing
     * the context classloader.
     */
    static class ContextClassLoaderThreadFactory implements ThreadFactory {
        private final ThreadFactory baseFactory;
        private final ClassLoader contextClassLoader;

        public ContextClassLoaderThreadFactory(ClassLoader contextClassLoader) {
            baseFactory = Executors.defaultThreadFactory();
            this.contextClassLoader = contextClassLoader;
        }

        @Override
        public Thread newThread(Runnable r) {
            Thread t = baseFactory.newThread(r);
            t.setContextClassLoader(contextClassLoader);
            return t;
        }
    }
    
    /**
     * Thread pool executor that instruments the various characteristics of the backing pool of threads and queue. This
     * executor assumes that all the tasks handled are of type {@link JobManager.InstrumentedJobFutureTask}
     */
    private static class InstrumentedThreadPoolExecutor extends ThreadPoolExecutor {

        private final RejectedExecutionHandler rejectedExecHandler = new RejectedExecutionHandler() {
            @Override
            public void rejectedExecution(Runnable r, ThreadPoolExecutor executor) {
                REJECTED_TASK_COUNT.increment();
                throw new RejectedExecutionException("Task " + r.toString() + " rejected from " + executor.toString());
            }
        };

        public InstrumentedThreadPoolExecutor(String threadPoolName, int corePoolSize, int maximumPoolSize, long keepAliveTime, TimeUnit unit,
                BlockingQueue<Runnable> workQueue, ThreadFactory threadFactory) {
            super(corePoolSize, maximumPoolSize, keepAliveTime, unit, workQueue, threadFactory);
            setRejectedExecutionHandler(rejectedExecHandler);
        }

        @Override
        public void execute(Runnable task) {
            TASK_COUNT.increment();
            super.execute(task);
        }

        @Override
        protected void beforeExecute(Thread worker, Runnable task) {
            InstrumentedJobFutureTask instrumentedTask = (InstrumentedJobFutureTask)task;
            TASK_QUEUE_WAIT_TIME.update(System.currentTimeMillis() - instrumentedTask.getTaskSubmissionTime());
            super.beforeExecute(worker, instrumentedTask);
        }

        @Override
        protected void afterExecute(Runnable task, Throwable t) {
            InstrumentedJobFutureTask instrumentedTask = (InstrumentedJobFutureTask)task;
            try {
                super.afterExecute(instrumentedTask, t);
            } finally {
                TASK_EXECUTION_TIME.update(System.currentTimeMillis() - instrumentedTask.getTaskExecutionStartTime());
                TASK_END_TO_END_TIME.update(System.currentTimeMillis() - instrumentedTask.getTaskSubmissionTime());
            }
        }
    }
}

