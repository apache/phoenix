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

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Callable;
import java.util.concurrent.FutureTask;
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
        return ((JobFutureTask)o).getJobId();
    }        

    public static interface JobRunnable<T> extends Runnable {
        public Object getJobId();
    }

    public static ThreadPoolExecutor createThreadPoolExec(int keepAliveMs, int size, int queueSize) {
        BlockingQueue<Runnable> queue;
        if (queueSize == 0) {
            queue = new SynchronousQueue<Runnable>(); // Specialized for 0 length.
        } else {
            queue = new JobManager<Runnable>(queueSize);
        }
        ThreadFactory threadFactory = new ThreadFactoryBuilder().setNameFormat(
				"phoenix-" + PHOENIX_POOL_INDEX.getAndIncrement()
						+ "-thread-%s").setDaemon(true).build();
        // For thread pool, set core threads = max threads -- we don't ever want to exceed core threads, but want to go up to core threads *before* using the queue.
        ThreadPoolExecutor exec = new ThreadPoolExecutor(size, size, keepAliveMs, TimeUnit.MILLISECONDS, queue, threadFactory) {
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
     * Delegating callable implementation that preserves the parentJobId and sets up thread tracker stuff before delegating to the actual command. 
     */
    public static interface JobCallable<T> extends Callable<T> {
        public Object getJobId();
    }
}

