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
package org.apache.phoenix.hbase.index;

import java.io.IOException;
import java.io.InterruptedIOException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import org.apache.hadoop.hbase.exceptions.TimeoutIOException;
import org.apache.htrace.Trace;
import org.apache.htrace.TraceScope;
import org.apache.phoenix.hbase.index.util.ImmutableBytesPtr;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 
 * Class, copied for the most part from HRegion.getRowLockInternal implementation
 * that manages reentrant row locks based on the row key. Phoenix needs to manage
 * it's own locking due to secondary indexes needing a consistent snapshot from
 * the time the mvcc is acquired until the time it is advanced (PHOENIX-4053).
 *
 */
public class LockManager {
    private static final Logger LOGGER = LoggerFactory.getLogger(LockManager.class);

    private final ConcurrentHashMap<ImmutableBytesPtr, RowLockContext> lockedRows =
            new ConcurrentHashMap<ImmutableBytesPtr, RowLockContext>();

    public LockManager () {
    }

    /**
     * Lock the row or throw otherwise
     * @param rowKey the row key
     * @return RowLock used to eventually release the lock 
     * @throws TimeoutIOException if the lock could not be acquired within the
     * allowed rowLockWaitDuration and InterruptedException if interrupted while
     * waiting to acquire lock.
     */
    public RowLock lockRow(ImmutableBytesPtr rowKey, int waitDuration) throws IOException {
        RowLockContext rowLockContext = null;
        RowLockImpl result = null;
        TraceScope traceScope = null;

        // If we're tracing start a span to show how long this took.
        if (Trace.isTracing()) {
            traceScope = Trace.startSpan("LockManager.getRowLock");
            traceScope.getSpan().addTimelineAnnotation("Getting a lock");
        }

        boolean success = false;
        try {
            // Keep trying until we have a lock or error out.
            // TODO: do we need to add a time component here?
            while (result == null) {

                // Try adding a RowLockContext to the lockedRows.
                // If we can add it then there's no other transactions currently running.
                rowLockContext = new RowLockContext(rowKey);
                RowLockContext existingContext = lockedRows.putIfAbsent(rowKey, rowLockContext);

                // if there was a running transaction then there's already a context.
                if (existingContext != null) {
                    rowLockContext = existingContext;
                }

                result = rowLockContext.newRowLock();
            }
            if (!result.getLock().tryLock(waitDuration, TimeUnit.MILLISECONDS)) {
                if (traceScope != null) {
                    traceScope.getSpan().addTimelineAnnotation("Failed to get row lock");
                }
                throw new TimeoutIOException("Timed out waiting for lock for row: " + rowKey);
            }
            rowLockContext.setThreadName(Thread.currentThread().getName());
            success = true;
            return result;
        } catch (InterruptedException ie) {
            LOGGER.warn("Thread interrupted waiting for lock on row: " + rowKey);
            InterruptedIOException iie = new InterruptedIOException();
            iie.initCause(ie);
            if (traceScope != null) {
                traceScope.getSpan().addTimelineAnnotation("Interrupted exception getting row lock");
            }
            Thread.currentThread().interrupt();
            throw iie;
        } finally {
            // On failure, clean up the counts just in case this was the thing keeping the context alive.
            if (!success && rowLockContext != null) rowLockContext.cleanUp();
            if (traceScope != null) {
                traceScope.close();
            }
        }
    }

    public RowLock lockRow(byte[] row, int waitDuration) throws IOException {
        // create an object to use a a key in the row lock map
        ImmutableBytesPtr rowKey = new ImmutableBytesPtr(row);
        return lockRow(rowKey, waitDuration);
    }

    /**
     * Unlock the row. We need this stateless way of unlocking because
     * we have no means of passing the RowLock instances between
     * coprocessor calls (see HBASE-18482). Once we have that, we
     * can have the caller collect RowLock instances and free when
     * needed.
     * @param row the row key
     * @throws IOException
     */
    public void unlockRow(byte[] row) throws IOException {
        ImmutableBytesPtr rowKey = new ImmutableBytesPtr(row);
        RowLockContext lockContext = lockedRows.get(rowKey);
        if (lockContext != null) {
            lockContext.releaseRowLock();
        }
    }

    class RowLockContext {
        private final ImmutableBytesPtr rowKey;
        // TODO: consider making this non atomic. It's only saving one
        // synchronization in the case of cleanup() when more than one
        // thread is holding on to the lock.
        private final AtomicInteger count = new AtomicInteger(0);
        private final ReentrantLock reentrantLock = new ReentrantLock(true);
        // TODO: remove once we can pass List<RowLock> as needed through
        // coprocessor calls.
        private volatile RowLockImpl rowLock = RowLockImpl.UNINITIALIZED;
        private String threadName;

        RowLockContext(ImmutableBytesPtr rowKey) {
            this.rowKey = rowKey;
        }

        RowLockImpl newRowLock() {
            count.incrementAndGet();
            synchronized (this) {
                if (rowLock != null) {
                    rowLock = new RowLockImpl(this, reentrantLock);
                    return rowLock;
                } else {
                    return null;
                }
            }
        }

        void releaseRowLock() {
            synchronized (this) {
                if (rowLock != null) {
                    rowLock.release();
                }
            }
        }
        
        void cleanUp() {
            long c = count.decrementAndGet();
            if (c <= 0) {
                synchronized (this) {
                    if (count.get() <= 0 && rowLock != null){
                        rowLock = null;
                        RowLockContext removed = lockedRows.remove(rowKey);
                        assert removed == this: "we should never remove a different context";
                    }
                }
            }
        }

        void setThreadName(String threadName) {
            this.threadName = threadName;
        }

        @Override
        public String toString() {
            return "RowLockContext{" +
                    "row=" + rowKey +
                    ", readWriteLock=" + reentrantLock +
                    ", count=" + count +
                    ", threadName=" + threadName +
                    '}';
        }
    }

    /**
     * Class used to represent a lock on a row.
     */
    public static class RowLockImpl implements RowLock {
        static final RowLockImpl UNINITIALIZED = new RowLockImpl();
        private final RowLockContext context;
        private final Lock lock;

        private RowLockImpl() {
            context = null;
            lock = null;
        }
        
        RowLockImpl(RowLockContext context, Lock lock) {
            this.context = context;
            this.lock = lock;
        }

        Lock getLock() {
            return lock;
        }

        @Override
        public void release() {
            lock.unlock();
            context.cleanUp();
        }

        @Override
        public ImmutableBytesPtr getRowKey() {
            return context.rowKey;
        }

        @Override
        public String toString() {
            return "RowLockImpl{" +
                    "context=" + context +
                    ", lock=" + lock +
                    '}';
        }
    }

    /**
     * Row lock held by a given thread.
     * One thread may acquire multiple locks on the same row simultaneously.
     * The locks must be released by calling release() from the same thread.
     */
    public interface RowLock {
        /**
         * Release the given lock.  If there are no remaining locks held by the current thread
         * then unlock the row and allow other threads to acquire the lock.
         * @throws IllegalArgumentException if called by a different thread than the lock owning
         *     thread
         */
        void release();

        ImmutableBytesPtr getRowKey();
    }

}
