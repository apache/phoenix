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
import java.util.concurrent.locks.ReentrantLock;

import org.apache.hadoop.hbase.exceptions.TimeoutIOException;
import org.apache.phoenix.hbase.index.util.ImmutableBytesPtr;
import org.apache.phoenix.util.EnvironmentEdgeManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 
 * Manages reentrant row locks based on row keys. Phoenix needs to manage
 * its own locking due to secondary indexes needing a consistent snapshot from
 * the time the mvcc is acquired until the time it is advanced (PHOENIX-4053).
 *
 */
public class LockManager {
    private static final Logger LOGGER = LoggerFactory.getLogger(LockManager.class);

    private final ConcurrentHashMap<ImmutableBytesPtr, RowLockImpl> lockedRows =
            new ConcurrentHashMap<>();

    public LockManager () {
    }

    /**
     * Lock the row or throw otherwise
     * @param rowKey
     * @param waitDuration
     * @return
     * @throws TimeoutIOException if the lock could not be acquired within the
     * allowed rowLockWaitDuration and InterruptedException if interrupted while
     * waiting to acquire lock.
     */
    public RowLock lockRow(ImmutableBytesPtr rowKey, int waitDuration) throws IOException {
        long startTime = EnvironmentEdgeManager.currentTimeMillis();
        RowLockImpl rowLock = new RowLockImpl(rowKey);
        while (true) {
            RowLockImpl existingRowLock = lockedRows.putIfAbsent(rowKey, rowLock);
            if (existingRowLock == null) {
                // The row was not locked
                return rowLock;
            }
            // The row is already locked by a different thread. Wait for the lock to be released
            // for waitDuration time
            RowLockImpl usableRowLock = existingRowLock.lock(waitDuration);
            if (usableRowLock != null) {
                return usableRowLock;
            }
            // The existing lock was released and removed from the hash map before the current
            // thread attempt to lock
            long now = EnvironmentEdgeManager.currentTimeMillis();
            long timePassed = now - startTime;
            if (timePassed > waitDuration) {
                throw new TimeoutIOException("Timed out waiting for lock for row: " + rowKey);
            }
            waitDuration -= timePassed;
        }
    }

    public RowLock lockRow(byte[] row, int waitDuration) throws IOException {
        ImmutableBytesPtr rowKey = new ImmutableBytesPtr(row);
        return lockRow(rowKey, waitDuration);
    }

    /**
     * Class used to represent a lock on a row.
     */
    public class RowLockImpl implements RowLock {
        private final ImmutableBytesPtr rowKey;
        private int count = 1;
        private boolean usable = true;
        private final ReentrantLock lock = new ReentrantLock(true);
        private String threadName;

        private RowLockImpl(ImmutableBytesPtr rowKey) {
            this.rowKey = rowKey;
            lock.lock();
            threadName = Thread.currentThread().getName();
        }

        RowLockImpl lock(long waitDuration) throws IOException{
            synchronized (this) {
                if (!usable) {
                    return null;
                }
                count++;
            }
            boolean success = false;
            threadName = Thread.currentThread().getName();
            try {
                if (!lock.tryLock(waitDuration, TimeUnit.MILLISECONDS)) {
                    throw new TimeoutIOException("Timed out waiting for lock for row: " + rowKey);
                }
                success = true;
            } catch (InterruptedException ie) {
                LOGGER.warn("Thread interrupted waiting for lock on row: " + rowKey);
                InterruptedIOException iie = new InterruptedIOException();
                iie.initCause(ie);
                Thread.currentThread().interrupt();
                throw iie;
            } finally {
                if (!success) {
                    synchronized (this) {
                        count--;
                    }
                }
            }
            if (success) {
                return this;
            }
            return null;
        }

        @Override
        public void release() {
            synchronized (this) {
                lock.unlock();
                count--;
                if (count == 0) {
                    RowLockImpl removed = lockedRows.remove(rowKey);
                    assert removed == this : "We should never remove a different lock";
                    usable = false;
                } else {
                    assert count > 0 : "Reference count should never be less than zero";
                }
            }
        }

        @Override
        public ImmutableBytesPtr getRowKey() {
            return rowKey;
        }

        @Override
        public String toString() {
            return "RowLockImpl{" +
                    "row=" + rowKey +
                    ", count=" + count +
                    ", threadName=" + threadName +
                    ", lock=" + lock +
                    ", usable=" + usable +
                    "}";
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
