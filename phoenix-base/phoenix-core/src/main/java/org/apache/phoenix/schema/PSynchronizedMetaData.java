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
package org.apache.phoenix.schema;

import java.sql.SQLException;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import javax.annotation.concurrent.GuardedBy;

import org.apache.phoenix.parse.PFunction;
import org.apache.phoenix.parse.PSchema;

public class PSynchronizedMetaData implements PMetaData {

    @GuardedBy("readWriteLock")
    private PMetaData delegate;
    private final ReadWriteLock readWriteLock = new ReentrantReadWriteLock();

    public PSynchronizedMetaData(PMetaData metadata) {
        this.delegate = metadata;
    }
    
    @Override
    public Iterator<PTable> iterator() {
        readWriteLock.readLock().lock();
        try {
            return delegate.iterator();
        }
        finally {
            readWriteLock.readLock().unlock();
        }
    }

    @Override
    public int size() {
        readWriteLock.readLock().lock();
        try {
            return delegate.size();
        }
        finally {
            readWriteLock.readLock().unlock();
        }
    }

    @Override
    public PMetaData clone() {
        readWriteLock.readLock().lock();
        try {
            return delegate.clone();
        }
        finally {
            readWriteLock.readLock().unlock();
        }
    }

    @Override
    public void addTable(PTable table, long resolvedTime) throws SQLException {
        readWriteLock.writeLock().lock();
        try {
            delegate.addTable(table, resolvedTime);
        }
        finally {
            readWriteLock.writeLock().unlock();
        }
    }

    @Override
    public PTableRef getTableRef(PTableKey key) throws TableNotFoundException {
        readWriteLock.readLock().lock();
        try {
            return delegate.getTableRef(key);
        }
        finally {
            readWriteLock.readLock().unlock();
        }
    }

    @Override
    public void updateResolvedTimestamp(PTable table, long resolvedTimestamp) throws SQLException {
        readWriteLock.writeLock().lock();
        try {
            delegate.updateResolvedTimestamp(table, resolvedTimestamp);
        }
        finally {
            readWriteLock.writeLock().unlock();
        }
    }

    @Override
    public void pruneTables(Pruner pruner) {
        readWriteLock.writeLock().lock();
        try {
            delegate.pruneTables(pruner);
        }
        finally {
            readWriteLock.writeLock().unlock();
        }
    }

    @Override
    public PFunction getFunction(PTableKey key) throws FunctionNotFoundException {
        readWriteLock.readLock().lock();
        try {
            return delegate.getFunction(key);
        }
        finally {
            readWriteLock.readLock().unlock();
        }
    }

    @Override
    public void removeTable(PName tenantId, String tableName, String parentTableName,
            long tableTimeStamp) throws SQLException {
        readWriteLock.writeLock().lock();
        try {
            delegate.removeTable(tenantId, tableName, parentTableName, tableTimeStamp);
        }
        finally {
            readWriteLock.writeLock().unlock();
        }
    }

    @Override
    public void pruneFunctions(Pruner pruner) {
        readWriteLock.writeLock().lock();
        try {
            delegate.pruneFunctions(pruner);
        }
        finally {
            readWriteLock.writeLock().unlock();
        }
    }

    @Override
    public long getAge(PTableRef ref) {
        readWriteLock.readLock().lock();
        try {
            return delegate.getAge(ref);
        }
        finally {
            readWriteLock.readLock().unlock();
        }
    }

    @Override
    public PSchema getSchema(PTableKey key) throws SchemaNotFoundException {
        readWriteLock.readLock().lock();
        try {
            return delegate.getSchema(key);
        }
        finally {
            readWriteLock.readLock().unlock();
        }
    }

    @Override
    public void removeColumn(PName tenantId, String tableName, List<PColumn> columnsToRemove,
            long tableTimeStamp, long tableSeqNum, long resolvedTime) throws SQLException {
        readWriteLock.writeLock().lock();
        try {
            delegate.removeColumn(tenantId, tableName, columnsToRemove, tableTimeStamp, tableSeqNum,
                resolvedTime);
        }
        finally {
            readWriteLock.writeLock().unlock();
        }
    }

    @Override
    public void addFunction(PFunction function) throws SQLException {
        readWriteLock.writeLock().lock();
        try {
            delegate.addFunction(function);
        }
        finally {
            readWriteLock.writeLock().unlock();
        }
    }

    @Override
    public void removeFunction(PName tenantId, String function, long functionTimeStamp)
            throws SQLException {
        readWriteLock.writeLock().lock();
        try {
            delegate.removeFunction(tenantId, function, functionTimeStamp);
        }
        finally {
            readWriteLock.writeLock().unlock();
        }
    }

    @Override
    public void addSchema(PSchema schema) throws SQLException {
        readWriteLock.writeLock().lock();
        try {
            delegate.addSchema(schema);
        }
        finally {
            readWriteLock.writeLock().unlock();
        }
    }

    @Override
    public void removeSchema(PSchema schema, long schemaTimeStamp) {
        readWriteLock.writeLock().lock();
        try {
            delegate.removeSchema(schema, schemaTimeStamp);
        }
        finally {
            readWriteLock.writeLock().unlock();
        }
    }

}
