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

package org.apache.phoenix.coprocessorclient;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.StampedLock;

/**
 * This class holds the index, mapping row-key matcher patterns to tableIds.
 * Assumes byte[] are UTF-8 encoded.
 * This class is thread safe.
 */
public class RowKeyMatcher {
    private static final Logger LOGGER = LoggerFactory.getLogger(RowKeyMatcher.class);

    public static final int R = 256;
    private TrieNode root = new TrieNode();
    private final AtomicInteger numEntries = new AtomicInteger(0);

    // Basic Trie node implementation
    class TrieNode {
        private Integer tableId = null;
        TrieNode[] next = new TrieNode[R];
        private final StampedLock sl = new StampedLock();
        private TrieNode tryOptimisticGet(int pos) {
            long stamp = sl.tryOptimisticRead();
            TrieNode nextNode = this.next[pos];
            if (!sl.validate(stamp)) {
                stamp = sl.readLock();
                try {
                    nextNode = this.next[pos];
                } finally {
                    sl.unlockRead(stamp);
                }
            }
            return nextNode;
        }

        protected void put(int pos, byte[] key, int val, int depth) {
            long stamp = sl.writeLock();
            try {
                this.next[pos] = RowKeyMatcher.this.put(this.next[pos], key, val, depth, true);
            }
            finally {
                sl.unlock(stamp);
            }

        }

        protected void registerTableId(int tableId) {
            long stamp = sl.writeLock();
            try {
                if (this.tableId == null) {
                    this.tableId = tableId;
                    numEntries.incrementAndGet();
                }
            }
            finally {
                sl.unlock(stamp);
            }
        }
    }

    // return the number of prefixes that this index has.
    public int getNumEntries() {
        return numEntries.get();
    }

    // return the Id associated with the rowkey.
    public Integer match(byte[] rowkey, int offset) {
        return get(rowkey, offset);
    }

    public Integer get(byte[] key, int offset) {
        TrieNode node = get(root, key, offset);
        if (node == null)
            return null;
        return node.tableId;
    }
    private TrieNode get(TrieNode node, byte[] key, int depth) {
        if (node == null) {
            return null;
        }

        if (node.tableId != null) {
            return node;
        }
        if (key.length == depth) {
            return node;
        }

        int index = key[depth] & 0xFF;
        if (LOGGER.isTraceEnabled()) {
            LOGGER.trace(String.format("depth = %d, index = %d", depth, index));
        }

        return get(node.tryOptimisticGet(index), key, depth + 1);
    }

    // Associate prefix key with the supplied Id.
    public void put(byte[] key, int tableId) {
        root = put(root, key, tableId, 0, false);
    }

    // helper method to recursively add the key to trie.
    private TrieNode put(TrieNode node, byte[] key, int tableId, int depth, boolean isLocked) {

        if (node == null) {
            node = new TrieNode();
        }

        if (key.length == depth) {
            node.registerTableId(tableId);
            return node;
        }

        int index = key[depth] & 0xFF;
        if (LOGGER.isTraceEnabled()) {
            LOGGER.trace(String.format("depth = %d, index = %d", depth, index));
        }

        if (!isLocked && node.next[index] == null) {
            node.put(index, key, tableId, depth + 1);
        }
        else {
            node.next[index] = put(node.next[index], key, tableId, depth + 1, isLocked);
        }

        return node;
    }
}
