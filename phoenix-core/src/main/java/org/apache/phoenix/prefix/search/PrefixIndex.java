package org.apache.phoenix.prefix.search;

import org.apache.hadoop.hbase.util.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.StampedLock;

/**
 * This class holds the index, mapping row-key prefixes to tableIds.
 * Assumes byte[] are UTF-8 encoded.
 * This class is thread safe.
 */
public class PrefixIndex {
	private static final Logger LOGGER = LoggerFactory.getLogger(PrefixIndex.class);

	public static final int R = 256;
	private TrieNode root = new TrieNode();
	private final AtomicInteger validPrefixes = new AtomicInteger(0);

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
				this.next[pos] = PrefixIndex.this.put(this.next[pos], key, val, depth, true);
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
					validPrefixes.incrementAndGet();
				}
			}
			finally {
				sl.unlock(stamp);
			}
		}
	}

	// return the number of prefixes that this index has.
	public int getValidPrefixes() {
		return validPrefixes.get();
	}

	// return the Id associated with the prefix.
	public Integer getTableIdWithPrefix(byte[] prefix, int offset) {
		return get(prefix, offset);
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

		int index = (int) key[depth] % (int) R;
		int originalIndex = index;
		int convertedIndex = index;
		if (index < 0) {
			convertedIndex = 128 + (128 + index);
			index = convertedIndex;
		}
		if (LOGGER.isTraceEnabled()) {
			LOGGER.trace(String.format("%d, %d, %d", depth, originalIndex, convertedIndex));
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

		int index = (int) key[depth] % (int) R;
		int originalIndex = index;
		int convertedIndex = index;
		if (index < 0) {
			convertedIndex = 128 + (128 + index);
			index = convertedIndex;
		}
		if (LOGGER.isTraceEnabled()) {
			LOGGER.trace(String.format("%d, %d, %d", depth, originalIndex, convertedIndex));
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
