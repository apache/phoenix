package org.apache.phoenix.end2end.prefix;

import org.apache.hadoop.hbase.util.Bytes;
import org.apache.phoenix.prefix.search.PrefixIndex;
import org.apache.phoenix.prefix.table.TableTTLInfoCache;
import org.apache.phoenix.prefix.table.TableTTLInfo;

import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

public class TableTTLInfoTestHelper {

	private static final int DEFAULT_INDEXER_THREAD_POOL_SIZE = 6;
	private static final int DEFAULT_INDEXER_QUEUE_SIZE = 1024;
	private final TableTTLInfoCache ttlInfoCache = new TableTTLInfoCache();
	private final PrefixIndex index  = new PrefixIndex();
	private final ThreadPoolExecutor indexerPool;

	public TableTTLInfoTestHelper() {
		indexerPool = new ThreadPoolExecutor(DEFAULT_INDEXER_THREAD_POOL_SIZE,
               DEFAULT_INDEXER_THREAD_POOL_SIZE, 60, TimeUnit.SECONDS,
                new LinkedBlockingQueue<Runnable>(DEFAULT_INDEXER_QUEUE_SIZE));
        indexerPool.allowCoreThreadTimeOut(true);
	}

	public TableTTLInfoCache getTtlInfoCache() {
		return ttlInfoCache;
	}

	public PrefixIndex getIndex() {
		return index;
	}

	public CompletableFuture<Boolean> indexTableInfoFromSampleData(List<TableTTLInfo> tableList) {
		CompletableFuture<Boolean> indexStatus =  new CompletableFuture<Boolean>();
		Thread thread = new Thread(() -> {
			try {
				tableList.forEach(m -> {
					addTabletoIndex(m);
					System.out.println("Adding table :" + Bytes.toStringBinary(m.getTableName()));
				});
				indexStatus.complete(true);
			} catch (Exception e) {
				System.out.println("Failed to parse file: " + e.getMessage());
				indexStatus.completeExceptionally(e);
			}
			
		});
        thread.setDaemon(true);
        thread.start();
        
        return indexStatus;
	}

	public CompletableFuture<Boolean> indexTableInfoFromCatalog(String url, String parentSchemaName, String parentTableName) {
		CompletableFuture<Boolean> indexStatus =  new CompletableFuture<Boolean>();
		Thread thread = new Thread(() -> {
			try {
				List<TableTTLInfo> tableList = getTableInfoFromCatalog(url, parentSchemaName, parentTableName);
				tableList.forEach(m -> {
					System.out.println("Adding table : " + m);
					addTabletoIndex(m);
				});
				indexStatus.complete(true);
			} catch (Exception e) {
				System.out.println("Failed to read db: " + e.getMessage());
				indexStatus.completeExceptionally(e);
			}

		});
		thread.setDaemon(true);
		thread.start();

		return indexStatus;
	}

	public List<TableTTLInfo> getTableInfoFromCatalog(String url, String parentSchemaName, String parentTableName) {
		try {
			List<TableTTLInfo> tableList = RowKeyPrefixTestUtils.getRowKeyPrefixesForTable(url, parentSchemaName, parentTableName);
			return  tableList;
		} catch (Exception e) {
			System.out.println("Failed to read db: " + e.getMessage());
			return null;
		}
	}

	public TableTTLInfo findTable(String prefix, int offset, boolean binaryMode) {
		Integer tableId = -1;
		if (binaryMode) {
			tableId = index.getTableIdWithPrefix(Bytes.fromHex(prefix), offset);
		} else {
			tableId = index.getTableIdWithPrefix(prefix.getBytes(StandardCharsets.UTF_8), offset);
		}
		return ttlInfoCache.getTableById(tableId);
	}

	private int addTabletoIndex(TableTTLInfo tableTTLInfo) {
		int tableId = ttlInfoCache.addTable(tableTTLInfo);
		index.put(tableTTLInfo.getPrefix(), tableId);
		return tableId;
	}
}