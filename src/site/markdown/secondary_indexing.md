# Secondary Indexing

Secondary indexes are an orthogonal way to access data from its primary access path. In HBase, you have a single index that is lexicographically sorted on 
the primary row key. Access to records in any way other than through the primary row requires scanning over potentially all the rows in the table to test them against your filter. With secondary indexing, the columns you index form an alternate row key to allow point lookups and range scans along this new axis. Phoenix is particularly powerful in that we provide _covered_ indexes - we do not need to go back to the primary table once we have found the index entry. Instead, we bundle the data we care about right in the index rows, saving read-time overhead.

Phoenix supports two main forms of indexing: mutable and immutable indexing. They are useful in different scenarios and have their own failure profiles and performance characteristics. Both indexes are 'global' indexes - they live on their own tables and are copies of primary table data, which Phoenix ensures remain in-sync.

# Mutable Indexing

Often, the rows you are inserting are changing - pretty much any time you are not doing time-series data. In this case, use mutable indexing to ensure that your index is properly maintained as your data changes.

All the performance penalties for indexes occur at write time. We intercept the primary table updates on write ([DELETE](language/index.html#delete), [UPSERT VALUES](language/index.html#upsert_values) and [UPSERT SELECT](language/index.html#upsert_select)), build the index update and then sent any necessary updates to all interested index tables. At read time, Phoenix will select the index table to use that will produce the fastest query time and directly scan it just like any other HBase table.

## Example

Given the schema shown here:

    CREATE TABLE my_table (k VARCHAR PRIMARY KEY, v1 VARCHAR, v2 BIGINT);
you'd create an index on the v1 column like this:

    CREATE INDEX my_index ON my_table (v1);
A table may contain any number of indexes, but note that your write speed will drop as you add additional indexes.

We can also include columns from the data table in the index apart from the indexed columns. This allows an index to be used more frequently, as it will only be used if all columns referenced in the query are contained by it.

    CREATE INDEX my_index ON my_table (v1) INCLUDE (v2);
In addition, multiple columns may be indexed and their values may be stored in ascending or descending order.

    CREATE INDEX my_index ON my_table (v2 DESC, v1) INCLUDE (v3);
Finally, just like with the <code>CREATE TABLE</code> statement, the <code>CREATE INDEX</code> statement may pass through properties to apply to the underlying HBase table, including the ability to salt it:

    CREATE INDEX my_index ON my_table (v2 DESC, v1) INCLUDE (v3)
        SALT_BUCKETS=10, DATA_BLOCK_ENCODING='NONE';
Note that if the primary table is salted, then the index is automatically salted in the same way. In addition, the MAX_FILESIZE for the index is adjusted down, relative to the size of the primary versus index table. For more on salting see [here](salted.html).

# Immutable Indexing

Immutable indexing targets use cases that are _write once_, _append only_; this is common in time-series data, where you log once, but read multiple times. In this case, the indexing is managed entirely on the client - either we successfully write all the primary and index data or we return a failure to the client. Since once written, rows are never updated, no incremental index maintenance is required. This reduces the overhead of secondary indexing at write time. However, keep in mind that immutable indexing are only applicable in a limited set of use cases.

## Example

To use immutable indexing, supply an <code>IMMUTABLE_ROWS=true</code> property when you create your table like this:

    CREATE TABLE my_table (k VARCHAR PRIMARY KEY, v VARCHAR) IMMUTABLE_ROWS=true;

Other than that, all of the previous examples are identical for immutable indexing.

If you have an existing table that you'd like to switch from immutable indexing to mutable indexing, use the <code>ALTER TABLE</code> command as show below:

    ALTER TABLE my_table SET IMMUTABLE_ROWS=false;
For the complete syntax, see our [Language Reference Guide](language/index.html#create_index).

## Data Guarantees and Failure Management

On successful return to the client, all data is guaranteed to be written to all interested indexes and the primary table. For each individual data row, updates are an all-or-nothing, with a small gap of being behind. From the perspective of a single client, it either thinks all-or-none of the update worked.

We maintain index update durability by adding the index updates to the Write-Ahead-Log (WAL) entry of the primary table row. Only after the WAL entry is successfully synced to disk do we attempt to make the index/primary table updates. We write the index updates in parallel by default, leading to very high throughput. If the server crashes while we are writing the index updates, we replay the all the index updates to the index tables in the WAL recovery process and rely on the idempotence of the updates to ensure correctness. Therefore, index tables are only every a single edit ahead of the primary table.

Its important to note several points:
 * We _do not provide full transactions_ so you could see the index table out of sync with the primary table.
  * As noted above, this is ok as we are only a very small bit ahead and out of sync for very short periods
 * Each data row and its index row(s) are guaranteed to to be written or lost - we never see partial updates
 * All data is first written to index tables before the primary table

### Singular Write Path

There is a single write path that guarantees the failure properties. All writes to the HRegion get intercepted by our coprocessor. We then build the index updates based on the pending update (or updates, in the case of the batch). These update are then appended to the WAL entry for the original update.

If we get any failure up to this point, we return the failure to the client and no data is persisted or made visible to the client. 

Once the WAL is written, we ensure that the index and primary table data will become visible, even in the case of a failure.
 * If the server does _not_ crash, we just insert the index updates to their respective tables.
 * If the server _does_ crash, we then replay the index updates with the usual WAL replay mechanism
    ** If any of the index updates fails, we then fail the server, ensuring we get the WAL replay of the updates later.

### Failure Policy

In the event that the region server handling the data updates cannot write to the region server handling the index updates, the index is automatically disabled and will no longer be considered for use in queries (as it will no longer be in sync with the data table). To use it again, it must be manually rebuilt with the following command:

```
ALTER INDEX my_index ON my_table REBUILD;
```

If we cannot disable the index, then the server will be immediately aborted. If the abort fails, we call System.exit on the JVM, forcing the server to die. By killing the server, we ensure that the WAL will be replayed on recovery, replaying the index updates to their appropriate tables.

**WARNING: indexing has the potential to bring down your entire cluster very quickly.**

If the index tables are not setup correctly (Phoenix ensures that they are), this failure policy can cause a cascading failure as each region server attempts and fails to write the index update, subsequently killing itself to ensure the visibility concerns outlined above.

## Setup

Only mutable indexing requires special configuration options in the region server to run - phoenix ensures that they are setup correctly when you enable mutable indexing on the table; if the correct properties are not set, you will not be able to turn it on.

You will need to add the following parameters to `hbase-site.xml`:
```
<property>
  <name>hbase.regionserver.wal.codec</name>
  <value>org.apache.hadoop.hbase.regionserver.wal.IndexedWALEditCodec</value>
</property>
```

This enables custom WAL edits to be written, ensuring proper writing/replay of the index updates. This codec supports the usual host of WALEdit options, most notably WALEdit compression.

## Tuning
Out the box, indexing is pretty fast. However, to optimize for your particular environment and workload, there are several properties you can tune.

All the following parameters must be set in `hbase-site.xml` - they are true for the entire cluster and all index tables, as well as across all regions on the same server (so, for instance, a single server would not write to too many different index tables at once).

1. index.builder.threads.max
 * Number of threads to used to build the index update from the primary table update
 * Increasing this value overcomes the bottleneck of reading the current row state from the underlying HRegion. Tuning this value too high will just bottleneck at the HRegion as it will not be able to handle too many concurrent scan requests as well as general thread-swapping concerns.
 * **Default: 10**

2. index.builder.threads.keepalivetime
 * Amount of time in seconds after we expire threads in the builder thread pool.
 * Unused threads are immediately released after this amount of time and not core threads are retained (though this last is a small concern as tables are expected to sustain a fairly constant write load), but simultaneously allows us to drop threads if we are not seeing the expected load.
 * **Default: 60**

3. index.writer.threads.max
 * Number of threads to use when writing to the target index tables.
 * The first level of parallelization, on a per-table basis - it should roughly correspond to the number of index tables
 * **Default: 10**

4. index.writer.threads.keepalivetime
 * Amount of time in seconds after we expire threads in the writer thread pool.
 * Unused threads are immediately released after this amount of time and not core threads are retained (though this last is a small concern as tables are expected to sustain a fairly constant write load), but simultaneously allows us to drop threads if we are not seeing the expected load.
 * **Default: 60**

5. hbase.htable.threads.max
 * Number of threads each index HTable can use for writes.
 * Increasing this allows more concurrent index updates (for instance across batches), leading to high overall throughput.
 * **Default: 2,147,483,647**

6. hbase.htable.threads.keepalivetime
 * Amount of time in seconds after we expire threads in the HTable's thread pool.
 * Using the "direct handoff" approach, new threads will only be created if it is necessary and will grow unbounded. This could be bad but HTables  only create as many Runnables as there are region servers; therefore, it also scales when new region servers are added.
 * **Default: 60** 
 
7. index.tablefactory.cache.size
 * Number of index HTables we should keep in cache.
 * Increasing this number ensures that we do not need to recreate an HTable for each attempt to write to an index table. Conversely, you could see memory pressure if this value is set too high.
 * **Default: 10**

# Performance
We track secondary index performance via our [performance framework](http://phoenix-bin.github.io/client/performance/latest.htm). This is a generic test of performance based on defaults - your results will vary based on hardware specs as well as you individual configuration.

That said, we have seen secondary indexing (both immutable and mutable) go as quickly as < 2x the regular write path on a small, (3 node) desktop-based cluster. This is actually a phenomenal as we have to write to multiple tables as well as build the index update.

# Presentations
There have been several presentations given on how secondary indexing works in Phoenix that have a more indepth look at how indexing works (with pretty pictures!):
 * [San Francisco HBase Meetup](http://files.meetup.com/1350427/PhoenixIndexing-SF-HUG_09-26-13.pptx) - Sept. 26, 2013
 * [Los Anglees HBase Meetup](http://www.slideshare.net/jesse_yates/phoenix-secondary-indexing-la-hug-sept-9th-2013) - Sept, 4th, 2013
