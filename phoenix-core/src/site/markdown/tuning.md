# Configuration and Tuning

Phoenix provides many different knobs and dials to configure and tune the system to run more optimally on your cluster. The configuration is done through a series of Phoenix-specific properties specified for the most part in your client-side <code>hbase-site.xml</code> file. In addition to these properties, there are of course all the <a href="http://hbase.apache.org/book/config.files.html" target="_blank">HBase configuration</a> properties with the most important ones documented <a href="http://hbase.apache.org/book/important_configurations.html" target="_blank">here</a>. This blog will focus on the Phoenix-specific properties and touch on some important considerations to maximize Phoenix and HBase performance.<br />
<br />
The table below outlines the full set of Phoenix-specific configuration properties and their defaults. Of these, we'll talk in depth about some of the most important ones below.<br />
<br />
<table border="1">
    <tbody>
<tr><td><b>Property</b></td><td><b>Description</b></td><td><b>Default
</b></td></tr>
<tr><td><small>phoenix.query.timeoutMs</small></td><td style="text-align: left;">Number of milliseconds
    after which a query will timeout on the client. Default is 10 min.</td><td>600000
</td></tr>
<tr><td><small>phoenix.query.keepAliveMs</small></td><td style="text-align: left;">When the number of
      threads is greater than the core in the client side thread pool
      executor, this is the maximum time in milliseconds that excess idle
      threads will wait for a new tasks before
terminating. Default is 60 sec.</td><td>60000</td></tr>
<tr><td><small>phoenix.query.threadPoolSize</small></td><td style="text-align: left;">Number of threads
      in client side thread pool executor. As the number of machines/cores
      in the cluster grows, this value should be
increased.</td><td>128</td></tr>
<tr><td><small>phoenix.query.queueSize</small></td><td>Max queue depth
of the
      bounded round robin backing the client side thread pool executor,
      beyond which an attempt to queue additional work is
      rejected by throwing an exception. If zero, a SynchronousQueue is used
      instead of the bounded round robin queue.</td><td>500</td></tr>
<tr><td><small>phoenix.query.spoolThresholdBytes</small></td><td style="text-align: left;">Threshold
      size in bytes after which results from parallelly executed
      query results are spooled to disk. Default is 20 mb.</td><td>20971520</td></tr>
<tr><td><small>phoenix.query.maxSpoolToDiskBytes</small></td><td style="text-align: left;">Threshold
      size in bytes upto which results from parallelly executed
      query results are spooled to disk above which the query will fail. Default is 1 gb.</td><td>1024000000</td></tr>
<tr><td><small>phoenix.query.maxGlobalMemoryPercentage</small></td><td style="text-align: left;">Percentage of total heap memory (i.e. Runtime.getRuntime().totalMemory()) that all threads may use. Only course grain memory usage is tracked, mainly accounting for memory usage in the intermediate map built during group by aggregation.  When this limit is reached the clients block attempting to get more memory, essentially throttling memory usage. Defaults to 50%</td><td>50</td></tr>
<tr><td><small>phoenix.query.maxGlobalMemoryWaitMs</small></td><td style="text-align: left;">Maximum
      amount of time that a client will block while waiting for more memory
      to become available.  After this amount of time, an
<code>InsufficientMemoryException</code> is
      thrown. Default is 10 sec.</td><td>10000</td></tr>
<tr><td><small>phoenix.query.maxTenantMemoryPercentage</small></td><td style="text-align: left;">Maximum
      percentage of <code>phoenix.query.maxGlobalMemoryPercentage</code> that
any one tenant is allowed to consume. After this percentage, an
<code>InsufficientMemoryException</code> is
      thrown. Default is 100%</td><td>100</td></tr>
<tr><td><small>phoenix.query.targetConcurrency</small></td><td style="text-align: left;">Target concurrent
      threads to use for a query. It serves as a soft limit on the number of
      scans into which a query may be split. The value should not exceed the hard limit imposed by<code> phoenix.query.maxConcurrency</code>.</td><td>32</td></tr>
<tr><td><small>phoenix.query.maxConcurrency</small></td><td style="text-align: left;">Maximum concurrent
      threads to use for a query. It servers as a hard limit on the number
      of scans into which a query may be split. A soft limit is imposed by
<code>phoenix.query.targetConcurrency</code>.</td><td>64</td></tr>
<tr><td><small>phoenix.query.dateFormat</small></td><td style="text-align: left;">Default pattern to use
      for conversion of a date to/from a string, whether through the
      <code>TO_CHAR(&lt;date&gt;)</code> or
<code>TO_DATE(&lt;date-string&gt;)</code> functions, or through
<code>resultSet.getString(&lt;date-column&gt;)</code>.</td><td>yyyy-MM-dd HH:mm:ss</td></tr>
<tr><td><small>phoenix.query.statsUpdateFrequency</small></td><td style="text-align: left;">The frequency
      in milliseconds at which the stats for each table will be
updated. Default is 15 min.</td><td>900000</td></tr>
<tr><td><small>phoenix.query.maxStatsAge</small></td><td>The maximum age of
      stats in milliseconds after which they will no longer be used (i.e. the stats were not able to be updated in this amount of time and thus are considered too old). Default is 1 day.</td><td>1</td></tr>
<tr><td><small>phoenix.mutate.maxSize</small></td><td style="text-align: left;">The maximum number of rows
      that may be batched on the client
      before a commit or rollback must be called.</td><td>500000</td></tr>
<tr><td><small>phoenix.mutate.batchSize</small></td><td style="text-align: left;">The number of rows that are batched together and automatically committed during the execution of an
      <code>UPSERT SELECT</code> or <code>DELETE</code> statement. This property may be
overridden at connection
      time by specifying the <code>UpsertBatchSize</code>
      property value. Note that the connection property value does not affect the batch size used by the coprocessor when these statements are executed completely on the server side.</td><td>1000</td></tr>
<tr><td><small>phoenix.query.regionBoundaryCacheTTL</small></td><td style="text-align: left;">The time-to-live
      in milliseconds of the region boundary cache used to guide the split
      points for query parallelization. Default is 15 sec.</td><td>15000</td></tr>
<tr><td><small>phoenix.query.maxIntraRegionParallelization</small></td><td style="text-align: left;">The maximum number of threads that will be spawned to process data within a single region during query execution</td><td>64</td></tr>
<tr><td><small>phoenix.query.rowKeyOrderSaltedTable</small></td><td style="text-align: left;">Whether or not a non aggregate query returns rows in row key order for salted tables. If this option is turned on, split points may not be specified at table create time, but instead the default splits on each salt bucket must be used. Default is true</td><td>true</td></tr></tbody></table>
<br />
<h4>
Parallelization</h4>
Phoenix breaks up aggregate queries into multiple scans and runs them in parallel through custom aggregating coprocessors to improve performance.&nbsp;Hari Kumar, from Ericsson Labs, did a good job of explaining the performance benefits of parallelization and coprocessors <a href="http://labs.ericsson.com/blog/hbase-performance-tuners" target="_blank">here</a>. One of the most important factors in getting good query performance with Phoenix is to ensure that table splits are well balanced. This includes having regions of equal size as well as an even distribution across region servers. There are open source tools such as&nbsp;<a href="http://www.sentric.ch/blog/hbase-split-visualisation-introducing-hannibal" target="_blank">Hannibal</a>&nbsp;that can help you monitor this. By having an even distribution of data, every thread spawned by the Phoenix client will have an equal amount of work to process, thus reducing the time it takes to get the results back. <br />
<br />
The <code>phoenix.query.targetConcurrency</code> and <code>phoenix.query.maxConcurrency</code> control how a query is broken up into multiple scans on the client side. The idea for parallelization of queries is to align the scan boundaries with region boundaries. If rows are not evenly distributed across regions, using this scheme compensates for regions that have more rows than others, by applying tighter splits and therefore spawning off more scans over the overloaded regions.<br />
<br />
The split points for parallelization are computed as follows. Let's suppose:<br />
<ul>
<li><code>t</code> is the target concurrency</li>
<li><code>m</code> is the max concurrency</li>
<li><code>r</code> is the number of regions we need to scan</li>
</ul>
<code>if r &gt;= t</code><br />
&nbsp;&nbsp; scan using regional boundaries<br />
<code>else if r/2 &gt; t</code><br />
&nbsp;&nbsp; split each region in s splits such that: <code>s = max(x) where s * x &lt; m</code><br />
<code>else</code><br />
&nbsp;&nbsp; split each region in s splits such that:&nbsp; <code>s = max(x) where s * x &lt; t</code><br />
<br />
Depending on the number of cores in your client machine and the size of your cluster, the <code>phoenix.query.threadPoolSize</code>, <code>phoenix.query.queueSize</code>,<code> phoenix.query.maxConcurrency</code>, and <code>phoenix.query.targetConcurrency</code> may all be increased to allow more threads to process a query in parallel. This will allow Phoenix to divide up a query into more scans that may then be executed in parallel, thus reducing latency.<br />
<br />
This approach is not without its limitations. The primary issue is that Phoenix does not have sufficient information to divide up a region into equal data sizes. If the query results span many regions of data, this is not a problem, since regions are more or less of equal size. However, if a query accesses only a few regions, this can be an issue. The best Phoenix can do is to divide up the key space between the start and end key evenly. If there's any skew in the data, then some scans are bound to bear the brunt of the work. You can adjust <code>phoenix.query.maxIntraRegionParallelization</code> to a smaller number to decrease the number of threads spawned per region if you find that throughput is suffering.<br />
<br />
For example, let's say a row key is comprised of a five digit zip code in California, declared as a CHAR(5). Phoenix only knows that the column has 5 characters. In theory, the byte array could vary from five 0x01 bytes to five 0xff bytes (or what ever is the largest valid UTF-8 encoded single byte character). While in actuality, the range is from&nbsp;90001 to 96162. Since Phoenix doesn't know this, it'll divide up the region based on the theoretical range and all of the work will end up being done by the single thread that has the range encompassing the actual data. The same thing will occur with a DATE column, since the theoretical range is from 1970 to&nbsp;2038, while in actuality the date is probably +/- a year from the current date. Even if Phoenix uses better defaults for the start and end range rather than the theoretical min and max, it would not usually help - there's just too much variability across domains.<br />
<br />
One solution to this problem is to maintain statistics for a table to feed into the parallelization process to ensure an even data distribution. This is the solution we're working on, as described in more detail in this <a href="https://github.com/forcedotcom/phoenix/issues/49" target="_blank">issue</a>.<br />
<h4>
Batching</h4>
An important HBase configuration property <code>hbase.client.scanner.caching</code> controls scanner caching, that is how many rows are returned from the server in a single round trip when a scan is performed. Although this is less important for aggregate queries, since the Phoenix coprocessors are performing the aggregation instead of returning all the data back to the client, it is important for non aggregate queries. If unset, Phoenix defaults this property to 1000.<br />
<br />
On the DML side of the fence, performance may improve by turning the connection auto commit to on for multi-row mutations such as those that can occur with <code>DELETE</code> and <code>UPSERT SELECT</code>. In this case, if possible, the mutation will be performed completely on the server side without returning data back to the client. However, when performing single row mutations, such as <code>UPSERT VALUES</code>, the opposite is true: auto commit should be off and a reasonable number of rows should be batched together for a single commit to reduce RPC traffic.<br />
<h3>
Measuring Performance</h3>
One way to get a feeling for how to configure these properties is to use the performance.sh shell script provided in the bin directory of the installation tar.<br />
<br />
<b>Usage: </b><code>performance.sh &lt;zookeeper&gt; &lt;row count&gt;</code><br />
<b>Example: </b><code>performance.sh localhost 1000000</code><br />
<br />
This will create a new table named <code>performance_1000000</code> and upsert 1000000 rows. The schema and data generated is similar to <code>examples/web_stat.sql</code> and <code>examples/web_stat.csv</code>. On the console it will measure the time it takes to:<br />
<ul>
<li>upsert these rows</li>
<li>run queries that perform <code>COUNT</code>, <code>GROUP BY</code>, and <code>WHERE</code> clause filters</li>
</ul>
For convenience, an <code>hbase-site.xml</code> file is included in the bin directory and pre-configured to already be on the classpath during script execution.<br />
<br />
Here is a screenshot of the performance.sh script in action:<br />
<div class="separator" style="clear: both; text-align: center;">
<a href="http://1.bp.blogspot.com/-VhinivNOJmI/URWBGLYTiHI/AAAAAAAAAQU/Dp9lbH2CxYE/s1600/performance_script.png" imageanchor="1" style="margin-left: 1em; margin-right: 1em;"><img border="0" height="640" src="http://1.bp.blogspot.com/-VhinivNOJmI/URWBGLYTiHI/AAAAAAAAAQU/Dp9lbH2CxYE/s640/performance_script.png" width="497" /></a></div>
<h3>
&nbsp;Conclusion</h3>
Phoenix has many knobs and dials to tailor the system to your use case. From controlling the level of parallelization, to the size of batches, to the consumption of resource, <i>there's a knob for that</i>. &nbsp;These controls are not without there limitations, however. There's still more work to be done and we'd love to hear your ideas on what you'd like to see made more configurable.<br />
<br />
