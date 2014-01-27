# Bulk CSV Data Load using Map-Reduce

Phoenix v 2.1 provides support for loading CSV data into a new/existing Phoenix table using Hadoop Map-Reduce. This provides a means of bulk loading CSV data in parallel through map-reduce, yielding better performance in comparison with the existing [psql csv loader](download.html#Loading-Data).

####Sample input CSV data:

```
12345, John, Doe
67890, Mary, Poppins
```

####Compatible Phoenix schema to hold the above CSV data:

     CREATE TABLE ns.example (
        my_pk bigint not null,
        m.first_name varchar(50),
        m.last_name varchar(50) 
        CONSTRAINT pk PRIMARY KEY (my_pk))

<table>
<tr><td>Row Key</td><td colspan="2" bgcolor="#00FF00"><center>Column Family (m)</center></td></tr>
<tr><td><strong>my_pk</strong> BIGINT</td><td><strong>first_name</strong> VARCHAR(50)</td><td><strong>last_name</strong> VARCHAR(50)</td></tr>
<tr><td>12345</td><td>John</td><td>Doe</td></tr>
<tr><td>67890</td><td>Mary</td><td>Poppins</td></tr>
</table>


####How to run?

1- Please make sure that Hadoop cluster is working correctly and you are able to run any job like [this](http://wiki.apache.org/hadoop/WordCount). 

2- Copy latest phoenix-[version].jar to hadoop/lib folder on each node or add it to Hadoop classpath.

3- Run the bulk loader job using the script /bin/csv-bulk-loader.sh as below:

```
./csv-bulk-loader.sh <option value>

<option>  <value>
-i        CSV data file path in hdfs (mandatory)
-s        Phoenix schema name (mandatory if not default)
-t        Phoenix table name (mandatory)
-sql      Phoenix create table sql file path (mandatory)
-zk       Zookeeper IP:<port> (mandatory)
-mr       MapReduce Job Tracker IP:<port> (mandatory)
-hd       HDFS NameNode IP:<port> (mandatory)
-o        Output directory path in hdfs (optional)
-idx      Phoenix index table name (optional, not yet supported)
-error    Ignore error while reading rows from CSV ? (1-YES | 0-NO, default-1) (optional)
-help     Print all options (optional)
```
Example

```
./csv-bulk-loader.sh -i hdfs://server:9000/mydir/data.csv -s ns -t example -sql ~/Documents/createTable.sql -zk server:2181 -hd hdfs://server:9000 -mr server:9001
```

This would create the phoenix table "ns.example" as specified in createTable.sql and will then load the CSV data from the file "data.csv" located in HDFS into the table.

##### Notes
1. You must provide an explicit column family name in your CREATE TABLE statement for your non primary key columns, as the default column family used by Phoenix is treated specially by HBase because it starts with an underscore.
2. The current bulk loader does not support the migration of index related data yet. So, if you have created your phoenix table with index, please use the [psql CSV loader](download.html#Loading-Data). 
3. In case you want to further optimize the map-reduce performance, please refer to the current map-reduce optimization params in the file "src/main/config/csv-bulk-load-config.properties". In case you modify this list, please re-build the phoenix jar and re-run the job as described above.
