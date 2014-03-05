SqlLine
=======
https://github.com/julianhyde/sqlline

Execute SQL from command line. Sqlline manual is available at http://www.hydromatic.net/sqlline/manual.html
	
	Usage: 
	$ sqlline.sh <zookeeper> <optional_sql_file> 
	Example: 
	$ sqlline.sh localhost
	$ sqlline.sh localhost ../examples/STOCK_SYMBOL.sql

psql.sh
=======

Usage: psql [-t table-name] [-h comma-separated-column-names | in-line] <zookeeper>  <path-to-sql-or-csv-file>...

Example 1. Create table, upsert row and run query using single .sql file
./psql localhost ../examples/STOCK_SYMBOL.sql

Example 2. Create table, load CSV data and run queries using .csv and .sql files:
./psql.sh localhost ../examples/WEB_STAT.sql ../examples/WEB_STAT.csv ../examples/WEB_STAT_QUERIES.sql

Note: Please see comments in WEB_STAT_QUERIES.sql for the sample queries being executed

performance.sh
==============

Usage: performance <zookeeper> <row count>

Example: Generates and upserts 1000000 rows and time basic queries on this data
./performance.sh localhost 1000000

csv-bulk-loader.sh
==================

Usage: csv-bulk-loader <option value>
Note: phoenix-[version].jar needs to be on Hadoop classpath on each node

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
-error    Ignore error while reading rows from CSV? (1-YES | 0-NO, default-1) (optional)
-help     Print all options (optional)

Example:
./csv-bulk-loader.sh -i hdfs://server:9000/mydir/data.csv -s ns -t example -sql ~/Documents/createTable.sql -zk server:2181 -hd hdfs://server:9000 -mr server:9001


