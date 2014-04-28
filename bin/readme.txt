SqlLine
=======
https://github.com/julianhyde/sqlline

Execute SQL from command line. Sqlline manual is available at http://www.hydromatic.net/sqlline/manual.html
	
	Usage: 
	$ sqlline.py <zookeeper> <optional_sql_file> 
	Example: 
	$ sqlline.py localhost
	$ sqlline.py localhost <examples dir>/STOCK_SYMBOL.sql

psql.py
=======

Usage: psql [-t table-name] [-h comma-separated-column-names | in-line] <zookeeper>  <path-to-sql-or-csv-file>...

Example 1. Create table, upsert row and run query using single .sql file
./psql.py localhost <examples dir>/STOCK_SYMBOL.sql

Example 2. Create table, load CSV data and run queries using .csv and .sql files:
./psql.py localhost <examples dir>/WEB_STAT.sql <examples dir>/WEB_STAT.csv <examples dir>/WEB_STAT_QUERIES.sql

Note: Please see comments in WEB_STAT_QUERIES.sql for the sample queries being executed

performance.py
==============

Usage: performance <zookeeper> <row count>

Example: Generates and upserts 1000000 rows and time basic queries on this data
./performance.py localhost 1000000

CSV MapReduce Loader
====================

Usage: hadoop jar phoneix-[version]-mapreduce.jar <parameters>

 -a,--array-delimiter <arg>   Array element delimiter (optional)
 -c,--import-columns <arg>    Comma-separated list of columns to be
                              imported
 -d,--delimiter <arg>         Input delimiter, defaults to comma
 -g,--ignore-errors           Ignore input errors
 -h,--help                    Show this help and quit
 -i,--input <arg>             Input CSV path (mandatory)
 -o,--output <arg>            Output path for temporary HFiles (optional)
 -s,--schema <arg>            Phoenix schema name (optional)
 -t,--table <arg>             Phoenix table name (mandatory)
 -z,--zookeeper <arg>         Zookeeper quorum to connect to (optional)

