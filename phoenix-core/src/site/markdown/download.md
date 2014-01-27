## Available Phoenix Downloads

### Download link will be available soon.

<br/>

### Installation ###
To install a pre-built phoenix, use these directions:

* Download and expand the latest phoenix-[version]-install.tar
* Add the phoenix-[version].jar to the classpath of every HBase region server. An easy way to do this is to copy it into the HBase lib directory.
* Restart all region servers.
* Add the phoenix-[version]-client.jar to the classpath of any Phoenix client.

### Getting Started ###
Wanted to get started quickly? Take a look at our [FAQs](faq.html) and take our quick start guide [here](Phoenix-in-15-minutes-or-less.html).

<h4>Command Line</h4>

A terminal interface to execute SQL from the command line is now bundled with Phoenix. To start it, execute the following from the bin directory:

	$ sqlline.sh localhost

To execute SQL scripts from the command line, you can include a SQL file argument like this:

	$ sqlline.sh localhost ../examples/stock_symbol.sql

![sqlline](images/sqlline.png)

For more information, see the [manual](http://www.hydromatic.net/sqlline/manual.html).

<h5>Loading Data</h5>

In addition, you can use the bin/psql.sh to load CSV data or execute SQL scripts. For example:

        $ psql.sh localhost ../examples/web_stat.sql ../examples/web_stat.csv ../examples/web_stat_queries.sql

Other alternatives include:
* Using our [map-reduce based CSV loader](mr_dataload.html) for bigger data sets
* [Mapping an existing HBase table to a Phoenix table](index.html#Mapping-to-an-Existing-HBase-Table) and using the [UPSERT SELECT](language/index.html#upsert_select) command to populate a new table.
* Populating the table through our [UPSERT VALUES](language/index.html#upsert_values) command.

<h4>SQL Client</h4>

If you'd rather use a client GUI to interact with Phoenix, download and install [SQuirrel](http://squirrel-sql.sourceforge.net/). Since Phoenix is a JDBC driver, integration with tools such as this are seamless. Here are the setup steps necessary:

1. Remove prior phoenix-[version]-client.jar from the lib directory of SQuirrel
2. Copy the phoenix-[version]-client.jar into the lib directory of SQuirrel (Note that on a Mac, this is the *internal* lib directory).
3. Start SQuirrel and add new driver to SQuirrel (Drivers -> New Driver)
4. In Add Driver dialog box, set Name to Phoenix
5. Press List Drivers button and org.apache.phoenix.jdbc.PhoenixDriver should be automatically populated in the Class Name textbox. Press OK to close this dialog.
6. Switch to Alias tab and create the new Alias (Aliases -> New Aliases)
7. In the dialog box, Name: _any name_, Driver: Phoenix, User Name: _anything_, Password: _anything_
8. Construct URL as follows: jdbc:phoenix: _zookeeper quorum server_. For example, to connect to a local HBase use: jdbc:phoenix:localhost
9. Press Test (which should succeed if everything is setup correctly) and press OK to close.
10. Now double click on your newly created Phoenix alias and click Connect. Now you are ready to run SQL queries against Phoenix.

Through SQuirrel, you can issue SQL statements in the SQL tab (create tables, insert data, run queries), and inspect table metadata in the Object tab (i.e. list tables, their columns, primary keys, and types).

![squirrel](images/squirrel.png)

### Samples ###
The best place to see samples are in our unit tests under src/test/java. The ones in the endToEnd package are tests demonstrating how to use all aspects of the Phoenix JDBC driver. We also have some examples in the examples directory.

### Phoenix Client - Server Compatibility

Major and minor version should match between client and server (patch version can mismatch). Following is the list of compatible client and server version(s). It is recommended that same client and server version are used. 

Phoenix Client Version | Compatible Server Versions
-----------------------|---
1.0.0 | 1.0.0
1.1.0 | 1.1.0
1.2.0 | 1.2.0, 1.2.1
1.2.1 | 1.2.0, 1.2.1
2.0.0 | 2.0.0, 2.0.1, 2.0.2
2.0.1 | 2.0.0, 2.0.1, 2.0.2
2.0.2 | 2.0.0, 2.0.1, 2.0.2
2.1.0 | 2.1.0, 2.1.1, 2.1.2
2.1.1 | 2.1.0, 2.1.1, 2.1.2
2.1.2 | 2.1.0, 2.1.1, 2.1.2
2.2.0 | 2.2.0, 2.2.1
2.2.1 | 2.2.0, 2.2.1

[![githalytics.com alpha](https://cruel-carlota.pagodabox.com/33878dc7c0522eed32d2d54db9c59f78 "githalytics.com")](http://githalytics.com/forcedotcom/phoenix.git)
