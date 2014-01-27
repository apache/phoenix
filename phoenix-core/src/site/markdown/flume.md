# Apache Flume Plugin

The plugin enables us to reliably and efficiently stream large amounts of data/logs onto HBase using the Phoenix API. The necessary configuration of the custom Phoenix sink and the Event Serializer has to be configured in the Flume configuration file for the Agent. Currently, the only supported Event serializer is a RegexEventSerializer which primarily breaks the Flume Event body based on the regex specified in the configuration file.   

#### Prerequisites:

* Phoenix v 3.0.0 SNAPSHOT +
* Flume 1.4.0 +

#### Installation & Setup:

1. Download and build Phoenix v 0.3.0 SNAPSHOT
2. Follow the instructions as specified [here](building.html) to build the project as the Flume plugin is still under beta
3. Create a directory plugins.d within $FLUME_HOME directory. Within that, create a sub-directories phoenix-sink/lib 
4. Copy the generated phoenix-3.0.0-SNAPSHOT-client.jar onto $FLUME_HOME/plugins.d/phoenix-sink/lib

#### Configuration:
  
Property Name             |Default| Description
--------------------------|-------|---
type                      |       |org.apache.phoenix.flume.sink.PhoenixSink
batchSize                 |100    |Default number of events per transaction 
zookeeperQuorum           |       |Zookeeper quorum of the HBase cluster
table                     |       |The name of the table in HBase to write to.
ddl                       |       |The CREATE TABLE query for the HBase table where the events will be                                                    upserted to. If specified, the query will be executed. Recommended to include the IF NOT EXISTS clause in the ddl.
serializer                |regex  |Event serializers for processing the Flume Event . Currently , only regex is supported.
serializer.regex          |(.*)   |The regular expression for parsing the event. 
serializer.columns        |       |The columns that will be extracted from the Flume event for inserting         into HBase. 
serializer.headers        |       |Headers of the Flume Events that go as part of the UPSERT query. The  data type for these columns are VARCHAR by default.
serializer.rowkeyType     |     |A custom row key generator . Can be one of timestamp,date,uuid,random and     nanotimestamp. This should be configured in cases  where we need a custom row key value to be auto generated and set for the primary key column.


For an example configuration for ingesting Apache access logs onto Phoenix, see [this](https://github.com/forcedotcom/phoenix/blob/master/src/main/config/apache-access-logs.properties) property file. Here we are using UUID as a row key generator for the primary key.	
		   	
#### Starting the agent:
       $ bin/flume-ng agent -f conf/flume-conf.properties -c ./conf -n agent

#### Monitoring:
   For monitoring the agent and the sink process , enable JMX via flume-env.sh($FLUME_HOME/conf/flume-env.sh) script. Ensure you have the following line uncommented.
   
    JAVA_OPTS="-Xms1g -Xmx1g -Dcom.sun.management.jmxremote -Dcom.sun.management.jmxremote.port=3141 -Dcom.sun.management.jmxremote.authenticate=false -Dcom.sun.management.jmxremote.ssl=false"   	
	
