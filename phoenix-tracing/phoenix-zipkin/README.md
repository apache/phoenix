# Phoenix Zipkin TracingWebApp
1. Build the web application-
 *mvn clean install*

2. Start the Phoenix-Zipkin
 *java -jar ./target/phoenix-zipkin-4.8.0-HBase-1.2-SNAPSHOT.jar*

3. View Web application on Web Browser -
 *http://localhost:8865/*

 ###Note
 You can set the port of the Phoenix-Zipkin by `-Dserver.port={portNo}`

 eg:
 `java -jar -Dserver.port=8088 ./target/phoenix-zipkin-4.8.0-HBase-1.2-SNAPSHOT.jar ` server will start in 8088


 To change the zookeeper host

 `-Dphoenix.host={hostname}`

 To change the zookeeper port

 `-Dphoenix.port={portNo}`

 eg:
 `java -jar -Dphoenix.host=pc -Dphoenix.port=2181 ./target/phoenix-zipkin-4.8.0-HBase-1.2-SNAPSHOT.jar `