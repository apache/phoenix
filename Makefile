HOST=tph15nn
HOST_DN=tph15dn1
SSH=ssh
#HOST=thb4nn
#HOST_DN=thb4dn1
SQL=./hash_join_query.sql
ZOOKEEPER=localhost
VERSION=4.14.0-HBase-1.4
PHX_JAR=phoenix-$(VERSION)-client.jar
PHX_THIN_JAR=phoenix-$(VERSION)-thin-client.jar
PHX_SERVER_JAR=phoenix-$(VERSION)-server.jar
CUSTOM_SQLLINE=/usr/lib/phoenix/bin/marcell-sqlline.py
IBD_VALIDATE_NN=enn
# IBD_VALIDATE_DNS=`cat ibdvalidate_ips_5`
# COMMANDS=$(patsubst %, %.cmd, $(IBD_VALIDATE_DNS))

build:
	mvn package -DskipTests

sync:
	scp ./phoenix-client/target/$(PHX_JAR) $(HOST):
	scp ./phoenix-server/target/$(PHX_SERVER_JAR) $(HOST):
	scp ./phoenix-client/target/$(PHX_JAR) $(HOST_DN):
	scp ./phoenix-server/target/$(PHX_SERVER_JAR) $(HOST_DN):
	scp $(SQL) $(HOST):

	ssh $(HOST) 'sudo rm /usr/lib/phoenix/*.jar'
	ssh $(HOST_DN) 'sudo rm /usr/lib/phoenix/*.jar'

	ssh $(HOST) 'sudo mv $(PHX_JAR) /usr/lib/phoenix/$(PHX_JAR)'
	ssh $(HOST) 'sudo mv $(PHX_SERVER_JAR) /usr/lib/phoenix/$(PHX_SERVER_JAR)'
	ssh $(HOST_DN) 'sudo mv $(PHX_JAR) /usr/lib/phoenix/$(PHX_JAR)'
	ssh $(HOST_DN) 'sudo mv $(PHX_SERVER_JAR) /usr/lib/phoenix/$(PHX_SERVER_JAR)'

	ssh $(HOST_DN) 'sudo stop hbase-regionserver'
	ssh $(HOST) 'sudo stop hbase-master'
	sleep 5
	ssh $(HOST) 'sudo start hbase-master'
	sleep 5
	ssh $(HOST_DN) 'sudo start hbase-regionserver'

run: build sync
	ssh $(HOST) '$(CUSTOM_SQLLINE) $(ZOOKEEPER) $(SQL)'
	ssh $(HOST) '$(CUSTOM_SQLLINE) $(ZOOKEEPER) $(SQL)'

rerun:
	ssh $(HOST) '$(CUSTOM_SQLLINE) $(ZOOKEEPER) $(SQL)'

rerun2:
	ssh $(HOST_DN) 'sudo stop hbase-regionserver'
	ssh $(HOST_DN) 'sudo start hbase-regionserver'
	ssh $(HOST) '$(CUSTOM_SQLLINE) $(ZOOKEEPER) $(SQL)'
#	ssh $(HOST) '$(CUSTOM_SQLLINE) $(ZOOKEEPER) $(SQL)'

pull_ibd_validate_ips:
	AWS_PROFILE=live aws ec2 describe-instances --filter Name=tag:Name,Values="mortutay: emr-ibdvalidate-5" >out
	cat out | jq -r .Reservations[].Instances[].PrivateIpAddress | sort -u > ibdvalidate_ips_5

sync_ibd:
	for IP in `cat ibdvalidate_ips_5 | grep 10.55.20.3` ; do \
		echo $$IP; \
		scp -i ~/.ssh/hadoopclient.pem ./phoenix-client/target/$(PHX_JAR) hadoop@$$IP:/tmp; \
		scp -i ~/.ssh/hadoopclient.pem ./phoenix-server/target/$(PHX_SERVER_JAR) hadoop@$$IP:/tmp; \
		$(SSH) -i ~/.ssh/hadoopclient.pem hadoop@$$IP 'cd /usr/lib/phoenix ; sudo rm phoenix-client.jar'; \
		$(SSH) -i ~/.ssh/hadoopclient.pem hadoop@$$IP 'cd /usr/lib/phoenix ; sudo rm phoenix-server.jar'; \
		$(SSH) -i ~/.ssh/hadoopclient.pem hadoop@$$IP 'sudo mv /tmp/$(PHX_JAR) /usr/lib/phoenix'; \
		$(SSH) -i ~/.ssh/hadoopclient.pem hadoop@$$IP 'sudo mv /tmp/$(PHX_SERVER_JAR) /usr/lib/phoenix'; \
		$(SSH) -i ~/.ssh/hadoopclient.pem hadoop@$$IP 'cd /usr/lib/phoenix ; sudo ln -s $(PHX_JAR) phoenix-client.jar'; \
		$(SSH) -i ~/.ssh/hadoopclient.pem hadoop@$$IP 'cd /usr/lib/phoenix ; sudo ln -s $(PHX_SERVER_JAR) phoenix-server.jar'; \
	done

sync_master:
	scp -i ~/.ssh/hadoopclient.pem ./phoenix-client/target/$(PHX_JAR) hadoop@$(IBD_VALIDATE_NN):/tmp
	scp -i ~/.ssh/hadoopclient.pem ./phoenix-queryserver-client/target/$(PHX_THIN_JAR) hadoop@$(IBD_VALIDATE_NN):/tmp
	scp -i ~/.ssh/hadoopclient.pem ./phoenix-server/target/$(PHX_SERVER_JAR) hadoop@$(IBD_VALIDATE_NN):/tmp
	ssh -i ~/.ssh/hadoopclient.pem hadoop@$(IBD_VALIDATE_NN) 'hdfs dfs -rm /$(PHX_JAR) || true'
	ssh -i ~/.ssh/hadoopclient.pem hadoop@$(IBD_VALIDATE_NN) 'hdfs dfs -rm /$(PHX_THIN_JAR) || true'
	ssh -i ~/.ssh/hadoopclient.pem hadoop@$(IBD_VALIDATE_NN) 'hdfs dfs -rm /$(PHX_SERVER_JAR) || true'
	ssh -i ~/.ssh/hadoopclient.pem hadoop@$(IBD_VALIDATE_NN) 'hdfs dfs -put /tmp/$(PHX_JAR) /$(PHX_JAR)'
	ssh -i ~/.ssh/hadoopclient.pem hadoop@$(IBD_VALIDATE_NN) 'hdfs dfs -put /tmp/$(PHX_THIN_JAR) /$(PHX_THIN_JAR)'
	ssh -i ~/.ssh/hadoopclient.pem hadoop@$(IBD_VALIDATE_NN) 'hdfs dfs -put /tmp/$(PHX_SERVER_JAR) /$(PHX_SERVER_JAR)'

# $(IBD_VALIDATE_DNS):
# $(COMMANDS): %.cmd: %
# 		echo $<

# sync_all: $(COMMANDS)


restart_ibd:
	for IP in `cat ibdvalidate_ips_5 | grep -v $(IBD_VALIDATE_NN)` ; do \
		$(SSH) -i ~/.ssh/hadoopclient.pem hadoop@$$IP 'sudo stop hbase-regionserver ; sudo start hbase-regionserver'; \
	done
	$(SSH) -i ~/.ssh/hadoopclient.pem hadoop@$$IBD_VALIDATE_NN 'sudo stop hbase-master ; sudo start hbase-master'; \


RS_MACHINES=10.55.17.122 10.55.17.137 10.55.17.177 10.55.17.29 10.55.17.62 10.55.17.70 10.55.17.79 10.55.18.163 10.55.18.167 10.55.18.216 10.55.18.33 10.55.18.4 10.55.18.40 10.55.18.53 10.55.19.217 10.55.19.237 10.55.19.3 10.55.20.118 10.55.20.12 10.55.20.194 10.55.20.200 10.55.20.21 10.55.20.240 10.55.21.113 10.55.21.93 10.55.22.162 10.55.22.191 10.55.22.235 10.55.23.122 10.55.23.14 10.55.23.145 10.55.24.125 10.55.24.159 10.55.24.2 10.55.24.27 10.55.24.41 10.55.25.156 10.55.25.171 10.55.25.200 10.55.26.196 10.55.27.102 10.55.27.124 10.55.27.46 10.55.28.249 10.55.28.250 10.55.28.46 10.55.28.51 10.55.29.161 10.55.29.196 10.55.29.3 10.55.29.64 10.55.30.111 10.55.30.162 10.55.30.215 10.55.30.33 10.55.31.10 10.55.31.113 10.55.31.127 10.55.31.165 10.55.31.218 #10.55.20.3
ALL_MACHINES=10.55.17.122 10.55.17.137 10.55.17.177 10.55.17.29 10.55.17.62 10.55.17.70 10.55.17.79 10.55.18.163 10.55.18.167 10.55.18.216 10.55.18.33 10.55.18.4 10.55.18.40 10.55.18.53 10.55.19.217 10.55.19.237 10.55.19.3 10.55.20.118 10.55.20.12 10.55.20.194 10.55.20.200 10.55.20.21 10.55.20.240 10.55.21.113 10.55.21.93 10.55.22.162 10.55.22.191 10.55.22.235 10.55.23.122 10.55.23.14 10.55.23.145 10.55.24.125 10.55.24.159 10.55.24.2 10.55.24.27 10.55.24.41 10.55.25.156 10.55.25.171 10.55.25.200 10.55.26.196 10.55.27.102 10.55.27.124 10.55.27.46 10.55.28.249 10.55.28.250 10.55.28.46 10.55.28.51 10.55.29.161 10.55.29.196 10.55.29.3 10.55.29.64 10.55.30.111 10.55.30.162 10.55.30.215 10.55.30.33 10.55.31.10 10.55.31.113 10.55.31.127 10.55.31.165 10.55.31.218 10.55.20.3

COMMANDS=$(patsubst %, %.sync, $(ALL_MACHINES))
$(ALL_MACHINES):
$(COMMANDS): %.sync: %
	echo $<
	ssh $< "rm /tmp/$(PHX_JAR) || true"
	ssh $< "hdfs dfs -get /$(PHX_JAR) /tmp/$(PHX_JAR)"
	ssh $< "rm /tmp/$(PHX_THIN_JAR) || true"
	ssh $< "hdfs dfs -get /$(PHX_THIN_JAR) /tmp/$(PHX_THIN_JAR)"
	ssh $< "rm /tmp/$(PHX_SERVER_JAR) || true"
	ssh $< "hdfs dfs -get /$(PHX_SERVER_JAR) /tmp/$(PHX_SERVER_JAR)"
	ssh $< 'cd /usr/lib/phoenix ; sudo rm phoenix-client.jar || true';
	ssh $< 'cd /usr/lib/phoenix ; sudo rm phoenix-thin-client.jar || true';
	ssh $< 'cd /usr/lib/phoenix ; sudo rm phoenix-server.jar || true';
	ssh $< 'cd /usr/lib/phoenix ; sudo rm phoenix-*-client.jar || true';
	ssh $< 'cd /usr/lib/phoenix ; sudo rm phoenix-*-thin-client.jar || true';
	ssh $< 'cd /usr/lib/phoenix ; sudo rm phoenix-*-server.jar || true';
	ssh $< 'sudo mv /tmp/$(PHX_JAR) /usr/lib/phoenix';
	ssh $< 'sudo mv /tmp/$(PHX_THIN_JAR) /usr/lib/phoenix';
	ssh $< 'sudo mv /tmp/$(PHX_SERVER_JAR) /usr/lib/phoenix';
	ssh $< 'cd /usr/lib/phoenix ; sudo ln -s $(PHX_JAR) phoenix-client.jar';
	ssh $< 'cd /usr/lib/phoenix ; sudo ln -s $(PHX_THIN_JAR) phoenix-thin-client.jar';
	ssh $< 'cd /usr/lib/phoenix ; sudo ln -s $(PHX_SERVER_JAR) phoenix-server.jar';
sync_all: $(COMMANDS)

LOG_COMMANDS=$(patsubst %, %.log, $(RS_MACHINES))
$(RS_MACHINES):
$(LOG_COMMANDS): %.log: %
	echo $<
	ssh $< "tail -n 500 /mnt/var/log/hbase/hbase-hbase-regionserver-ip-*.out" >$<.out
	ssh $< "tail -n 500 /mnt/var/log/hbase/hbase-hbase-regionserver-ip-*.log" >>$<.out
log_all: $(LOG_COMMANDS)

RESTART_COMMANDS=$(patsubst %, %.restart, $(RS_MACHINES))
$(RS_MACHINES):
$(RESTART_COMMANDS): %.restart: %
	ssh -i ~/.ssh/hadoopclient.pem hadoop@$< 'sudo stop hbase-regionserver ; sleep 5 ; sudo start hbase-regionserver';
restart_all: $(RESTART_COMMANDS)

restart_master:
	$(SSH) -i ~/.ssh/hadoopclient.pem hadoop@$(IBD_VALIDATE_NN) 'sudo stop hbase-master ; sleep 5 ; sudo start hbase-master'; \

PQS_MACHINES=pqsnode1.pqs-ibdvalidate-10.live.23andme.net pqsnode2.pqs-ibdvalidate-10.live.23andme.net pqsnode3.pqs-ibdvalidate-10.live.23andme.net pqsnode4.pqs-ibdvalidate-10.live.23andme.net pqsnode5.pqs-ibdvalidate-10.live.23andme.net pqsnode6.pqs-ibdvalidate-10.live.23andme.net pqsnode7.pqs-ibdvalidate-10.live.23andme.net pqsnode8.pqs-ibdvalidate-10.live.23andme.net pqsnode9.pqs-ibdvalidate-10.live.23andme.net pqsnode10.pqs-ibdvalidate-10.live.23andme.net
PQS_COMMANDS=$(patsubst %, %.pqs_sync, $(PQS_MACHINES))

$(PQS_MACHINES):
$(PQS_COMMANDS): %.pqs_sync: %
	echo $<
	scp ./phoenix-client/target/$(PHX_JAR) $<:/tmp/$(PHX_JAR)
	scp ./phoenix-queryserver-client/target/$(PHX_THIN_JAR) $<:/tmp/$(PHX_THIN_JAR)
	scp ./phoenix-server/target/$(PHX_SERVER_JAR) $<:/tmp/$(PHX_SERVER_JAR)
pqs_sync_all: $(PQS_COMMANDS)

PQS_MV_COMMANDS=$(patsubst %, %.pqs_mv, $(PQS_MACHINES))

$(PQS_MACHINES):
$(PQS_MV_COMMANDS): %.pqs_mv: %
	echo $<
#	ssh $< 'sudo mv /tmp/phoenix-4.11.0-HBase-1.3-client.jar.bak /opt/apache-phoenix-4.11.0-HBase-1.3-bin/phoenix-4.11.0-HBase-1.3-client.jar'
#	ssh $< 'sudo mv /tmp/phoenix-4.11.0-HBase-1.3-thin-client.jar.bak /opt/apache-phoenix-4.11.0-HBase-1.3-bin/phoenix-4.11.0-HBase-1.3-thin-client.jar'
#	ssh $< 'sudo mv /tmp/phoenix-4.11.0-HBase-1.3-server.jar.bak /opt/apache-phoenix-4.11.0-HBase-1.3-bin/phoenix-4.11.0-HBase-1.3-server.jar'
#	ssh $< 'sudo mv /opt/apache-phoenix-4.11.0-HBase-1.3-bin/phoenix-4.11.0-HBase-1.3-client.jar /tmp/phoenix-4.11.0-HBase-1.3-client.jar.bak'
#	ssh $< 'sudo mv /opt/apache-phoenix-4.11.0-HBase-1.3-bin/phoenix-4.11.0-HBase-1.3-thin-client.jar /tmp/phoenix-4.11.0-HBase-1.3-thin-client.jar.bak'
#	ssh $< 'sudo mv /opt/apache-phoenix-4.11.0-HBase-1.3-bin/phoenix-4.11.0-HBase-1.3-server.jar /tmp/phoenix-4.11.0-HBase-1.3-server.jar.bak'
	ssh $< 'sudo mv /tmp/$(PHX_JAR) /opt/apache-phoenix-4.11.0-HBase-1.3-bin/$(PHX_JAR)'
	ssh $< 'sudo mv /tmp/$(PHX_THIN_JAR) /opt/apache-phoenix-4.11.0-HBase-1.3-bin/$(PHX_THIN_JAR)'
	ssh $< 'sudo mv /tmp/$(PHX_SERVER_JAR) /opt/apache-phoenix-4.11.0-HBase-1.3-bin/$(PHX_SERVER_JAR)'

pqs_mv_all: $(PQS_MV_COMMANDS)
