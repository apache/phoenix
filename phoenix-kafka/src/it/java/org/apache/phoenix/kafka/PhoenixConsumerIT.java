/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.phoenix.kafka;

import static org.apache.phoenix.util.TestUtil.TEST_PROPERTIES;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Properties;

import org.I0Itec.zkclient.ZkClient;
import org.apache.flume.Context;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.phoenix.end2end.BaseHBaseManagedTimeIT;
import org.apache.phoenix.flume.DefaultKeyGenerator;
import org.apache.phoenix.flume.FlumeConstants;
import org.apache.phoenix.flume.serializer.EventSerializers;
import org.apache.phoenix.kafka.consumer.PhoenixConsumer;
import org.apache.phoenix.util.PropertiesUtil;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.google.common.io.Resources;

import kafka.admin.AdminUtils;
import kafka.server.KafkaConfig;
import kafka.server.KafkaServer;
import kafka.utils.MockTime;
import kafka.utils.TestUtils;
import kafka.utils.Time;
import kafka.utils.ZKStringSerializer$;
import kafka.utils.ZkUtils;
import kafka.zk.EmbeddedZookeeper;

public class PhoenixConsumerIT extends BaseHBaseManagedTimeIT {
    private static final String ZKHOST = "127.0.0.1";
    private static final String BROKERHOST = "127.0.0.1";
    private static final String BROKERPORT = "9092";
    private static final String TOPIC = "topic1";
    private KafkaServer kafkaServer;
    private PhoenixConsumer pConsumer;
    private EmbeddedZookeeper zkServer;
    private ZkClient zkClient;
    private Connection conn;

    @Before
    public void setUp() throws IOException, SQLException {
        // setup Zookeeper
        zkServer = new EmbeddedZookeeper();
        String zkConnect = ZKHOST + ":" + zkServer.port();
        zkClient = new ZkClient(zkConnect, 30000, 30000, ZKStringSerializer$.MODULE$);
        ZkUtils zkUtils = ZkUtils.apply(zkClient, false);

        // setup Broker
        Properties brokerProps = new Properties();
        brokerProps.setProperty("zookeeper.connect", zkConnect);
        brokerProps.setProperty("broker.id", "0");
        brokerProps.setProperty("log.dirs",
            Files.createTempDirectory("kafka-").toAbsolutePath().toString());
        brokerProps.setProperty("listeners", "PLAINTEXT://" + BROKERHOST + ":" + BROKERPORT);
        KafkaConfig config = new KafkaConfig(brokerProps);
        Time mock = new MockTime();
        kafkaServer = TestUtils.createServer(config, mock);
        kafkaServer.startup();

        // create topic
        AdminUtils.createTopic(zkUtils, TOPIC, 1, 1, new Properties());

        pConsumer = new PhoenixConsumer();
        
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        conn = DriverManager.getConnection(getUrl(), props);
    }

    @Test
    public void testPhoenixConsumerWithFile() throws SQLException {
        String consumerPath = "consumer.props";
        PhoenixConsumerThread pConsumerThread = new PhoenixConsumerThread(pConsumer, consumerPath);
        pConsumerThread.properties.setProperty(FlumeConstants.CONFIG_JDBC_URL, getUrl());
        Thread phoenixConsumer = new Thread(pConsumerThread);

        String producerPath = "producer.props";
        KafkaProducerThread kProducerThread = new KafkaProducerThread(producerPath, TOPIC);
        Thread kafkaProducer = new Thread(kProducerThread);

        phoenixConsumer.start();

        try {
            phoenixConsumer.join(10000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        kafkaProducer.start();

        try {
            kafkaProducer.join();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        if (!kafkaProducer.isAlive()) {
            System.out.println("kafka producer is not alive");
            pConsumer.stop();
        }
        
        // Verify our serializer wrote out data
        ResultSet rs = conn.createStatement().executeQuery("SELECT * FROM SAMPLE1");
        assertTrue(rs.next());
        assertTrue(rs.getFetchSize() > 0);
        rs.close();
    }
    
    @Test
    public void testPhoenixConsumerWithProperties() throws SQLException {
        
        final String fullTableName = "SAMPLE2";
        final String ddl = "CREATE TABLE IF NOT EXISTS SAMPLE2(uid VARCHAR NOT NULL,c1 VARCHAR,c2 VARCHAR,c3 VARCHAR CONSTRAINT pk PRIMARY KEY(uid))\n";
        
        Properties consumerProperties = new Properties();
        consumerProperties.setProperty(FlumeConstants.CONFIG_TABLE, fullTableName);
        consumerProperties.setProperty(FlumeConstants.CONFIG_JDBC_URL, getUrl());
        consumerProperties.setProperty(FlumeConstants.CONFIG_SERIALIZER,EventSerializers.REGEX.name());
        consumerProperties.setProperty(FlumeConstants.CONFIG_TABLE_DDL, ddl);
        consumerProperties.setProperty(FlumeConstants.CONFIG_SERIALIZER_PREFIX + FlumeConstants.CONFIG_REGULAR_EXPRESSION,"([^\\,]*),([^\\,]*),([^\\,]*)");
        consumerProperties.setProperty(FlumeConstants.CONFIG_SERIALIZER_PREFIX + FlumeConstants.CONFIG_COLUMN_NAMES,"c1,c2,c3");
        consumerProperties.setProperty(FlumeConstants.CONFIG_SERIALIZER_PREFIX + FlumeConstants.CONFIG_ROWKEY_TYPE_GENERATOR, DefaultKeyGenerator.UUID.name());
        consumerProperties.setProperty(KafkaConstants.BOOTSTRAP_SERVERS, "localhost:9092");
        consumerProperties.setProperty(KafkaConstants.TOPICS, "topic1,topic2");
        consumerProperties.setProperty(KafkaConstants.TIMEOUT, "100");
        
        PhoenixConsumerThread pConsumerThread = new PhoenixConsumerThread(pConsumer, consumerProperties);
        Thread phoenixConsumer = new Thread(pConsumerThread);

        Properties producerProperties = new Properties();
        producerProperties.setProperty(KafkaConstants.BOOTSTRAP_SERVERS, "localhost:9092");
        producerProperties.setProperty(KafkaConstants.KEY_SERIALIZER, KafkaConstants.DEFAULT_KEY_SERIALIZER);
        producerProperties.setProperty(KafkaConstants.VALUE_SERIALIZER, KafkaConstants.DEFAULT_VALUE_SERIALIZER);
        producerProperties.setProperty("auto.commit.interval.ms", "1000");
        
        KafkaProducerThread kProducerThread = new KafkaProducerThread(producerProperties, TOPIC);
        Thread kafkaProducer = new Thread(kProducerThread);

        phoenixConsumer.start();

        try {
            phoenixConsumer.join(10000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        kafkaProducer.start();

        try {
            kafkaProducer.join();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        if (!kafkaProducer.isAlive()) {
            System.out.println("kafka producer is not alive");
            pConsumer.stop();
        }
        
        // Verify our serializer wrote out data
        ResultSet rs = conn.createStatement().executeQuery("SELECT * FROM SAMPLE2");
        assertTrue(rs.next());
        assertTrue(rs.getFetchSize() > 0);
        rs.close();
    }

    @After
    public void cleanUp() throws Exception {
        kafkaServer.shutdown();
        zkClient.close();
        zkServer.shutdown();
        conn.close();
    }

    class PhoenixConsumerThread implements Runnable {
        PhoenixConsumer pConsumer;
        Properties properties;

        PhoenixConsumerThread(PhoenixConsumer pConsumer, String path) {
            this.pConsumer = pConsumer;
            try (InputStream props = Resources.getResource(path).openStream()) {
                Properties properties = new Properties();
                properties.load(props);
                this.properties = properties;
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        
        PhoenixConsumerThread(PhoenixConsumer pConsumer, Properties properties) {
            this.pConsumer = pConsumer;
            this.properties = properties;
        }

        @Override
        public void run() {
            // intialize the kafka
            pConsumer.intializeKafka(properties);

            // configure the phoenix
            Context context = pConsumer.prepareContext();
            pConsumer.configure(context);

            // start the kafka consumer
            pConsumer.start();

            // process kafka messages
            pConsumer.process();
        }
    }

    class KafkaProducerThread implements Runnable {
        KafkaProducer<String, String> producer;
        String topic;

        KafkaProducerThread(String path, String topic) {
            this.topic = topic;
            try (InputStream props = Resources.getResource(path).openStream()) {
                Properties properties = new Properties();
                properties.load(props);
                producer = new KafkaProducer<>(properties);
            } catch (IOException e) {
                e.printStackTrace();
            }
        }

        KafkaProducerThread(Properties properties, String topic) {
            this.topic = topic;
            producer = new KafkaProducer<>(properties);
        }

        @Override
        public void run() {
            try {
                for (int i = 1; i <= 10; i++) {
                    String message = String.format("%s,%.3f,%d", "msg" + i, i * 2000f, i);
                    producer.send(new ProducerRecord<String, String>(topic, message));
                    producer.flush();
                    Thread.sleep(100);
                }
            } catch (Throwable throwable) {
                System.out.printf("%s", throwable.fillInStackTrace());
            } finally {
                producer.close();
            }
        }
    }
}
