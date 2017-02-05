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
package org.apache.phoenix.kafka.consumer;

import java.io.IOException;
import java.io.InputStream;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;
import java.util.Random;

import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.event.EventBuilder;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.phoenix.flume.FlumeConstants;
import org.apache.phoenix.flume.serializer.EventSerializer;
import org.apache.phoenix.flume.serializer.EventSerializers;
import org.apache.phoenix.kafka.KafkaConstants;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;
import com.google.common.collect.Lists;

public class PhoenixConsumer {
    private static final Logger logger = LoggerFactory.getLogger(PhoenixConsumer.class);

    private KafkaConsumer<String, String> consumer = null;
    private Properties properties = new Properties();
    private Integer batchSize;
    private long timeout;
    private EventSerializer serializer;
    private Boolean process = true;
    
    public PhoenixConsumer() {

    }
    
    public PhoenixConsumer(Configuration conf) throws IOException {
        // intialize the kafka
        intializeKafka(conf);

        // configure the phoenix
        Context context = prepareContext();
        configure(context);

        // start the kafka consumer
        start();

        // process kafka messages
        process();
    }
    
    /**
     * Initializes the kafka with properties file.
     * @param path
     * @throws IOException 
     */
    public void intializeKafka(Configuration conf) throws IOException {
    	// get the kafka consumer file
    	String file = conf.get("kafka.consumer.file");
        Preconditions.checkNotNull(file,"File path cannot be empty, please specify in the arguments");
        
        Path path = new Path(file);
        FileSystem fs = FileSystem.get(conf);
        try (InputStream props = fs.open(path)) {
            properties.load(props);
        } catch (IOException e) {
            e.printStackTrace();
        }
        
        intializeKafka(properties);
    }
    
    /**
     * Initializes the kafka with properties.
     * @param properties
     */
    public void intializeKafka(Properties properties) {
        this.properties = properties;
        
        String servers = properties.getProperty(KafkaConstants.BOOTSTRAP_SERVERS);
        Preconditions.checkNotNull(servers,"Bootstrap Servers cannot be empty, please specify in the configuration file");
        properties.setProperty(KafkaConstants.BOOTSTRAP_SERVERS, servers);
        
        if (properties.getProperty(KafkaConstants.GROUP_ID) == null) {
            properties.setProperty(KafkaConstants.GROUP_ID, "group-" + new Random().nextInt(100000));
        }
        
        if (properties.getProperty(KafkaConstants.TIMEOUT) == null) {
            properties.setProperty(KafkaConstants.TIMEOUT, String.valueOf(KafkaConstants.DEFAULT_TIMEOUT));
        }

        String topics = properties.getProperty(KafkaConstants.TOPICS);
        Preconditions.checkNotNull(topics,"Topics cannot be empty, please specify in the configuration file");
        
        properties.setProperty(KafkaConstants.KEY_DESERIALIZER, KafkaConstants.DEFAULT_KEY_DESERIALIZER);
        
        properties.setProperty(KafkaConstants.VALUE_DESERIALIZER, KafkaConstants.DEFAULT_VALUE_DESERIALIZER);
        
        this.consumer = new KafkaConsumer<>(properties);
        consumer.subscribe(Arrays.asList(topics.split(",")));
    }
  
    /**
     * Convert the properties to context
     */
    public Context prepareContext() {
        Map<String, String> map = new HashMap<String, String>();
        for (Entry<Object, Object> entry : properties.entrySet()) {
            map.put((String) entry.getKey(), (String) entry.getValue());
        }
        return new Context(map);
    }

    /**
     * Configure the context
     */
    public void configure(Context context){
        this.timeout = context.getLong(KafkaConstants.TIMEOUT, KafkaConstants.DEFAULT_TIMEOUT);
        this.batchSize = context.getInteger(FlumeConstants.CONFIG_BATCHSIZE, FlumeConstants.DEFAULT_BATCH_SIZE);
        final String eventSerializerType = context.getString(FlumeConstants.CONFIG_SERIALIZER);
        
        Preconditions.checkNotNull(eventSerializerType,"Event serializer cannot be empty, please specify in the configuration file");
        initializeSerializer(context,eventSerializerType);
    }
    
    /**
     * Process the kafka messages
     */
    public void process() {
        int timeouts = 0;
        // noinspection InfiniteLoopStatement
        while (process) {
            // read records with a short timeout.
            // If we time out, we don't really care.
            // Assuming only key & value text data
            ConsumerRecords<String, String> records = consumer.poll(this.timeout);
            if (records.count() == 0) {
                timeouts++;
            } else {
                System.out.printf("Got %d records after %d timeouts\n", records.count(), timeouts);
                timeouts = 0;
            }

            if (!records.isEmpty()) {
                List<Event> events = Lists.newArrayListWithCapacity(records.count());
                for (ConsumerRecord<String, String> record : records) {
                    Event event = EventBuilder.withBody(Bytes.toBytes(record.value()));
                    events.add(event);
                }
                // save to Hbase
                try {
                    serializer.upsertEvents(events);
                } catch (SQLException e) {
                    e.printStackTrace();
                }
            }
        }
    }

    /**
     * start the serializer
     */
    public void start() {
        logger.info("Starting consumer {} ", this.getClass());
        try {
            serializer.initialize();
        } catch (Exception ex) {
            logger.error("Error {} in initializing the serializer.", ex.getMessage());
            Throwables.propagate(ex);
        }
    }

    /**
     * stop the consumer & serializer
     */
    public void stop() {
        this.close();
        consumer.close();
        try {
            serializer.close();
        } catch (SQLException e) {
            logger.error(" Error while closing connection {} for consumer.", e.getMessage());
        }
    }
    
    /**
     * make the changes to stop in gracefully
     */
    public void close(){
        this.process = false;
        try {
            Thread.sleep(30000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    /**
     * Initializes the serializer for kafka messages.
     * @param context
     * @param eventSerializerType
     */
    private void initializeSerializer(final Context context, final String eventSerializerType) {
        String serializerClazz = null;
        EventSerializers eventSerializer = null;

        try {
            eventSerializer = EventSerializers.valueOf(eventSerializerType.toUpperCase());
        } catch (IllegalArgumentException iae) {
            serializerClazz = eventSerializerType;
        }

        final Context serializerContext = new Context();
        serializerContext.putAll(context.getSubProperties(FlumeConstants.CONFIG_SERIALIZER_PREFIX));
        copyPropertiesToSerializerContext(context,serializerContext);
        
        try {
            @SuppressWarnings("unchecked")
            Class<? extends EventSerializer> clazz = null;
            if (serializerClazz == null) {
                clazz = (Class<? extends EventSerializer>) Class.forName(eventSerializer.getClassName());
            } else {
                clazz = (Class<? extends EventSerializer>) Class.forName(serializerClazz);
            }

            serializer = clazz.newInstance();
            serializer.configure(serializerContext);
        } catch (Exception e) {
            logger.error("Could not instantiate event serializer.", e);
            Throwables.propagate(e);
        }
    }
    
    /**
     * Copy properties to serializer context.
     * @param context
     * @param serializerContext
     */
    private void copyPropertiesToSerializerContext(Context context, Context serializerContext) {
        serializerContext.put(FlumeConstants.CONFIG_TABLE_DDL,context.getString(FlumeConstants.CONFIG_TABLE_DDL));
        serializerContext.put(FlumeConstants.CONFIG_TABLE,context.getString(FlumeConstants.CONFIG_TABLE));
        serializerContext.put(FlumeConstants.CONFIG_ZK_QUORUM,context.getString(FlumeConstants.CONFIG_ZK_QUORUM));
        serializerContext.put(FlumeConstants.CONFIG_JDBC_URL,context.getString(FlumeConstants.CONFIG_JDBC_URL));
        serializerContext.put(FlumeConstants.CONFIG_BATCHSIZE,context.getString(FlumeConstants.CONFIG_BATCHSIZE));
    }

}
