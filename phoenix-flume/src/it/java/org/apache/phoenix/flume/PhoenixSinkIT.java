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
package org.apache.phoenix.flume;

import static org.apache.phoenix.util.TestUtil.TEST_PROPERTIES;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.util.Properties;

import org.apache.flume.Channel;
import org.apache.flume.Context;
import org.apache.flume.Sink;
import org.apache.flume.SinkFactory;
import org.apache.flume.Transaction;
import org.apache.flume.channel.MemoryChannel;
import org.apache.flume.conf.Configurables;
import org.apache.flume.event.EventBuilder;
import org.apache.flume.lifecycle.LifecycleState;
import org.apache.flume.sink.DefaultSinkFactory;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.phoenix.end2end.BaseHBaseManagedTimeIT;
import org.apache.phoenix.flume.serializer.CustomSerializer;
import org.apache.phoenix.flume.serializer.EventSerializers;
import org.apache.phoenix.flume.sink.NullPhoenixSink;
import org.apache.phoenix.flume.sink.PhoenixSink;
import org.apache.phoenix.util.PropertiesUtil;
import org.apache.phoenix.util.TestUtil;
import org.junit.Assert;
import org.junit.Test;


public class PhoenixSinkIT extends BaseHBaseManagedTimeIT {

    private Context sinkContext;
    private PhoenixSink sink;
   
   
    @Test
    public void testSinkCreation() {
        SinkFactory factory = new DefaultSinkFactory ();
        Sink sink = factory.create("PhoenixSink__", "org.apache.phoenix.flume.sink.PhoenixSink");
        Assert.assertNotNull(sink);
        Assert.assertTrue(PhoenixSink.class.isInstance(sink));
    }
    @Test
    public void testConfiguration () {
        
        sinkContext = new Context ();
        sinkContext.put(FlumeConstants.CONFIG_TABLE, "test");
        sinkContext.put(FlumeConstants.CONFIG_JDBC_URL, getUrl());
        sinkContext.put(FlumeConstants.CONFIG_SERIALIZER,EventSerializers.REGEX.name());
        sinkContext.put(FlumeConstants.CONFIG_SERIALIZER_PREFIX + FlumeConstants.CONFIG_COLUMN_NAMES,"col1,col2");
        sinkContext.put(FlumeConstants.CONFIG_SERIALIZER_PREFIX + FlumeConstants.CONFIG_ROWKEY_TYPE_GENERATOR,DefaultKeyGenerator.TIMESTAMP.name());

        sink = new PhoenixSink();
        Configurables.configure(sink, sinkContext);
    }
    
    
    
    @Test(expected= NullPointerException.class)
    public void testInvalidConfiguration () {
        
        sinkContext = new Context ();
        sinkContext.put(FlumeConstants.CONFIG_JDBC_URL, getUrl());
        sinkContext.put(FlumeConstants.CONFIG_SERIALIZER,EventSerializers.REGEX.name());
        sinkContext.put(FlumeConstants.CONFIG_SERIALIZER_PREFIX + FlumeConstants.CONFIG_COLUMN_NAMES,"col1,col2");
        sinkContext.put(FlumeConstants.CONFIG_SERIALIZER_PREFIX + FlumeConstants.CONFIG_ROWKEY_TYPE_GENERATOR,DefaultKeyGenerator.TIMESTAMP.name());

        sink = new PhoenixSink();
        Configurables.configure(sink, sinkContext);
    }
    
    @Test(expected=RuntimeException.class)
    public void testInvalidConfigurationOfSerializer () {
        
        sinkContext = new Context ();
        sinkContext.put(FlumeConstants.CONFIG_TABLE, "test");
        sinkContext.put(FlumeConstants.CONFIG_JDBC_URL, getUrl());
        sinkContext.put(FlumeConstants.CONFIG_SERIALIZER,"unknown");
        sinkContext.put(FlumeConstants.CONFIG_SERIALIZER_PREFIX + FlumeConstants.CONFIG_COLUMN_NAMES,"col1,col2");
        sinkContext.put(FlumeConstants.CONFIG_SERIALIZER_PREFIX + FlumeConstants.CONFIG_ROWKEY_TYPE_GENERATOR,DefaultKeyGenerator.TIMESTAMP.name());

        sink = new PhoenixSink();
        Configurables.configure(sink, sinkContext);
    }
    
    @Test
    public void testInvalidTable() {
        sinkContext = new Context ();
        sinkContext.put(FlumeConstants.CONFIG_TABLE, "flume_test");
        sinkContext.put(FlumeConstants.CONFIG_JDBC_URL, getUrl());
        sinkContext.put(FlumeConstants.CONFIG_SERIALIZER, EventSerializers.REGEX.name());
        sinkContext.put(FlumeConstants.CONFIG_SERIALIZER_PREFIX + FlumeConstants.CONFIG_COLUMN_NAMES, "col1,col2");
        sinkContext.put(FlumeConstants.CONFIG_SERIALIZER_PREFIX + FlumeConstants.CONFIG_ROWKEY_TYPE_GENERATOR,DefaultKeyGenerator.TIMESTAMP.name());

        sink = new PhoenixSink();
        Configurables.configure(sink, sinkContext);

        final Channel channel = this.initChannel();
        sink.setChannel(channel);
        try {
            sink.start();
            fail();
        }catch(Exception e) {
            assertTrue(e.getMessage(), e.getMessage().contains("ERROR 1012 (42M03): Table undefined."));
        }
    }
    
    @Test
    public void testSinkLifecycle () {
        String tableName = generateUniqueName();

        String ddl = "CREATE TABLE " + tableName +
                "  (flume_time timestamp not null, col1 varchar , col2 varchar" +
                "  CONSTRAINT pk PRIMARY KEY (flume_time))\n";
        
        sinkContext = new Context ();
        sinkContext.put(FlumeConstants.CONFIG_TABLE,  tableName);
        sinkContext.put(FlumeConstants.CONFIG_JDBC_URL, getUrl());
        sinkContext.put(FlumeConstants.CONFIG_SERIALIZER,EventSerializers.REGEX.name());
        sinkContext.put(FlumeConstants.CONFIG_TABLE_DDL, ddl);
        sinkContext.put(FlumeConstants.CONFIG_SERIALIZER_PREFIX + FlumeConstants.CONFIG_REGULAR_EXPRESSION,"^([^\t]+)\t([^\t]+)$");
        sinkContext.put(FlumeConstants.CONFIG_SERIALIZER_PREFIX + FlumeConstants.CONFIG_COLUMN_NAMES,"col1,col2");
        sinkContext.put(FlumeConstants.CONFIG_SERIALIZER_PREFIX + FlumeConstants.CONFIG_ROWKEY_TYPE_GENERATOR,DefaultKeyGenerator.TIMESTAMP.name());


        sink = new PhoenixSink();
        Configurables.configure(sink, sinkContext);
        Assert.assertEquals(LifecycleState.IDLE, sink.getLifecycleState());

        final Channel channel = this.initChannel();
        sink.setChannel(channel);

        sink.start();
        Assert.assertEquals(LifecycleState.START, sink.getLifecycleState());
        sink.stop();
        Assert.assertEquals(LifecycleState.STOP, sink.getLifecycleState());
    }
    
    @Test
    public void testCreateTable () throws Exception {
        String tableName = generateUniqueName();
        String ddl = "CREATE TABLE " + tableName + " " +
                "  (flume_time timestamp not null, col1 varchar , col2 varchar" +
                "  CONSTRAINT pk PRIMARY KEY (flume_time))\n";

        final String fullTableName =  tableName;
        sinkContext = new Context ();
        sinkContext.put(FlumeConstants.CONFIG_TABLE, fullTableName);
        sinkContext.put(FlumeConstants.CONFIG_JDBC_URL, getUrl());
        sinkContext.put(FlumeConstants.CONFIG_SERIALIZER,EventSerializers.REGEX.name());
        sinkContext.put(FlumeConstants.CONFIG_TABLE_DDL, ddl);
        sinkContext.put(FlumeConstants.CONFIG_SERIALIZER_PREFIX + FlumeConstants.CONFIG_REGULAR_EXPRESSION,"^([^\t]+)\t([^\t]+)$");
        sinkContext.put(FlumeConstants.CONFIG_SERIALIZER_PREFIX + FlumeConstants.CONFIG_COLUMN_NAMES,"col1,col2");
        sinkContext.put(FlumeConstants.CONFIG_SERIALIZER_PREFIX + FlumeConstants.CONFIG_ROWKEY_TYPE_GENERATOR, DefaultKeyGenerator.TIMESTAMP.name());


        sink = new PhoenixSink();
        Configurables.configure(sink, sinkContext);
        Assert.assertEquals(LifecycleState.IDLE, sink.getLifecycleState());

        final Channel channel = this.initChannel();
        sink.setChannel(channel);
        
        sink.start();
        HBaseAdmin admin = driver.getConnectionQueryServices(getUrl(), TestUtil.TEST_PROPERTIES).getAdmin();
        try {
            boolean exists = admin.tableExists(fullTableName);
            Assert.assertTrue(exists);
        }finally {
            admin.close();
        }
    }

    @Test
    public void testExtendedSink() throws Exception {
        // Create a mock NullPhoenixSink which extends PhoenixSink, and verify configure is invoked()

        PhoenixSink sink = mock(NullPhoenixSink.class);
        sinkContext = new Context();
        sinkContext.put(FlumeConstants.CONFIG_TABLE, "FLUME_TEST_EXTENDED");
        sinkContext.put(FlumeConstants.CONFIG_JDBC_URL, getUrl());
        sinkContext.put(FlumeConstants.CONFIG_SERIALIZER, CustomSerializer.class.getName());
        sinkContext.put(FlumeConstants.CONFIG_SERIALIZER_PREFIX + FlumeConstants.CONFIG_COLUMN_NAMES, "ID, COUNTS");
        sinkContext.put(FlumeConstants.CONFIG_SERIALIZER_PREFIX + FlumeConstants.CONFIG_ROWKEY_TYPE_GENERATOR, DefaultKeyGenerator.TIMESTAMP.name());

        Configurables.configure(sink, sinkContext);
        verify(sink).configure(sinkContext);
    }

    @Test
    public void testExtendedSerializer() throws Exception {
        /*
        Sadly, we can't mock a serializer, as the PhoenixSink does a Class.forName() to instantiate
        it. Instead. we'll setup a Flume channel and verify the data our custom serializer wrote.
        */

        final String fullTableName = "FLUME_TEST_EXTENDED";
        final String ddl = "CREATE TABLE " + fullTableName + " (ID BIGINT NOT NULL PRIMARY KEY, COUNTS UNSIGNED_LONG)";
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        final Connection conn = DriverManager.getConnection(getUrl(), props);
        conn.createStatement().execute(ddl);
        conn.commit();

        sinkContext = new Context();
        sinkContext.put(FlumeConstants.CONFIG_TABLE, "FLUME_TEST_EXTENDED");
        sinkContext.put(FlumeConstants.CONFIG_JDBC_URL, getUrl());
        sinkContext.put(FlumeConstants.CONFIG_SERIALIZER, CustomSerializer.class.getName());
        sinkContext.put(FlumeConstants.CONFIG_SERIALIZER_PREFIX + FlumeConstants.CONFIG_COLUMN_NAMES, "ID, COUNTS");
        sinkContext.put(FlumeConstants.CONFIG_SERIALIZER_PREFIX + FlumeConstants.CONFIG_ROWKEY_TYPE_GENERATOR, DefaultKeyGenerator.TIMESTAMP.name());

        PhoenixSink sink = new PhoenixSink();
        Configurables.configure(sink, sinkContext);

        // Send a test event through Flume, using our custom serializer
        final Channel channel = this.initChannel();
        sink.setChannel(channel);
        sink.start();

        final Transaction transaction = channel.getTransaction();
        transaction.begin();
        channel.put(EventBuilder.withBody(Bytes.toBytes("test event")));
        transaction.commit();
        transaction.close();

        sink.process();
        sink.stop();

        // Verify our serializer wrote out data
        ResultSet rs = conn.createStatement().executeQuery("SELECT * FROM FLUME_TEST_EXTENDED");
        assertTrue(rs.next());
        assertTrue(rs.getLong(1) == 1L);
    }
    
    private Channel initChannel() {
        //Channel configuration
        Context channelContext = new Context();
        channelContext.put("capacity", "10000");
        channelContext.put("transactionCapacity", "200");

        Channel channel = new MemoryChannel();
        channel.setName("memorychannel");
        Configurables.configure(channel, channelContext);
        return channel;
    }
    
    
}
