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
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.apache.flume.Channel;
import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.EventDeliveryException;
import org.apache.flume.Transaction;
import org.apache.flume.channel.MemoryChannel;
import org.apache.flume.conf.Configurables;
import org.apache.flume.event.EventBuilder;
import org.apache.flume.lifecycle.LifecycleState;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.phoenix.end2end.BaseHBaseManagedTimeIT;
import org.apache.phoenix.flume.serializer.EventSerializers;
import org.apache.phoenix.flume.sink.PhoenixSink;
import org.apache.phoenix.util.PropertiesUtil;
import org.junit.Test;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

public class CsvEventSerializerIT extends BaseHBaseManagedTimeIT {

	private Context sinkContext;
	private PhoenixSink sink;

	@Test
	public void testWithDefaultDelimiters() throws EventDeliveryException, SQLException {

		final String fullTableName = "FLUME_CSV_TEST";

		String ddl = "CREATE TABLE IF NOT EXISTS " + fullTableName
				+ "  (flume_time timestamp not null, col1 varchar , col2 double, col3 varchar[], col4 integer[]"
				+ "  CONSTRAINT pk PRIMARY KEY (flume_time))\n";
		String columns = "col1,col2,col3,col4";
		String rowkeyType = DefaultKeyGenerator.TIMESTAMP.name();
		initSinkContext(fullTableName, ddl, columns, null, null, null, null, rowkeyType, null);

		sink = new PhoenixSink();
		Configurables.configure(sink, sinkContext);

		assertEquals(LifecycleState.IDLE, sink.getLifecycleState());

		final Channel channel = this.initChannel();
		sink.setChannel(channel);

		sink.start();

		final String eventBody = "kalyan,10.5,\"abc,pqr,xyz\",\"1,2,3,4\"";
		final Event event = EventBuilder.withBody(Bytes.toBytes(eventBody));
		// put event in channel
		Transaction transaction = channel.getTransaction();
		transaction.begin();
		channel.put(event);
		transaction.commit();
		transaction.close();

		sink.process();

		int rowsInDb = countRows(fullTableName);
		assertEquals(1, rowsInDb);

		sink.stop();
		assertEquals(LifecycleState.STOP, sink.getLifecycleState());

		dropTable(fullTableName);
	}

	@Test
	public void testKeyGenerator() throws EventDeliveryException, SQLException {

		final String fullTableName = "FLUME_CSV_TEST";
		initSinkContextWithDefaults(fullTableName);

		sink = new PhoenixSink();
		Configurables.configure(sink, sinkContext);

		assertEquals(LifecycleState.IDLE, sink.getLifecycleState());

		final Channel channel = this.initChannel();
		sink.setChannel(channel);

		sink.start();
		final String eventBody = "kalyan,10.5,\"abc,pqr,xyz\",\"1,2,3,4\"";
		final Event event = EventBuilder.withBody(Bytes.toBytes(eventBody));
		// put event in channel
		Transaction transaction = channel.getTransaction();
		transaction.begin();
		channel.put(event);
		transaction.commit();
		transaction.close();

		sink.process();

		int rowsInDb = countRows(fullTableName);
		assertEquals(1, rowsInDb);

		sink.stop();
		assertEquals(LifecycleState.STOP, sink.getLifecycleState());

		dropTable(fullTableName);
	}

	@Test
	public void testMismatchKeyGenerator() throws EventDeliveryException, SQLException {

		final String fullTableName = "FLUME_CSV_TEST";
		initSinkContextWithDefaults(fullTableName);
		setConfig(FlumeConstants.CONFIG_SERIALIZER_PREFIX + FlumeConstants.CONFIG_ROWKEY_TYPE_GENERATOR,
				DefaultKeyGenerator.UUID.name());

		sink = new PhoenixSink();
		Configurables.configure(sink, sinkContext);
		assertEquals(LifecycleState.IDLE, sink.getLifecycleState());

		final Channel channel = this.initChannel();
		sink.setChannel(channel);

		sink.start();
		final String eventBody = "kalyan,10.5,\"abc,pqr,xyz\",\"1,2,3,4\"";
		final Event event = EventBuilder.withBody(Bytes.toBytes(eventBody));
		// put event in channel
		Transaction transaction = channel.getTransaction();
		transaction.begin();
		channel.put(event);
		transaction.commit();
		transaction.close();

		try {
			sink.process();
			fail();
		} catch (Exception ex) {
			assertTrue(ex.getCause().getMessage().contains("java.lang.IllegalArgumentException: Invalid format:"));
		}

		dropTable(fullTableName);
	}

	@Test
	public void testMissingColumnsInEvent() throws EventDeliveryException, SQLException {

		final String fullTableName = "FLUME_CSV_TEST";
		initSinkContextWithDefaults(fullTableName);

		sink = new PhoenixSink();
		Configurables.configure(sink, sinkContext);
		assertEquals(LifecycleState.IDLE, sink.getLifecycleState());

		final Channel channel = this.initChannel();
		sink.setChannel(channel);

		sink.start();
		final String eventBody = "kalyan,\"abc,pqr,xyz\",\"1,2,3,4\"";
		final Event event = EventBuilder.withBody(Bytes.toBytes(eventBody));
		// put event in channel
		Transaction transaction = channel.getTransaction();
		transaction.begin();
		channel.put(event);
		transaction.commit();
		transaction.close();

		sink.process();

		int rowsInDb = countRows(fullTableName);
		assertEquals(0, rowsInDb);

		sink.stop();
		assertEquals(LifecycleState.STOP, sink.getLifecycleState());

		dropTable(fullTableName);
	}

	@Test
	public void testBatchEvents() throws EventDeliveryException, SQLException {

		final String fullTableName = "FLUME_CSV_TEST";
		initSinkContextWithDefaults(fullTableName);

		sink = new PhoenixSink();
		Configurables.configure(sink, sinkContext);
		assertEquals(LifecycleState.IDLE, sink.getLifecycleState());

		final Channel channel = this.initChannel();
		sink.setChannel(channel);

		sink.start();
		int numEvents = 150;
		String col1 = "val1";
		String a1 = "\"aaa,bbb,ccc\"";
		String a2 = "\"1,2,3,4\"";
		String eventBody = null;
		List<Event> eventList = Lists.newArrayListWithCapacity(numEvents);
		for (int i = 0; i < eventList.size(); i++) {
			eventBody = (col1 + i) + "," + i * 10.5 + "," + a1 + "," + a2;
			Event event = EventBuilder.withBody(Bytes.toBytes(eventBody));
			eventList.add(event);
		}

		// put event in channel
		Transaction transaction = channel.getTransaction();
		transaction.begin();
		for (Event event : eventList) {
			channel.put(event);
		}
		transaction.commit();
		transaction.close();

		sink.process();

		int rowsInDb = countRows(fullTableName);
		assertEquals(eventList.size(), rowsInDb);

		sink.stop();
		assertEquals(LifecycleState.STOP, sink.getLifecycleState());

		dropTable(fullTableName);
	}

	@Test
	public void testEventsWithHeaders() throws Exception {

		sinkContext = new Context();
		final String fullTableName = "FLUME_CSV_TEST";
		final String ddl = "CREATE TABLE IF NOT EXISTS "
				+ fullTableName
				+ "  (rowkey VARCHAR not null, col1 varchar , col2 double, col3 varchar[], col4 integer[], host varchar , source varchar \n"
				+ "  CONSTRAINT pk PRIMARY KEY (rowkey))\n";
		String columns = "col1,col2,col3,col4";
		String rowkeyType = DefaultKeyGenerator.UUID.name();
		String headers = "host,source";
		initSinkContext(fullTableName, ddl, columns, null, null, null, null, rowkeyType, headers);

		sink = new PhoenixSink();
		Configurables.configure(sink, sinkContext);
		assertEquals(LifecycleState.IDLE, sink.getLifecycleState());

		final Channel channel = this.initChannel();
		sink.setChannel(channel);

		sink.start();

		int numEvents = 10;
		String col1 = "val1";
		String a1 = "\"aaa,bbb,ccc\"";
		String a2 = "\"1,2,3,4\"";
		String hostHeader = "host1";
		String sourceHeader = "source1";
		String eventBody = null;
		List<Event> eventList = Lists.newArrayListWithCapacity(numEvents);
		for (int i = 0; i < numEvents; i++) {
			eventBody = (col1 + i) + "," + i * 10.5 + "," + a1 + "," + a2;
			Map<String, String> headerMap = Maps.newHashMapWithExpectedSize(2);
			headerMap.put("host", hostHeader);
			headerMap.put("source", sourceHeader);
			Event event = EventBuilder.withBody(Bytes.toBytes(eventBody), headerMap);
			eventList.add(event);
		}

		// put event in channel
		Transaction transaction = channel.getTransaction();
		transaction.begin();
		for (Event event : eventList) {
			channel.put(event);
		}
		transaction.commit();
		transaction.close();

		sink.process();

		final String query = " SELECT * FROM \n " + fullTableName;
		Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
		final ResultSet rs;
		final Connection conn = DriverManager.getConnection(getUrl(), props);
		try {
			rs = conn.createStatement().executeQuery(query);
			assertTrue(rs.next());
			assertEquals("host1", rs.getString("host"));
			assertEquals("source1", rs.getString("source"));

			assertTrue(rs.next());
			assertEquals("host1", rs.getString("host"));
			assertEquals("source1", rs.getString("source"));
		} finally {
			if (conn != null) {
				conn.close();
			}
		}
		sink.stop();
		assertEquals(LifecycleState.STOP, sink.getLifecycleState());

		dropTable(fullTableName);
	}

	private Channel initChannel() {
		// Channel configuration
		Context channelContext = new Context();
		channelContext.put("capacity", "10000");
		channelContext.put("transactionCapacity", "200");

		Channel channel = new MemoryChannel();
		channel.setName("memorychannel");
		Configurables.configure(channel, channelContext);
		return channel;
	}

	private void initSinkContext(final String fullTableName, final String ddl, final String columns,
			final String csvDelimiter, final String csvQuote, final String csvEscape, final String csvArrayDelimiter,
			final String rowkeyType, final String headers) {
		Preconditions.checkNotNull(fullTableName);
		sinkContext = new Context();
		sinkContext.put(FlumeConstants.CONFIG_TABLE, fullTableName);
		sinkContext.put(FlumeConstants.CONFIG_JDBC_URL, getUrl());
		sinkContext.put(FlumeConstants.CONFIG_SERIALIZER, EventSerializers.CSV.name());
		sinkContext.put(FlumeConstants.CONFIG_TABLE_DDL, ddl);
		sinkContext.put(FlumeConstants.CONFIG_SERIALIZER_PREFIX + FlumeConstants.CONFIG_COLUMN_NAMES, columns);
		if (null != csvDelimiter)
			sinkContext.put(FlumeConstants.CONFIG_SERIALIZER_PREFIX + FlumeConstants.CSV_DELIMITER, csvDelimiter);
		if (null != csvQuote)
			sinkContext.put(FlumeConstants.CONFIG_SERIALIZER_PREFIX + FlumeConstants.CSV_QUOTE, csvQuote);
		if (null != csvEscape)
			sinkContext.put(FlumeConstants.CONFIG_SERIALIZER_PREFIX + FlumeConstants.CSV_ESCAPE, csvEscape);
		if (null != csvArrayDelimiter)
			sinkContext.put(FlumeConstants.CONFIG_SERIALIZER_PREFIX + FlumeConstants.CSV_ARRAY_DELIMITER,
					csvArrayDelimiter);
		if (null != rowkeyType)
			sinkContext.put(FlumeConstants.CONFIG_SERIALIZER_PREFIX + FlumeConstants.CONFIG_ROWKEY_TYPE_GENERATOR,
					rowkeyType);
		if (null != headers)
			sinkContext.put(FlumeConstants.CONFIG_SERIALIZER_PREFIX + FlumeConstants.CONFIG_HEADER_NAMES, headers);
	}

	private void initSinkContextWithDefaults(final String fullTableName) {
		String ddl = "CREATE TABLE IF NOT EXISTS " + fullTableName
				+ "  (flume_time timestamp not null, col1 varchar , col2 double, col3 varchar[], col4 integer[]"
				+ "  CONSTRAINT pk PRIMARY KEY (flume_time))\n";
		String columns = "col1,col2,col3,col4";
		String rowkeyType = DefaultKeyGenerator.TIMESTAMP.name();
		initSinkContext(fullTableName, ddl, columns, null, null, null, null, rowkeyType, null);
	}

	private void setConfig(final String configName, final String configValue) {
		Preconditions.checkNotNull(sinkContext);
		Preconditions.checkNotNull(configName);
		Preconditions.checkNotNull(configValue);
		sinkContext.put(configName, configValue);
	}

	private int countRows(final String fullTableName) throws SQLException {
		Preconditions.checkNotNull(fullTableName);
		Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
		final Connection conn = DriverManager.getConnection(getUrl(), props);
		ResultSet rs = null;
		try {
			rs = conn.createStatement().executeQuery("select count(*) from " + fullTableName);
			int rowsCount = 0;
			while (rs.next()) {
				rowsCount = rs.getInt(1);
			}
			return rowsCount;

		} finally {
			if (rs != null) {
				rs.close();
			}
			if (conn != null) {
				conn.close();
			}
		}

	}

	private void dropTable(final String fullTableName) throws SQLException {
		Preconditions.checkNotNull(fullTableName);
		Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
		final Connection conn = DriverManager.getConnection(getUrl(), props);
		try {
			conn.createStatement().execute("drop table if exists " + fullTableName);
		} finally {
			if (conn != null) {
				conn.close();
			}
		}
	}

}
