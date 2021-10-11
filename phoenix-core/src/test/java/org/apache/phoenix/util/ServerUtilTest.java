/**
 *  Licensed to the Apache Software Foundation (ASF) under one or more
 *  contributor license agreements.  See the NOTICE file distributed with
 *  this work for additional information regarding copyright ownership.
 *  The ASF licenses this file to You under the Apache License, Version 2.0
 *  (the "License"); you may not use this file except in compliance with
 *  the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */
package org.apache.phoenix.util;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.DoNotRetryIOException;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.regionserver.HRegionServer;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.phoenix.hbase.index.table.HTableFactory;
import org.apache.phoenix.hbase.index.util.ImmutableBytesPtr;
import org.apache.phoenix.hbase.index.write.IndexWriterUtils;
import org.apache.phoenix.query.HBaseFactoryProvider;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;

import java.io.IOException;
import java.util.concurrent.ExecutorService;

public class ServerUtilTest {

    @Test
    public void testCoprocessorHConnectionGetTableWithClosedConnection() throws Exception {
        // Mock Connection object to throw IllegalArgumentException.
        Connection connection = Mockito.mock(Connection.class);
        Mockito.doThrow(new IllegalArgumentException()).when(connection).getTable(Mockito.any());
        Mockito.doThrow(new IllegalArgumentException()).when(connection).getTable(
            Mockito.any(), Mockito.<ExecutorService>any());
        Mockito.doReturn(true).when(connection).isClosed();

        // Spy CoprocessorHConnectionTableFactory
        RegionCoprocessorEnvironment mockEnv = Mockito.mock(RegionCoprocessorEnvironment.class);
        HTableFactory hTableFactory =  IndexWriterUtils.getDefaultDelegateHTableFactory(mockEnv);
        IndexWriterUtils.CoprocessorHConnectionTableFactory spyedObj = (IndexWriterUtils.
            CoprocessorHConnectionTableFactory)Mockito.spy(hTableFactory);
        Mockito.doReturn(connection).when(spyedObj).getConnection();

        try {
            spyedObj.getTable(new ImmutableBytesPtr(Bytes.toBytes("test_table")));
            Assert.fail("IOException exception expected as connection was closed");
        } catch(DoNotRetryIOException e) {
            Assert.fail("DoNotRetryIOException not expected instead should throw IOException");
        }catch (IOException e1) {
            try {
                spyedObj.getTable(new ImmutableBytesPtr(Bytes.toBytes("test_table")), null);
                Assert.fail("IOException exception expected as connection was closed");
            } catch (IOException e2) {
                // IO Exception is expected. Should fail is any other exception.
            }
        }
    }
}
