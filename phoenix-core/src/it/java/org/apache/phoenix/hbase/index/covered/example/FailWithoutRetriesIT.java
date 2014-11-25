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
package org.apache.phoenix.hbase.index.covered.example;

import static org.apache.phoenix.query.BaseTest.setUpConfigForMiniCluster;
import static org.junit.Assert.fail;

import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.DoNotRetryIOException;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.RetriesExhaustedWithDetailsException;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.phoenix.end2end.NeedsOwnMiniClusterTest;
import org.apache.phoenix.hbase.index.IndexTestingUtils;
import org.apache.phoenix.hbase.index.Indexer;
import org.apache.phoenix.hbase.index.TableName;
import org.apache.phoenix.hbase.index.covered.IndexUpdate;
import org.apache.phoenix.hbase.index.covered.TableState;
import org.apache.phoenix.hbase.index.util.IndexManagementUtil;
import org.apache.phoenix.index.BaseIndexCodec;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;


/**
 * If {@link DoNotRetryIOException} is not subclassed correctly (with the {@link String}
 * constructor), {@link MultiResponse#readFields(java.io.DataInput)} will not correctly deserialize
 * the exception, and just return <tt>null</tt> to the client, which then just goes and retries.
 */
@Category(NeedsOwnMiniClusterTest.class)
public class FailWithoutRetriesIT {

  private static final Log LOG = LogFactory.getLog(FailWithoutRetriesIT.class);
  @Rule
  public TableName table = new TableName();

  private static final HBaseTestingUtility UTIL = new HBaseTestingUtility();

  private String getIndexTableName() {
    return Bytes.toString(table.getTableName()) + "_index";
  }

  public static class FailingTestCodec extends BaseIndexCodec {

    @Override
    public Iterable<IndexUpdate> getIndexDeletes(TableState state) throws IOException {
      throw new RuntimeException("Intentionally failing deletes for "
          + FailWithoutRetriesIT.class.getName());
    }

    @Override
    public Iterable<IndexUpdate> getIndexUpserts(TableState state) throws IOException {
      throw new RuntimeException("Intentionally failing upserts for "
          + FailWithoutRetriesIT.class.getName());
    }

  }

  @BeforeClass
  public static void setupCluster() throws Exception {
    // setup and verify the config
    Configuration conf = UTIL.getConfiguration();
    setUpConfigForMiniCluster(conf);
    IndexTestingUtils.setupConfig(conf);
    IndexManagementUtil.ensureMutableIndexingCorrectlyConfigured(conf);
    // start the cluster
    UTIL.startMiniCluster();
  }

  @AfterClass
  public static void teardownCluster() throws Exception {
    UTIL.shutdownMiniCluster();
  }

  /**
   * If this test times out, then we didn't fail quickly enough. {@link Indexer} maybe isn't
   * rethrowing the exception correctly?
   * <p>
   * We use a custom codec to enforce the thrown exception.
   * @throws Exception
   */
  @Test(timeout = 300000)
  public void testQuickFailure() throws Exception {
    // incorrectly setup indexing for the primary table - target index table doesn't exist, which
    // should quickly return to the client
    byte[] family = Bytes.toBytes("family");
    ColumnGroup fam1 = new ColumnGroup(getIndexTableName());
    // values are [col1]
    fam1.add(new CoveredColumn(family, CoveredColumn.ALL_QUALIFIERS));
    CoveredColumnIndexSpecifierBuilder builder = new CoveredColumnIndexSpecifierBuilder();
    // add the index family
    builder.addIndexGroup(fam1);
    // usually, we would create the index table here, but we don't for the sake of the test.

    // setup the primary table
    String primaryTable = Bytes.toString(table.getTableName());
    @SuppressWarnings("deprecation")
    HTableDescriptor pTable = new HTableDescriptor(primaryTable);
    pTable.addFamily(new HColumnDescriptor(family));
    // override the codec so we can use our test one
    builder.build(pTable, FailingTestCodec.class);

    // create the primary table
    HBaseAdmin admin = UTIL.getHBaseAdmin();
    admin.createTable(pTable);
    Configuration conf = new Configuration(UTIL.getConfiguration());
    // up the number of retries/wait time to make it obvious that we are failing with retries here
    conf.setInt(HConstants.HBASE_CLIENT_RETRIES_NUMBER, 20);
    conf.setLong(HConstants.HBASE_CLIENT_PAUSE, 1000);
    HTable primary = new HTable(conf, primaryTable);
    primary.setAutoFlush(false, true);

    // do a simple put that should be indexed
    Put p = new Put(Bytes.toBytes("row"));
    p.add(family, null, Bytes.toBytes("value"));
    primary.put(p);
    try {
      primary.flushCommits();
      fail("Shouldn't have gotten a successful write to the primary table");
    } catch (RetriesExhaustedWithDetailsException e) {
      LOG.info("Correclty got a failure of the put!");
    }
    primary.close();
  }
}