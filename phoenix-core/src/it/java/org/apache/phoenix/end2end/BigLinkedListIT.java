/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.phoenix.end2end;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.phoenix.query.QueryServices;
import org.apache.phoenix.query.QueryServicesOptions;
import org.apache.phoenix.util.ReadOnlyProps;

import org.junit.BeforeClass;
import org.junit.Test;

import com.google.common.collect.Maps;

/**
 * This is an integration test borrowed from goraci, written by Keith Turner,
 * which is in turn inspired by the Accumulo test called continous ingest (ci).
 * The original source code can be found here:
 * https://github.com/keith-turner/goraci
 * https://github.com/enis/goraci/
 *
 * Apache Accumulo [0] has a simple test suite that verifies that data is not
 * lost at scale. This test suite is called continuous ingest. This test runs
 * many ingest clients that continually create linked lists containing 25
 * million nodes. At some point the clients are stopped and a map reduce job is
 * run to ensure no linked list has a hole. A hole indicates data was lost.··
 *
 * The nodes in the linked list are random. This causes each linked list to
 * spread across the table. Therefore if one part of a table loses data, then it
 * will be detected by references in another part of the table.
 *
 * THE ANATOMY OF THE TEST
 *
 * Below is rough sketch of how data is written. For specific details look at
 * the Generator code.
 *
 * 1 Write out 1 million nodes· 2 Flush the client· 3 Write out 1 million that
 * reference previous million· 4 If this is the 25th set of 1 million nodes,
 * then update 1st set of million to point to last· 5 goto 1
 *
 * The key is that nodes only reference flushed nodes. Therefore a node should
 * never reference a missing node, even if the ingest client is killed at any
 * point in time.
 *
 * When running this test suite w/ Accumulo there is a script running in
 * parallel called the Aggitator that randomly and continuously kills server
 * processes.·· The outcome was that many data loss bugs were found in Accumulo
 * by doing this.· This test suite can also help find bugs that impact uptime
 * and stability when· run for days or weeks.··
 *
 * When generating data, its best to have each map task generate a multiple of
 * 25 million. The reason for this is that circular linked list are generated
 * every 25M. Not generating a multiple in 25M will result in some nodes in the
 * linked list not having references. The loss of an unreferenced node can not
 * be detected.
 *
 * Some ASCII art time:
 * <p>
 * [ . . . ] represents one batch of random longs of length WIDTH
 * <pre>
 *                _________________________
 *               |                  ______ |
 *               |                 |      ||
 *             .-+-----------------+-----.||
 *             | |                 |     |||
 * first   = [ . . . . . . . . . . . ]   |||
 *             ^ ^ ^ ^ ^ ^ ^ ^ ^ ^ ^     |||
 *             | | | | | | | | | | |     |||
 * prev    = [ . . . . . . . . . . . ]   |||
 *             ^ ^ ^ ^ ^ ^ ^ ^ ^ ^ ^     |||
 *             | | | | | | | | | | |     |||
 * current = [ . . . . . . . . . . . ]   |||
 *                                       |||
 * ...                                   |||
 *                                       |||
 * last    = [ . . . . . . . . . . . ]   |||
 *             ^ ^ ^ ^ ^ ^ ^ ^ ^ ^ ^_____|||
 *             |                 |________||
 *             |___________________________|
 * </pre>
 */
public class BigLinkedListIT extends BaseOwnClusterIT {

  static final Log LOG = LogFactory.getLog(BigLinkedListIT.class);

  protected static String TABLE_NAME_KEY = "BigLinkedListIT.table";
  protected static String DEFAULT_TABLE_NAME = "BigLinkedListIT";

  protected static final String NUM_ITERATIONS_KEY = "BigLinkedListIT.iterations";
  protected static final int DEFAULT_NUM_ITERATIONS = 1;

  protected static final String NUM_MAPPERS_KEY = "BigLinkedListIT.map.tasks";
  protected static final int DEFAULT_NUM_MAPPERS = 1;

  protected static final String NUM_REDUCERS_KEY = "BigLinkedListIT.reduce.tasks";
  protected static final int DEFAULT_NUM_REDUCERS = 1;

  protected static final String OUTPUT_DIR_KEY = "BigLinkedListIT.output.dir";
  protected static final String DEFAULT_OUTPUT_DIR = "/tmp/BigLinkedListIT";

  /** How many rows to write per map task. This has to be a multiple of 25M */
  protected static final String GENERATOR_NUM_ROWS_PER_MAP_KEY =
      "BigLinkedListIT.generator.num_rows";
  protected static final long DEFAULT_GENERATOR_NUM_ROWS_PER_MAP = 25000000;

  protected static final String GENERATOR_WIDTH_KEY = "BigLinkedListIT.generator.width";
  protected static final int DEFAULT_GENERATOR_WIDTH = 1000000;

  protected static final String GENERATOR_WRAP_KEY = "BigLinkedListIT.generator.wrap";
  protected static final int DEFAULT_GENERATOR_WRAP = 25;

  public static Map<String, String> getServerProperties() {
    Map<String, String> serverProps = Maps.newHashMap();
    serverProps.put(QueryServices.EXTRA_JDBC_ARGUMENTS_ATTRIB,
      QueryServicesOptions.DEFAULT_EXTRA_JDBC_ARGUMENTS);
    // TODO: Configuration properties for site conf go here
    return serverProps;
  }

  public static Map<String, String> getTestProperties() {
    Map<String, String> testProps = Maps.newHashMap();
    // Global properties
    testProps.put(TABLE_NAME_KEY, System.getProperty(TABLE_NAME_KEY, DEFAULT_TABLE_NAME));
    testProps.put(NUM_ITERATIONS_KEY, System.getProperty(NUM_ITERATIONS_KEY,
      Integer.toString(DEFAULT_NUM_ITERATIONS)));
    testProps.put(NUM_MAPPERS_KEY, System.getProperty(NUM_MAPPERS_KEY,
      Integer.toString(DEFAULT_NUM_MAPPERS)));
    testProps.put(NUM_REDUCERS_KEY, System.getProperty(NUM_REDUCERS_KEY,
      Integer.toString(DEFAULT_NUM_REDUCERS)));
    testProps.put(OUTPUT_DIR_KEY, System.getProperty(OUTPUT_DIR_KEY, DEFAULT_OUTPUT_DIR));
    // Generator properties
    testProps.put(GENERATOR_NUM_ROWS_PER_MAP_KEY,
      System.getProperty(GENERATOR_NUM_ROWS_PER_MAP_KEY,
        Long.toString(DEFAULT_GENERATOR_NUM_ROWS_PER_MAP)));
    testProps.put(GENERATOR_WIDTH_KEY, System.getProperty(GENERATOR_WIDTH_KEY,
      Integer.toString(DEFAULT_GENERATOR_WIDTH)));
    testProps.put(GENERATOR_WRAP_KEY, System.getProperty(GENERATOR_WRAP_KEY,
      Integer.toString(DEFAULT_GENERATOR_WRAP)));
    return testProps;
  }

  static String getTableName(ReadOnlyProps props) {
    return props.get(TABLE_NAME_KEY, DEFAULT_TABLE_NAME);
  }

  @BeforeClass
  public static void doSetup() throws Exception {
    setUpTestDriver(new ReadOnlyProps(getServerProperties().entrySet().iterator()),
      ReadOnlyProps.EMPTY_PROPS);
  }

  @Test
  public void testBigLinkedList() throws Exception {
    ReadOnlyProps testProps = new ReadOnlyProps(getTestProperties().entrySet().iterator());
    createSchema(testProps);
    int numIterations = Integer.valueOf(testProps.get(NUM_ITERATIONS_KEY));
    int numMappers = Integer.parseInt(testProps.get(NUM_MAPPERS_KEY));
    long numNodes = Long.parseLong(testProps.get(GENERATOR_NUM_ROWS_PER_MAP_KEY));
    String outputDir = testProps.get(testProps.get(OUTPUT_DIR_KEY));
    int numReducers = Integer.parseInt(testProps.get(NUM_REDUCERS_KEY));
    int width = Integer.parseInt(testProps.get(GENERATOR_WIDTH_KEY));
    int wrapMultiplier = Integer.parseInt(testProps.get(GENERATOR_WRAP_KEY));
    long expectedNumNodes = 0;
    for (int i = 0; i < numIterations; i++) {
      LOG.info("Starting iteration = " + i);
      runGenerator(numMappers, numNodes, outputDir, width, wrapMultiplier);
      expectedNumNodes += numMappers * numNodes;
      runVerify(outputDir, numReducers, expectedNumNodes);
    }
  }

  private void runGenerator(int numMappers, long numNodes, String outputDir, int width,
      int wrapMultiplier) throws Exception {
    // TODO Auto-generated method stub
  }

  private void runVerify(String outputDir, int numReducers, long expectedNumNodes) 
    throws Exception {
    // TODO Auto-generated method stub
  }

  static void createSchema(ReadOnlyProps testProps) throws SQLException {
    final String tableName = getTableName(testProps);
    try (Connection conn = DriverManager.getConnection(getUrl())) {
      try (Statement stmt = conn.createStatement()) {
        stmt.execute(
          String.format("CREATE TABLE %s (ID BIGINT NOT NULL"
            + ", PREV BIGINT NOT NULL"
            + ", CLIENT VARCHAR NOT NULL"
            + ", COUNT INTEGER"
            + " CONSTRAINT PK PRIMARY KEY(ID))",
            tableName));
      }
    }
  }

  static class CINode {
    byte[] key;
    byte[] prev;
    String client;
    long count;
  }

}
