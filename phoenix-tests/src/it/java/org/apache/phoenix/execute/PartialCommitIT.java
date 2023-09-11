/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you maynot use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicablelaw or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.phoenix.execute;

import static org.apache.phoenix.thirdparty.com.google.common.collect.Lists.newArrayList;
import static java.util.Collections.singletonList;
import static org.apache.phoenix.monitoring.GlobalClientMetrics.GLOBAL_MUTATION_BATCH_FAILED_COUNT;
import static org.apache.phoenix.monitoring.MetricType.MUTATION_BATCH_FAILED_SIZE;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Arrays;
import java.util.Collection;
import java.util.Comparator;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.hbase.DoNotRetryIOException;
import org.apache.hadoop.hbase.HBaseIOException;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Durability;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.coprocessor.ObserverContext;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.coprocessor.SimpleRegionObserver;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.wal.WALEdit;

import org.apache.phoenix.end2end.NeedsOwnMiniClusterTest;
import org.apache.phoenix.execute.MutationState.MultiRowMutationState;
import org.apache.phoenix.hbase.index.Indexer;
import org.apache.phoenix.jdbc.PhoenixConnection;
import org.apache.phoenix.monitoring.GlobalMetric;
import org.apache.phoenix.monitoring.MetricType;
import org.apache.phoenix.query.BaseTest;
import org.apache.phoenix.query.QueryServices;
import org.apache.phoenix.schema.TableRef;
import org.apache.phoenix.util.PhoenixRuntime;
import org.apache.phoenix.util.ReadOnlyProps;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

import org.apache.phoenix.thirdparty.com.google.common.base.Preconditions;
import org.apache.phoenix.thirdparty.com.google.common.collect.Lists;
import org.apache.phoenix.thirdparty.com.google.common.collect.Maps;

@Category(NeedsOwnMiniClusterTest.class)
@RunWith(Parameterized.class)
// Needs to extend BaseUniqueNamesOwnClusterIT due to installation of FailingRegionObserver coprocessor
public class PartialCommitIT extends BaseTest {
    
	private final String aSuccessTable;
	private final String bFailureTable;
	private final String cSuccessTable;
    private final String upsertToFail;
    private final String upsertSelectToFail;
    private final String deleteToFail;
    private static final String TABLE_NAME_TO_FAIL = "B_FAILURE_TABLE";
    private static final byte[] ROW_TO_FAIL_UPSERT_BYTES = Bytes.toBytes("fail me upsert");
    private static final byte[] ROW_TO_FAIL_DELETE_BYTES = Bytes.toBytes("fail me delete");
    
    @BeforeClass
    public static synchronized void doSetup() throws Exception {
        Map<String, String> serverProps = Maps.newHashMapWithExpectedSize(3);
        serverProps.put("hbase.coprocessor.region.classes", FailingRegionObserver.class.getName());
        serverProps.put("hbase.coprocessor.abortonerror", "false");
        serverProps.put(Indexer.CHECK_VERSION_CONF_KEY, "false");
        Map<String, String> clientProps = Maps.newHashMapWithExpectedSize(2);
        clientProps.put(QueryServices.TRANSACTIONS_ENABLED, "true");
        clientProps.put(QueryServices.COLLECT_REQUEST_LEVEL_METRICS, String.valueOf(true));
        setUpTestDriver(new ReadOnlyProps(serverProps.entrySet().iterator()), new ReadOnlyProps(clientProps.entrySet().iterator()));
    }
    
    private final boolean transactional;
    private final String transactionProvider;
    
    @Parameters(name="PartialCommitIT_transactionProvider={0}")
    public static synchronized Collection<Object[]> data() {
        return Arrays.asList(new Object[][] { { "OMID" } });
    }

    public PartialCommitIT(String transactionProvider) {
        this.transactionProvider = transactionProvider;
        this.transactional = transactionProvider != null;
		aSuccessTable = generateUniqueName();
		bFailureTable = TABLE_NAME_TO_FAIL + generateUniqueName();
		cSuccessTable = generateUniqueName();
	    upsertToFail = "upsert into " + bFailureTable + " values ('" + Bytes.toString(ROW_TO_FAIL_UPSERT_BYTES) + "', 'boom!')";
	    upsertSelectToFail = "upsert into " + bFailureTable + " select k, c from " + aSuccessTable;
	    deleteToFail = "delete from " + bFailureTable + "  where k='" + Bytes.toString(ROW_TO_FAIL_DELETE_BYTES) + "'";
	}
    
    private void createTables() throws Exception {
        try (Connection con = DriverManager.getConnection(getUrl())) {
            Statement sta = con.createStatement();
            sta.execute("create table " + aSuccessTable + " (k varchar primary key, c varchar)" + (transactional ? (" TRANSACTIONAL=true,TRANSACTION_PROVIDER='" + transactionProvider + "'") : ""));
            sta.execute("create table " + bFailureTable + " (k varchar primary key, c varchar)" + (transactional ? (" TRANSACTIONAL=true,TRANSACTION_PROVIDER='" + transactionProvider + "'") : ""));
            sta.execute("create table " + cSuccessTable + " (k varchar primary key, c varchar)" + (transactional ? (" TRANSACTIONAL=true,TRANSACTION_PROVIDER='" + transactionProvider + "'") : ""));
        }
    }
    
    private void populateTables() throws Exception {
        try (Connection con = DriverManager.getConnection(getUrl())) {
            con.setAutoCommit(false);
            Statement sta = con.createStatement();
            List<String> tableNames = Lists.newArrayList(aSuccessTable, bFailureTable, cSuccessTable);
            for (String tableName : tableNames) {
                sta.execute("upsert into " + tableName + " values ('z', 'z')");
                sta.execute("upsert into " + tableName + " values ('zz', 'zz')");
                sta.execute("upsert into " + tableName + " values ('zzz', 'zzz')");
            }
            con.commit();
        }
    }
    
    @Before
    public void resetGlobalMetrics() throws Exception {
        createTables();
        populateTables();
        for (GlobalMetric m : PhoenixRuntime.getGlobalPhoenixClientMetrics()) {
            m.reset();
        }
    }
    
    @Test
    public void testNoFailure() throws SQLException {
        int[] expectedUncommittedStatementIndexes = new int[0];
        testPartialCommit(singletonList("upsert into " + aSuccessTable + " values ('testNoFailure', 'a')"),
                expectedUncommittedStatementIndexes, false,
                singletonList("select count(*) from " + aSuccessTable + " where k='testNoFailure'"),
                singletonList(new Integer(1)),expectedUncommittedStatementIndexes.length);
    }

    @Test
    public void testUpsertFailure() throws SQLException {
        int[] expectedUncommittedStatementIndexes = transactional ? new int[] {0,1,2} : new int[]{1};
        testPartialCommit(newArrayList("upsert into " + aSuccessTable + " values ('testUpsertFailure1', 'a')",
                        upsertToFail,
                        "upsert into " + aSuccessTable + " values ('testUpsertFailure2', 'b')"),
                expectedUncommittedStatementIndexes, true,
                newArrayList("select count(*) from " + aSuccessTable + " where k like 'testUpsertFailure_'",
                        "select count(*) from " + bFailureTable + " where k = '" + Bytes.toString(ROW_TO_FAIL_UPSERT_BYTES) + "'"),
                transactional ? newArrayList(new Integer(0), new Integer(0)) : newArrayList(new Integer(2), new Integer(0)),
                expectedUncommittedStatementIndexes.length);
    }
    
    @Test
    //fail
    public void testUpsertSelectFailure() throws SQLException {
        int[] expectedUncommittedStatementIndexes = transactional ? new int[] {0,1} : new int[]{1};
        try (Connection con = DriverManager.getConnection(getUrl())) {
            con.createStatement().execute("upsert into " + aSuccessTable + " values ('" + Bytes.toString(ROW_TO_FAIL_UPSERT_BYTES) + "', 'boom!')");
            con.commit();
        }

        //In this test we manually pass 5 for numFailureInTransactons. This is because there are 5 statements committed in aSuccessTable before the upsertSelectToFail statement
        // where values are taken from the aSuccessTable into the bFailureTable. Since upsertion into bFailureTable is going to fail and it is transactional all the statements
        // are going to be counted as failed hence the total number of failures are 5.
        testPartialCommit(newArrayList("upsert into " + aSuccessTable + " values ('testUpsertSelectFailure', 'a')",
                        upsertSelectToFail),
                expectedUncommittedStatementIndexes, true,
                newArrayList("select count(*) from " + aSuccessTable + " where k in ('testUpsertSelectFailure', '" + Bytes.toString(ROW_TO_FAIL_UPSERT_BYTES) + "')",
                        "select count(*) from " + bFailureTable + " where k = '" + Bytes.toString(ROW_TO_FAIL_UPSERT_BYTES) + "'"),
                transactional ? newArrayList(new Integer(1) /* from commit above */, new Integer(0)) : newArrayList(new Integer(2), new Integer(0)),
                5);

    }
    @Test
    public void testDeleteFailure() throws SQLException {
        int[] expectedUncommittedStatementIndexes = transactional ? new int[] {0,1,2} : new int[]{1};
        testPartialCommit(newArrayList("upsert into " + aSuccessTable + " values ('testDeleteFailure1', 'a')",
                        deleteToFail,
                        "upsert into " + aSuccessTable + " values ('testDeleteFailure2', 'b')"),
                expectedUncommittedStatementIndexes, true,
                newArrayList("select count(*) from " + aSuccessTable + " where k like 'testDeleteFailure_'",
                        "select count(*) from " + bFailureTable + " where k = 'z'"),
                transactional ? newArrayList(new Integer(0), new Integer(1) /* original row */) : newArrayList(new Integer(2), new Integer(1)),
                expectedUncommittedStatementIndexes.length);
    }
    
    /**
     * {@link MutationState} keeps mutations ordered lexicographically by table name.
     * @throws SQLException 
     */
    @Test
    public void testOrderOfMutationsIsPredicatable() throws SQLException{
        int[] expectedUncommittedStatementIndexes = transactional ? new int[] {0,1,2} : new int[]{0,1};
        testPartialCommit(newArrayList("upsert into " + cSuccessTable + " values ('testOrderOfMutationsIsPredicatable', 'c')", // will fail because c_success_table is after b_failure_table by table sort order
                         upsertToFail,
                        "upsert into " + aSuccessTable + " values ('testOrderOfMutationsIsPredicatable', 'a')"), // will succeed because a_success_table is before b_failure_table by table sort order
                expectedUncommittedStatementIndexes, true,
                newArrayList("select count(*) from " + cSuccessTable + " where k='testOrderOfMutationsIsPredicatable'",
                        "select count(*) from " + aSuccessTable + " where k='testOrderOfMutationsIsPredicatable'",
                        "select count(*) from " + bFailureTable + " where k = '" + Bytes.toString(ROW_TO_FAIL_UPSERT_BYTES) + "'"),
                transactional ? newArrayList(new Integer(0), new Integer(0), new Integer(0)) : newArrayList(new Integer(0), new Integer(1), new Integer(0)),
                expectedUncommittedStatementIndexes.length);
    }
    
    @Test
    public void testStatementOrderMaintainedInConnection() throws SQLException {
        int[] expectedUncommittedStatementIndexes = transactional ? new int[] {0,1,2,4} : new int[]{2,4};
        testPartialCommit(newArrayList("upsert into " + aSuccessTable + " values ('testStatementOrderMaintainedInConnection', 'a')",
                        "upsert into " + aSuccessTable + " select k, c from " + cSuccessTable,
                        deleteToFail,
                        "select * from " + aSuccessTable + "",
                        upsertToFail),
                expectedUncommittedStatementIndexes, true,
                newArrayList("select count(*) from " + aSuccessTable + " where k='testStatementOrderMaintainedInConnection' or k like 'z%'", // rows left: zz, zzz, checkThatAllStatementTypesMaintainOrderInConnection
                        "select count(*) from " + bFailureTable + " where k = '" + Bytes.toString(ROW_TO_FAIL_UPSERT_BYTES) + "'",
                        "select count(*) from " + bFailureTable + " where k = 'z'"),
                transactional ? newArrayList(new Integer(3) /* original rows */, new Integer(0), new Integer(1) /* original row */) : newArrayList(new Integer(4), new Integer(0), new Integer(1)), expectedUncommittedStatementIndexes.length);
    }
    
    private void testPartialCommit(List<String> statements, int[] expectedUncommittedStatementIndexes, boolean willFail, List<String> countStatementsForVerification,
                                   List<Integer> expectedCountsForVerification, int numFailureInTransactons) throws SQLException {
        Preconditions.checkArgument(countStatementsForVerification.size() == expectedCountsForVerification.size());
        
        try (Connection con = getConnectionWithTableOrderPreservingMutationState()) {
            con.setAutoCommit(false);
            Statement sta = con.createStatement();
            for (String statement : statements) {
                sta.execute(statement);
            }
            try {
                con.commit();
                if (willFail) {
                    fail("Expected at least one statement in the list to fail");
                } else {
                    assertEquals(0, con.unwrap(PhoenixConnection.class).getStatementExecutionCounter()); // should have been reset to 0 in commit()
                }
            } catch (SQLException sqle) {
                if (!willFail) {
                    fail("Expected no statements to fail");
                }
                assertEquals(CommitException.class, sqle.getClass());
                int[] uncommittedStatementIndexes = ((CommitException)sqle).getUncommittedStatementIndexes();
                assertArrayEquals(expectedUncommittedStatementIndexes, uncommittedStatementIndexes);
                Map<String, Map<MetricType, Long>> mutationWriteMetrics = PhoenixRuntime.getWriteMetricInfoForMutationsSinceLastReset(con);
                assertEquals(numFailureInTransactons, mutationWriteMetrics.get(bFailureTable).get(MUTATION_BATCH_FAILED_SIZE).intValue());
                assertEquals(numFailureInTransactons, GLOBAL_MUTATION_BATCH_FAILED_COUNT.getMetric().getValue());
            }
            
            
            // verify data in HBase
            for (int i = 0; i < countStatementsForVerification.size(); i++) {
                String countStatement = countStatementsForVerification.get(i);
                ResultSet rs = sta.executeQuery(countStatement);
                if (!rs.next()) {
                    fail("Expected a single row from count query");
                }
                assertEquals(expectedCountsForVerification.get(i).intValue(), rs.getInt(1));
            }
        }
    }
    
    private PhoenixConnection getConnectionWithTableOrderPreservingMutationState() throws SQLException {
        try (PhoenixConnection con = DriverManager.getConnection(getUrl()).unwrap(PhoenixConnection.class)) {
            final Map<TableRef, List<MultiRowMutationState>> mutations = Maps.newTreeMap(new TableRefComparator());
            // passing a null mutation state forces the connection.newMutationState() to be used to create the MutationState
            return new PhoenixConnection(con, (MutationState)null) {
                @Override
                protected MutationState newMutationState(int maxSize, long maxSizeBytes) {
                    return new MutationState(maxSize, maxSizeBytes, this, mutations, false, null);
                };
            };
        }
    }

    public static class FailingRegionObserver extends SimpleRegionObserver {
        @Override
        public void prePut(ObserverContext<RegionCoprocessorEnvironment> c, Put put, WALEdit edit,
                final Durability durability) throws HBaseIOException {
            if (shouldFail(c, put)) {
                // throwing anything other than instances of IOException result
                // in this coprocessor being unloaded
                // DoNotRetryIOException tells HBase not to retry this mutation
                // multiple times
                throw new DoNotRetryIOException();
            }
        }
        
        @Override
        public void preDelete(ObserverContext<RegionCoprocessorEnvironment> c,
                Delete delete, WALEdit edit, Durability durability) throws IOException {
            if (shouldFail(c, delete)) {
                // throwing anything other than instances of IOException result
                // in this coprocessor being unloaded
                // DoNotRetryIOException tells HBase not to retry this mutation
                // multiple times
                throw new DoNotRetryIOException();
            }
        }
        
        private boolean shouldFail(ObserverContext<RegionCoprocessorEnvironment> c, Mutation m) {
            String tableName = c.getEnvironment().getRegion().getRegionInfo().getTable().getNameAsString();
            // deletes on transactional tables are converted to put, so use a single helper method
            return tableName.contains(TABLE_NAME_TO_FAIL) && 
            		(Bytes.equals(ROW_TO_FAIL_UPSERT_BYTES, m.getRow()) || Bytes.equals(ROW_TO_FAIL_DELETE_BYTES, m.getRow()));
        }
        
    }
    
    /**
     * Used for ordering {@link MutationState#mutations} map.
     */
    private static class TableRefComparator implements Comparator<TableRef> {
        @Override
        public int compare(TableRef tr1, TableRef tr2) {
            return tr1.getTable().getPhysicalName().getString().compareTo(tr2.getTable().getPhysicalName().getString());
        }
    }
}
