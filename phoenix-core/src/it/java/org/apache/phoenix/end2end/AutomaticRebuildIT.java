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
package org.apache.phoenix.end2end;

import static org.apache.phoenix.util.TestUtil.TEST_PROPERTIES;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Arrays;
import java.util.Collection;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

import org.apache.hadoop.hbase.DoNotRetryIOException;
import org.apache.hadoop.hbase.HBaseIOException;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.coprocessor.ObserverContext;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.coprocessor.SimpleRegionObserver;
import org.apache.hadoop.hbase.regionserver.MiniBatchOperationInProgress;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.phoenix.jdbc.PhoenixDatabaseMetaData;
import org.apache.phoenix.query.QueryConstants;
import org.apache.phoenix.query.QueryServices;
import org.apache.phoenix.query.QueryServicesOptions;
import org.apache.phoenix.schema.PIndexState;
import org.apache.phoenix.schema.PTableType;
import org.apache.phoenix.util.PropertiesUtil;
import org.apache.phoenix.util.ReadOnlyProps;
import org.apache.phoenix.util.SchemaUtil;
import org.apache.phoenix.util.StringUtil;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

import com.google.common.collect.Maps;

/**
 * Tests for the {@link AutomaticRebuildIT}
 */
@RunWith(Parameterized.class)
public class AutomaticRebuildIT extends BaseOwnClusterIT {

	private final boolean localIndex;
	protected boolean isNamespaceEnabled = false;
	protected final String tableDDLOptions;

	public AutomaticRebuildIT(boolean localIndex) {
		this.localIndex = localIndex;
		StringBuilder optionBuilder = new StringBuilder();
		optionBuilder.append(" SPLIT ON(1,2)");
		this.tableDDLOptions = optionBuilder.toString();
	}

	@BeforeClass
	public static void doSetup() throws Exception {
		Map<String, String> serverProps = Maps.newHashMapWithExpectedSize(7);
		serverProps.put(QueryServices.EXTRA_JDBC_ARGUMENTS_ATTRIB, QueryServicesOptions.DEFAULT_EXTRA_JDBC_ARGUMENTS);
		serverProps.put("hbase.coprocessor.region.classes", FailingRegionObserver.class.getName());
		serverProps.put(" yarn.scheduler.capacity.maximum-am-resource-percent", "1.0");
		serverProps.put(HConstants.HBASE_CLIENT_RETRIES_NUMBER, "2");
		serverProps.put(HConstants.HBASE_RPC_TIMEOUT_KEY, "10000");
		serverProps.put("hbase.client.pause", "5000");
		serverProps.put(QueryServices.INDEX_FAILURE_HANDLING_REBUILD_PERIOD, "1000");
		serverProps.put(QueryServices.INDEX_FAILURE_HANDLING_REBUILD_NUMBER_OF_BATCHES_PER_TABLE, "5");
		Map<String, String> clientProps = Maps.newHashMapWithExpectedSize(1);
		setUpTestDriver(new ReadOnlyProps(serverProps.entrySet().iterator()),
				new ReadOnlyProps(clientProps.entrySet().iterator()));
	}

	@Parameters(name = "localIndex = {0}")
	public static Collection<Boolean[]> data() {
		return Arrays.asList(new Boolean[][] { { false }, { true } });
	}

	@Test
	public void testSecondaryAutomaticRebuildIndex() throws Exception {
		String schemaName = generateUniqueName();
		String dataTableName = generateUniqueName();
		String fullTableName = SchemaUtil.getTableName(schemaName, dataTableName);
		final String indxTable = String.format("%s_%s", dataTableName, FailingRegionObserver.INDEX_NAME);
		Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
		props.setProperty(QueryServices.TRANSACTIONS_ENABLED, Boolean.TRUE.toString());
		props.setProperty(QueryServices.EXPLAIN_ROW_COUNT_ATTRIB, Boolean.FALSE.toString());
		props.setProperty(QueryServices.IS_NAMESPACE_MAPPING_ENABLED, Boolean.toString(isNamespaceEnabled));
		final Connection conn = DriverManager.getConnection(getUrl(), props);
		Statement stmt = conn.createStatement();
		try {
			if (isNamespaceEnabled) {
				conn.createStatement().execute("CREATE SCHEMA IF NOT EXISTS " + schemaName);
			}
			stmt.execute(String.format(
					"CREATE TABLE %s (ID BIGINT NOT NULL, NAME VARCHAR, ZIP INTEGER CONSTRAINT PK PRIMARY KEY(ID ROW_TIMESTAMP)) %s",
					fullTableName, tableDDLOptions));
			String upsertQuery = String.format("UPSERT INTO %s VALUES(?, ?, ?)", fullTableName);
			PreparedStatement stmt1 = conn.prepareStatement(upsertQuery);
			FailingRegionObserver.FAIL_WRITE = false;
			// insert two rows
			upsertRow(stmt1, 1000);
			upsertRow(stmt1, 2000);

			conn.commit();
			stmt.execute(String.format("CREATE %s INDEX %s ON %s  (LPAD(UPPER(NAME),11,'x')||'_xyz') ",
					(localIndex ? "LOCAL" : ""), indxTable, fullTableName));
			FailingRegionObserver.FAIL_WRITE = true;
			upsertRow(stmt1, 3000);
			upsertRow(stmt1, 4000);
			upsertRow(stmt1, 5000);
			try {
				conn.commit();
				fail();
			} catch (SQLException e) {
			} catch (Exception e) {
			}
			FailingRegionObserver.FAIL_WRITE = false;
			ResultSet rs = conn.getMetaData().getTables(null, StringUtil.escapeLike(schemaName), indxTable,
					new String[] { PTableType.INDEX.toString() });
			assertTrue(rs.next());
			assertEquals(indxTable, rs.getString(3));
			String indexState = rs.getString("INDEX_STATE");
			assertEquals(PIndexState.DISABLE.toString(), indexState);
			assertFalse(rs.next());
			upsertRow(stmt1, 6000);
			upsertRow(stmt1, 7000);
			conn.commit();
			int maxTries = 4, nTries = 0;
			boolean isInactive = false;
			do {
				rs = conn.createStatement()
						.executeQuery(String.format("SELECT " + PhoenixDatabaseMetaData.INDEX_STATE + ","
								+ PhoenixDatabaseMetaData.INDEX_DISABLE_TIMESTAMP + " FROM "
								+ PhoenixDatabaseMetaData.SYSTEM_CATALOG_NAME + " ("
								+ PhoenixDatabaseMetaData.ASYNC_REBUILD_TIMESTAMP + " bigint) where "
								+ PhoenixDatabaseMetaData.TABLE_SCHEM + "='" + schemaName + "' and "
								+ PhoenixDatabaseMetaData.TABLE_NAME + "='" + indxTable + "'"));
				rs.next();
				if (PIndexState.INACTIVE.getSerializedValue().equals(rs.getString(1)) && rs.getLong(2) > 3000) {
					isInactive = true;
					break;
				}
				Thread.sleep(10 * 1000); // sleep 10 secs
			} while (++nTries < maxTries);
			assertTrue(isInactive);
			nTries = 0;
			boolean isActive = false;
			do {
				Thread.sleep(15 * 1000); // sleep 15 secs
				rs = conn.getMetaData().getTables(null, StringUtil.escapeLike(schemaName), indxTable,
						new String[] { PTableType.INDEX.toString() });
				assertTrue(rs.next());
				if (PIndexState.ACTIVE.toString().equals(rs.getString("INDEX_STATE"))) {
					isActive = true;
					break;
				}
			} while (++nTries < maxTries);
			assertTrue(isActive);

		} finally {
			conn.close();
		}
	}

	public static void upsertRow(PreparedStatement stmt, int i) throws SQLException {
		// insert row
		stmt.setInt(1, i);
		stmt.setString(2, "uname" + String.valueOf(i));
		stmt.setInt(3, 95050 + i);
		stmt.executeUpdate();
	}

	public static class FailingRegionObserver extends SimpleRegionObserver {
		public static volatile boolean FAIL_WRITE = false;
		public static final String INDEX_NAME = "IDX";

		@Override
		public void preBatchMutate(ObserverContext<RegionCoprocessorEnvironment> c,
				MiniBatchOperationInProgress<Mutation> miniBatchOp) throws HBaseIOException {
			if (c.getEnvironment().getRegionInfo().getTable().getNameAsString().contains(INDEX_NAME) && FAIL_WRITE) {
				throw new DoNotRetryIOException();
			}
			Mutation operation = miniBatchOp.getOperation(0);
			Set<byte[]> keySet = operation.getFamilyMap().keySet();
			for (byte[] family : keySet) {
				if (Bytes.toString(family).startsWith(QueryConstants.LOCAL_INDEX_COLUMN_FAMILY_PREFIX) && FAIL_WRITE) {
					throw new DoNotRetryIOException();
				}
			}
		}

	}

}
