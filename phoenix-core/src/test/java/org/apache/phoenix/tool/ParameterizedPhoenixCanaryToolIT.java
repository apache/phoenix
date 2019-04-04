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
package org.apache.phoenix.tool;

import com.google.common.collect.Maps;
import com.google.gson.Gson;

import org.apache.phoenix.end2end.NeedsOwnMiniClusterTest;
import org.apache.phoenix.query.BaseTest;
import org.apache.phoenix.query.QueryServices;
import org.apache.phoenix.util.ReadOnlyProps;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileReader;
import java.io.FilenameFilter;
import java.io.IOException;
import java.io.InputStream;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import static org.apache.phoenix.tool.PhoenixCanaryTool.propFileName;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

@RunWith(Parameterized.class)
@Category(NeedsOwnMiniClusterTest.class)
public class ParameterizedPhoenixCanaryToolIT extends BaseTest {

	private static final Logger LOGGER =
			LoggerFactory.getLogger(ParameterizedPhoenixCanaryToolIT.class);
	private static final String stdOutSink
			= "org.apache.phoenix.tool.PhoenixCanaryTool$StdOutSink";
	private static final String fileOutSink
			= "org.apache.phoenix.tool.PhoenixCanaryTool$FileOutSink";

	private static Map<String, String> serverProps = Maps.newHashMapWithExpectedSize(2);
	private static Map<String, String> clientProps = Maps.newHashMapWithExpectedSize(2);
	private static String connString = "";
	private static Properties canaryProp = new Properties();
	private static Connection connection = null;
	private boolean isNamespaceEnabled;
	private boolean isPositiveTestType;
	private List<String> cmd = new ArrayList<>();
	private String resultSinkOption;
	private ByteArrayOutputStream out = new ByteArrayOutputStream();

	public ParameterizedPhoenixCanaryToolIT(boolean isPositiveTestType,
			boolean isNamespaceEnabled, String resultSinkOption) {
		this.isPositiveTestType = isPositiveTestType;
		this.isNamespaceEnabled = isNamespaceEnabled;
		this.resultSinkOption = resultSinkOption;
	}

	@Parameterized.Parameters(name = "ParameterizedPhoenixCanaryToolIT_isPositiveTestType={0}," +
			"isNamespaceEnabled={1},resultSinkOption={2}")
	public static Collection parametersList() {
		return Arrays.asList(new Object[][] {
			{true, true, stdOutSink},
			{true, true, fileOutSink},
			{false, true, stdOutSink},
			{false, true, fileOutSink},
			{true, false, stdOutSink},
			{true, false, fileOutSink},
			{false, false, stdOutSink},
			{false, false, fileOutSink}
		});
	}

	@Before
	public void setup() throws Exception {
		String createSchema;
		String createTable;

		if(needsNewCluster()) {
			setClientSideNamespaceProperties();
			setServerSideNamespaceProperties();
			tearDownMiniClusterAsync(1);
			setUpTestDriver(new ReadOnlyProps(serverProps.entrySet().iterator()),
					new ReadOnlyProps(clientProps.entrySet().iterator()));
			LOGGER.info("New cluster is spinned up with test parameters " +
					"isPositiveTestType" + this.isPositiveTestType +
					"isNamespaceEnabled" + this.isNamespaceEnabled +
					"resultSinkOption" + this.resultSinkOption);
			connString = BaseTest.getUrl();
			connection = getConnection();
		}

		if (this.isNamespaceEnabled) {
			createSchema = "CREATE SCHEMA IF NOT EXISTS TEST";
			connection.createStatement().execute(createSchema);
		}
		createTable = "CREATE TABLE IF NOT EXISTS TEST.PQSTEST " +
						"(mykey INTEGER NOT NULL PRIMARY KEY, mycolumn VARCHAR," +
						" insert_date TIMESTAMP)";
		connection.createStatement().execute(createTable);
		cmd.add("--constring");
		cmd.add(connString);
		cmd.add("--logsinkclass");
		cmd.add(this.resultSinkOption);
		if (this.resultSinkOption.contains(stdOutSink)) {
			System.setOut(new java.io.PrintStream(out));
		} else {
			loadCanaryPropertiesFile(canaryProp);
		}
	}

	private boolean needsNewCluster() {
		if (connection == null) {
			return true;
		}
		if (!clientProps.get(QueryServices.IS_SYSTEM_TABLE_MAPPED_TO_NAMESPACE)
				.equalsIgnoreCase(String.valueOf(this.isNamespaceEnabled))) {
			return true;
		}
		return false;
	}

	private void setClientSideNamespaceProperties() {

		clientProps.put(QueryServices.IS_SYSTEM_TABLE_MAPPED_TO_NAMESPACE,
				String.valueOf(this.isNamespaceEnabled));

		clientProps.put(QueryServices.IS_NAMESPACE_MAPPING_ENABLED,
				String.valueOf(this.isNamespaceEnabled));
	}

	private Connection getConnection() throws SQLException {
		Properties props = new Properties();
		props.setProperty(QueryServices.IS_SYSTEM_TABLE_MAPPED_TO_NAMESPACE,
				String.valueOf(this.isNamespaceEnabled));

		props.setProperty(QueryServices.IS_NAMESPACE_MAPPING_ENABLED,
				String.valueOf(this.isNamespaceEnabled));
		return DriverManager.getConnection(connString, props);
	}

	void setServerSideNamespaceProperties() {
		serverProps.put(QueryServices.IS_NAMESPACE_MAPPING_ENABLED,
				String.valueOf(this.isNamespaceEnabled));
		serverProps.put(QueryServices.IS_SYSTEM_TABLE_MAPPED_TO_NAMESPACE,
				String.valueOf(this.isNamespaceEnabled));
	}

	/*
	*	This test runs in the test suit with
	*	combination of parameters provided.
	*	It tests the tool in positive type where test expects to pass
	*	and negative type where test expects to fail.
 	*/
	@Test
	public void phoenixCanaryToolTest() throws SQLException, IOException {
		if (!isPositiveTestType) {
			dropTestTable();
		}
		PhoenixCanaryTool.main(cmd.toArray(new String[cmd.size()]));
		Boolean result = getAggregatedResult();
		if (isPositiveTestType) {
			assertTrue(result);
		} else {
			assertFalse(result);
		}
	}

	private Boolean getAggregatedResult() throws IOException {
		HashMap<String, Boolean> resultsMap;
		Boolean result = true;
		resultsMap = parsePublishedResults();
		for (Boolean b : resultsMap.values()) {
			result = result && b;
		}
		return result;
	}

	private HashMap<String, Boolean> parsePublishedResults() throws IOException {
		Gson parser = new Gson();
		CanaryTestResult[] results;
		HashMap<String, Boolean> resultsMap = new HashMap<>();

		if (this.resultSinkOption.contains(fileOutSink)) {
			File resultFile = getTestResultsFile();
			results = parser.fromJson(new FileReader(resultFile),
					CanaryTestResult[].class);
		} else {
			String result = out.toString();
			results = parser.fromJson(result, CanaryTestResult[].class);
		}
		for (CanaryTestResult r : results) {
			resultsMap.put(r.getTestName(), r.isSuccessful());
		}
		return resultsMap;
	}

	private File getTestResultsFile() {
		File[] files = getLogFileList();
		return files[0];
	}

	@After
	public void teardown() throws SQLException {
		if (this.isNamespaceEnabled) {
			dropTestTableAndSchema();
		} else {
			dropTestTable();
		}
		if (this.resultSinkOption.contains(fileOutSink)) {
			deleteResultSinkFile();
		}
	}

	private void deleteResultSinkFile() {
		File[] files = getLogFileList();
		for (final File file : files) {
			if (!file.delete()) {
				System.err.println("Can't remove " + file.getAbsolutePath());
			}
		}
	}

	private File[] getLogFileList() {
		File dir = new File(canaryProp.getProperty("file.location"));
		return dir.listFiles(new FilenameFilter() {
			@Override
			public boolean accept(File dir, String name) {
				return name.endsWith(".log");
			}
		});
	}

	private void loadCanaryPropertiesFile(Properties prop) {
		InputStream input = ClassLoader.getSystemResourceAsStream(propFileName);
		try {
			prop.load(input);
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	private void dropTestTable() throws SQLException {
		String dropTable = "DROP TABLE IF EXISTS TEST.PQSTEST";
		connection.createStatement().execute(dropTable);
	}

	private void dropTestTableAndSchema() throws SQLException {
		dropTestTable();
		String dropSchema = "DROP SCHEMA IF EXISTS TEST";
		connection.createStatement().execute(dropSchema);
	}

}
