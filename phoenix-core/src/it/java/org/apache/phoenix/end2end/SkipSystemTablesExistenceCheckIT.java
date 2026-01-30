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

import static org.apache.hadoop.hbase.coprocessor.CoprocessorHost.REGIONSERVER_COPROCESSOR_CONF_KEY;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.SYSTEM_CATALOG_SCHEMA;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

import java.io.IOException;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Properties;
import java.util.Set;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.NamespaceNotFoundException;
import org.apache.hadoop.hbase.TableName;
import org.apache.phoenix.jdbc.ConnectionInfo;
import org.apache.phoenix.jdbc.PhoenixTestDriver;
import org.apache.phoenix.query.ConfigurationFactory;
import org.apache.phoenix.query.ConnectionQueryServices;
import org.apache.phoenix.query.ConnectionQueryServicesImpl;
import org.apache.phoenix.query.HBaseFactoryProvider;
import org.apache.phoenix.query.QueryServices;
import org.apache.phoenix.query.QueryServicesTestImpl;
import org.apache.phoenix.util.InstanceResolver;
import org.apache.phoenix.util.ReadOnlyProps;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;

/**
 * Tests focus on the {@code phoenix.skip.system.tables.existence.check} property, testing:
 * <ul>
 * <li>default value</li>
 * <li>value set by {@link Properties} from the client</li>
 * <li>value set in a HBase configuration XML</li>
 * </ul>
 */
@Category(NeedsOwnMiniClusterTest.class)
public class SkipSystemTablesExistenceCheckIT {

  private static HBaseTestingUtility hbaseTestUtil;
  private static Configuration conf;
  private Set<String> hbaseTables;
  private static final Set<String> PHOENIX_SYSTEM_TABLES =
    new HashSet<>(Arrays.asList("SYSTEM.CATALOG", "SYSTEM.SEQUENCE", "SYSTEM.STATS",
      "SYSTEM.FUNCTION", "SYSTEM.MUTEX", "SYSTEM.LOG", "SYSTEM.CHILD_LINK", "SYSTEM.TASK",
      "SYSTEM.TRANSFORM", "SYSTEM.CDC_STREAM_STATUS", "SYSTEM.CDC_STREAM",
      "SYSTEM.PHOENIX_INDEX_TOOL_RESULT", "SYSTEM.PHOENIX_INDEX_TOOL"));

  private static class PhoenixSystemTablesCreationTestDriver extends PhoenixTestDriver {
    private static ConnectionQueryServices cqs;
    private final ReadOnlyProps overrideProps;

    PhoenixSystemTablesCreationTestDriver(ReadOnlyProps props) {
      overrideProps = props;
    }

    @Override
    public synchronized ConnectionQueryServices getConnectionQueryServices(String url,
      Properties info) throws SQLException {
      QueryServicesTestImpl qsti = new QueryServicesTestImpl(getDefaultProps(), overrideProps);
      if (cqs == null) {
        cqs = new ConnectionQueryServicesImpl(qsti,
          ConnectionInfo.create(url, qsti.getProps(), info), info);
        cqs.init(url, info);
      }
      return cqs;
    }

    public synchronized ConnectionQueryServices getConnectionQueryServices(String url,
      Configuration configuration, Properties info) throws SQLException {
      QueryServicesTestImpl qsti = new QueryServicesTestImpl(getDefaultProps(), overrideProps);
      if (cqs == null) {
        ConnectionInfo connectionInfo =
          ConnectionInfo.create(url, configuration, qsti.getProps(), info);
        cqs = new ConnectionQueryServicesImpl(qsti, connectionInfo, info);
        cqs.init(url, info);
      }
      return cqs;
    }
  }

  /**
   * Make sure the ConnectionInfo doesn't try to pull a default Configuration
   */
  @BeforeClass
  public static synchronized void setUp() throws Exception {
    InstanceResolver.clearSingletons();
    InstanceResolver.getSingleton(ConfigurationFactory.class, new ConfigurationFactory() {
      @Override
      public Configuration getConfiguration() {
        return new Configuration(conf);
      }

      @Override
      public Configuration getConfiguration(Configuration confToClone) {
        Configuration copy = new Configuration(conf);
        copy.addResource(confToClone);
        return copy;
      }
    });
  }

  @AfterClass
  public static synchronized void cleanUp() {
    InstanceResolver.clearSingletons();
  }

  /**
   * Before each test initiates a new minicluster, thus avoiding interference with the
   * configurations.
   * @throws Exception if the mini cluster couldn't be started.
   */
  @Before
  public void init() throws Exception {
    hbaseTestUtil = new HBaseTestingUtility();
    conf = hbaseTestUtil.getConfiguration();
    conf.set(QueryServices.IS_NAMESPACE_MAPPING_ENABLED, Boolean.FALSE.toString());
    // Avoid multiple clusters trying to bind to the master's info port (16010)
    conf.setInt(HConstants.MASTER_INFO_PORT, -1);
    conf.set(REGIONSERVER_COPROCESSOR_CONF_KEY,
      PhoenixRegionServerEndpointTestImpl.class.getName());
    hbaseTestUtil.startMiniCluster(1);
  }

  /**
   * After each test shuts down the minicluster.
   * @throws IOException if the mini cluster couldn't be started.
   */
  @After
  public void reset() throws IOException {
    hbaseTestUtil.shutdownMiniCluster();
  }

  /**
   * Tests the presence of Phoenix system tables creation when the
   * {@code phoenix.skip.system.tables.existence.check} is used with the default value of:
   * {@code False}
   */
  @Test
  public void skipSystemTablesExistenceCheckDefaultValue() throws Exception {
    Properties clientProps = getNSMappingDisabledProperties();
    // PhoenixConnection ignored =
    // (PhoenixConnection) DriverManager.getConnection(getJdbcUrl(), clientProps);
    PhoenixSystemTablesCreationTestDriver driver =
      new PhoenixSystemTablesCreationTestDriver(ReadOnlyProps.EMPTY_PROPS);
    driver.getConnectionQueryServices(getJdbcUrl(), clientProps);
    hbaseTables = getHBaseTables();
    assertEquals(PHOENIX_SYSTEM_TABLES, hbaseTables);
    assertFalse(isSystemNamespaceCreated());
  }

  /**
   * Tests the lack of Phoenix system tables creation when the corresponding setting is disabled
   * with Phoenix client {@link Properties}.
   */
  @Test
  public void skipSystemTablesExistenceCheckProperty() throws Exception {
    Properties clientProps = getNSMappingDisabledProperties();
    clientProps.setProperty(QueryServices.SKIP_SYSTEM_TABLES_EXISTENCE_CHECK,
      Boolean.TRUE.toString());
    PhoenixSystemTablesCreationTestDriver driver =
      new PhoenixSystemTablesCreationTestDriver(ReadOnlyProps.EMPTY_PROPS);
    driver.getConnectionQueryServices(getJdbcUrl(), clientProps);
    hbaseTables = getHBaseTables();
    assertEquals(new HashSet<String>(), hbaseTables);
    assertFalse(isSystemNamespaceCreated());
  }

  /**
   * Tests the lack of Phoenix system tables creation when the corresponding setting is disabled
   * from a HBase configuration file {@code skip-system-tables-existence-check.xml}.
   * @throws Exception if any error occurs during the test execution.
   */
  @Test
  public void skipSystemTablesExistenceCheckXML() throws Exception {
    Properties clientProps = getNSMappingDisabledProperties();
    conf = HBaseFactoryProvider.getConfigurationFactory().getConfiguration();
    conf.addResource("skip-system-tables-existence-check.xml");
    PhoenixSystemTablesCreationTestDriver driver =
      new PhoenixSystemTablesCreationTestDriver(ReadOnlyProps.EMPTY_PROPS);
    driver.getConnectionQueryServices(getJdbcUrl(), conf, clientProps);
    hbaseTables = getHBaseTables();
    assertEquals(new HashSet<String>(), hbaseTables);
    assertFalse(isSystemNamespaceCreated());
  }

  /**
   * Get the connection string for the mini-cluster
   * @return Phoenix connection string
   */
  private String getJdbcUrl() {
    return "jdbc:phoenix:localhost:" + hbaseTestUtil.getZkCluster().getClientPort() + ":/hbase";
  }

  /**
   * Return all created HBase tables
   * @return Set of HBase table name strings
   * @throws IOException if there is a problem listing all HBase tables
   */
  private Set<String> getHBaseTables() throws IOException {
    Set<String> tables = new HashSet<>();
    for (TableName tn : hbaseTestUtil.getAdmin().listTableNames()) {
      tables.add(tn.getNameAsString());
    }
    return tables;
  }

  /**
   * Check whether the SYSTEM namespace has been created
   * @return {@code true} if the system namespace exists, {@code false} otherwise.
   */
  private boolean isSystemNamespaceCreated() throws IOException {
    try {
      hbaseTestUtil.getAdmin().getNamespaceDescriptor(SYSTEM_CATALOG_SCHEMA);
    } catch (NamespaceNotFoundException ex) {
      return false;
    }
    return true;
  }

  /**
   * Disable namespace mapping
   * @return A new {@link Properties} object with namespace mapping disabled.
   */
  private Properties getNSMappingDisabledProperties() {
    Properties clientProps = new Properties();
    clientProps.setProperty(QueryServices.IS_NAMESPACE_MAPPING_ENABLED, Boolean.FALSE.toString());
    clientProps.setProperty(QueryServices.IS_SYSTEM_TABLE_MAPPED_TO_NAMESPACE,
      Boolean.FALSE.toString());
    return clientProps;
  }
}
