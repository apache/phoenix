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
package org.apache.phoenix.queryserver.server;

import java.io.File;
import java.io.IOException;

import org.apache.calcite.avatica.server.DoAsRemoteUserCallback;
import org.apache.calcite.avatica.server.HttpServer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.phoenix.query.QueryServices;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import static org.mockito.Mockito.*;

public class QueryServerConfigurationTest {
  private static final Configuration CONF = HBaseConfiguration.create();

  @Rule public TemporaryFolder testFolder = new TemporaryFolder();

  private HttpServer.Builder builder;
  private QueryServer queryServer;

  @Before
  public void setup() throws IOException {
    File keytabFile = testFolder.newFile("test.keytab");
    CONF.set(QueryServices.QUERY_SERVER_KEYTAB_FILENAME_ATTRIB, keytabFile.getAbsolutePath());
    builder = mock(HttpServer.Builder.class);
    queryServer = new QueryServer(new String[0], CONF);
  }

  @Test
  public void testSpnegoEnabled() throws IOException {
    // SPENEGO settings will be provided to the builder when enabled
    doReturn(builder).when(builder).withSpnego(anyString(), any(String[].class));
    configureAndVerifyImpersonation(builder, false);
    // A keytab file will also be provided for automatic login
    verify(builder).withAutomaticLogin(any(File.class));
  }

  @Test
  public void testSpnegoDisabled() throws IOException {
    configureAndVerifyImpersonation(builder, true);
    verify(builder, never()).withSpnego(anyString(), any(String[].class));
    verify(builder, never()).withAutomaticLogin(any(File.class));
  }

  private void configureAndVerifyImpersonation(HttpServer.Builder builder, boolean disableSpnego)
      throws IOException {
    queryServer.configureClientAuthentication(builder, disableSpnego);
    verify(builder).withImpersonation(any(DoAsRemoteUserCallback.class));
  }
}
