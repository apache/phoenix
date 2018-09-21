/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to you under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
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

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.calcite.avatica.server.AvaticaServerConfiguration;
import org.apache.calcite.avatica.server.ServerCustomizer;
import org.apache.hadoop.conf.Configuration;
import org.apache.phoenix.query.QueryServices;
import org.apache.phoenix.queryserver.server.ServerCustomizersFactory;
import org.apache.phoenix.util.InstanceResolver;
import org.eclipse.jetty.security.ConstraintMapping;
import org.eclipse.jetty.security.ConstraintSecurityHandler;
import org.eclipse.jetty.security.HashLoginService;
import org.eclipse.jetty.security.authentication.BasicAuthenticator;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.util.security.Constraint;
import org.eclipse.jetty.util.security.Credential;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ServerCustomizersIT extends BaseHBaseManagedTimeIT {
    private static final Logger LOG = LoggerFactory.getLogger(ServerCustomizersIT.class);
    private static final String USER_AUTHORIZED = "user3";
    private static final String USER_NOT_AUTHORIZED = "user1";
    private static final String USER_PW = "s3cr3t";

    private static QueryServerTestUtil PQS_UTIL;

    @Rule
    public ExpectedException expected = ExpectedException.none();

    @BeforeClass
    public static void setup() throws Exception {
        Configuration conf = getTestClusterConfig();
        conf.set(QueryServices.QUERY_SERVER_CUSTOMIZERS_ENABLED, "true");
        PQS_UTIL = new QueryServerTestUtil(conf);
        PQS_UTIL.startLocalHBaseCluster(ServerCustomizersIT.class);
        // Register a test jetty server customizer
        InstanceResolver.clearSingletons();
        InstanceResolver.getSingleton(ServerCustomizersFactory.class, new ServerCustomizersFactory() {
            @Override
            public List<ServerCustomizer<Server>> createServerCustomizers(Configuration conf,
                                                                          AvaticaServerConfiguration avaticaServerConfiguration) {
                return Collections.<ServerCustomizer<Server>>singletonList(new TestServerCustomizer());
            }
        });
        PQS_UTIL.startQueryServer();
    }

    @AfterClass
    public static void teardown() throws Exception {
        // Remove custom singletons for future tests
        InstanceResolver.clearSingletons();
        if (PQS_UTIL != null) {
            PQS_UTIL.stopQueryServer();
            PQS_UTIL.stopLocalHBaseCluster();
        }
    }

    @Test
    public void testUserAuthorized() throws Exception {
        try (Connection conn = DriverManager.getConnection(PQS_UTIL.getUrl(
                getBasicAuthParams(USER_AUTHORIZED)));
                Statement stmt = conn.createStatement()) {
            Assert.assertFalse("user3 should have access", stmt.execute(
                "create table "+ServerCustomizersIT.class.getSimpleName()+" (pk integer not null primary key)"));
        }
    }

    @Test
    public void testUserNotAuthorized() throws Exception {
        expected.expect(RuntimeException.class);
        expected.expectMessage("HTTP/401");
        try (Connection conn = DriverManager.getConnection(PQS_UTIL.getUrl(
                getBasicAuthParams(USER_NOT_AUTHORIZED)));
                Statement stmt = conn.createStatement()) {
            Assert.assertFalse(stmt.execute(
                    "select access from database"));
        }
    }

    private Map<String, String> getBasicAuthParams(String user) {
        Map<String, String> params = new HashMap<>();
        params.put("authentication", "BASIC");
        params.put("avatica_user", user);
        params.put("avatica_password", USER_PW);
        return params;
    }

    /**
     * Contrived customizer that enables BASIC auth for a single user
     */
    public static class TestServerCustomizer implements ServerCustomizer<Server> {
        @Override
        public void customize(Server server) {
            LOG.debug("Customizing server to allow requests for {}", USER_AUTHORIZED);
            HashLoginService login = new HashLoginService();
            login.putUser(USER_AUTHORIZED, Credential.getCredential(USER_PW), new String[] {"users"});
            login.setName("users");

            Constraint constraint = new Constraint();
            constraint.setName(Constraint.__BASIC_AUTH);
            constraint.setRoles(new String[]{"users"});
            constraint.setAuthenticate(true);

            ConstraintMapping cm = new ConstraintMapping();
            cm.setConstraint(constraint);
            cm.setPathSpec("/*");

            ConstraintSecurityHandler security = new ConstraintSecurityHandler();
            security.setAuthenticator(new BasicAuthenticator());
            security.setRealmName("users");
            security.addConstraintMapping(cm);
            security.setLoginService(login);

            // chain the PQS handler to security
            security.setHandler(server.getHandlers()[0]);
            server.setHandler(security);
        }
    }
}
