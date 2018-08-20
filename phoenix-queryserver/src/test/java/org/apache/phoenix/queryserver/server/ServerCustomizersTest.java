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

import java.util.Collections;
import java.util.List;

import org.apache.calcite.avatica.server.HttpServer;
import org.apache.calcite.avatica.server.AvaticaServerConfiguration;
import org.apache.calcite.avatica.server.ServerCustomizer;
import org.apache.hadoop.conf.Configuration;
import org.apache.phoenix.query.QueryServices;
import org.apache.phoenix.util.InstanceResolver;
import org.eclipse.jetty.server.Server;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import static org.mockito.Matchers.*;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

public class ServerCustomizersTest {
    @Before @After
    public void clearSingletons() {
        // clean up singletons
        InstanceResolver.clearSingletons();
    }

    @Test
    public void testDefaultFactory() {
        QueryServer queryServer = new QueryServer();
        AvaticaServerConfiguration avaticaServerConfiguration = null;
        // the default factory creates an empty list of server customizers
        List<ServerCustomizer<Server>> customizers =
                queryServer.createServerCustomizers(new Configuration(), avaticaServerConfiguration);
        Assert.assertEquals(0, customizers.size());
    }

    @Test
    public void testUseProvidedCustomizers() {
        AvaticaServerConfiguration avaticaServerConfiguration = null;
        final List<ServerCustomizer<Server>> expected =
            Collections.<ServerCustomizer<Server>> singletonList(new ServerCustomizer<Server>() {
              @Override
              public void customize(Server server) {
                // no-op customizer
              }
        });
        // Register the server customizer list
        InstanceResolver.getSingleton(ServerCustomizersFactory.class, new ServerCustomizersFactory() {
            @Override
            public List<ServerCustomizer<Server>> createServerCustomizers(Configuration conf,
                                                                          AvaticaServerConfiguration avaticaServerConfiguration) {
                return expected;
            }
        });
        Configuration conf = new Configuration(false);
        conf.set(QueryServices.QUERY_SERVER_CUSTOMIZERS_ENABLED, "true");
        QueryServer queryServer = new QueryServer();
        List<ServerCustomizer<Server>> actual = queryServer.createServerCustomizers(conf, avaticaServerConfiguration);
        Assert.assertEquals("Customizers are different", expected, actual);
    }

    @Test
    @SuppressWarnings("unchecked")
    public void testEnableCustomizers() {
        AvaticaServerConfiguration avaticaServerConfiguration = null;
        HttpServer.Builder builder = mock(HttpServer.Builder.class);
        Configuration conf = new Configuration(false);
        conf.set(QueryServices.QUERY_SERVER_CUSTOMIZERS_ENABLED, "true");
        QueryServer queryServer = new QueryServer();
        queryServer.enableServerCustomizersIfNecessary(builder, conf, avaticaServerConfiguration);
        verify(builder).withServerCustomizers(anyList(), any(Class.class));
    }
}