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
package org.apache.phoenix.queryserver.server;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import org.apache.calcite.avatica.server.HttpServer;
import org.apache.calcite.avatica.server.RemoteUserExtractionException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.authorize.AuthorizationException;
import org.apache.hadoop.security.authorize.ProxyUsers;
import org.apache.phoenix.queryserver.server.QueryServer.PhoenixRemoteUserExtractor;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.servlet.http.HttpServletRequest;

/**
 * Tests for the RemoteUserExtractor Method Avatica provides for Phoenix to implement.
 */
public class PhoenixRemoteUserExtractorTest {
  private static final Logger LOG = LoggerFactory.getLogger(PhoenixRemoteUserExtractorTest.class);

  @Test
  public void testWithRemoteUserExtractorSuccess() {
    HttpServletRequest request = mock(HttpServletRequest.class);
    when(request.getRemoteUser()).thenReturn("proxyserver");
    when(request.getParameter("doAs")).thenReturn("enduser");
    when(request.getRemoteAddr()).thenReturn("localhost:1234");

    Configuration conf = new Configuration(false);
    conf.set("hadoop.proxyuser.proxyserver.groups", "*");
    conf.set("hadoop.proxyuser.proxyserver.hosts", "*");
    conf.set("phoenix.queryserver.withRemoteUserExtractor", "true");
    ProxyUsers.refreshSuperUserGroupsConfiguration(conf);

    PhoenixRemoteUserExtractor extractor = new PhoenixRemoteUserExtractor(conf);
    try {
      assertEquals("enduser", extractor.extract(request));
    } catch (RemoteUserExtractionException e) {
      LOG.info(e.getMessage());
    }
  }

  @Test
  public void testNoRemoteUserExtractorParam() {
    HttpServletRequest request = mock(HttpServletRequest.class);
    when(request.getRemoteUser()).thenReturn("proxyserver");
    when(request.getRemoteAddr()).thenReturn("localhost:1234");

    Configuration conf = new Configuration(false);
    conf.set("hadoop.proxyuser.proxyserver.groups", "*");
    conf.set("hadoop.proxyuser.proxyserver.hosts", "*");
    conf.set("phoenix.queryserver.withRemoteUserExtractor", "true");
    ProxyUsers.refreshSuperUserGroupsConfiguration(conf);

    PhoenixRemoteUserExtractor extractor = new PhoenixRemoteUserExtractor(conf);
    try {
      assertEquals("proxyserver", extractor.extract(request));
    } catch (RemoteUserExtractionException e) {
      LOG.info(e.getMessage());
    }
  }

  @Test
  public void testDoNotUseRemoteUserExtractor() {

    HttpServer.Builder builder = mock(HttpServer.Builder.class);
    Configuration conf = new Configuration(false);
    QueryServer queryServer = new QueryServer();
    queryServer.setRemoteUserExtractorIfNecessary(builder, conf);
    verify(builder, never()).withRemoteUserExtractor(any(PhoenixRemoteUserExtractor.class));
  }

  @Test
  public void testUseRemoteUserExtractor() {

    HttpServer.Builder builder = mock(HttpServer.Builder.class);
    Configuration conf = new Configuration(false);
    conf.set("phoenix.queryserver.withRemoteUserExtractor", "true");
    QueryServer queryServer = new QueryServer();
    queryServer.setRemoteUserExtractorIfNecessary(builder, conf);
    verify(builder).withRemoteUserExtractor(any(PhoenixRemoteUserExtractor.class));
  }

}
