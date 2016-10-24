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
import static org.junit.Assert.assertTrue;

import java.util.concurrent.Callable;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.authorize.ProxyUsers;
import org.apache.phoenix.queryserver.server.QueryServer.PhoenixDoAsCallback;
import org.junit.Test;

/**
 * Tests for the authorization callback hook Avatica provides for Phoenix to implement.
 */
public class PhoenixDoAsCallbackTest {

    @Test
    public void ugiInstancesAreCached() throws Exception {
        Configuration conf = new Configuration(false);
        UserGroupInformation serverUgi = UserGroupInformation.createUserForTesting("server", new String[0]);
        PhoenixDoAsCallback callback = new PhoenixDoAsCallback(serverUgi, conf);

        UserGroupInformation ugi1 = callback.createProxyUser("user1");
        assertEquals(1, callback.getCache().size());
        assertTrue(ugi1.getRealUser() == serverUgi);
        UserGroupInformation ugi2 = callback.createProxyUser("user2");
        assertEquals(2, callback.getCache().size());
        assertTrue(ugi2.getRealUser() == serverUgi);

        UserGroupInformation ugi1Reference = callback.createProxyUser("user1");
        assertTrue(ugi1 == ugi1Reference);
        assertEquals(2, callback.getCache().size());
    }

    @Test
    public void proxyingUsersAreCached() throws Exception {
      Configuration conf = new Configuration(false);
      // The user "server" can impersonate anyone
      conf.set("hadoop.proxyuser.server.groups", "*");
      conf.set("hadoop.proxyuser.server.hosts", "*");
      // Trigger ProxyUsers to refresh itself with the above configuration
      ProxyUsers.refreshSuperUserGroupsConfiguration(conf);
      UserGroupInformation serverUgi = UserGroupInformation.createUserForTesting("server", new String[0]);
      PhoenixDoAsCallback callback = new PhoenixDoAsCallback(serverUgi, conf);

      UserGroupInformation user1 = callback.doAsRemoteUser("user1", "localhost:1234", new Callable<UserGroupInformation>() {
          public UserGroupInformation call() throws Exception {
            return UserGroupInformation.getCurrentUser();
          }
      });

      UserGroupInformation user2 = callback.doAsRemoteUser("user2", "localhost:1235", new Callable<UserGroupInformation>() {
          public UserGroupInformation call() throws Exception {
            return UserGroupInformation.getCurrentUser();
          }
      });

      UserGroupInformation user1Reference = callback.doAsRemoteUser("user1", "localhost:1234", new Callable<UserGroupInformation>() {
          public UserGroupInformation call() throws Exception {
            return UserGroupInformation.getCurrentUser();
          }
      });

      // The UserGroupInformation.getCurrentUser() actually returns a new UGI instance, but the internal
      // subject is the same. We can verify things will work as expected that way.
      assertNotEquals(user1.hashCode(), user2.hashCode());
      assertEquals("These should be the same (cached) instance", user1.hashCode(), user1Reference.hashCode());
      assertEquals("These should be the same (cached) instance", user1, user1Reference);
    }
}
