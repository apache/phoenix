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
package org.apache.phoenix.queryserver.client;

import java.security.PrivilegedExceptionAction;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.UserGroupInformation;

import sqlline.SqlLine;

/**
 * Utility class which automatically performs a Kerberos login and then launches sqlline. Tries to
 * make a pre-populated ticket cache (via kinit before launching) transparently work.
 */
public class SqllineWrapper {
  public static final String HBASE_AUTHENTICATION_ATTR = "hbase.security.authentication";
  public static final String QUERY_SERVER_SPNEGO_AUTH_DISABLED_ATTRIB = "phoenix.queryserver.spnego.auth.disabled";
  public static final boolean DEFAULT_QUERY_SERVER_SPNEGO_AUTH_DISABLED = false;

  static UserGroupInformation loginIfNecessary(Configuration conf) {
    // Try to avoid HBase dependency too. Sadly, we have to bring in all of hadoop-common for this..
    if ("kerberos".equalsIgnoreCase(conf.get(HBASE_AUTHENTICATION_ATTR))) {
      // sun.security.krb5.principal is the property for setting the principal name, if that
      // isn't set, fall back to user.name and hope for the best.
      String principal = System.getProperty("sun.security.krb5.principal", System.getProperty("user.name"));
      try {
        // We got hadoop-auth via hadoop-common, so might as well use it.
        return UserGroupInformation.getUGIFromTicketCache(null, principal);
      } catch (Exception e) {
        throw new RuntimeException("Kerberos login failed using ticket cache. Did you kinit?", e);
      }
    }
    return null;
  }

  private static String[] updateArgsForKerberos(String[] origArgs) {
    String[] newArgs = new String[origArgs.length];
    for (int i = 0; i < origArgs.length; i++) {
      String arg = origArgs[i];
      newArgs[i] = arg;

      if (arg.equals("-u")) {
        // Get the JDBC url which is the next argument
        i++;
        arg = origArgs[i];
        if (!arg.contains("authentication=")) {
          arg = arg + ";authentication=SPNEGO";
        }
        newArgs[i] = arg;
      }
    }
    return newArgs;
  }

  public static void main(String[] args) throws Exception {
    final Configuration conf = new Configuration(false);
    conf.addResource("hbase-site.xml");

    // Check if the server config says SPNEGO auth is actually disabled.
    final boolean disableSpnego = conf.getBoolean(QUERY_SERVER_SPNEGO_AUTH_DISABLED_ATTRIB,
        DEFAULT_QUERY_SERVER_SPNEGO_AUTH_DISABLED);
    if (disableSpnego) {
      SqlLine.main(args);
    }

    UserGroupInformation ugi = loginIfNecessary(conf);

    if (null != ugi) {
      final String[] updatedArgs = updateArgsForKerberos(args);
      ugi.doAs(new PrivilegedExceptionAction<Void>() {
        @Override
        public Void run() throws Exception {
          SqlLine.main(updatedArgs);
          return null;
        }
      });
    } else {
      SqlLine.main(args);
    }
  }

}
