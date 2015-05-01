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

import com.google.common.annotations.VisibleForTesting;
import org.apache.calcite.avatica.Meta;
import org.apache.calcite.avatica.remote.LocalService;
import org.apache.calcite.avatica.remote.Service;
import org.apache.calcite.avatica.server.AvaticaHandler;
import org.apache.calcite.avatica.server.HttpServer;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.util.Strings;
import org.apache.hadoop.net.DNS;
import org.apache.hadoop.security.SecurityUtil;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.util.Arrays;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

/**
 * A query server for Phoenix over Calcite's Avatica.
 */
public final class Main extends Configured implements Tool, Runnable {

  public static final String QUERY_SERVER_META_FACTORY_KEY =
      "phoenix.queryserver.metafactory.class";

  public static final String QUERY_SERVER_HTTP_PORT_KEY =
      "phoenix.queryserver.http.port";
  public static final int DEFAULT_HTTP_PORT = 8765;

  public static final String KEYTAB_FILENAME_KEY = "phoenix.queryserver.keytab.file";
  public static final String KERBEROS_PRINCIPAL_KEY = "phoenix.queryserver.kerberos.principal";
  public static final String DNS_NAMESERVER_KEY = "phoenix.queryserver.dns.nameserver";
  public static final String DNS_INTERFACE_KEY = "phoenix.queryserver.dns.interface";
  public static final String HBASE_SECURITY_CONF_KEY = "hbase.security.authentication";

  protected static final Log LOG = LogFactory.getLog(Main.class);

  private final String[] argv;
  private final CountDownLatch runningLatch = new CountDownLatch(1);
  private HttpServer server = null;
  private int retCode = 0;
  private Throwable t = null;

  /** Constructor for use from {@link org.apache.hadoop.util.ToolRunner}. */
  public Main() {
    this(null, null);
  }

  /** Constructor for use as {@link java.lang.Runnable}. */
  public Main(String[] argv, Configuration conf) {
    this.argv = argv;
    setConf(conf);
  }

  /**
   * @return the port number this instance is bound to, or {@code -1} if the server is not running.
   */
  @VisibleForTesting
  public int getPort() {
    if (server == null) return -1;
    return server.getPort();
  }

  /**
   * @return the return code from running as a {@link Tool}.
   */
  @VisibleForTesting
  public int getRetCode() {
    return retCode;
  }

  /**
   * @return the throwable from an unsuccessful run, or null otherwise.
   */
  @VisibleForTesting
  public Throwable getThrowable() {
    return t;
  }

  /** Calling thread waits until the server is running. */
  public void awaitRunning() throws InterruptedException {
    runningLatch.await();
  }

  /** Calling thread waits until the server is running. */
  public void awaitRunning(long timeout, TimeUnit unit) throws InterruptedException {
    runningLatch.await(timeout, unit);
  }

  @Override
  public int run(String[] args) throws Exception {
    try {
      // handle secure cluster credentials
      if ("kerberos".equalsIgnoreCase(getConf().get(HBASE_SECURITY_CONF_KEY))) {
        String hostname = Strings.domainNamePointerToHostName(DNS.getDefaultHost(
            getConf().get(DNS_INTERFACE_KEY, "default"),
            getConf().get(DNS_NAMESERVER_KEY, "default")));
        if (LOG.isDebugEnabled()) {
          LOG.debug("Login to " + hostname + " using " + getConf().get(KEYTAB_FILENAME_KEY)
              + " and principal " + getConf().get(KERBEROS_PRINCIPAL_KEY) + ".");
        }
        SecurityUtil.login(getConf(), KEYTAB_FILENAME_KEY, KERBEROS_PRINCIPAL_KEY, hostname);
        LOG.info("Login successful.");
      }
      Class<? extends PhoenixMetaFactory> factoryClass = getConf().getClass(
          QUERY_SERVER_META_FACTORY_KEY, PhoenixMetaFactoryImpl.class, PhoenixMetaFactory.class);
      int port = getConf().getInt(QUERY_SERVER_HTTP_PORT_KEY, DEFAULT_HTTP_PORT);
      LOG.debug("Listening on port " + port);
      PhoenixMetaFactory factory =
          factoryClass.getDeclaredConstructor(Configuration.class).newInstance(getConf());
      Meta meta = factory.create(Arrays.asList(args));
      Service service = new LocalService(meta);
      server = new HttpServer(port, new AvaticaHandler(service));
      server.start();
      runningLatch.countDown();
      server.join();
      return 0;
    } catch (Throwable t) {
      LOG.fatal("Unrecoverable service error. Shutting down.", t);
      this.t = t;
      return -1;
    }
  }

  @Override public void run() {
    try {
      retCode = run(argv);
    } catch (Exception e) {
      // already logged
    }
  }

  public static void main(String[] argv) throws Exception {
    int ret = ToolRunner.run(HBaseConfiguration.create(), new Main(), argv);
    System.exit(ret);
  }
}
