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
import org.apache.calcite.avatica.remote.Driver;
import org.apache.calcite.avatica.remote.LocalService;
import org.apache.calcite.avatica.remote.Service;
import org.apache.calcite.avatica.server.HandlerFactory;
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
import org.apache.phoenix.query.QueryServices;
import org.apache.phoenix.query.QueryServicesOptions;
import org.eclipse.jetty.server.Handler;

import java.lang.management.ManagementFactory;
import java.lang.management.RuntimeMXBean;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

/**
 * A query server for Phoenix over Calcite's Avatica.
 */
public final class Main extends Configured implements Tool, Runnable {

  protected static final Log LOG = LogFactory.getLog(Main.class);

  private final String[] argv;
  private final CountDownLatch runningLatch = new CountDownLatch(1);
  private HttpServer server = null;
  private int retCode = 0;
  private Throwable t = null;

  /**
   * Log information about the currently running JVM.
   */
  public static void logJVMInfo() {
    // Print out vm stats before starting up.
    RuntimeMXBean runtime = ManagementFactory.getRuntimeMXBean();
    if (runtime != null) {
      LOG.info("vmName=" + runtime.getVmName() + ", vmVendor=" +
              runtime.getVmVendor() + ", vmVersion=" + runtime.getVmVersion());
      LOG.info("vmInputArguments=" + runtime.getInputArguments());
    }
  }

  /**
   * Logs information about the currently running JVM process including
   * the environment variables. Logging of env vars can be disabled by
   * setting {@code "phoenix.envvars.logging.disabled"} to {@code "true"}.
   * <p>If enabled, you can also exclude environment variables containing
   * certain substrings by setting {@code "phoenix.envvars.logging.skipwords"}
   * to comma separated list of such substrings.
   */
  public static void logProcessInfo(Configuration conf) {
    // log environment variables unless asked not to
    if (conf == null || !conf.getBoolean(QueryServices.QUERY_SERVER_ENV_LOGGING_ATTRIB, false)) {
      Set<String> skipWords = new HashSet<String>(QueryServicesOptions.DEFAULT_QUERY_SERVER_SKIP_WORDS);
      if (conf != null) {
        String[] confSkipWords = conf.getStrings(QueryServices.QUERY_SERVER_ENV_LOGGING_SKIPWORDS_ATTRIB);
        if (confSkipWords != null) {
          skipWords.addAll(Arrays.asList(confSkipWords));
        }
      }

      nextEnv:
      for (Map.Entry<String, String> entry : System.getenv().entrySet()) {
        String key = entry.getKey().toLowerCase();
        String value = entry.getValue().toLowerCase();
        // exclude variables which may contain skip words
        for(String skipWord : skipWords) {
          if (key.contains(skipWord) || value.contains(skipWord))
            continue nextEnv;
        }
        LOG.info("env:"+entry);
      }
    }
    // and JVM info
    logJVMInfo();
  }

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
    logProcessInfo(getConf());
    try {
      // handle secure cluster credentials
      if ("kerberos".equalsIgnoreCase(getConf().get(QueryServices.QUERY_SERVER_HBASE_SECURITY_CONF_ATTRIB))) {
        String hostname = Strings.domainNamePointerToHostName(DNS.getDefaultHost(
            getConf().get(QueryServices.QUERY_SERVER_DNS_INTERFACE_ATTRIB, "default"),
            getConf().get(QueryServices.QUERY_SERVER_DNS_NAMESERVER_ATTRIB, "default")));
        if (LOG.isDebugEnabled()) {
          LOG.debug("Login to " + hostname + " using " + getConf().get(QueryServices.QUERY_SERVER_KEYTAB_FILENAME_ATTRIB)
              + " and principal " + getConf().get(QueryServices.QUERY_SERVER_KERBEROS_PRINCIPAL_ATTRIB) + ".");
        }
        SecurityUtil.login(getConf(), QueryServices.QUERY_SERVER_KEYTAB_FILENAME_ATTRIB,
            QueryServices.QUERY_SERVER_KERBEROS_PRINCIPAL_ATTRIB, hostname);
        LOG.info("Login successful.");
      }
      Class<? extends PhoenixMetaFactory> factoryClass = getConf().getClass(
          QueryServices.QUERY_SERVER_META_FACTORY_ATTRIB, PhoenixMetaFactoryImpl.class, PhoenixMetaFactory.class);
      int port = getConf().getInt(QueryServices.QUERY_SERVER_HTTP_PORT_ATTRIB,
          QueryServicesOptions.DEFAULT_QUERY_SERVER_HTTP_PORT);
      LOG.debug("Listening on port " + port);
      PhoenixMetaFactory factory =
          factoryClass.getDeclaredConstructor(Configuration.class).newInstance(getConf());
      Meta meta = factory.create(Arrays.asList(args));
      final HandlerFactory handlerFactory = new HandlerFactory();
      Service service = new LocalService(meta);
      server = new HttpServer(port, getHandler(getConf(), service, handlerFactory));
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

  /**
   * Instantiates the Handler for use by the Avatica (Jetty) server.
   *
   * @param conf The configuration
   * @param service The Avatica Service implementation
   * @param handlerFactory Factory used for creating a Handler
   * @return The Handler to use based on the configuration.
   */
  Handler getHandler(Configuration conf, Service service, HandlerFactory handlerFactory) {
    String serializationName = conf.get(QueryServices.QUERY_SERVER_SERIALIZATION_ATTRIB,
        QueryServicesOptions.DEFAULT_QUERY_SERVER_SERIALIZATION);

    Driver.Serialization serialization;
    // Otherwise, use what was provided in the configuration
    try {
      serialization = Driver.Serialization.valueOf(serializationName);
    } catch (Exception e) {
      LOG.error("Unknown message serialization type for " + serializationName);
      throw e;
    }

    Handler handler = handlerFactory.getHandler(service, serialization);

    LOG.info("Instantiated " + handler.getClass() + " for QueryServer");

    return handler;
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
