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
import com.google.common.base.Preconditions;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;

import org.apache.calcite.avatica.Meta;
import org.apache.calcite.avatica.remote.Driver;
import org.apache.calcite.avatica.remote.LocalService;
import org.apache.calcite.avatica.remote.Service;
import org.apache.calcite.avatica.server.DoAsRemoteUserCallback;
import org.apache.calcite.avatica.server.HttpServer;
import org.apache.calcite.avatica.server.RemoteUserExtractor;
import org.apache.calcite.avatica.server.RemoteUserExtractionException;
import org.apache.calcite.avatica.server.HttpRequestRemoteUserExtractor;
import org.apache.calcite.avatica.server.HttpQueryStringParameterRemoteUserExtractor;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.util.Strings;
import org.apache.hadoop.net.DNS;
import org.apache.hadoop.security.SecurityUtil;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.authorize.ProxyUsers;
import org.apache.hadoop.security.authorize.AuthorizationException;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.phoenix.query.QueryServices;
import org.apache.phoenix.query.QueryServicesOptions;
import org.apache.phoenix.loadbalancer.service.LoadBalanceZookeeperConf;
import org.apache.phoenix.queryserver.register.Registry;

import java.io.File;
import java.lang.management.ManagementFactory;
import java.lang.management.RuntimeMXBean;
import java.net.InetAddress;
import java.security.PrivilegedExceptionAction;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.ServiceConfigurationError;
import java.util.ServiceLoader;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import javax.servlet.http.HttpServletRequest;

/**
 * A query server for Phoenix over Calcite's Avatica.
 */
public final class QueryServer extends Configured implements Tool, Runnable {

  protected static final Log LOG = LogFactory.getLog(QueryServer.class);

  private final String[] argv;
  private final CountDownLatch runningLatch = new CountDownLatch(1);
  private HttpServer server = null;
  private int retCode = 0;
  private Throwable t = null;
  private Registry registry;

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
      Set<String> skipWords = new HashSet<String>(
          QueryServicesOptions.DEFAULT_QUERY_SERVER_SKIP_WORDS);
      if (conf != null) {
        String[] confSkipWords = conf.getStrings(
            QueryServices.QUERY_SERVER_ENV_LOGGING_SKIPWORDS_ATTRIB);
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
  public QueryServer() {
    this(null, null);
  }

  /** Constructor for use as {@link java.lang.Runnable}. */
  public QueryServer(String[] argv, Configuration conf) {
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
    final boolean loadBalancerEnabled = getConf().getBoolean(QueryServices.PHOENIX_QUERY_SERVER_LOADBALANCER_ENABLED,
            QueryServicesOptions.DEFAULT_PHOENIX_QUERY_SERVER_LOADBALANCER_ENABLED);
    try {
      final boolean isKerberos = "kerberos".equalsIgnoreCase(getConf().get(
          QueryServices.QUERY_SERVER_HBASE_SECURITY_CONF_ATTRIB));
      final boolean disableSpnego = getConf().getBoolean(QueryServices.QUERY_SERVER_SPNEGO_AUTH_DISABLED_ATTRIB,
              QueryServicesOptions.DEFAULT_QUERY_SERVER_SPNEGO_AUTH_DISABLED);
      String hostname;
      final boolean disableLogin = getConf().getBoolean(QueryServices.QUERY_SERVER_DISABLE_KERBEROS_LOGIN,
              QueryServicesOptions.DEFAULT_QUERY_SERVER_DISABLE_KERBEROS_LOGIN);

      // handle secure cluster credentials
      if (isKerberos && !disableSpnego && !disableLogin) {
        hostname = Strings.domainNamePointerToHostName(DNS.getDefaultHost(
            getConf().get(QueryServices.QUERY_SERVER_DNS_INTERFACE_ATTRIB, "default"),
            getConf().get(QueryServices.QUERY_SERVER_DNS_NAMESERVER_ATTRIB, "default")));
        if (LOG.isDebugEnabled()) {
          LOG.debug("Login to " + hostname + " using " + getConf().get(
              QueryServices.QUERY_SERVER_KEYTAB_FILENAME_ATTRIB)
              + " and principal " + getConf().get(
                  QueryServices.QUERY_SERVER_KERBEROS_PRINCIPAL_ATTRIB) + ".");
        }
        SecurityUtil.login(getConf(), QueryServices.QUERY_SERVER_KEYTAB_FILENAME_ATTRIB,
            QueryServices.QUERY_SERVER_KERBEROS_PRINCIPAL_ATTRIB, hostname);
        LOG.info("Login successful.");
      } else {
        hostname = InetAddress.getLocalHost().getHostName();
        LOG.info(" Kerberos is off and hostname is : "+hostname);
      }

      Class<? extends PhoenixMetaFactory> factoryClass = getConf().getClass(
          QueryServices.QUERY_SERVER_META_FACTORY_ATTRIB, PhoenixMetaFactoryImpl.class,
          PhoenixMetaFactory.class);
      int port = getConf().getInt(QueryServices.QUERY_SERVER_HTTP_PORT_ATTRIB,
          QueryServicesOptions.DEFAULT_QUERY_SERVER_HTTP_PORT);
      LOG.debug("Listening on port " + port);
      PhoenixMetaFactory factory =
          factoryClass.getDeclaredConstructor(Configuration.class).newInstance(getConf());
      Meta meta = factory.create(Arrays.asList(args));
      Service service = new LocalService(meta);

      // Start building the Avatica HttpServer
      final HttpServer.Builder builder = new HttpServer.Builder().withPort(port)
          .withHandler(service, getSerialization(getConf()));

      // Enable SPNEGO and Impersonation when using Kerberos
      if (isKerberos) {
        UserGroupInformation ugi = UserGroupInformation.getCurrentUser();
        LOG.debug("Current user is " + ugi);
        if (!ugi.hasKerberosCredentials()) {
          ugi = UserGroupInformation.getLoginUser();
          LOG.debug("Current user does not have Kerberos credentials, using instead " + ugi);
        }

        // Make sure the proxyuser configuration is up to date
        ProxyUsers.refreshSuperUserGroupsConfiguration(getConf());

        String keytabPath = getConf().get(QueryServices.QUERY_SERVER_KEYTAB_FILENAME_ATTRIB);
        File keytab = new File(keytabPath);

        String realmsString = getConf().get(QueryServices.QUERY_SERVER_KERBEROS_ALLOWED_REALMS, null);
        String[] additionalAllowedRealms = null;
        if (null != realmsString) {
            additionalAllowedRealms = StringUtils.split(realmsString, ',');
        }

        // Enable SPNEGO and impersonation (through standard Hadoop configuration means)
        builder.withSpnego(ugi.getUserName(), additionalAllowedRealms)
            .withAutomaticLogin(keytab)
            .withImpersonation(new PhoenixDoAsCallback(ugi, getConf()));

      }
      setRemoteUserExtractorIfNecessary(builder, getConf());

      // Build and start the HttpServer
      server = builder.build();
      server.start();
      if (loadBalancerEnabled) {
        registerToServiceProvider(hostname);
      }
      runningLatch.countDown();
      server.join();
      return 0;
    } catch (Throwable t) {
      LOG.fatal("Unrecoverable service error. Shutting down.", t);
      this.t = t;
      return -1;
    } finally {
      if (loadBalancerEnabled) {
        unRegister();
      }
    }
  }

  public synchronized void stop() {
    server.stop();
  }

  public boolean registerToServiceProvider(String hostName)  {

    boolean success = true ;
    try {
      LoadBalanceZookeeperConf loadBalanceConfiguration = getLoadBalanceConfiguration();
      Preconditions.checkNotNull(loadBalanceConfiguration);
      this.registry = getRegistry();
      Preconditions.checkNotNull(registry);
      String zkConnectString = loadBalanceConfiguration.getZkConnectString();
      this.registry.registerServer(loadBalanceConfiguration, getPort(), zkConnectString, hostName);
    } catch(Throwable ex){
      LOG.debug("Caught an error trying to register with the load balancer", ex);
      success = false;
    } finally {
      return success;
    }
  }


  public LoadBalanceZookeeperConf getLoadBalanceConfiguration()  {
    ServiceLoader<LoadBalanceZookeeperConf> serviceLocator= ServiceLoader.load(LoadBalanceZookeeperConf.class);
    LoadBalanceZookeeperConf zookeeperConfig = null;
    try {
      if (serviceLocator.iterator().hasNext())
        zookeeperConfig = serviceLocator.iterator().next();
    } catch(ServiceConfigurationError ex) {
      LOG.debug("Unable to locate the service provider for load balancer configuration", ex);
    } finally {
      return zookeeperConfig;
    }
  }

  public Registry getRegistry()  {
    ServiceLoader<Registry> serviceLocator= ServiceLoader.load(Registry.class);
    Registry registry = null;
    try {
      if (serviceLocator.iterator().hasNext())
        registry = serviceLocator.iterator().next();
    } catch(ServiceConfigurationError ex) {
      LOG.debug("Unable to locate the zookeeper registry for the load balancer", ex);
    } finally {
      return registry;
    }
  }

  public boolean unRegister()  {
    boolean success = true;
    try {
      registry.unRegisterServer();
    }catch(Throwable ex) {
      LOG.debug("Caught an error while de-registering the query server from the load balancer",ex);
      success = false;
    } finally {
      return success;
    }
  }
  /**
   * Parses the serialization method from the configuration.
   *
   * @param conf The configuration to parse
   * @return The Serialization method
   */
  Driver.Serialization getSerialization(Configuration conf) {
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

    return serialization;
  }

  @Override public void run() {
    try {
      retCode = run(argv);
    } catch (Exception e) {
      // already logged
    }
  }

  // add remoteUserExtractor to builder if enabled
  @VisibleForTesting
  public void setRemoteUserExtractorIfNecessary(HttpServer.Builder builder, Configuration conf) {
    if (conf.getBoolean(QueryServices.QUERY_SERVER_WITH_REMOTEUSEREXTRACTOR_ATTRIB,
            QueryServicesOptions.DEFAULT_QUERY_SERVER_WITH_REMOTEUSEREXTRACTOR)) {
      builder.withRemoteUserExtractor(new PhoenixRemoteUserExtractor(conf));
    }
  }

  /**
   * Use the correctly way to extract end user.
   */

  static class PhoenixRemoteUserExtractor implements RemoteUserExtractor{
    private final HttpQueryStringParameterRemoteUserExtractor paramRemoteUserExtractor;
    private final HttpRequestRemoteUserExtractor requestRemoteUserExtractor;
    private final String userExtractParam;

    public PhoenixRemoteUserExtractor(Configuration conf) {
      this.requestRemoteUserExtractor = new HttpRequestRemoteUserExtractor();
      this.userExtractParam = conf.get(QueryServices.QUERY_SERVER_REMOTEUSEREXTRACTOR_PARAM,
              QueryServicesOptions.DEFAULT_QUERY_SERVER_REMOTEUSEREXTRACTOR_PARAM);
      this.paramRemoteUserExtractor = new HttpQueryStringParameterRemoteUserExtractor(userExtractParam);
    }

    @Override
    public String extract(HttpServletRequest request) throws RemoteUserExtractionException {
      if (request.getParameter(userExtractParam) != null) {
        String extractedUser = paramRemoteUserExtractor.extract(request);
        UserGroupInformation ugi = UserGroupInformation.createRemoteUser(request.getRemoteUser());
        UserGroupInformation proxyUser = UserGroupInformation.createProxyUser(extractedUser, ugi);

        // Check if this user is allowed to be impersonated.
        // Will throw AuthorizationException if the impersonation as this user is not allowed
        try {
          ProxyUsers.authorize(proxyUser, request.getRemoteAddr());
          return extractedUser;
        } catch (AuthorizationException e) {
          throw new RemoteUserExtractionException(e.getMessage(), e);
        }
      } else {
        if (LOG.isDebugEnabled()) {
          LOG.debug("The parameter (" + userExtractParam + ") used to extract the remote user doesn't exist in the request.");
        }
        return requestRemoteUserExtractor.extract(request);
      }

    }
  }

  /**
   * Callback to run the Avatica server action as the remote (proxy) user instead of the server.
   */
  static class PhoenixDoAsCallback implements DoAsRemoteUserCallback {
    private final UserGroupInformation serverUgi;
    private final LoadingCache<String,UserGroupInformation> ugiCache;

    public PhoenixDoAsCallback(UserGroupInformation serverUgi, Configuration conf) {
      this.serverUgi = Objects.requireNonNull(serverUgi);
      this.ugiCache = CacheBuilder.newBuilder()
          .initialCapacity(conf.getInt(QueryServices.QUERY_SERVER_UGI_CACHE_INITIAL_SIZE,
                  QueryServicesOptions.DEFAULT_QUERY_SERVER_UGI_CACHE_INITIAL_SIZE))
          .concurrencyLevel(conf.getInt(QueryServices.QUERY_SERVER_UGI_CACHE_CONCURRENCY,
                  QueryServicesOptions.DEFAULT_QUERY_SERVER_UGI_CACHE_CONCURRENCY))
          .maximumSize(conf.getLong(QueryServices.QUERY_SERVER_UGI_CACHE_MAX_SIZE,
                  QueryServicesOptions.DEFAULT_QUERY_SERVER_UGI_CACHE_MAX_SIZE))
          .build(new UgiCacheLoader(this.serverUgi));
    }

    @Override
    public <T> T doAsRemoteUser(String remoteUserName, String remoteAddress,
        final Callable<T> action) throws Exception {
      // We are guaranteed by Avatica that the `remoteUserName` is properly authenticated by the
      // time this method is called. We don't have to verify the wire credentials, we can assume the
      // user provided valid credentials for who it claimed it was.

      // Proxy this user on top of the server's user (the real user). Get a cached instance, the
      // LoadingCache will create a new instance for us if one isn't cached.
      UserGroupInformation proxyUser = createProxyUser(remoteUserName);

      // Execute the actual call as this proxy user
      return proxyUser.doAs(new PrivilegedExceptionAction<T>() {
        @Override
        public T run() throws Exception {
          return action.call();
        }
      });
    }

      @VisibleForTesting
      UserGroupInformation createProxyUser(String remoteUserName) throws ExecutionException {
          // PHOENIX-3164 UGI's hashCode and equals methods rely on reference checks, not
          // value-based checks. We need to make sure we return the same UGI instance for a remote
          // user, otherwise downstream code in Phoenix and HBase may not treat two of the same
          // calls from one user as equivalent.
          return ugiCache.get(remoteUserName);
      }

      @VisibleForTesting
      LoadingCache<String,UserGroupInformation> getCache() {
          return ugiCache;
      }
  }

  /**
   * CacheLoader implementation which creates a "proxy" UGI instance for the given user name.
   */
  static class UgiCacheLoader extends CacheLoader<String,UserGroupInformation> {
      private final UserGroupInformation serverUgi;

      public UgiCacheLoader(UserGroupInformation serverUgi) {
          this.serverUgi = Objects.requireNonNull(serverUgi);
      }

      @Override
      public UserGroupInformation load(String remoteUserName) throws Exception {
          return UserGroupInformation.createProxyUser(remoteUserName, serverUgi);
      }
  }

  public static void main(String[] argv) throws Exception {
    int ret = ToolRunner.run(HBaseConfiguration.create(), new QueryServer(), argv);
    System.exit(ret);
  }
}
