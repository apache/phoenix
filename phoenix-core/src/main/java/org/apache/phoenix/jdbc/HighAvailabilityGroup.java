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
package org.apache.phoenix.jdbc;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;
import org.apache.curator.RetryPolicy;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.recipes.cache.NodeCache;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.curator.utils.ZKPaths;
import org.apache.hadoop.hbase.util.PairOfSameType;
import org.apache.phoenix.exception.SQLExceptionCode;
import org.apache.phoenix.exception.SQLExceptionInfo;
import org.apache.phoenix.jdbc.ClusterRoleRecord.ClusterRole;
import org.apache.phoenix.thirdparty.com.google.common.annotations.VisibleForTesting;
import org.apache.phoenix.thirdparty.com.google.common.base.Preconditions;
import org.apache.phoenix.thirdparty.com.google.common.base.Strings;
import org.apache.phoenix.thirdparty.com.google.common.cache.Cache;
import org.apache.phoenix.thirdparty.com.google.common.cache.CacheBuilder;
import org.apache.phoenix.thirdparty.com.google.common.util.concurrent.ThreadFactoryBuilder;
import org.apache.phoenix.util.JDBCUtil;
import org.apache.phoenix.util.PhoenixRuntime;
import org.apache.zookeeper.data.Stat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import edu.umd.cs.findbugs.annotations.NonNull;

import java.io.IOException;
import java.sql.Connection;
import java.sql.Driver;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Properties;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicReference;

import static org.apache.phoenix.query.QueryServicesOptions.DEFAULT_CLIENT_CONNECTION_CACHE_MAX_DURATION;

/**
 * An high availability (HA) group is an association between a pair of HBase clusters, a group of
 * clients, and an HA policy.
 * <p>
 * This class is thread safe. Multiple threads may access an instance of this class, including
 * multiple clients that call init in order to create a connection, two cluster role managers that
 * watches node changes in ZooKeeper.
 * <p>
 * The lifecycle of an HA group is confined in the global cache, meaning clients can get an instance
 * from the cache but cannot construct or close an HA group instance.  The reason is that HA group
 * is a shared resource by many clients.  Closing it intentionally or accidentally by a client will
 * impact other connections in this group with unexpected behavior.
 */
@SuppressWarnings("UnstableApiUsage")
public class HighAvailabilityGroup {
    public static final String PHOENIX_HA_ATTR_PREFIX = "phoenix.ha.";
    public static final String PHOENIX_HA_GROUP_ATTR = PHOENIX_HA_ATTR_PREFIX + "group.name";
    /**
     * Should we fall back to single cluster when cluster role record is missing?
     */
    public static final String PHOENIX_HA_SHOULD_FALLBACK_WHEN_MISSING_CRR_KEY =
            PHOENIX_HA_ATTR_PREFIX + "fallback.enabled";
    public static final String PHOENIX_HA_SHOULD_FALLBACK_WHEN_MISSING_CRR_DEFAULT =
            String.valueOf(Boolean.TRUE);
    /**
     * The single-cluster connection URL when it needs to fall back.
     */
    public static final String PHOENIX_HA_FALLBACK_CLUSTER_KEY =
            PHOENIX_HA_ATTR_PREFIX + "fallback.cluster";
    public static final String PHOENIX_HA_ZOOKEEPER_ZNODE_NAMESPACE =
            "phoenix" + ZKPaths.PATH_SEPARATOR + "ha";

    public static final String PHOENIX_HA_ZK_CONNECTION_TIMEOUT_MS_KEY =
            PHOENIX_HA_ATTR_PREFIX + "zk.connection.timeout.ms";
    public static final int PHOENIX_HA_ZK_CONNECTION_TIMEOUT_MS_DEFAULT = 4_000;
    public static final String PHOENIX_HA_ZK_SESSION_TIMEOUT_MS_KEY =
            PHOENIX_HA_ATTR_PREFIX + "zk.session.timeout.ms";
    public static final int PHOENIX_HA_ZK_SESSION_TIMEOUT_MS_DEFAULT = 4_000;
    public static final String PHOENIX_HA_ZK_RETRY_BASE_SLEEP_MS_KEY =
            PHOENIX_HA_ATTR_PREFIX + "zk.retry.base.sleep.ms";

    public static final int PHOENIX_HA_ZK_RETRY_BASE_SLEEP_MS_DEFAULT = 1000;
    public static final String PHOENIX_HA_ZK_RETRY_MAX_KEY =
            PHOENIX_HA_ATTR_PREFIX + "zk.retry.max";
    public static final int PHOENIX_HA_ZK_RETRY_MAX_DEFAULT = 5;
    public static final String PHOENIX_HA_ZK_RETRY_MAX_SLEEP_MS_KEY =
            PHOENIX_HA_ATTR_PREFIX + "zk.retry.max.sleep.ms";
    public static final int PHOENIX_HA_ZK_RETRY_MAX_SLEEP_MS_DEFAULT = 10_000;
    public static final RetryPolicy RETRY_POLICY = new ExponentialBackoffRetry(
            PHOENIX_HA_ZK_RETRY_BASE_SLEEP_MS_DEFAULT,
            PHOENIX_HA_ZK_RETRY_MAX_DEFAULT,
            PHOENIX_HA_ZK_RETRY_MAX_SLEEP_MS_DEFAULT);

    public static final String PHOENIX_HA_TRANSITION_TIMEOUT_MS_KEY =
            PHOENIX_HA_ATTR_PREFIX + "transition.timeout.ms";
    public static final long PHOENIX_HA_TRANSITION_TIMEOUT_MS_DEFAULT = 5 * 60 * 1000; // 5 mins

    static final Logger LOG = LoggerFactory.getLogger(HighAvailabilityGroup.class);
    @VisibleForTesting
    static final Map<HAGroupInfo, HighAvailabilityGroup> GROUPS = new ConcurrentHashMap<>();
    @VisibleForTesting
    static final Cache<HAGroupInfo, Boolean> MISSING_CRR_GROUPS_CACHE = CacheBuilder.newBuilder()
            .expireAfterWrite(PHOENIX_HA_TRANSITION_TIMEOUT_MS_DEFAULT, TimeUnit.MILLISECONDS)
            .build();
    /**
     * The Curator client cache, one client instance per cluster.
     */
    @VisibleForTesting
    static final Cache<String, CuratorFramework> CURATOR_CACHE = CacheBuilder.newBuilder()
            .expireAfterAccess(DEFAULT_CLIENT_CONNECTION_CACHE_MAX_DURATION, TimeUnit.MILLISECONDS)
            .removalListener((notification) ->
                    ((CuratorFramework) Objects.requireNonNull(notification.getValue())).close())
            .build();
    /**
     * High availability group info.
     */
    private final HAGroupInfo info;
    /**
     * Client properties used to initialize this HA group.
     */
    private final Properties properties;
    /**
     * Executor service for the two role managers.
     */
    private final ExecutorService roleManagerExecutor = Executors.newFixedThreadPool(2,
            new ThreadFactoryBuilder().setDaemon(true).setNameFormat("phoenixHAGroup-%d").build());
    /**
     * The count down latch to make sure at least one role manager has pulled data from ZK.
     */
    private final CountDownLatch roleManagerLatch = new CountDownLatch(1);
    /**
     * Pair of role managers for watching cluster role records from the two ZK clusters.
     */
    private final AtomicReference<PairOfSameType<HAClusterRoleManager>> roleManagers
            = new AtomicReference<>();
    /**
     * Executor for applying the cluster role to this HA group.
     */
    private final ExecutorService nodeChangedExecutor = Executors.newFixedThreadPool(1);
    /**
     * Current cluster role record for this HA group.
     */
    private volatile ClusterRoleRecord roleRecord;
    /**
     * State of this HA group.
     */
    private volatile State state = State.UNINITIALIZED;

    /**
     * Private constructor.
     * <p>
     * To get an instance, please call {@link HighAvailabilityGroup#get(String, Properties)}.
     */
    private HighAvailabilityGroup(HAGroupInfo info, Properties properties) {
        this.info = info;
        this.properties = properties;
    }
    /**
     * This is for test usage only. In production, the record should be retrieved from ZooKeeper.
     */
    @VisibleForTesting
    HighAvailabilityGroup(HAGroupInfo info, Properties properties, ClusterRoleRecord record,
                          State state) {
        this.info = info;
        this.properties = properties;
        this.roleRecord = record;
        this.state = state;
    }

    public static HAGroupInfo getHAGroupInfo(String url, Properties properties)
            throws SQLException {
        if (url.startsWith(PhoenixRuntime.JDBC_PROTOCOL)) {
            url = url.substring(PhoenixRuntime.JDBC_PROTOCOL.length() + 1);
        }
        if (!(url.contains("[") && url.contains("|") && url.contains("]"))) {
            throw new SQLExceptionInfo.Builder(SQLExceptionCode.MALFORMED_CONNECTION_URL)
                    .setMessage(String.format("URL %s is not a valid HA connection string", url))
                    .build()
                    .buildException();
        }
        String additionalJDBCParams = null;
        int idx = url.indexOf("]");
        int extraIdx = url.indexOf(PhoenixRuntime.JDBC_PROTOCOL_SEPARATOR, idx + 1);
        if (extraIdx != -1) {
            // skip the JDBC_PROTOCOL_SEPARATOR
            additionalJDBCParams  = url.substring(extraIdx + 1);
        }

        url = url.substring(url.indexOf("[") + 1, url.indexOf("]"));
        String[] urls = url.split("\\|");

        String name = properties.getProperty(PHOENIX_HA_GROUP_ATTR);
        if (StringUtils.isEmpty(name)) {
            throw new SQLExceptionInfo.Builder(SQLExceptionCode.HA_INVALID_PROPERTIES)
                    .setMessage(String.format("HA group name can not be empty for HA URL %s", url))
                    .build()
                    .buildException();
        }
        return new HAGroupInfo(name, urls[0], urls[1], additionalJDBCParams);
    }

    /**
     * Get an instance of HA group given the HA connecting URL (with "|") and client properties.
     * <p>
     * The HA group does not have a public constructor. This method is the only public one for
     * getting an HA group instance. The reason is that, HA group is considered expensive to create
     * and maintain. Caching it will make it reusable for all connection requests to this group.
     * <p>
     * It will return the cached instance, if any, for the target HA group. The HA group creation
     * and initialization are blocking operations. Upon initialization failure, the HA group
     * information may be saved in a negative cache iff the cause is due to missing cluster role
     * records. In presence of empty (not null or exception) return value, client may choose to fall
     * back to a single cluster connection to compensate missing cluster role records.
     *
     * @return Optional of target HA group (initialized), or empty if missing cluster role records
     * @throws SQLException fails to get or initialize an HA group
     */
    public static Optional<HighAvailabilityGroup> get(String url, Properties properties)
            throws SQLException {
        HAGroupInfo info = getHAGroupInfo(url, properties);
        if (MISSING_CRR_GROUPS_CACHE.getIfPresent(info) != null) {
            return Optional.empty();
        }

        HighAvailabilityGroup haGroup = GROUPS.computeIfAbsent(info,
                haGroupInfo -> new HighAvailabilityGroup(haGroupInfo, properties));
        try {
            haGroup.init();
        } catch (Exception e) {
            GROUPS.remove(info);
            haGroup.close();
            try {
                CuratorFramework curator1 = CURATOR_CACHE.getIfPresent(info.getUrl1());
                CuratorFramework curator2 = CURATOR_CACHE.getIfPresent(info.getUrl2());
                if (curator1 != null && curator2 != null) {
                    Stat node1 = curator1.checkExists().forPath(info.getZkPath());
                    Stat node2 = curator2.checkExists().forPath(info.getZkPath());
                    if (node1 == null && node2 == null) {
                        // The HA group fails to initialize due to missing cluster role records on
                        // both ZK clusters. We will put this HA group into negative cache.
                        MISSING_CRR_GROUPS_CACHE.put(info, true);
                        return Optional.empty();
                    }
                }
            } catch (Exception e2) {
                LOG.error("HA group {} failed to initialized. Got exception when checking if znode"
                        + " exists on the two ZK clusters.", info, e2);
            }
            throw new SQLExceptionInfo.Builder(SQLExceptionCode.CANNOT_ESTABLISH_CONNECTION)
                    .setMessage(String.format("Cannot start HA group %s for URL %s", haGroup, url))
                    .setRootCause(e)
                    .build()
                    .buildException();
        }
        return Optional.of(haGroup);
    }

    /**
     * This method helps client to get the single cluster to fallback.
     * <p>
     * When getting HA group using {@link #get(String, Properties)}, it may return empty (not null
     * or exception) value. In that case client may choose to fall back to a single cluster
     * connection to compensate missing cluster role records instead of throw errors.
     *
     * @param url        The HA connection url optionally; empty optional if properties disables fallback
     * @param properties The client connection properties
     * @return The connection url of the single cluster to fall back
     * @throws SQLException if fails to get HA information and/or invalid properties are seen
     */
    static Optional<String> getFallbackCluster(String url, Properties properties) throws SQLException {
        HAGroupInfo haGroupInfo = getHAGroupInfo(url, properties);

        String fallback = properties.getProperty(PHOENIX_HA_SHOULD_FALLBACK_WHEN_MISSING_CRR_KEY,
                PHOENIX_HA_SHOULD_FALLBACK_WHEN_MISSING_CRR_DEFAULT);
        if (!Boolean.parseBoolean(fallback)) {
            LOG.info("Fallback to single cluster not enabled for the HA group {} per configuration."
                    + " HA url: '{}'.", haGroupInfo.getName(), url);
            return Optional.empty();
        }
        String fallbackCluster = properties.getProperty(PHOENIX_HA_FALLBACK_CLUSTER_KEY);
        if (StringUtils.isEmpty(fallbackCluster)) {
            fallbackCluster = haGroupInfo.getUrl1();
        }
        LOG.info("Falling back to single cluster '{}' for the HA group {} to serve HA connection "
                        + "request against url '{}'.",
                fallbackCluster, haGroupInfo.getName(), url);
        return Optional.of(fallbackCluster);
    }

    /**
     * Get an active curator ZK client for the given properties and ZK endpoint.
     * <p>
     * This can be from cached object since Curator should be shared per cluster.
     *
     * @param jdbcUrl    the ZK endpoint host:port or the JDBC connection String host:port:/hbase
     * @param properties the properties defining time out values and retry count
     * @return a new Curator framework client
     */
    @SuppressWarnings("UnstableApiUsage")
    public static CuratorFramework getCurator(String jdbcUrl, Properties properties)
            throws IOException {
        try {
            return CURATOR_CACHE.get(jdbcUrl, () -> {
                CuratorFramework curator = createCurator(jdbcUrl, properties);
                if (!curator.blockUntilConnected(PHOENIX_HA_ZK_CONNECTION_TIMEOUT_MS_DEFAULT,
                        TimeUnit.MILLISECONDS))
                    throw new RuntimeException("Failed to connect to the CuratorFramework in "
                            + "timeout " + PHOENIX_HA_ZK_CONNECTION_TIMEOUT_MS_DEFAULT + " ms");
                return curator;
            });
        } catch (Exception e) {
            LOG.error("Fail to get an active curator for url {}", jdbcUrl, e);
            // invalidate the cache when getting/creating throws exception
            CURATOR_CACHE.invalidate(jdbcUrl);
            throw new IOException(e);
        }
    }

    /**
     * Create a curator ZK client for the given properties and ZK endpoint.
     * <p>
     * Unless caller needs a new curator, it should use {@link #getCurator(String, Properties)}.
     */
    private static CuratorFramework createCurator(String jdbcUrl, Properties properties) {
        // Get the ZK endpoint in host:port format by removing JDBC protocol and HBase root node
        final String zkUrl;
        if (jdbcUrl.startsWith(PhoenixRuntime.JDBC_PROTOCOL)) {
            jdbcUrl = jdbcUrl.substring(PhoenixRuntime.JDBC_PROTOCOL.length() + 1);
        }
        Preconditions.checkArgument(!StringUtils.isEmpty(jdbcUrl), "JDBC url is empty!");
        jdbcUrl = jdbcUrl.replaceAll("\\\\:", "=");
        String[] parts = jdbcUrl.split(":");
        if (parts.length == 0 || parts.length > 3) {
            throw new IllegalArgumentException("Invalid JDBC url!" + jdbcUrl);
        }
        // The URL is already normalised
        zkUrl = parts[0].replaceAll("=", ":");

        // Get timeout and retry counts
        String connectionTimeoutMsProp = properties.getProperty(
                PHOENIX_HA_ZK_CONNECTION_TIMEOUT_MS_KEY);
        final int connectionTimeoutMs = !StringUtils.isEmpty(connectionTimeoutMsProp)
                ? Integer.parseInt(connectionTimeoutMsProp)
                : PHOENIX_HA_ZK_CONNECTION_TIMEOUT_MS_DEFAULT;
        String sessionTimeoutMsProps = properties.getProperty(PHOENIX_HA_ZK_SESSION_TIMEOUT_MS_KEY);
        final int sessionTimeoutMs = !StringUtils.isEmpty(sessionTimeoutMsProps)
                ? Integer.parseInt(sessionTimeoutMsProps)
                : PHOENIX_HA_ZK_SESSION_TIMEOUT_MS_DEFAULT;
        final RetryPolicy retryPolicy = createRetryPolicy(properties);

        CuratorFramework curator = CuratorFrameworkFactory
                .builder()
                .connectString(zkUrl)
                .namespace(PHOENIX_HA_ZOOKEEPER_ZNODE_NAMESPACE)
                .connectionTimeoutMs(connectionTimeoutMs)
                .sessionTimeoutMs(sessionTimeoutMs)
                .retryPolicy(retryPolicy)
                .canBeReadOnly(true)
                .build();
        curator.start();
        return curator;
    }

    /**
     * Create a Curator retry policy from properties.
     * <p>
     * If properties is null, return a default retry policy.
     *
     * @param properties properties defining timeout and max retries
     * @return a retry policy which can be used for Curator operations
     */
    public static RetryPolicy createRetryPolicy(Properties properties) {
        if (properties == null) {
            return RETRY_POLICY;
        }
        String baseSleepTimeMsProp = properties.getProperty(PHOENIX_HA_ZK_RETRY_BASE_SLEEP_MS_KEY);
        int baseSleepTimeMs = StringUtils.isNotEmpty(baseSleepTimeMsProp)
                ? Integer.parseInt(baseSleepTimeMsProp)
                : PHOENIX_HA_ZK_RETRY_BASE_SLEEP_MS_DEFAULT;
        String maxRetriesProp = properties.getProperty(PHOENIX_HA_ZK_RETRY_MAX_KEY);
        int maxRetries = StringUtils.isNotEmpty(maxRetriesProp)
                ? Integer.parseInt(maxRetriesProp)
                : PHOENIX_HA_ZK_RETRY_MAX_DEFAULT;
        String maxSleepTimeMsProp = properties.getProperty(PHOENIX_HA_ZK_RETRY_MAX_SLEEP_MS_KEY);
        int maxSleepTimeMs = StringUtils.isNotEmpty(maxSleepTimeMsProp)
                ? Integer.parseInt(maxSleepTimeMsProp)
                : PHOENIX_HA_ZK_RETRY_MAX_SLEEP_MS_DEFAULT;
        return new ExponentialBackoffRetry(baseSleepTimeMs, maxRetries, maxSleepTimeMs);
    }

    /**
     * Initialize this HA group by registering ZK watchers and getting initial cluster role record.
     * <p>
     * If this is already initialized, calling this method is a no-op. This method is lock free as
     * current thread will either return fast or wait for the in-progress initialization or timeout.
     */
    public void init() throws IOException {
        if (state != State.UNINITIALIZED) {
            return;
        }

        PairOfSameType<HAClusterRoleManager> newRoleManagers = new PairOfSameType<>(
                new HAClusterRoleManager(info.urls.getFirst(), properties),
                new HAClusterRoleManager(info.urls.getSecond(), properties));
        if (!roleManagers.compareAndSet(null, newRoleManagers)) {
            LOG.info("Someone already started role managers; waiting for that one...");
            waitForInitialization(properties);
            return;
        }

        Future<?> f1 = roleManagerExecutor.submit(newRoleManagers.getFirst());
        Future<?> f2 = roleManagerExecutor.submit(newRoleManagers.getSecond());
        try {
            waitForInitialization(properties);
        } catch (IOException e) {
            // HA group that fails to initialize will not be kept in the global cache.
            // Next connection request will create and initialize a new HA group.
            // Before returning in case of exception, following code will cancel the futures.
            f1.cancel(true);
            f2.cancel(true);
            throw e;
        }

        assert roleRecord != null;
        LOG.info("Initial cluster role for HA group {} is {}", info, roleRecord);
    }

    /**
     * Helper method that will block current thread until the HA group is initialized.
     * <p>
     * After returning, the HA state might not be in READY state. That is possible when a new ZK
     * node change is detected triggering HA group to become IN_TRANSIT state.
     *
     * @param properties the connection properties
     * @throws IOException when current HA group is not initialized before timeout
     */
    private void waitForInitialization(Properties properties) throws IOException {
        String connectionTimeoutMsProp = properties.getProperty(
                PHOENIX_HA_ZK_CONNECTION_TIMEOUT_MS_KEY);
        int timeout = !StringUtils.isEmpty(connectionTimeoutMsProp)
                ? Integer.parseInt(connectionTimeoutMsProp)
                : PHOENIX_HA_ZK_CONNECTION_TIMEOUT_MS_DEFAULT;
        boolean started = false;
        try {
            started = roleManagerLatch.await(timeout, TimeUnit.MILLISECONDS);
        } catch (InterruptedException e) {
            LOG.warn("Got interrupted when waiting for cluster role managers to start", e);
            Thread.currentThread().interrupt();
        }
        if (!started) {
            LOG.warn("Timed out {}ms waiting for HA group '{}' to be initialized.", timeout, info);
            throw new IOException("Fail to initialize HA group " + info);
        }
    }

    /**
     * Create a JDBC connection in this high availability group.
     *
     * @param properties connection properties
     * @return a JDBC connection implementation
     * @throws SQLException if fails to connect a JDBC connection
     */
    public Connection connect(Properties properties) throws SQLException {
        if (state != State.READY) {
            throw new SQLExceptionInfo
                    .Builder(SQLExceptionCode.CANNOT_ESTABLISH_CONNECTION)
                    .setMessage("HA group is not ready!")
                    .setHaGroupInfo(info.toString())
                    .build()
                    .buildException();
        }
        return roleRecord.getPolicy().provide(this, properties);
    }

    /**
     * Get a Phoenix connection against the current active HBase cluster.
     * <p>
     * If there is no active cluster, it will throw exception instead of blocking or retrying.
     *
     * @param properties connection properties
     * @return a Phoenix connection to current active HBase cluster
     * @throws SQLException if fails to get a connection
     */
    PhoenixConnection connectActive(final Properties properties) throws SQLException {
        try {
            Optional<String> url = roleRecord.getActiveUrl();
            if (state == State.READY && url.isPresent()) {
                PhoenixConnection conn = connectToOneCluster(url.get(), properties);
                // After connection is created, double check if the cluster is still ACTIVE
                // This is to make sure the newly created connection will not be returned to client
                // if the target cluster is not active any more. This can happen during failover.
                boolean isActive;
                try {
                    isActive = isActive(conn);
                } catch (Exception e) {
                    conn.close();
                    throw e;
                }

                if (state == State.READY && isActive) {
                    return conn;
                } else {
                    conn.close();
                    throw new SQLExceptionInfo
                            .Builder(SQLExceptionCode.HA_CLOSED_AFTER_FAILOVER)
                            .setMessage("Cluster is not active any more in HA group. Please retry.")
                            .setHaGroupInfo(info.toString())
                            .build()
                            .buildException();
                }
            } else {
                LOG.error("Not able to connect to active cluster, state: {}, active exist: {}",
                        state, url.isPresent());
                throw new SQLExceptionInfo
                        .Builder(SQLExceptionCode.HA_NO_ACTIVE_CLUSTER)
                        .setMessage("Cannot connect to HA group because it has no active cluster")
                        .setHaGroupInfo(info.toString())
                        .build()
                        .buildException();
            }
        } catch (SQLException e) {
            LOG.error("Failed to connect to active cluster in HA group {}, record: {}", info,
                    roleRecord, e);
            throw new SQLExceptionInfo
                    .Builder(SQLExceptionCode.CANNOT_ESTABLISH_CONNECTION)
                    .setMessage("Failed to connect to active cluster in HA group")
                    .setHaGroupInfo(info.toString())
                    .setRootCause(e)
                    .build()
                    .buildException();
        }
    }

    /**
     * @return true if the given phoenix connection points to ACTIVE cluster, else false
     */
    boolean isActive(PhoenixConnection connection) {
        if (state != State.READY || connection == null) {
            return false;
        }
        return roleRecord.getActiveUrl()
                .equals(Optional.of(JDBCUtil.formatZookeeperUrl(connection.getURL())));
    }

    /**
     * Connect to an HBase cluster in this HA group with given url and client properties.
     * <p>
     * The URL should belong to one of the two ZK clusters in this HA group. It returns the Phoenix
     * connection to the given cluster without checking the context of the cluster's role. Please
     * use {@link #connectActive(Properties)} to connect to the ACTIVE cluster.
     */
    PhoenixConnection connectToOneCluster(String url, Properties properties) throws SQLException {
        Preconditions.checkNotNull(url);
        if (url.startsWith(PhoenixRuntime.JDBC_PROTOCOL)) {
            Preconditions.checkArgument(url.length() > PhoenixRuntime.JDBC_PROTOCOL.length(),
                    "The URL '" + url + "' is not a valid Phoenix connection string");
        }
        url = JDBCUtil.formatZookeeperUrl(url);
        Preconditions.checkArgument(url.equals(info.getUrl1()) || url.equals(info.getUrl2()),
                "The URL '" + url + "' does not belong to this HA group " + info);

        String jdbcString = info.getJDBCUrl(url);

        ClusterRole role = roleRecord.getRole(url);
        if (!role.canConnect()) {
            throw new SQLExceptionInfo.Builder(SQLExceptionCode.HA_CLUSTER_CAN_NOT_CONNECT)
                    .setMessage("Can not connect to cluster '" + url + "' in '" + role + "' role")
                    .build()
                    .buildException();
        }

        // Get driver instead of using PhoenixDriver.INSTANCE since it can be test or mocked driver
        Driver driver = DriverManager.getDriver(jdbcString);
        Preconditions.checkArgument(driver instanceof PhoenixEmbeddedDriver,
                "No JDBC driver is registered for Phoenix high availability (HA) framework");
        return ((PhoenixEmbeddedDriver) driver).getConnectionQueryServices(jdbcString, properties)
                .connect(jdbcString, properties);
    }

    @VisibleForTesting
    HAGroupInfo getGroupInfo() {
        return info;
    }

    Properties getProperties() {
        return properties;
    }

    public ClusterRoleRecord getRoleRecord() {
        return roleRecord;
    }

    /**
     * Package private close method.
     * <p>
     * Once this HA group is closed, it can not be re-opened again. Use a new object if necessary.
     * This method is package private because we do not want to expose the lifecycle management
     * methods to public. Constructor is also private (or package-private visible for testing).
     * The lifecycle management is confined to this class because an HA group is a shared resource.
     * Someone calling close on this would make it unusable, since the state would become closed.
     */
    void close() {
        roleManagerExecutor.shutdownNow();
        try {
            // TODO: Parameterize and set in future work item for pluggable
            if (!roleManagerExecutor.awaitTermination(PHOENIX_HA_ZK_SESSION_TIMEOUT_MS_DEFAULT,
                    TimeUnit.MILLISECONDS)) {
                LOG.error("Fail to shut down role managers service for HA group: {}", info);
            }
        } catch (InterruptedException e) {
            LOG.warn("HA group {} close() got interrupted when closing role managers", info, e);
            // (Re-)Cancel if current thread also interrupted
            roleManagerExecutor.shutdownNow();
            // Preserve interrupt status
            Thread.currentThread().interrupt();
        }
        state = State.CLOSED;
    }

    @Override
    public String toString() {
        return roleRecord == null
                ? "HighAvailabilityGroup{roleRecord=null, info=" + info + ", state=" + state + "}"
                : "HighAvailabilityGroup{roleRecord=" + roleRecord + ", state=" + state + "}";
    }

    /**
     * Set the new cluster role record for this HA group.
     * <p>
     * Calling this method will make HA group be in transition state where no request can be served.
     * The data source may come from either of the two clusters as seen by the ZK watcher.
     *
     * @param newRoleRecord the new cluster role record to set
     * @return true if the new record is set as current one; false otherwise
     */
    private synchronized boolean applyClusterRoleRecord(@NonNull ClusterRoleRecord newRoleRecord) {
        if (roleRecord == null) {
            roleRecord = newRoleRecord;
            state = State.READY;
            LOG.info("HA group {} is now in {} state after getting initial V{} role record: {}",
                    info, state, roleRecord.getVersion(), roleRecord);
            LOG.debug("HA group {} is ready", this);
            return true;
        }

        if (!newRoleRecord.isNewerThan(roleRecord)) {
            LOG.warn("Does not apply new cluster role record as it does not have higher version. "
                    + "Existing record: {}, new record: {}", roleRecord, newRoleRecord);
            return false;
        }

        if (!roleRecord.hasSameInfo(newRoleRecord)) {
            LOG.error("New record {} has different HA group information from old record {}",
                    newRoleRecord, roleRecord);
            return false;
        }

        final ClusterRoleRecord oldRecord = roleRecord;
        state = State.IN_TRANSITION;
        LOG.info("HA group {} is in {} to set V{} record", info, state, newRoleRecord.getVersion());
        Future<?> future = nodeChangedExecutor.submit(() -> {
            try {
                roleRecord.getPolicy().transitClusterRole(this, roleRecord, newRoleRecord);
            } catch (SQLException e) {
                throw new CompletionException(e);
            }
        });

        // TODO: save timeout in the HA group info (aka cluster role record) instead in properties
        String transitionTimeoutProp = properties.getProperty(PHOENIX_HA_TRANSITION_TIMEOUT_MS_KEY);
        long maxTransitionTimeMs = StringUtils.isNotEmpty(transitionTimeoutProp)
                ? Long.parseLong(transitionTimeoutProp)
                : PHOENIX_HA_TRANSITION_TIMEOUT_MS_DEFAULT;
        try {
            future.get(maxTransitionTimeMs, TimeUnit.MILLISECONDS);
        } catch (InterruptedException ie) {
            LOG.error("Got interrupted when transiting cluster roles for HA group {}", info, ie);
            future.cancel(true);
            Thread.currentThread().interrupt();
            return false;
        } catch (ExecutionException | TimeoutException e) {
            LOG.error("HA group {} failed to transit cluster roles per policy {} to new record {}",
                    info, roleRecord.getPolicy(), newRoleRecord, e);
            // Calling back HA policy function for cluster switch is conducted with best effort.
            // HA group continues transition when its HA policy fails to deal with context switch
            // (e.g. to close existing connections)
            // The goal here is to gain higher availability even though existing resources against
            // previous ACTIVE cluster may have not been closed cleanly.
        }
        roleRecord = newRoleRecord;
        state = State.READY;
        LOG.info("HA group {} is in {} state after applying V{} role record. Old: {}, new: {}",
                info, state, roleRecord.getVersion(), oldRecord, roleRecord);
        LOG.debug("HA group is ready: {}", this);
        return true;
    }

    /**
     * Local state of this HA group object, which transits upon explicit call (e.g. init) or when
     * the cluster role change is detected.
     * <p>
     * - UNINITIALIZED is the state when this HA group has not been initialized. Once the HA group
     * is initialized, it will never go to this state again.
     * - READY is the state when this HA group can serve client request. There is not necessarily
     * an active HBase cluster since a standby cluster may be sufficient per HA policy.
     * - IN_TRANSITION is the state where HA group is dealing with cluster role changes and all
     * client connection requests are rejected.
     * - CLOSED is the state where the HA group is closed. Once the HA group is closed, it will
     * never leave this state.
     */
    enum State {UNINITIALIZED, READY, IN_TRANSITION, CLOSED}

    /**
     * An HAGroupInfo contains information of an HA group.
     * <p>
     * It is constructed based on client input, including the JDBC connection string and properties.
     * Objects of this class are used as the keys of HA group cache {@link #GROUPS}.
     * <p>
     * This class is immutable.
     */
    @VisibleForTesting
    static final class HAGroupInfo {
        private final String name;
        private final PairOfSameType<String> urls;
        private final String additionalJDBCParams;

        HAGroupInfo(String name, String url1, String url2, String additionalJDBCParams) {
            Preconditions.checkNotNull(name);
            Preconditions.checkNotNull(url1);
            Preconditions.checkNotNull(url2);
            this.name = name;
            url1 = JDBCUtil.formatZookeeperUrl(url1);
            url2 = JDBCUtil.formatZookeeperUrl(url2);
            Preconditions.checkArgument(!url1.equals(url2), "Two clusters have the same ZK!");
            // Ignore the given order of url1 and url2, and reorder for equals comparison.
            if (url1.compareTo(url2) > 0) {
                this.urls = new PairOfSameType<>(url2, url1);
            } else {
                this.urls = new PairOfSameType<>(url1, url2);
            }
            this.additionalJDBCParams = additionalJDBCParams;
        }

        HAGroupInfo(String name, String url1, String url2) {
            this(name, url1, url2, null);
        }

        public String getName() {
            return name;
        }

        public String getUrl1() {
            return urls.getFirst();
        }

        public String getUrl2() {
            return urls.getSecond();
        }

        public String getJDBCUrl(String zkUrl) {
            Preconditions.checkArgument(zkUrl.equals(getUrl1()) || zkUrl.equals(getUrl2()),
                    "The URL '" + zkUrl + "' does not belong to this HA group " + this);
            StringBuilder sb = new StringBuilder();
            sb.append(PhoenixRuntime.JDBC_PROTOCOL_ZK);
            sb.append(PhoenixRuntime.JDBC_PROTOCOL_SEPARATOR);
            sb.append(zkUrl);
            if (!Strings.isNullOrEmpty(additionalJDBCParams)) {
                sb.append(PhoenixRuntime.JDBC_PROTOCOL_SEPARATOR);
                sb.append(additionalJDBCParams);
            }
            return sb.toString();
        }

        public String getJDBCUrl1() {
            return getJDBCUrl(getUrl1());
        }

        public String getJDBCUrl2() {
            return getJDBCUrl(getUrl2());
        }

        /**
         * Helper method to return the znode path in the Phoenix HA namespace.
         */
        String getZkPath() {
            return ZKPaths.PATH_SEPARATOR + name;
        }

        @Override
        public String toString() {
            return String.format("%s[%s|%s]", name, urls.getFirst(), urls.getSecond());
        }

        @Override
        public boolean equals(Object other) {
            if (other == null) {
                return false;
            }
            if (other == this) {
                return true;
            }
            if (other.getClass() != getClass()) {
                return false;
            }
            HAGroupInfo otherInfo = (HAGroupInfo) other;
            return new EqualsBuilder()
                    .append(name, otherInfo.name)
                    .append(urls, otherInfo.urls)
                    .isEquals();
        }

        @Override
        public int hashCode() {
            return new HashCodeBuilder(17, 37)
                    .append(name)
                    .append(urls).hashCode();
        }
    }

    /**
     * Maintains the client view of cluster roles for the HA group using data retrieved from one ZK.
     * <p>
     * It is a runnable to keep setting up the curator and the node cache. It will also register
     * the node watcher so any znode data change will trigger a callback function updating HA group.
     */
    private final class HAClusterRoleManager implements Runnable {
        private final String jdbcUrl;
        private final Properties properties;
        private NodeCache cache;

        /**
         * Constructor which creates and starts the ZK watcher.
         *
         * @param jdbcUrl    JDBC url without jdbc:phoenix prefix which may be host:port:/hbase format
         * @param properties The properties defining ZK client timeouts and retries
         */
        HAClusterRoleManager(String jdbcUrl, Properties properties) {
            this.jdbcUrl = jdbcUrl;
            this.properties = properties;
        }

        @Override
        public void run() {
            final String zpath = info.getZkPath();
            while (!Thread.currentThread().isInterrupted()) {
                try {
                    cache = new NodeCache(getCurator(jdbcUrl, properties), zpath);
                    cache.getListenable().addListener(this::nodeChanged);
                    cache.start();
                    return; // return after building the initial node cache
                } catch (InterruptedException e) {
                    LOG.warn("HA cluster role manager thread for '{}' is interrupted, exiting",
                            jdbcUrl, e);
                    break;
                } catch (Throwable t) {
                    LOG.warn("Fail to start node cache on '{}' for '{}'. Retry", jdbcUrl, zpath, t);
                    try {
                        // TODO: do better than fixed time sleep
                        Thread.sleep(1_000);
                    } catch (InterruptedException e) {
                        LOG.warn("HA cluster role manager thread for '{}' is interrupted, exiting",
                                jdbcUrl, e);
                        break;
                    }
                }
            }
        }

        /**
         * Call back functions when a cluster role change is notified by this ZK cluster.
         */
        private void nodeChanged() {
            byte[] data = cache.getCurrentData().getData();
            Optional<ClusterRoleRecord> newRecordOptional = ClusterRoleRecord.fromJson(data);
            if (!newRecordOptional.isPresent()) {
                LOG.error("Fail to deserialize new record; keep current record {}", roleRecord);
                return;
            }
            ClusterRoleRecord newRecord = newRecordOptional.get();
            LOG.info("HA group {} got a record from cluster {}: {}", info.name, jdbcUrl, newRecord);

            if (applyClusterRoleRecord(newRecord)) {
                LOG.info("Successfully apply new cluster role record from cluster '{}', "
                        + "new record: {}", jdbcUrl, newRecord);
                roleManagerLatch.countDown();
            }
        }
    }
}
