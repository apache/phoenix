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

import org.apache.commons.lang3.ObjectUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;
import org.apache.curator.RetryPolicy;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.curator.utils.ZKPaths;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.util.PairOfSameType;
import org.apache.phoenix.exception.SQLExceptionCode;
import org.apache.phoenix.exception.SQLExceptionInfo;
import org.apache.phoenix.jdbc.ClusterRoleRecord.ClusterRole;
import org.apache.phoenix.jdbc.ClusterRoleRecord.RegistryType;
import org.apache.phoenix.query.HBaseFactoryProvider;
import org.apache.phoenix.thirdparty.com.google.common.annotations.VisibleForTesting;
import org.apache.phoenix.thirdparty.com.google.common.base.Preconditions;
import org.apache.phoenix.thirdparty.com.google.common.cache.Cache;
import org.apache.phoenix.thirdparty.com.google.common.cache.CacheBuilder;
import org.apache.phoenix.util.GetClusterRoleRecordUtil;
import org.apache.phoenix.util.JDBCUtil;
import org.apache.phoenix.util.PhoenixRuntime;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


import java.io.IOException;
import java.sql.Connection;
import java.sql.Driver;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.HashSet;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import static org.apache.phoenix.query.QueryServicesOptions.DEFAULT_CLIENT_CONNECTION_CACHE_MAX_DURATION;
import static org.apache.phoenix.util.PhoenixRuntime.JDBC_PROTOCOL_SEPARATOR;

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

    public static final String PHOENIX_HA_CRR_POLLER_INTERVAL_MS_KEY =
            PHOENIX_HA_ATTR_PREFIX + "crr.poller.interval.ms";
    public static final String PHOENIX_HA_CRR_POLLER_INTERVAL_MS_DEFAULT = "5000"; // 5 seconds

    public static final String PHOENIX_HA_CRR_REGISTRY_TYPE_KEY =
            PHOENIX_HA_ATTR_PREFIX + "crr.registry.type";

    /**
     * The frequency to cache the ClusterRoleRecord for this HA group.
     */
    public static final String PHOENIX_HA_CRR_CACHE_FREQUENCY_MS_KEY =
            PHOENIX_HA_ATTR_PREFIX + "crr.cache.frequency.ms";
    public static final long PHOENIX_HA_CRR_CACHE_FREQUENCY_MS_DEFAULT = 2000; // 2 seconds

    public static final RegistryType DEFAULT_PHOENIX_HA_CRR_REGISTRY_TYPE = RegistryType.RPC;

    static final Logger LOG = LoggerFactory.getLogger(HighAvailabilityGroup.class);

    /**
     * Two maps to store client provided info mapping to HighAvailabilityGroup.
     * GROUPS which store HAGroupInfo (name and url of clusters where CRR resides)
     * to HighAvailabilityGroup mapping, which is the information required to get roleRecord
     * and URLS which store HAGroupInfo to HAURLInfo (name, principal) 1:n mapping
     * which represents a given group of clients trying to connect to a HighAvailabilityGroup,
     * this info is required to fetch the CQSI(s) linked to given HighAvailabilityGroup in case
     * of failover or a change where CQSIs needs to be closed and invalidated
     *
     * HAURLInfo is stored in {@link ParallelPhoenixContext} and {@link FailoverPhoenixContext}
     * for the current given connection
     *
     */
    @VisibleForTesting
    static final Map<HAGroupInfo, HighAvailabilityGroup> GROUPS = new ConcurrentHashMap<>();
    static final Map<HAGroupInfo, Set<HAURLInfo>> URLS = new ConcurrentHashMap<>();
    @VisibleForTesting
    static final Cache<HAGroupInfo, Boolean> MISSING_CRR_GROUPS_CACHE = CacheBuilder.newBuilder()
            .expireAfterWrite(PHOENIX_HA_TRANSITION_TIMEOUT_MS_DEFAULT, TimeUnit.MILLISECONDS)
            .build();
    /**
     * The Curator client cache, one client instance per cluster and namespace combination.
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
     * Configuration to be used for default properties values
     */
    private final Configuration config;
    /**
     * Current cluster role record for this HA group.
     */
    private volatile ClusterRoleRecord roleRecord;
    /**
     * Executor for applying the cluster role to this HA group.
     */
    private final ExecutorService crrChangedExecutor = Executors.newFixedThreadPool(1);
    /**
     * State of this HA group.
     */
    private volatile State state = State.UNINITIALIZED;
    private volatile long lastClusterRoleRecordRefreshTime = 0;
    private volatile long clusterRoleRecordCacheFrequency = PHOENIX_HA_CRR_CACHE_FREQUENCY_MS_DEFAULT;
    private final ReadWriteLock rwLock = new ReentrantReadWriteLock();
    private final Lock readLock = rwLock.readLock();
    private final Lock writeLock = rwLock.writeLock();

    /**
     * Private constructor.
     * <p>
     * To get an instance, please call {@link HighAvailabilityGroup#get(String, Properties)}.
     */
    private HighAvailabilityGroup(HAGroupInfo info, Properties properties) {
        this.info = info;
        this.properties = properties;
        this.config = HBaseFactoryProvider.getConfigurationFactory().getConfiguration();
        // Get the cluster role record cache frequency from properties and if properties is not set, then get it from config
        this.clusterRoleRecordCacheFrequency = Long.parseLong(properties.getProperty(PHOENIX_HA_CRR_CACHE_FREQUENCY_MS_KEY,
            config.get(PHOENIX_HA_CRR_CACHE_FREQUENCY_MS_KEY, String.valueOf(PHOENIX_HA_CRR_CACHE_FREQUENCY_MS_DEFAULT))));
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
        this.config = HBaseFactoryProvider.getConfigurationFactory().getConfiguration();
        // Get the cluster role record cache frequency from properties and if properties is not set, then get it from config
        this.clusterRoleRecordCacheFrequency = Long.parseLong(properties.getProperty(PHOENIX_HA_CRR_CACHE_FREQUENCY_MS_KEY,
            config.get(PHOENIX_HA_CRR_CACHE_FREQUENCY_MS_KEY, String.valueOf(PHOENIX_HA_CRR_CACHE_FREQUENCY_MS_DEFAULT))));
    }

    /**
     * Get an instance of {@link HAURLInfo} given the HA connecting URL (with "|") and client properties.
     * Here we do parsing of url and try to extract principal and other additional params
     * @throws SQLException if fails to get HA information and/or invalid properties are seen
     */
    public static HAURLInfo getUrlInfo(String url, Properties properties) throws SQLException {
        //Check if HA group name is provided in the properties if not throw an exception
        String name = properties.getProperty(PHOENIX_HA_GROUP_ATTR);
        if (StringUtils.isEmpty(name)) {
            throw new SQLExceptionInfo.Builder(SQLExceptionCode.HA_INVALID_PROPERTIES)
                    .setMessage(String.format("HA group name can not be empty for HA URL %s", url))
                    .build()
                    .buildException();
        }
        url = checkUrl(url);
        String principal = null;
        String additionalJDBCParams = null;
        int idx = url.indexOf("]");
        int extraIdx = url.indexOf(JDBC_PROTOCOL_SEPARATOR, idx + 1);
        if (extraIdx != -1) {
            //after quorums there should be a separator
            if (extraIdx != idx + 1) {
                throw new SQLExceptionInfo.Builder(SQLExceptionCode.MALFORMED_CONNECTION_URL)
                        .setMessage(String.format("URL %s is not a valid HA connection string",
                                url))
                        .build()
                        .buildException();
            }
            additionalJDBCParams  = url.substring(extraIdx + 1);
            //Get the principal
            extraIdx = additionalJDBCParams.indexOf(JDBC_PROTOCOL_SEPARATOR);
            if (extraIdx != -1) {
                if (extraIdx != 0) {
                    principal = additionalJDBCParams.substring(0, extraIdx);
                }
                //Storing terminator as part of additional Params
                additionalJDBCParams = additionalJDBCParams.substring(extraIdx + 1);
            } else {
                extraIdx = additionalJDBCParams.indexOf(PhoenixRuntime.JDBC_PROTOCOL_TERMINATOR);
                if (extraIdx != -1) {
                    //Not storing terminator to make it consistent.
                    principal = additionalJDBCParams.substring(0, extraIdx);
                    additionalJDBCParams = String.valueOf(PhoenixRuntime.JDBC_PROTOCOL_TERMINATOR);
                } else {
                    principal = additionalJDBCParams;
                    additionalJDBCParams = null;
                }
            }
        } else {
            extraIdx = url.indexOf(PhoenixRuntime.JDBC_PROTOCOL_TERMINATOR, idx + 1);
            if (extraIdx != -1) {
                //There is something in between zkquorum and terminator but no separator(s),
                //So not sure what it is
                if (extraIdx != idx + 1) {
                    throw new SQLExceptionInfo.Builder(SQLExceptionCode.MALFORMED_CONNECTION_URL)
                            .setMessage(String.format("URL %s is not a valid HA connection string",
                                    url))
                            .build()
                            .buildException();
                } else {
                    additionalJDBCParams = url.substring(extraIdx);
                }
            }
        }

        //If additional parameter is only ; then making it null as while building back jdbc url
        //if we don't have principal we need additional checks to make sure we are not appending
        //additionalJDBCParams after JDBC_PROTOCOL_SEPARATOR.
        additionalJDBCParams = additionalJDBCParams != null
                ? (additionalJDBCParams.equals(String.valueOf(PhoenixRuntime.JDBC_PROTOCOL_TERMINATOR))
                    ? null : additionalJDBCParams) : null;

        HAURLInfo haurlInfo = new HAURLInfo(name, principal, additionalJDBCParams);
        HAGroupInfo info = getHAGroupInfo(url, properties);
        URLS.computeIfAbsent(info, haGroupInfo -> new HashSet<>()).add(haurlInfo);
        return haurlInfo;
    }

    private static HAGroupInfo getHAGroupInfo(String url, Properties properties)
            throws SQLException {
        String name = properties.getProperty(PHOENIX_HA_GROUP_ATTR);
        if (StringUtils.isEmpty(name)) {
            throw new SQLExceptionInfo.Builder(SQLExceptionCode.HA_INVALID_PROPERTIES)
                    .setMessage(String.format("HA group name can not be empty for HA URL %s", url))
                    .build()
                    .buildException();
        }
        url = checkUrl(url);
        url = url.substring(url.indexOf("[") + 1, url.indexOf("]"));
        String [] urls = url.split("\\|");
        return new HAGroupInfo(name, urls[0], urls[1]);
    }

    /**
     * checks if the given url is appropriate for HA Connections, HA URL will only work for RPC Registry
     * so it won't accept MASTER or ZK protocols but if no protocol is specified, it will be assumed to be RPC
     * @param url url passed by clients to phoenix-client
     * @return the url without protocol
     * @throws SQLException MalformedConnectionUrlException if url is not a valid HA url
     */
    private static String checkUrl(String url) throws SQLException {
        if (url.startsWith(PhoenixRuntime.JDBC_PROTOCOL_RPC)) {
            url = url.substring(PhoenixRuntime.JDBC_PROTOCOL_RPC.length() + 1);
        } else if (url.startsWith(PhoenixRuntime.JDBC_PROTOCOL_MASTER)) {
            throwMalFormedConnectionUrlException(String.format("Protocol specified is MASTER, which " +
                    "is not acceptable for HA Connections, only RPC Registry is mandated, for url %s", url));
        } else if (url.startsWith(PhoenixRuntime.JDBC_PROTOCOL_ZK)){
            throwMalFormedConnectionUrlException(String.format("Protocol specified is ZK, which " +
                    "is not acceptable for HA Connections only RPC Registry is mandated, for url %s", url));
        } else if (url.startsWith(PhoenixRuntime.JDBC_PROTOCOL)) {
            url = url.substring(PhoenixRuntime.JDBC_PROTOCOL.length() + 1);
        }

        //Check if url is a valid HA URL
        if (!(url.contains("[") && url.contains("|") && url.contains("]"))) {
            throw new SQLExceptionInfo.Builder(SQLExceptionCode.MALFORMED_CONNECTION_URL)
                    .setMessage(String.format("URL %s is not a valid HA connection string", url))
                    .build()
                    .buildException();
        }
        return url;
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

        //create a cache for missing CRR to prevent unnecessary exceptions (cache expires in 5 minutes)
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

            if (e instanceof SQLException && ((SQLException)e).getErrorCode()
                        == SQLExceptionCode.CLUSTER_ROLE_RECORD_NOT_FOUND.getErrorCode()) {

                LOG.error("HA group {} failed to initialized. Got exception when getting " +
                        "ClusterRoleRecord for HA group {}", info, e.getCause());
                //If the exception is due to missing CRR, we will put this HA group into negative
                //cache to prevent unnecessary computations of trying to get CRR for every
                //connection cache expires every 5 secs
                MISSING_CRR_GROUPS_CACHE.put(info, true);
                return Optional.empty();
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
     * @return The connection url of the single cluster to fall back on,
     * with a fully qualified JDBC protocol
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
            LOG.error("Fallback to single cluster is enabled for the HA group {} but cluster key is"
                    + "empty per configuration 'phoenix.ha.fallback.cluster', and boostrap url "
                    + "cannot be used as fallback cluster as it can be different that urls present in"
                    + "ClusterRoleRecords which are source of truth. HA url: '{}'.", haGroupInfo.getName(), url);
            return Optional.empty();
        }

        // Ensure the fallback cluster URL includes the JDBC protocol prefix
        if (!fallbackCluster.startsWith(PhoenixRuntime.JDBC_PROTOCOL_RPC)) {
            fallbackCluster = PhoenixRuntime.JDBC_PROTOCOL_RPC
                    + JDBC_PROTOCOL_SEPARATOR + fallbackCluster;
        }

        LOG.info("Falling back to single cluster '{}' for the HA group {} to serve HA connection "
                        + "request against url '{}'.",
                fallbackCluster, haGroupInfo.getName(), url);
        return Optional.of(fallbackCluster);
    }

    /**
     * Generate cache key for curator cache based on URL and namespace.
     *
     * @param jdbcUrl the ZK endpoint host:port or the JDBC connection String host:port:/hbase
     * @param namespace the ZooKeeper namespace, uses default if null
     * @return cache key string
     */
    @VisibleForTesting
    static String generateCacheKey(String jdbcUrl, String namespace) {
        String effectiveNamespace = namespace != null ? namespace : PHOENIX_HA_ZOOKEEPER_ZNODE_NAMESPACE;
        return jdbcUrl + ":" + effectiveNamespace;
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
        return getCurator(jdbcUrl, properties, PHOENIX_HA_ZOOKEEPER_ZNODE_NAMESPACE);
    }

    /**
     * Get an active curator ZK client for the given properties, ZK endpoint and namespace.
     * <p>
     * This can be from cached object since Curator should be shared per cluster.
     *
     * @param jdbcUrl    the ZK endpoint host:port or the JDBC connection String host:port:/hbase
     * @param properties the properties defining time out values and retry count
     * @param namespace  the ZooKeeper namespace to use, defaults to PHOENIX_HA_ZOOKEEPER_ZNODE_NAMESPACE if null
     * @return a new Curator framework client
     */
    @SuppressWarnings("UnstableApiUsage")
    public static CuratorFramework getCurator(String jdbcUrl, Properties properties, String namespace)
            throws IOException {
        // Use namespace as part of cache key to avoid conflicts between different namespaces
        String effectiveNamespace = namespace != null ? namespace : PHOENIX_HA_ZOOKEEPER_ZNODE_NAMESPACE;
        String cacheKey = generateCacheKey(jdbcUrl, namespace);
        try {
            return CURATOR_CACHE.get(cacheKey, () -> {
                CuratorFramework curator = createCurator(jdbcUrl, properties, effectiveNamespace);
                try {
                    if (!curator.blockUntilConnected(PHOENIX_HA_ZK_CONNECTION_TIMEOUT_MS_DEFAULT,
                            TimeUnit.MILLISECONDS)) {
                        throw new RuntimeException("Failed to connect to the CuratorFramework in "
                                + "timeout " + PHOENIX_HA_ZK_CONNECTION_TIMEOUT_MS_DEFAULT + " ms");
                    }
                } catch (Exception e) {
                    LOG.warn("HA cluster role manager getCurator thread for '{}' is interrupted"
                        + ", closing CuratorFramework", jdbcUrl, e);
                    curator.close();
                    if (e instanceof InterruptedException) {
                        Thread.currentThread().interrupt();
                    }
                    throw e;
                }
                return curator;
            });
        } catch (Exception e) {
            LOG.error("Fail to get an active curator for url {}", jdbcUrl, e);
            // invalidate the cache when getting/creating throws exception
            CURATOR_CACHE.invalidate(cacheKey);
            throw new IOException(e);
        }
    }

    /**
     * Create a curator ZK client for the given properties, ZK endpoint and namespace.
     * <p>
     * Unless caller needs a new curator, it should use {@link #getCurator(String, Properties)}.
     *
     * @param jdbcUrl the ZK endpoint host:port or the JDBC connection String host:port:/hbase
     * @param properties the properties defining time out values and retry count
     * @param namespace the ZooKeeper namespace to use, defaults to PHOENIX_HA_ZOOKEEPER_ZNODE_NAMESPACE if null
     * @return a new Curator framework client
     */
    private static CuratorFramework createCurator(String jdbcUrl, Properties properties, String namespace) {
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
                .namespace(namespace != null ? namespace : PHOENIX_HA_ZOOKEEPER_ZNODE_NAMESPACE)
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
     * Initialize this HA group by getting initial ClusterRoleRecord from RegionServer Endpoints.
     * <p>
     * If this is already initialized, calling this method is a no-op. This method is lock free as
     * current thread will either return fast or wait for the in-progress initialization or timeout.
     */
    public void init() throws IOException, SQLException {
        if (state != State.UNINITIALIZED) {
            return;
        }

        ClusterRoleRecord roleRecordFromEndpoint = getClusterRoleRecordFromEndpoint();

        LOG.info("Initial cluster role for HA group {} is {}", info, roleRecordFromEndpoint);
        roleRecord = roleRecordFromEndpoint;
        state = State.READY;
    }

    /**
     * Create a JDBC connection in this high availability group.
     *
     * @param properties connection properties
     * @return a JDBC connection implementation
     * @throws SQLException if fails to connect a JDBC connection
     */
    public Connection connect(Properties properties, HAURLInfo haurlInfo) throws SQLException {
        if (state != State.READY) {
            throw new SQLExceptionInfo
                    .Builder(SQLExceptionCode.CANNOT_ESTABLISH_CONNECTION)
                    .setMessage("HA group is not ready!")
                    .setHaGroupInfo(info.toString())
                    .build()
                    .buildException();
        }
        return roleRecord.getPolicy().provide(this, properties, haurlInfo);
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
    PhoenixConnection connectActive(final Properties properties, final HAURLInfo haurlInfo)
            throws SQLException {
        try {
            Optional<String> url = roleRecord.getActiveUrl();
            if (state == State.READY && url.isPresent()) {
                PhoenixConnection conn = connectToOneCluster(url.get(), properties, haurlInfo);
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
                .equals(Optional.of(JDBCUtil.formatUrl(connection.getURL())));
    }

    /**
     * Connect to an HBase cluster in this HA group with given url and client properties.
     * <p>
     * The URL should belong to one of the two ZK clusters in this HA group. It returns the Phoenix
     * connection to the given cluster without checking the context of the cluster's role. Please
     * use {@link #connectActive(Properties, HAURLInfo)} to connect to the ACTIVE cluster.
     */
    PhoenixConnection connectToOneCluster(String url, Properties properties, HAURLInfo haurlInfo)
            throws SQLException {
        Preconditions.checkNotNull(url);
        if (url.startsWith(PhoenixRuntime.JDBC_PROTOCOL)) {
            Preconditions.checkArgument(url.length() > PhoenixRuntime.JDBC_PROTOCOL.length(),
                    "The URL '" + url + "' is not a valid Phoenix connection string");
        }
        //we don't need to normalize here? as we already store url in roleRecord object as
        //normalized, or we are just normalizing for tests?
        url = JDBCUtil.formatUrl(url, roleRecord.getRegistryType());

        String jdbcString = getJDBCUrl(url, haurlInfo, roleRecord.getRegistryType());

        ClusterRole role = roleRecord.getRole(url);
        if (!role.canConnect()) {
            throw new SQLExceptionInfo.Builder(SQLExceptionCode.HA_CLUSTER_CAN_NOT_CONNECT)
                    .setMessage("Can not connect to cluster '" + url + "' in '" + role + "' role")
                    .build()
                    .buildException();
        }

        //Get driver instead of using PhoenixDriver.INSTANCE since it can be a test or mocked driver
        Driver driver = DriverManager.getDriver(jdbcString);
        Preconditions.checkArgument(driver instanceof PhoenixEmbeddedDriver,
                "No JDBC driver is registered for Phoenix high availability (HA) framework");
        return ((PhoenixEmbeddedDriver) driver).getConnectionQueryServices(jdbcString, properties)
                .connect(jdbcString, properties, this);
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
        state = State.CLOSED;
    }

    @Override
    public String toString() {
        return roleRecord == null
                ? "HighAvailabilityGroup{roleRecord=null, info=" + info + ", state=" + state + "}"
                : "HighAvailabilityGroup{roleRecord=" + roleRecord + ", state=" + state + "}";
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
     * Objects of this class are used as the keys of HA group cache {@link #GROUPS} and HA url info cache
     * {@link #URLS}.
     * <p>
     * This class is immutable.
     */
    @VisibleForTesting
    static final class HAGroupInfo {
        private final String name;
        private final PairOfSameType<String> urls;

        HAGroupInfo(String name, String url1, String url2) {
            Preconditions.checkNotNull(name);
            Preconditions.checkNotNull(url1);
            Preconditions.checkNotNull(url2);
            this.name = name;
            //Normalizing these urls with ZK protocol as these are the ZK urls of clusters where
            //roleRecords resides.
            url1 = JDBCUtil.formatUrl(url1, DEFAULT_PHOENIX_HA_CRR_REGISTRY_TYPE);
            url2 = JDBCUtil.formatUrl(url2, DEFAULT_PHOENIX_HA_CRR_REGISTRY_TYPE);
            Preconditions.checkArgument(!url1.equals(url2), "Two clusters have the same urls!");
            // Ignore the given order of url1 and url2, and reorder for equals comparison.
            if (url1.compareTo(url2) > 0) {
                this.urls = new PairOfSameType<>(url2, url1);
            } else {
                this.urls = new PairOfSameType<>(url1, url2);
            }
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

        public String getJDBCUrl1(HAURLInfo haURLInfo) {
            return getJDBCUrl(getUrl1(), haURLInfo, DEFAULT_PHOENIX_HA_CRR_REGISTRY_TYPE);
        }

        public String getJDBCUrl2(HAURLInfo haURLInfo) {
            return getJDBCUrl(getUrl2(), haURLInfo, DEFAULT_PHOENIX_HA_CRR_REGISTRY_TYPE);
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
     * Helper method to construct the jdbc url back from the given information for a HAGroup
     * Url are expected to be passed in correct format i.e. zk1\\:port1,zk2\\:port2,zk3\\:port3,zk4\\:port4,zk5\\:port5::znode
     * or master1\\:port1,master2\\:port2,master3\\:port3,master4\\:port4,master5\\:port5
     * @param url contains host and port part of jdbc url, to get ha url in jdbc format
     *            this function can be used, but needs url in ha format already i.e. [url1|url2]
     *            for MASTER and RPC registry
     * @param haURLInfo contains principal and additional information
     * @param type Registry Type for which url has to be constructed
     * @return jdbc url in proper format i.e. jdbc:phoenix+<registry>:url:principal:additionalParam
     * example :- jdbc:phoenix+zk:zk1\\:port1,zk2\\:port2,zk3\\:port3,zk4\\:port4,zk5\\:port5::znode:principal:additionalParams
     * or jdbc:phoenix+master:master1\\:port1,master2\\:port2,master3\\:port3,master4\\:port4,master5\\:port5:::principal:additionParams
     */
    public static String getJDBCUrl(String url, HAURLInfo haURLInfo,
                                    ClusterRoleRecord.RegistryType type) {
        //Need extra separator for Master and RPC connections for principal as no znode path is there
        boolean extraSeparator = false;
        StringBuilder sb = new StringBuilder();
        switch (type) {
            case ZK:
                sb.append(PhoenixRuntime.JDBC_PROTOCOL_ZK);
                break;
            case RPC:
                sb.append(PhoenixRuntime.JDBC_PROTOCOL_RPC);
                extraSeparator = true;
                break;
            case MASTER:
                sb.append(PhoenixRuntime.JDBC_PROTOCOL_MASTER);
                extraSeparator = true;
                break;
            default:
                sb.append(PhoenixRuntime.JDBC_PROTOCOL);
        }
        sb.append(JDBC_PROTOCOL_SEPARATOR);
        sb.append(url);
        if (haURLInfo != null) {
            if (ObjectUtils.anyNotNull(haURLInfo.getPrincipal(), haURLInfo.getAdditionalJDBCParams())) {
                if (extraSeparator) {
                    //For Master and RPC connection url we need 2 extra separator between port and
                    //principal as there is no ZNode
                    sb.append(JDBC_PROTOCOL_SEPARATOR).
                            append(JDBC_PROTOCOL_SEPARATOR);
                }
                sb.append(haURLInfo.getPrincipal() == null ? JDBC_PROTOCOL_SEPARATOR
                        : JDBC_PROTOCOL_SEPARATOR + haURLInfo.getPrincipal());
            }
            if (ObjectUtils.anyNotNull(haURLInfo.getAdditionalJDBCParams())) {
                sb.append(JDBC_PROTOCOL_SEPARATOR).
                        append(haURLInfo.getAdditionalJDBCParams());
            }
        }
        return sb.toString();
    }

    /**
     * Helper method to construct the jdbc HA url for the given urls and HAURLInfo
     * @param url1 url1 of the HA group
     * @param url2 url2 of the HA group
     * @param haURLInfo HAURLInfo object containing principal and additional params
     * @return jdbc HA url in proper format i.e. jdbc:phoenix+rpc:[url1|url2]:principal:additionalParams
     * example :- jdbc:phoenix+rpc:[master1\\:port1,master2\\:port2,master3\\:port3,master4\\:port4,master5\\:port5|
     *    peer_master1\\:port1,peer_master2\\:port2,peer_master3\\:port3,peer_master4\\:port4,peer_master5\\:port5]:principal:additionParams
     */
    public static String getJDBCHAUrl(String url1, String url2, HAURLInfo haURLInfo) {
        StringBuilder sb = new StringBuilder();
        sb.append(PhoenixRuntime.JDBC_PROTOCOL_RPC).append(JDBC_PROTOCOL_SEPARATOR);
        sb.append("[").append(url1).append("|").append(url2).append("]");
        if (haURLInfo != null) {
            if (ObjectUtils.anyNotNull(haURLInfo.getPrincipal(), haURLInfo.getAdditionalJDBCParams())) {
                sb.append(haURLInfo.getPrincipal() == null ? JDBC_PROTOCOL_SEPARATOR
                        : JDBC_PROTOCOL_SEPARATOR + haURLInfo.getPrincipal());
            }
            if (ObjectUtils.anyNotNull(haURLInfo.getAdditionalJDBCParams())) {
                sb.append(JDBC_PROTOCOL_SEPARATOR).
                        append(haURLInfo.getAdditionalJDBCParams());
            }
        }
        return sb.toString();
    }


    private static void throwMalFormedConnectionUrlException(String message) throws SQLException {
        throw new SQLExceptionInfo.Builder(SQLExceptionCode.MALFORMED_CONNECTION_URL)
                .setMessage(message)
                .build()
                .buildException();
    }



    /**
     * Method to get ClusterRoleRecord from RegionServer Endpoints from either of the clusters.
     * @return ClusterRoleRecord from the first available cluster
     * @throws SQLException if there is an error getting the ClusterRoleRecord
     */
    private ClusterRoleRecord getClusterRoleRecordFromEndpoint() throws SQLException {
        long pollerInterval = Long.parseLong(properties.getProperty(PHOENIX_HA_CRR_POLLER_INTERVAL_MS_KEY,
                config.get(PHOENIX_HA_CRR_POLLER_INTERVAL_MS_KEY, PHOENIX_HA_CRR_POLLER_INTERVAL_MS_DEFAULT)));

        //Get the CRR via RSEndpoint for cluster 1
        try {
            return GetClusterRoleRecordUtil.fetchClusterRoleRecord(info.getUrl1(), info.getName(), this, pollerInterval, properties);
        } catch (Exception e) {
            //Got exception from cluster 1 when trying to get CRR, try cluster 2
            return GetClusterRoleRecordUtil.fetchClusterRoleRecord(info.getUrl2(), info.getName(), this, pollerInterval, properties);
        }
    }

    /**
     * Refresh the cluster role record for this HA group.
     * <p>
     * This method will get the new ClusterRoleRecord from RegionServer Endpoints and apply it to the HA group.
     * and it will call to transition the Policy's connections according to the new record.
     * <p>
     * This method uses a read-write lock pattern to allow multiple concurrent callers to check if a refresh
     * is needed while ensuring only one thread performs the actual refresh operation at a time. This improves
     * throughput by avoiding blocking all callers during the quick check phase.
     *
     * @return true if the new record is set as current one; false otherwise
     * @throws SQLException if there is an error getting the ClusterRoleRecord
     */
    public boolean refreshClusterRoleRecord(boolean forceRefresh) throws SQLException {
        // Allow concurrent reads to return fast in case refresh is not needed
        readLock.lock();
        try {
            if (!forceRefresh && !shouldRefreshRoleRecord()) {
                return true;
            }
        } finally {
            readLock.unlock();
        }

        // Take a write lock to apply the refresh of roleRecord
        writeLock.lock();
        try {
            // Re-check the condition under the write lock to prevent race conditions.
            // Another thread might have already done the refresh while we were waiting for the
            // write lock.
            if (!forceRefresh && !shouldRefreshRoleRecord()) {
                return true;
            }

            ClusterRoleRecord newRoleRecord = getClusterRoleRecordFromEndpoint();
            if (roleRecord == null) {
                roleRecord = newRoleRecord;
                lastClusterRoleRecordRefreshTime = System.currentTimeMillis();
                state = State.READY;
                LOG.info("HA group {} is now in {} state after getting initial V{} role record: {}",
                        info, state, roleRecord.getVersion(), roleRecord);
                LOG.debug("HA group {} is ready", this);
                return true;
            }

            //Check if newRoleRecord has same Info (HAGroupName and Policy)
            if (!roleRecord.hasSameInfo(newRoleRecord)) {
                LOG.error("New record {} has different HA group information (haGroupName/Policy) from" +
                        " old record {}, which is not expected", newRoleRecord, roleRecord);
                return false;
            }

            //Check if newRoleRecord is actually new
            if (roleRecord.equals(newRoleRecord)) {
                LOG.debug("New Role Record is same as current RoleRecord, no need to refresh {}",
                        roleRecord.toString());
                lastClusterRoleRecordRefreshTime = System.currentTimeMillis();
                return true;
            }

            final ClusterRoleRecord oldRecord = roleRecord;
            state = State.IN_TRANSITION;
            LOG.info("HA group {} is in {} to set V{} record", info, state, newRoleRecord.getVersion());
            Future<?> future = crrChangedExecutor.submit(() -> {
                try {
                    roleRecord.getPolicy().transitClusterRoleRecord(this, roleRecord, newRoleRecord);
                } catch (SQLException e) {
                    throw new CompletionException(e);
                }
            });

            String transitionTimeoutProp = properties.getProperty(
                    PHOENIX_HA_TRANSITION_TIMEOUT_MS_KEY, config.get(
                            PHOENIX_HA_TRANSITION_TIMEOUT_MS_KEY));
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
                LOG.error("HA group {} failed to transit cluster roles per policy {} to new " +
                                "record {}", info, roleRecord.getPolicy(), newRoleRecord, e);
                //Rethrow the Role transitions not allowed exceptions
                if (e.getCause() != null && e.getCause().getCause() != null) {
                    if (e.getCause().getCause() instanceof SQLException && ((SQLException)e
                            .getCause().getCause()).getErrorCode() == SQLExceptionCode
                            .HA_ROLE_TRANSITION_NOT_ALLOWED.getErrorCode()) {
                        state = State.READY;
                        throw (SQLException)e.getCause().getCause();
                    }
                }
                // Calling back HA policy function for cluster switch is conducted with best effort.
                // HA group continues transition when its HA policy fails to deal with context switch
                // (e.g. to close existing connections)
                // The goal here is to gain higher availability even though existing resources against
                // previous ACTIVE cluster may have not been closed cleanly.
            }
            //Update the role record and the last refresh time
            roleRecord = newRoleRecord;
            lastClusterRoleRecordRefreshTime = System.currentTimeMillis();
            state = State.READY;
            LOG.info("HA group {} is in {} state, Old: {}, new: {}", info, state, oldRecord,
                    roleRecord);
            LOG.debug("HA group is ready: {}", this);
            return true;
        } finally {
            writeLock.unlock();
        }
    }

    /**
     * Check if we should refresh the RoleRecord based on cache frequency if the cacheAge is
     * greater than clusterRoleRecordCacheFrequency, then return true
     */
    public boolean shouldRefreshRoleRecord() {
        if (roleRecord == null) {
            return true; // Always refresh if no role record
        }
        long cacheAge = System.currentTimeMillis() - lastClusterRoleRecordRefreshTime;
        return cacheAge >= clusterRoleRecordCacheFrequency;
    }

}
