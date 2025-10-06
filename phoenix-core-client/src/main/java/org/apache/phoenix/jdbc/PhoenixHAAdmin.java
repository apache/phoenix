/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.phoenix.jdbc;

import java.io.Closeable;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import org.apache.commons.lang3.StringUtils;
import org.apache.curator.RetryPolicy;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.recipes.atomic.AtomicValue;
import org.apache.curator.framework.recipes.atomic.DistributedAtomicValue;
import org.apache.curator.utils.ZKPaths;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.util.PairOfSameType;
import org.apache.phoenix.util.JDBCUtil;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.phoenix.jdbc.ClusterRoleRecord.RegistryType;
import org.apache.phoenix.thirdparty.com.google.common.base.Preconditions;

/**
 * Helper class to update cluster role record for a ZK cluster. The ZK client this accessor has is
 * confined to a single ZK cluster, but it can be used to operate multiple HA groups that are
 * associated with this cluster. This is not thread-safe yet. Multiple threads can update CRRs at
 * same time potentially causing inconsistency.
 */
public class PhoenixHAAdmin implements Closeable {

  /**
   * Wrapper class for static accessor
   */
  public static class HighAvailibilityCuratorProvider {

    public static final HighAvailibilityCuratorProvider INSTANCE =
      new HighAvailibilityCuratorProvider();

    /**
     * Gets curator blocking if necessary to create it
     */
    public CuratorFramework getCurator(String zkUrl, Properties properties) throws IOException {
      return HighAvailabilityGroup.getCurator(zkUrl, properties);
    }
  }

  private static final Logger LOG = LoggerFactory.getLogger(PhoenixHAAdmin.class);

  /** The fully qualified ZK URL for an HBase cluster in format host:port:/hbase */
  private final String zkUrl;
  /** Configuration of this command line tool. */
  private final Configuration conf;
  /** Client properties which has copies of configuration defining ZK timeouts / retries. */
  private final Properties properties = new Properties();
  /** Curator Provider **/
  private final HighAvailibilityCuratorProvider highAvailibilityCuratorProvider;

  public PhoenixHAAdmin(Configuration conf) {
    this(getLocalZkUrl(conf), conf, HighAvailibilityCuratorProvider.INSTANCE);
  }

  public PhoenixHAAdmin(String zkUrl, Configuration conf) {
    this(zkUrl, conf, HighAvailibilityCuratorProvider.INSTANCE);
  }

  public PhoenixHAAdmin(String zkUrl, Configuration conf,
    HighAvailibilityCuratorProvider highAvailibilityCuratorProvider) {
    Preconditions.checkNotNull(zkUrl);
    Preconditions.checkNotNull(conf);
    Preconditions.checkNotNull(highAvailibilityCuratorProvider);
    this.zkUrl = JDBCUtil.formatUrl(zkUrl, RegistryType.ZK);
    this.conf = conf;
    conf.iterator().forEachRemaining(k -> properties.setProperty(k.getKey(), k.getValue()));
    this.highAvailibilityCuratorProvider = highAvailibilityCuratorProvider;
  }

  /**
   * Helper method to get local ZK fully qualified URL (host:port:/hbase) from configuration.
   */
  public static String getLocalZkUrl(Configuration conf) {
    String localZkQuorum = conf.get(HConstants.ZOOKEEPER_QUORUM);
    if (StringUtils.isEmpty(localZkQuorum)) {
      String msg = "ZK quorum not found by looking up key " + HConstants.ZOOKEEPER_QUORUM;
      LOG.error(msg);
      throw new IllegalArgumentException(msg);
    }

    String portStr = conf.get(HConstants.ZOOKEEPER_CLIENT_PORT);
    int port = HConstants.DEFAULT_ZOOKEEPER_CLIENT_PORT;
    if (portStr != null) {
      try {
        port = Integer.parseInt(portStr);
      } catch (NumberFormatException e) {
        String msg =
          String.format("Unrecognized ZK port '%s' in ZK quorum '%s'", portStr, localZkQuorum);
        LOG.error(msg, e);
        throw new IllegalArgumentException(msg, e);
      }
    }

    String localZkRoot =
      conf.get(HConstants.ZOOKEEPER_ZNODE_PARENT, HConstants.DEFAULT_ZOOKEEPER_ZNODE_PARENT);
    return String.format("%s:%d:%s", localZkQuorum, port, localZkRoot);
  }

  /**
   * Gets curator from the cache if available otherwise calls into getCurator to make it.
   */
  public CuratorFramework getCurator() throws IOException {
    return highAvailibilityCuratorProvider.getCurator(zkUrl, properties);
  }

  /**
   * Check if current cluster is ACTIVE role for the given HA group. In case of Exception when it
   * fails to read cluster role data from the current cluster, it will assume current cluster is not
   * ACTIVE. Callers should be aware of "false positive" possibility especially due to connectivity
   * issue between this tool and remote ZK cluster.
   * @param haGroupName the HA group name; a cluster can be associated with multiple HA groups
   * @return true if current cluster is ACTIVE role, otherwise false
   */
  boolean isCurrentActiveCluster(String haGroupName) {
    try {
      byte[] data = getCurator().getData().forPath(toPath(haGroupName));

      Optional<ClusterRoleRecord> record = ClusterRoleRecord.fromJson(data);
      return record.isPresent()
        && record.get().getRole(zkUrl) == ClusterRoleRecord.ClusterRole.ACTIVE;
    } catch (KeeperException.NoNodeException ne) {
      LOG.info("No role record found for HA group {} on '{}', assuming it is not active",
        haGroupName, zkUrl);
      return false;
    } catch (Exception e) {
      LOG.warn("Got exception when reading record for {} on cluster {}", haGroupName, zkUrl, e);
      return false;
    }
  }

  /**
   * This lists all cluster role records stored in the zookeeper nodes. This read-only operation and
   * hence no side effect on the ZK cluster.
   */
  public List<ClusterRoleRecord> listAllClusterRoleRecordsOnZookeeper() throws IOException {
    List<String> haGroupNames;
    try {
      haGroupNames = getCurator().getChildren().forPath(ZKPaths.PATH_SEPARATOR);
    } catch (Exception e) {
      String msg = String.format("Got exception when listing all HA groups in %s", zkUrl);
      LOG.error(msg);
      throw new IOException(msg, e);
    }

    List<ClusterRoleRecord> records = new ArrayList<>();
    List<String> failedHaGroups = new ArrayList<>();
    for (String haGroupName : haGroupNames) {
      try {
        byte[] data = getCurator().getData().forPath(ZKPaths.PATH_SEPARATOR + haGroupName);
        Optional<ClusterRoleRecord> record = ClusterRoleRecord.fromJson(data);
        if (record.isPresent()) {
          records.add(record.get());
        } else { // fail to deserialize data from JSON
          failedHaGroups.add(haGroupName);
        }
      } catch (Exception e) {
        LOG.warn("Got exception when reading data for HA group {}", haGroupName, e);
        failedHaGroups.add(haGroupName);
      }
    }

    if (!failedHaGroups.isEmpty()) {
      String msg = String.format(
        "Found following HA groups: %s. Fail to read cluster "
          + "role records for following HA groups: %s",
        String.join(",", haGroupNames), String.join(",", failedHaGroups));
      LOG.error(msg);
      throw new IOException(msg);
    }
    return records;
  }

  /**
   * Helper method to write the given cluster role records into the ZK clusters respectively. //
   * TODO: add retry logics
   * @param records  The cluster role record list to save on ZK
   * @param forceful if true, this method will ignore errors on other clusters; otherwise it will
   *                 not update next cluster (in order) if there is any failure on current cluster
   * @return a map of HA group name to list cluster's url for cluster role record failing to write
   */
  public Map<String, List<String>> syncClusterRoleRecords(List<ClusterRoleRecord> records,
    boolean forceful) throws IOException {
    Map<String, List<String>> failedHaGroups = new HashMap<>();
    for (ClusterRoleRecord record : records) {
      String haGroupName = record.getHaGroupName();
      try (
        PhoenixHAAdmin admin1 =
          new PhoenixHAAdmin(record.getUrl1(), conf, HighAvailibilityCuratorProvider.INSTANCE);
        PhoenixHAAdmin admin2 =
          new PhoenixHAAdmin(record.getUrl2(), conf, HighAvailibilityCuratorProvider.INSTANCE)) {
        // Update the cluster previously ACTIVE cluster first.
        // It reduces the chances of split-brain between clients and clusters.
        // If can not determine previous ACTIVE cluster, update new STANDBY cluster first.
        final PairOfSameType<PhoenixHAAdmin> pair;
        if (admin1.isCurrentActiveCluster(haGroupName)) {
          pair = new PairOfSameType<>(admin1, admin2);
        } else if (admin2.isCurrentActiveCluster(haGroupName)) {
          pair = new PairOfSameType<>(admin2, admin1);
        } else if (record.getRole(admin1.getZkUrl()) == ClusterRoleRecord.ClusterRole.STANDBY) {
          pair = new PairOfSameType<>(admin1, admin2);
        } else {
          pair = new PairOfSameType<>(admin2, admin1);
        }
        try {
          pair.getFirst().createOrUpdateDataOnZookeeper(record);
        } catch (IOException e) {
          LOG.error("Error to create or update data on Zookeeper, cluster={}, record={}",
            pair.getFirst(), record);
          failedHaGroups.computeIfAbsent(haGroupName, (k) -> new ArrayList<>())
            .add(pair.getFirst().getZkUrl());
          if (!forceful) {
            LOG.error("-forceful option is not enabled by command line options, "
              + "skip writing record {} to ZK clusters", record);
            // skip writing this record to second ZK cluster, so we should report that
            failedHaGroups.computeIfAbsent(haGroupName, (k) -> new ArrayList<>())
              .add(pair.getSecond().getZkUrl());
            continue; // do not update this record on second cluster
          }
        }
        try {
          pair.getSecond().createOrUpdateDataOnZookeeper(record);
        } catch (IOException e) {
          LOG.error("Error to create or update data on Zookeeper, cluster={}, record={}",
            pair.getFirst(), record);
          failedHaGroups.computeIfAbsent(haGroupName, (k) -> new ArrayList<>())
            .add(pair.getSecond().getZkUrl());
        }
      }
    }
    return failedHaGroups;
  }

  /**
   * Verify cluster role records stored in local ZK nodes, and repair with remote znodes for any
   * inconsistency.
   * @return a list of HA group names with inconsistent cluster role records, or empty list
   */
  List<String> verifyAndRepairWithRemoteZnode() throws Exception {
    List<String> inconsistentHaGroups = new ArrayList<>();
    for (ClusterRoleRecord record : listAllClusterRoleRecordsOnZookeeper()) {
      // the remote znodes may be on different ZK clusters.
      if (record.getRole(zkUrl) == ClusterRoleRecord.ClusterRole.UNKNOWN) {
        LOG.warn("Unknown cluster role for cluster '{}' in record {}", zkUrl, record);
        continue;
      }
      String remoteZkUrl = record.getUrl1().equals(zkUrl) ? record.getUrl2() : record.getUrl1();
      try (PhoenixHAAdmin remoteAdmin =
        new PhoenixHAAdmin(remoteZkUrl, conf, HighAvailibilityCuratorProvider.INSTANCE)) {
        ClusterRoleRecord remoteRecord;
        try {
          String zPath = toPath(record.getHaGroupName());
          byte[] data = remoteAdmin.getCurator().getData().forPath(zPath);
          Optional<ClusterRoleRecord> recordOptional = ClusterRoleRecord.fromJson(data);
          if (!recordOptional.isPresent()) {
            remoteAdmin.createOrUpdateDataOnZookeeper(record);
            continue;
          }
          remoteRecord = recordOptional.get();
        } catch (KeeperException.NoNodeException ne) {
          LOG.warn("No record znode yet, creating for HA group {} on {}", record.getHaGroupName(),
            remoteAdmin);
          remoteAdmin.createDataOnZookeeper(record);
          LOG.info("Created znode on cluster {} with record {}", remoteAdmin, record);
          continue;
        } catch (Exception e) {
          LOG.error("Error to get data on remote cluster {} for HA group {}", remoteAdmin,
            record.getHaGroupName(), e);
          continue;
        }

        if (!record.getHaGroupName().equals(remoteRecord.getHaGroupName())) {
          inconsistentHaGroups.add(record.getHaGroupName());
          LOG.error("INTERNAL ERROR: got cluster role record for different HA groups."
            + " Local record: {}, remote record: {}", record, remoteRecord);
        } else if (remoteRecord.isNewerThan(record)) {
          createOrUpdateDataOnZookeeper(remoteRecord);
        } else if (record.isNewerThan(remoteRecord)) {
          remoteAdmin.createOrUpdateDataOnZookeeper(record);
        } else if (record.equals(remoteRecord)) {
          LOG.info("Cluster role record {} is consistent", record);
        } else {
          inconsistentHaGroups.add(record.getHaGroupName());
          LOG.error(
            "Cluster role record for HA group {} is inconsistent. On cluster "
              + "{} the record is {}; on cluster {} the record is {}",
            record.getHaGroupName(), this, record, remoteAdmin, remoteRecord);
        }
      }
    }
    return inconsistentHaGroups;
  }

  /**
   * This updates the cluster role data on the zookeeper it connects to. To avoid conflicts, it does
   * CAS (compare-and-set) when updating. The constraint is that the given record's version should
   * be larger the existing record's version. This is a way to help avoiding manual update
   * conflicts. If the given record can not meet version check, it will reject the update request
   * and client (human operator or external system) should retry.
   * @param record the new cluster role record to be saved on ZK
   * @return true if the data on ZK is updated otherwise false
   * @throws IOException if it fails to update the cluster role data on ZK
   */
  public boolean createOrUpdateDataOnZookeeper(ClusterRoleRecord record) throws IOException {
    String haGroupName = record.getHaGroupName();
    byte[] data;
    try {
      data = getCurator().getData().forPath(toPath(haGroupName)); // Get initial data
    } catch (KeeperException.NoNodeException ne) {
      LOG.info("No record znode yet, creating for HA group {} on {}", haGroupName, zkUrl);
      createDataOnZookeeper(record);
      LOG.info("Created znode for HA group {} with record data {} on {}", haGroupName, record,
        zkUrl);
      return true;
    } catch (Exception e) {
      String msg =
        String.format("Fail to read cluster role record data for HA group %s " + "on cluster '%s'",
          haGroupName, zkUrl);
      LOG.error(msg, e);
      throw new IOException(msg, e);
    }

    Optional<ClusterRoleRecord> existingRecordOptional = ClusterRoleRecord.fromJson(data);
    if (!existingRecordOptional.isPresent()) {
      String msg = String.format(
        "Fail to parse existing cluster role record data for HA " + "group %s", haGroupName);
      LOG.error(msg);
      throw new IOException(msg);
    }

    ClusterRoleRecord existingRecord = existingRecordOptional.get();
    if (record.getVersion() < existingRecord.getVersion()) {
      String msg = String.format(
        "Invalid new cluster role record for HA group '%s' "
          + "because new record's version V%d is smaller than existing V%d. "
          + "Existing role record: %s. New role record fail to save: %s",
        haGroupName, record.getVersion(), existingRecord.getVersion(), existingRecord, record);
      LOG.warn(msg);
      return false; // return instead of error out to tolerate
    }

    if (record.getVersion() == existingRecord.getVersion()) {
      if (record.equals(existingRecord)) {
        LOG.debug("Cluster role does not change since last update on ZK.");
        return false; // no need to update iff they are the same.
      } else {
        String msg = String.format(
          "Invalid new cluster role record for HA group '%s' "
            + "because it has the same version V%d but inconsistent data. "
            + "Existing role record: %s. New role record fail to save: %s",
          haGroupName, record.getVersion(), existingRecord, record);
        LOG.error(msg);
        throw new IOException(msg);
      }
    }

    return updateDataOnZookeeper(existingRecord, record);
  }

  /**
   * Helper to create the znode on the ZK cluster.
   */
  private void createDataOnZookeeper(ClusterRoleRecord record) throws IOException {
    String haGroupName = record.getHaGroupName();
    // znode path for given haGroup name assuming namespace (prefix) has been set.
    String haGroupPath = toPath(haGroupName);
    try {
      getCurator().create().creatingParentsIfNeeded().withMode(CreateMode.PERSISTENT)
        .forPath(haGroupPath, ClusterRoleRecord.toJson(record));
    } catch (KeeperException.NodeExistsException nee) {
      // this method assumes that the znode doesn't exist yet, but it could have been
      // created between now and the last time we checked. We swallow the exception and
      // rely on our caller to check to make sure the znode that's saved is correct
      LOG.warn("Znode for HA group {} already exists. ", haGroupPath, nee);
    } catch (Exception e) {
      LOG.error("Fail to initialize the znode for HA group {} with record data {}", haGroupPath,
        record, e);
      throw new IOException("Fail to initialize znode for HA group " + haGroupPath, e);
    }
  }

  /**
   * Helper to update the znode on ZK cluster assuming current data is the given old record.
   */
  private boolean updateDataOnZookeeper(ClusterRoleRecord oldRecord, ClusterRoleRecord newRecord)
    throws IOException {
    // znode path for given haGroup name assuming namespace (prefix) has been set.
    String haGroupPath = toPath(newRecord.getHaGroupName());
    RetryPolicy retryPolicy = HighAvailabilityGroup.createRetryPolicy(properties);
    try {
      DistributedAtomicValue v = new DistributedAtomicValue(getCurator(), haGroupPath, retryPolicy);
      AtomicValue<byte[]> result =
        v.compareAndSet(ClusterRoleRecord.toJson(oldRecord), ClusterRoleRecord.toJson(newRecord));
      LOG.info("Updated cluster role record ({}->{}) for HA group {} on cluster '{}': {}",
        oldRecord.getVersion(), newRecord.getVersion(), newRecord.getHaGroupName(), zkUrl,
        result.succeeded() ? "succeeded" : "failed");
      LOG.debug("Old DistributedAtomicValue: {}, New DistributedAtomicValue: {},",
        new String(result.preValue(), StandardCharsets.UTF_8),
        new String(result.postValue(), StandardCharsets.UTF_8));
      return result.succeeded();
    } catch (Exception e) {
      String msg = String.format(
        "Fail to update cluster role record to ZK for the HA " + "group %s due to '%s'."
          + "Existing role record: %s. New role record fail to save: %s",
        haGroupPath, e.getMessage(), oldRecord, newRecord);
      LOG.error(msg, e);
      throw new IOException(msg, e);
    }
  }

  /**
   * Helper method to get ZK path for an HA group given the HA group name. It assumes the ZK
   * namespace (prefix) has been set.
   */
  public static String toPath(String haGroupName) {
    return ZKPaths.PATH_SEPARATOR + haGroupName;
  }

  public String getZkUrl() {
    return zkUrl;
  }

  @Override
  public void close() {
    LOG.debug("PhoenixHAAdmin for {} is now closed.", zkUrl);
  }

  @Override
  public String toString() {
    return zkUrl;
  }
}
