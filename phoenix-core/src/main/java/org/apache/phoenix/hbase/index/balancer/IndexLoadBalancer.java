/**
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
package org.apache.phoenix.hbase.index.balancer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.ClusterStatus;
import org.apache.hadoop.hbase.HBaseIOException;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.master.LoadBalancer;
import org.apache.hadoop.hbase.master.MasterServices;
import org.apache.hadoop.hbase.master.RegionPlan;
import org.apache.hadoop.hbase.master.RegionState;
import org.apache.hadoop.hbase.master.RegionStates;
import org.apache.hadoop.hbase.master.balancer.StochasticLoadBalancer;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.phoenix.util.EnvironmentEdgeManager;
import org.apache.hadoop.util.ReflectionUtils;

/**
 * <p>This class is an extension of the load balancer class. 
 * It allows to co-locate the regions of the user table and the regions of corresponding
 * index table if any.</p> 
 * 
 * </>roundRobinAssignment, retainAssignment -> index regions will follow the actual table regions. 
 * randomAssignment, balancerCluster -> either index table or actual table region(s) will follow
 * each other based on which ever comes first.</p> 
 * 
 * <p>In case of master failover there is a chance that the znodes of the index
 * table and actual table are left behind. Then in that scenario we may get randomAssignment for
 * either the actual table region first or the index table region first.</p>
 * 
 * <p>In case of balancing by table any table can balance first.</p>
 * 
 */

public class IndexLoadBalancer implements LoadBalancer {

    private static final Log LOG = LogFactory.getLog(IndexLoadBalancer.class);

    public static final byte[] PARENT_TABLE_KEY = Bytes.toBytes("PARENT_TABLE");

    public static final String INDEX_BALANCER_DELEGATOR = "hbase.index.balancer.delegator.class";

    private LoadBalancer delegator;

    private MasterServices master;

    private Configuration conf;

    private ClusterStatus clusterStatus;

    private static final Random RANDOM = new Random(EnvironmentEdgeManager.currentTimeMillis());

    Map<TableName, TableName> userTableVsIndexTable = new HashMap<TableName, TableName>();

    Map<TableName, TableName> indexTableVsUserTable = new HashMap<TableName, TableName>();

    /**
     * Maintains colocation information of user regions and corresponding index regions.
     */
    private Map<TableName, Map<ImmutableBytesWritable, ServerName>> colocationInfo =
            new ConcurrentHashMap<TableName, Map<ImmutableBytesWritable, ServerName>>();

    private Set<TableName> balancedTables = new HashSet<TableName>();

    private boolean stopped = false;

    @Override
    public void initialize() throws HBaseIOException {
        Class<? extends LoadBalancer> delegatorKlass =
                conf.getClass(INDEX_BALANCER_DELEGATOR, StochasticLoadBalancer.class,
                    LoadBalancer.class);
        this.delegator = ReflectionUtils.newInstance(delegatorKlass, conf);
        this.delegator.setClusterStatus(clusterStatus);
        this.delegator.setMasterServices(this.master);
        this.delegator.initialize();
        try {
            populateTablesToColocate(this.master.getTableDescriptors().getAll());
        } catch (IOException e) {
            throw new HBaseIOException(e);
        }
    }

    @Override
    public Configuration getConf() {
        return conf;
    }

    @Override
    public void setConf(Configuration configuration) {
        this.conf = configuration;
    }

    @Override
    public void onConfigurationChange(Configuration conf) {
        setConf(conf);
    }

    @Override
    public void setClusterStatus(ClusterStatus st) {
        this.clusterStatus = st;
    }

    public Map<TableName, Map<ImmutableBytesWritable, ServerName>> getColocationInfo() {
        return colocationInfo;
    }

    @Override
    public void setMasterServices(MasterServices masterServices) {
        this.master = masterServices;
    }

    @Override
    public List<RegionPlan> balanceCluster(Map<ServerName, List<HRegionInfo>> clusterState)
            throws HBaseIOException {
        synchronized (this.colocationInfo) {
            boolean balanceByTable = conf.getBoolean("hbase.master.loadbalance.bytable", false);
            List<RegionPlan> regionPlans = null;

            TableName tableName = null;
            if (balanceByTable) {
                Map<ImmutableBytesWritable, ServerName> tableKeys = null;
                for (Entry<ServerName, List<HRegionInfo>> serverVsRegionList : clusterState
                        .entrySet()) {
                    ServerName sn = serverVsRegionList.getKey();
                    List<HRegionInfo> regionInfos = serverVsRegionList.getValue();
                    if (regionInfos.isEmpty()) {
                        continue;
                    }
                    if (!isTableColocated(regionInfos.get(0).getTable())) {
                        return this.delegator.balanceCluster(clusterState);
                    }
                    // Just get the table name from any one of the values in the regioninfo list
                    if (tableName == null) {
                        tableName = regionInfos.get(0).getTable();
                        tableKeys = this.colocationInfo.get(tableName);
                    }
                    // Check and modify the colocation info map based on values of cluster state
                    // because we
                    // will
                    // call balancer only when the cluster is in stable and reliable state.
                    if (tableKeys != null) {
                        for (HRegionInfo hri : regionInfos) {
                            updateServer(tableKeys, sn, hri);
                        }
                    }
                }
                // If user table is already balanced find the index table plans from the user table
                // plans
                // or vice verca.
                TableName mappedTableName = getMappedTableToColocate(tableName);
                if (balancedTables.contains(mappedTableName)) {
                    balancedTables.remove(mappedTableName);
                    regionPlans = new ArrayList<RegionPlan>();
                    return prepareRegionPlansForClusterState(clusterState, regionPlans);
                } else {
                    balancedTables.add(tableName);
                    regionPlans = this.delegator.balanceCluster(clusterState);
                    if (regionPlans == null) {
                        if (LOG.isDebugEnabled()) {
                            LOG.debug(tableName + " regions already balanced.");
                        }
                        return null;
                    } else {
                        updateRegionPlans(regionPlans);
                        return regionPlans;
                    }
                }

            } else {
                if (LOG.isDebugEnabled()) {
                    LOG.debug("Seperating user tables and index tables regions of "
                            + "each region server in the cluster.");
                }
                Map<ServerName, List<HRegionInfo>> userClusterState =
                        new HashMap<ServerName, List<HRegionInfo>>();
                Map<ServerName, List<HRegionInfo>> indexClusterState =
                        new HashMap<ServerName, List<HRegionInfo>>();
                for (Entry<ServerName, List<HRegionInfo>> serverVsRegionList : clusterState
                        .entrySet()) {
                    ServerName sn = serverVsRegionList.getKey();
                    List<HRegionInfo> regionsInfos = serverVsRegionList.getValue();
                    List<HRegionInfo> idxRegionsToBeMoved = new ArrayList<HRegionInfo>();
                    List<HRegionInfo> userRegionsToBeMoved = new ArrayList<HRegionInfo>();
                    for (HRegionInfo hri : regionsInfos) {
                        if (hri.isMetaRegion()) {
                            continue;
                        }
                        tableName = hri.getTable();
                        // Check and modify the colocation info map based on values of cluster state
                        // because we
                        // will
                        // call balancer only when the cluster is in stable and reliable state.
                        if (isTableColocated(tableName)) {
                            // table name may change every time thats why always need to get table
                            // entries.
                            Map<ImmutableBytesWritable, ServerName> tableKeys =
                                    this.colocationInfo.get(tableName);
                            if (tableKeys != null) {
                                updateServer(tableKeys, sn, hri);
                            }
                        }
                        if (indexTableVsUserTable.containsKey(tableName)) {
                            idxRegionsToBeMoved.add(hri);
                            continue;
                        }
                        userRegionsToBeMoved.add(hri);
                    }
                    // there may be dummy entries here if assignments by table is set
                    userClusterState.put(sn, userRegionsToBeMoved);
                    indexClusterState.put(sn, idxRegionsToBeMoved);
                }

                regionPlans = this.delegator.balanceCluster(userClusterState);
                if (regionPlans == null) {
                    if (LOG.isDebugEnabled()) {
                        LOG.debug("User region plan is null.");
                    }
                    regionPlans = new ArrayList<RegionPlan>();
                } else {
                    updateRegionPlans(regionPlans);
                }
                return prepareRegionPlansForClusterState(indexClusterState, regionPlans);
            }
        }
    }

    private void updateServer(Map<ImmutableBytesWritable, ServerName> tableKeys, ServerName sn,
            HRegionInfo hri) {
        ImmutableBytesWritable startKey = new ImmutableBytesWritable(hri.getStartKey());
        ServerName existingServer = tableKeys.get(startKey);
        if (!sn.equals(existingServer)) {
            if (LOG.isDebugEnabled()) {
                LOG.debug("There is a mismatch in the existing server name for the region " + hri
                        + ".  Replacing the server " + existingServer + " with " + sn + ".");
            }
            tableKeys.put(startKey, sn);
        }
    }

    /**
     * Prepare region plans for cluster state
     * @param clusterState if balancing is table wise then cluster state contains only indexed or
     *            index table regions, otherwise it contains all index tables regions.
     * @param regionPlans
     * @return
     */
    private List<RegionPlan> prepareRegionPlansForClusterState(
            Map<ServerName, List<HRegionInfo>> clusterState, List<RegionPlan> regionPlans) {
        if (regionPlans == null) regionPlans = new ArrayList<RegionPlan>();
        ImmutableBytesWritable startKey = new ImmutableBytesWritable();
        for (Entry<ServerName, List<HRegionInfo>> serverVsRegionList : clusterState.entrySet()) {
            List<HRegionInfo> regionInfos = serverVsRegionList.getValue();
            ServerName server = serverVsRegionList.getKey();
            for (HRegionInfo regionInfo : regionInfos) {
                if (!isTableColocated(regionInfo.getTable())) continue;
                TableName mappedTableName = getMappedTableToColocate(regionInfo.getTable());
                startKey.set(regionInfo.getStartKey());
                ServerName sn = this.colocationInfo.get(mappedTableName).get(startKey);
                if (sn.equals(server)) {
                    continue;
                } else {
                    RegionPlan rp = new RegionPlan(regionInfo, server, sn);
                    if (LOG.isDebugEnabled()) {
                        LOG.debug("Selected server " + rp.getDestination()
                                + " as destination for region "
                                + regionInfo.getRegionNameAsString() + " from colocation info.");
                    }
                    regionOnline(regionInfo, rp.getDestination());
                    regionPlans.add(rp);
                }
            }
        }
        return regionPlans;
    }

    private void updateRegionPlans(List<RegionPlan> regionPlans) {
        for (RegionPlan regionPlan : regionPlans) {
            HRegionInfo hri = regionPlan.getRegionInfo();
            if (!isTableColocated(hri.getTable())) continue;
            if (LOG.isDebugEnabled()) {
                LOG.debug("Saving region plan of region " + hri.getRegionNameAsString() + '.');
            }
            regionOnline(hri, regionPlan.getDestination());
        }
    }

    @Override
    public Map<ServerName, List<HRegionInfo>> roundRobinAssignment(List<HRegionInfo> regions,
            List<ServerName> servers) throws HBaseIOException {
        List<HRegionInfo> userRegions = new ArrayList<HRegionInfo>();
        List<HRegionInfo> indexRegions = new ArrayList<HRegionInfo>();
        for (HRegionInfo hri : regions) {
            seperateUserAndIndexRegion(hri, userRegions, indexRegions);
        }
        Map<ServerName, List<HRegionInfo>> bulkPlan = null;
        if (!userRegions.isEmpty()) {
            bulkPlan = this.delegator.roundRobinAssignment(userRegions, servers);
            // This should not happen.
            if (null == bulkPlan) {
                if (LOG.isDebugEnabled()) {
                    LOG.debug("No region plans selected for user regions in roundRobinAssignment.");
                }
                return null;
            }
            savePlan(bulkPlan);
        }
        bulkPlan = prepareIndexRegionsPlan(indexRegions, bulkPlan, servers);
        return bulkPlan;
    }

    private void seperateUserAndIndexRegion(HRegionInfo hri, List<HRegionInfo> userRegions,
            List<HRegionInfo> indexRegions) {
        if (indexTableVsUserTable.containsKey(hri.getTable())) {
            indexRegions.add(hri);
            return;
        }
        userRegions.add(hri);
    }

    private Map<ServerName, List<HRegionInfo>> prepareIndexRegionsPlan(
            List<HRegionInfo> indexRegions, Map<ServerName, List<HRegionInfo>> bulkPlan,
            List<ServerName> servers) throws HBaseIOException {
        if (null != indexRegions && !indexRegions.isEmpty()) {
            if (null == bulkPlan) {
                bulkPlan = new ConcurrentHashMap<ServerName, List<HRegionInfo>>();
            }
            for (HRegionInfo hri : indexRegions) {
                if (LOG.isDebugEnabled()) {
                    LOG.debug("Preparing region plan for index region "
                            + hri.getRegionNameAsString() + '.');
                }
                ServerName destServer = getDestServerForIdxRegion(hri);
                List<HRegionInfo> destServerRegions = null;
                if (destServer == null) destServer = this.randomAssignment(hri, servers);
                if (destServer != null) {
                    destServerRegions = bulkPlan.get(destServer);
                    if (null == destServerRegions) {
                        destServerRegions = new ArrayList<HRegionInfo>();
                        bulkPlan.put(destServer, destServerRegions);
                    }
                    if (LOG.isDebugEnabled()) {
                        LOG.debug("Server " + destServer + " selected for region "
                                + hri.getRegionNameAsString() + '.');
                    }
                    destServerRegions.add(hri);
                    regionOnline(hri, destServer);
                }
            }
        }
        return bulkPlan;
    }

    private ServerName getDestServerForIdxRegion(HRegionInfo hri) {
        // Every time we calculate the table name because in case of master restart the index
        // regions
        // may be coming for different index tables.
        TableName actualTable = getMappedTableToColocate(hri.getTable());
        ImmutableBytesWritable startkey = new ImmutableBytesWritable(hri.getStartKey());
        synchronized (this.colocationInfo) {

            Map<ImmutableBytesWritable, ServerName> tableKeys = colocationInfo.get(actualTable);
            if (null == tableKeys) {
                // Can this case come
                return null;
            }
            if (tableKeys.containsKey(startkey)) {
                // put index region location if corresponding user region found in regionLocation
                // map.
                ServerName sn = tableKeys.get(startkey);
                regionOnline(hri, sn);
                return sn;
            }
        }
        return null;
    }

    private void savePlan(Map<ServerName, List<HRegionInfo>> bulkPlan) {
        synchronized (this.colocationInfo) {
            for (Entry<ServerName, List<HRegionInfo>> e : bulkPlan.entrySet()) {
                if (LOG.isDebugEnabled()) {
                    LOG.debug("Saving user regions' plans for server " + e.getKey() + '.');
                }
                for (HRegionInfo hri : e.getValue()) {
                    if (!isTableColocated(hri.getTable())) continue;
                    regionOnline(hri, e.getKey());
                }
                if (LOG.isDebugEnabled()) {
                    LOG.debug("Saved user regions' plans for server " + e.getKey() + '.');
                }
            }
        }
    }

    @Override
    public Map<ServerName, List<HRegionInfo>> retainAssignment(
            Map<HRegionInfo, ServerName> regions, List<ServerName> servers) throws HBaseIOException {
        Map<HRegionInfo, ServerName> userRegionsMap =
                new ConcurrentHashMap<HRegionInfo, ServerName>();
        List<HRegionInfo> indexRegions = new ArrayList<HRegionInfo>();
        for (Entry<HRegionInfo, ServerName> e : regions.entrySet()) {
            seperateUserAndIndexRegion(e, userRegionsMap, indexRegions, servers);
        }
        Map<ServerName, List<HRegionInfo>> bulkPlan = null;
        if (!userRegionsMap.isEmpty()) {
            bulkPlan = this.delegator.retainAssignment(userRegionsMap, servers);
            if (bulkPlan == null) {
                if (LOG.isDebugEnabled()) {
                    LOG.debug("Empty region plan for user regions.");
                }
                return null;
            }
            savePlan(bulkPlan);
        }
        bulkPlan = prepareIndexRegionsPlan(indexRegions, bulkPlan, servers);
        return bulkPlan;
    }

    private void seperateUserAndIndexRegion(Entry<HRegionInfo, ServerName> e,
            Map<HRegionInfo, ServerName> userRegionsMap, List<HRegionInfo> indexRegions,
            List<ServerName> servers) {
        HRegionInfo hri = e.getKey();
        if (indexTableVsUserTable.containsKey(hri.getTable())) {
            indexRegions.add(hri);
            return;
        }
        if (e.getValue() == null) {
            userRegionsMap.put(hri, servers.get(RANDOM.nextInt(servers.size())));
        } else {
            userRegionsMap.put(hri, e.getValue());
        }
    }

    @Override
    public Map<HRegionInfo, ServerName> immediateAssignment(List<HRegionInfo> regions,
            List<ServerName> servers) throws HBaseIOException {
        return this.delegator.immediateAssignment(regions, servers);
    }

    @Override
    public ServerName randomAssignment(HRegionInfo regionInfo, List<ServerName> servers)
            throws HBaseIOException {
        if (!isTableColocated(regionInfo.getTable())) {
            return this.delegator.randomAssignment(regionInfo, servers);
        }
        ServerName sn = getServerNameFromMap(regionInfo, servers);
        if (sn == null) {
            if (LOG.isDebugEnabled()) {
                LOG.debug("No server found for region " + regionInfo.getRegionNameAsString() + '.');
            }
            sn = getRandomServer(regionInfo, servers);
        }
        if (LOG.isDebugEnabled()) {
            LOG.debug("Destination server for region " + regionInfo.getRegionNameAsString()
                    + " is " + ((sn == null) ? "null" : sn.toString()) + '.');
        }
        return sn;
    }

    private ServerName getRandomServer(HRegionInfo regionInfo, List<ServerName> servers)
            throws HBaseIOException {
        ServerName sn = null;
        sn = this.delegator.randomAssignment(regionInfo, servers);
        if (sn == null) return null;
        regionOnline(regionInfo, sn);
        return sn;
    }

    private ServerName getServerNameFromMap(HRegionInfo regionInfo, List<ServerName> onlineServers) {
        TableName tableName = regionInfo.getTable();
        TableName mappedTable = getMappedTableToColocate(regionInfo.getTable());
        ImmutableBytesWritable startKey = new ImmutableBytesWritable(regionInfo.getStartKey());
        synchronized (this.colocationInfo) {
            Map<ImmutableBytesWritable, ServerName> correspondingTableKeys =
                    this.colocationInfo.get(mappedTable);
            Map<ImmutableBytesWritable, ServerName> actualTableKeys =
                    this.colocationInfo.get(tableName);

            if (null != correspondingTableKeys) {
                if (correspondingTableKeys.containsKey(startKey)) {
                    ServerName previousServer = null;
                    if (null != actualTableKeys) {
                        previousServer = actualTableKeys.get(startKey);
                    }
                    ServerName sn = correspondingTableKeys.get(startKey);
                    if (null != previousServer) {
                        // if servername of index region and user region are same in colocationInfo
                        // clean
                        // previous plans and return null
                        if (previousServer.equals(sn)) {
                            correspondingTableKeys.remove(startKey);
                            actualTableKeys.remove(startKey);
                            if (LOG.isDebugEnabled()) {
                                LOG
                                        .debug("Both user region plan and corresponding index region plan "
                                                + "in colocation info are same. Hence clearing the plans to select new plan"
                                                + " for the region "
                                                + regionInfo.getRegionNameAsString() + ".");
                            }
                            return null;
                        }
                    }
                    if (sn != null && onlineServers.contains(sn)) {
                        if (LOG.isDebugEnabled()) {
                            LOG.debug("Updating the region plan of the region "
                                    + regionInfo.getRegionNameAsString() + " with server " + sn);
                        }
                        regionOnline(regionInfo, sn);
                        return sn;
                    } else if (sn != null) {
                        if (LOG.isDebugEnabled()) {
                            LOG.debug("The location " + sn + " of region with start key"
                                    + Bytes.toStringBinary(regionInfo.getStartKey())
                                    + " is not in online. Selecting other region server.");
                        }
                        return null;
                    }
                }
            } else {
                if (LOG.isDebugEnabled()) {
                    LOG.debug("No region plans in colocationInfo for table " + mappedTable);
                }
            }
            return null;
        }
    }

    @Override
    public void regionOnline(HRegionInfo regionInfo, ServerName sn) {
        TableName tableName = regionInfo.getTable();
        synchronized (this.colocationInfo) {
            Map<ImmutableBytesWritable, ServerName> tabkeKeys = this.colocationInfo.get(tableName);
            if (tabkeKeys == null) {
                tabkeKeys = new ConcurrentHashMap<ImmutableBytesWritable, ServerName>();
                this.colocationInfo.put(tableName, tabkeKeys);
            }
            tabkeKeys.put(new ImmutableBytesWritable(regionInfo.getStartKey()), sn);
        }
    }

    public void clearTableRegionPlans(TableName tableName) {
        if (LOG.isDebugEnabled()) {
            LOG.debug("Clearing regions plans from colocationInfo for table " + tableName);
        }
        synchronized (this.colocationInfo) {
            this.colocationInfo.remove(tableName);
        }
    }

    @Override
    public void regionOffline(HRegionInfo regionInfo) {
        TableName tableName = regionInfo.getTable();
        synchronized (this.colocationInfo) {
            Map<ImmutableBytesWritable, ServerName> tableKeys = this.colocationInfo.get(tableName);
            if (null == tableKeys) {
                if (LOG.isDebugEnabled()) {
                    LOG.debug("No regions of table " + tableName + " in the colocationInfo.");
                }
            } else {
                tableKeys.remove(new ImmutableBytesWritable(regionInfo.getStartKey()));
                if (LOG.isDebugEnabled()) {
                    LOG.debug("The regioninfo " + regionInfo + " removed from the colocationInfo");
                }
            }
        }
    }

    @Override
    public boolean isStopped() {
        return stopped;
    }

    @Override
    public void stop(String why) {
        LOG.info("Load Balancer stop requested: " + why);
        stopped = true;
    }

    public void populateTablesToColocate(Map<String, HTableDescriptor> tableDescriptors) {
        HTableDescriptor desc = null;
        for (Entry<String, HTableDescriptor> entry : tableDescriptors.entrySet()) {
            desc = entry.getValue();
            if (desc.getValue(PARENT_TABLE_KEY) != null) {
                addTablesToColocate(TableName.valueOf(desc.getValue(PARENT_TABLE_KEY)), desc
                        .getTableName());
            }
        }
    }

    /**
     * Add tables whose regions to co-locate.
     * @param userTable
     * @param indexTable
     */
    public void addTablesToColocate(TableName userTable, TableName indexTable) {
        if (userTable.equals(indexTable)) {
            throw new IllegalArgumentException("Tables to colocate should not be same.");
        } else if (isTableColocated(userTable)) {
            throw new IllegalArgumentException("User table already colocated with table "
                    + getMappedTableToColocate(userTable));
        } else if (isTableColocated(indexTable)) {
            throw new IllegalArgumentException("Index table is already colocated with table "
                    + getMappedTableToColocate(indexTable));
        }
        userTableVsIndexTable.put(userTable, indexTable);
        indexTableVsUserTable.put(indexTable, userTable);
    }

    /**
     * Removes the specified table and corresponding table from co-location.
     * @param table
     */
    public void removeTablesFromColocation(TableName table) {
        TableName other = userTableVsIndexTable.remove(table);
        if (other != null) {
            indexTableVsUserTable.remove(other);
        } else {
            other = indexTableVsUserTable.remove(table);
            if (other != null) userTableVsIndexTable.remove(other);
        }
    }

    /**
     * Return mapped table to co-locate.
     * @param tableName
     * @return index table if the specified table is user table or vice versa.
     */
    public TableName getMappedTableToColocate(TableName tableName) {
        TableName other = userTableVsIndexTable.get(tableName);
        return other == null ? indexTableVsUserTable.get(tableName) : other;
    }

    public boolean isTableColocated(TableName table) {
        return userTableVsIndexTable.containsKey(table) || indexTableVsUserTable.containsKey(table);
    }

    /**
     * Populates table's region locations into co-location info from master.
     * @param table
     */
    public void populateRegionLocations(TableName table) {
        synchronized (this.colocationInfo) {
            if (!isTableColocated(table)) {
                throw new IllegalArgumentException("Specified table " + table
                        + " should be in one of the tables to co-locate.");
            }
            RegionStates regionStates = this.master.getAssignmentManager().getRegionStates();
            List<HRegionInfo> onlineRegions = regionStates.getRegionsOfTable(table);
            for (HRegionInfo hri : onlineRegions) {
                regionOnline(hri, regionStates.getRegionServerOfRegion(hri));
            }
            Map<String, RegionState> regionsInTransition = regionStates.getRegionsInTransition();
            for (RegionState regionState : regionsInTransition.values()) {
                if (table.equals(regionState.getRegion().getTable())
                        && regionState.getServerName() != null) {
                    regionOnline(regionState.getRegion(), regionState.getServerName());
                }
            }
        }
    }
}
