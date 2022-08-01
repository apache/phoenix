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
package org.apache.phoenix.util;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.UnsupportedEncodingException;
import java.net.HttpURLConnection;
import java.net.MalformedURLException;
import java.net.ProtocolException;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.ZooKeeperConnectionException;
import org.apache.hadoop.hbase.zookeeper.ZKWatcher;
import org.apache.hadoop.mapreduce.MRJobConfig;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.proto.YarnServerResourceManagerServiceProtos.ActiveRMInfoProto;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.Stat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.protobuf.InvalidProtocolBufferException;

public class PhoenixMRJobUtil {

    private static final String YARN_LEADER_ELECTION = "/yarn-leader-election";
    private static final String ACTIVE_STANDBY_ELECTOR_LOCK = "ActiveStandbyElectorLock";
    private static final String RM_APPS_GET_ENDPOINT = "/ws/v1/cluster/apps";

    public static final String PHOENIX_INDEX_MR_QUEUE_NAME_PROPERTY =
            "phoenix.index.mr.scheduler.capacity.queuename";
    public static final String PHOENIX_INDEX_MR_MAP_MEMORY_PROPERTY =
            "phoenix.index.mr.scheduler.capacity.mapMemoryMB";
    public static final String PHOENIX_MR_CONCURRENT_MAP_LIMIT_PROPERTY =
    		"phoenix.mr.concurrent.map.limit";

    // Default MR Capacity Scheduler Configurations for Phoenix MR Index Build
    // Jobs
    public static final String DEFAULT_QUEUE_NAME = "default";
    public static final int DEFAULT_MR_CONCURRENT_MAP_LIMIT = 20;
    public static final int DEFAULT_MAP_MEMROY_MB = 5120;
    public static final String XMX_OPT = "-Xmx";

    public static final String RM_HTTP_SCHEME = "http";
    // TODO - Move these as properties?
    public static final int RM_CONNECT_TIMEOUT_MILLIS = 10 * 1000;
    public static final int RM_READ_TIMEOUT_MILLIS = 10 * 60 * 1000;

    private static final Logger LOGGER = LoggerFactory.getLogger(PhoenixMRJobUtil.class);

    public static final String PHOENIX_MR_SCHEDULER_TYPE_NAME = "phoenix.index.mr.scheduler.type";

    public enum MR_SCHEDULER_TYPE {
        CAPACITY, FAIR, NONE
    };

    public static String getRMWebAddress(Configuration config){
        return config.get(YarnConfiguration.RM_WEBAPP_ADDRESS,
                YarnConfiguration.DEFAULT_RM_WEBAPP_ADDRESS);
    }

    public static String getRMWebAddress(Configuration config, String Rmid){
        return config.get(YarnConfiguration.RM_WEBAPP_ADDRESS + "." + Rmid,
                YarnConfiguration.DEFAULT_RM_WEBAPP_ADDRESS);
    }

    public static String getActiveResourceManagerAddress(Configuration config, String zkQuorum)
            throws IOException, InterruptedException, KeeperException,
            InvalidProtocolBufferException, ZooKeeperConnectionException {
        // In case of yarn HA is enabled
        ZKWatcher zkw = null;
        ZooKeeper zk = null;
        String activeRMHost = null;
        try {
            zkw = new ZKWatcher(config, "get-active-yarnmanager", null);
            zk = new ZooKeeper(zkQuorum, 30000, zkw, false);

            List<String> children = zk.getChildren(YARN_LEADER_ELECTION, zkw);
            for (String subEntry : children) {
                List<String> subChildern =
                        zk.getChildren(YARN_LEADER_ELECTION + "/" + subEntry, zkw);
                for (String eachEntry : subChildern) {
                    if (eachEntry.contains(ACTIVE_STANDBY_ELECTOR_LOCK)) {
                        String path =
                                YARN_LEADER_ELECTION + "/" + subEntry + "/"
                                        + ACTIVE_STANDBY_ELECTOR_LOCK;
                        byte[] data = zk.getData(path, zkw, new Stat());
                        ActiveRMInfoProto proto = ActiveRMInfoProto.parseFrom(data);
                        String RmId = proto.getRmId();
                        LOGGER.info("Active RmId : " + RmId);

                        activeRMHost = PhoenixMRJobUtil.getRMWebAddress(config, RmId);
                        LOGGER.info("activeResourceManagerHostname = " + activeRMHost);

                    }
                }
            }
        } finally {
            if (zkw != null) zkw.close();
            if (zk != null) zk.close();
        }
        // In case of yarn HA is NOT enabled
        if (activeRMHost == null) {
            activeRMHost = PhoenixMRJobUtil.getRMWebAddress(config);
            LOGGER.info("ResourceManagerAddress from config = " + activeRMHost);
        }

        return activeRMHost;
    }

    public static String getJobsInformationFromRM(String rmAddress,
            Map<String, String> urlParams) throws MalformedURLException, ProtocolException,
            UnsupportedEncodingException, IOException {
        HttpURLConnection con = null;
        String response = null;
        String url = null;

        try {
            StringBuilder urlBuilder = new StringBuilder();

            urlBuilder.append(RM_HTTP_SCHEME + "://").append(rmAddress)
                    .append(RM_APPS_GET_ENDPOINT);

            if (urlParams != null && urlParams.size() != 0) {
                urlBuilder.append("?");
                for (String key : urlParams.keySet()) {
                    urlBuilder.append(key + "=" + urlParams.get(key) + "&");
                }
                urlBuilder.delete(urlBuilder.length() - 1, urlBuilder.length());
            }

            url = urlBuilder.toString();
            LOGGER.info("Attempt to get running/submitted jobs information from RM URL = " + url);

            URL obj = new URL(url);
            con = (HttpURLConnection) obj.openConnection();
            con.setInstanceFollowRedirects(true);
            con.setRequestMethod("GET");

            con.setConnectTimeout(RM_CONNECT_TIMEOUT_MILLIS);
            con.setReadTimeout(RM_READ_TIMEOUT_MILLIS);

            response = getTextContent(con.getInputStream());
        } finally {
            if (con != null) con.disconnect();
        }

        LOGGER.info("Result of attempt to get running/submitted jobs from RM - URL=" + url
                + ",ResponseCode=" + con.getResponseCode() + ",Response=" + response);

        return response;
    }

    public static String getTextContent(InputStream is) throws IOException {
        BufferedReader in = null;
        StringBuilder response = null;
        try {
            in = new BufferedReader(new InputStreamReader(is, StandardCharsets.UTF_8));
            String inputLine;
            response = new StringBuilder();
            while ((inputLine = in.readLine()) != null) {
                response.append(inputLine).append("\n");
            }
        } finally {
            if (in != null) in.close();
            if (is != null) {
                is.close();
            }
        }
        return response.toString();
    }

    public static void shutdown(ExecutorService pool) throws InterruptedException {
        pool.shutdown();
        LOGGER.debug("Shutdown called");
        pool.awaitTermination(200, TimeUnit.MILLISECONDS);
        LOGGER.debug("Await termination called to wait for 200 msec");
        if (!pool.isShutdown()) {
            pool.shutdownNow();
            LOGGER.debug("Await termination called to wait for 200 msec");
            pool.awaitTermination(100, TimeUnit.MILLISECONDS);
        }
        if (!pool.isShutdown()) {
            LOGGER.warn("Pool did not shutdown");
        }
    }

    /**
     * This method set the configuration values for Capacity scheduler.
     * @param conf - Configuration to which Capacity Queue information to be added
     */
    public static void updateCapacityQueueInfo(Configuration conf) {
        conf.set(MRJobConfig.QUEUE_NAME,
            conf.get(PHOENIX_INDEX_MR_QUEUE_NAME_PROPERTY, DEFAULT_QUEUE_NAME));

        conf.setInt(MRJobConfig.JOB_RUNNING_MAP_LIMIT,
                conf.getInt(PHOENIX_MR_CONCURRENT_MAP_LIMIT_PROPERTY, DEFAULT_MR_CONCURRENT_MAP_LIMIT));

        int mapMemoryMB = conf.getInt(PHOENIX_INDEX_MR_MAP_MEMORY_PROPERTY, DEFAULT_MAP_MEMROY_MB);

        conf.setInt(MRJobConfig.MAP_MEMORY_MB, mapMemoryMB);
        conf.set(MRJobConfig.MAP_JAVA_OPTS, XMX_OPT + ((int) (mapMemoryMB * 0.9)) + "m");

        LOGGER.info("Queue Name=" + conf.get(MRJobConfig.QUEUE_NAME) + ";" + "Map Meory MB="
                + conf.get(MRJobConfig.MAP_MEMORY_MB) + ";" + "Map Java Opts="
                + conf.get(MRJobConfig.MAP_JAVA_OPTS));
    }
}
