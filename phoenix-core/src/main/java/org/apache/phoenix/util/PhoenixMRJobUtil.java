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
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.ZooKeeperConnectionException;
import org.apache.hadoop.hbase.zookeeper.ZooKeeperWatcher;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.proto.YarnServerResourceManagerServiceProtos.ActiveRMInfoProto;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.Stat;
import org.codehaus.jettison.json.JSONException;

import com.google.protobuf.InvalidProtocolBufferException;

public class PhoenixMRJobUtil {

    private static final String YARN_LEADER_ELECTION = "/yarn-leader-election";
    private static final String ACTIVE_STANDBY_ELECTOR_LOCK = "ActiveStandbyElectorLock";
    private static final String RM_APPS_GET_ENDPOINT = "/ws/v1/cluster/apps";

    // Reduced HBase Client Retries
    private static final int CLIENT_RETRIES_NUMBER = 2;
    private static final long CLIENT_PAUSE_TIME = 1000;
    private static final int ZOOKEEPER_RECOVERY_RETRY_COUNT = 1;

    public static final String PHOENIX_INDEX_MR_QUEUE_NAME_PROPERTY =
            "phoenix.index.mr.scheduler.capacity.queuename";
    public static final String PHOENIX_INDEX_MR_MAP_MEMORY_PROPERTY =
            "phoenix.index.mr.scheduler.capacity.mapMemoryMB";

    // Default MR Capacity Scheduler Configurations for Phoenix MR Index Build
    // Jobs
    public static final String QUEUE_NAME = "mapreduce.job.queuename";
    public static final String DEFAULT_QUEUE_NAME = "default";
    public static final String MAP_MEMORY_MB = "mapreduce.map.memory.mb";
    public static final int DEFAULT_MAP_MEMROY_MB = 5120;
    public static final String MAP_JAVA_OPTS = "mapreduce.map.java.opts";
    public static final String XMX_OPT = "-Xmx";

    public static final String RM_HTTP_SCHEME = "http";
    // TODO - Move these as properties?
    public static final int RM_CONNECT_TIMEOUT_MILLIS = 10 * 1000;
    public static final int RM_READ_TIMEOUT_MILLIS = 10 * 60 * 1000;

    private static final Log LOG = LogFactory.getLog(PhoenixMRJobUtil.class);

    public static final String PHOENIX_MR_SCHEDULER_TYPE_NAME = "phoenix.index.mr.scheduler.type";

    public enum MR_SCHEDULER_TYPE {
        CAPACITY, FAIR, NONE
    };

    public static String getActiveResourceManagerHost(Configuration config, String zkQuorum)
            throws IOException, InterruptedException, JSONException, KeeperException,
            InvalidProtocolBufferException, ZooKeeperConnectionException {
        ZooKeeperWatcher zkw = null;
        ZooKeeper zk = null;
        String activeRMHost = null;
        try {
            zkw = new ZooKeeperWatcher(config, "get-active-yarnmanager", null);
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
                        proto.getRmId();
                        LOG.info("Active RmId : " + proto.getRmId());

                        activeRMHost =
                                config.get(YarnConfiguration.RM_HOSTNAME + "." + proto.getRmId());
                        LOG.info("activeResourceManagerHostname = " + activeRMHost);

                    }
                }
            }
        } finally {
            if (zkw != null) zkw.close();
            if (zk != null) zk.close();
        }

        return activeRMHost;
    }

    public static String getJobsInformationFromRM(String rmhost, int rmport,
            Map<String, String> urlParams) throws MalformedURLException, ProtocolException,
            UnsupportedEncodingException, IOException {
        HttpURLConnection con = null;
        String response = null;
        String url = null;

        try {
            StringBuilder urlBuilder = new StringBuilder();

            urlBuilder.append(RM_HTTP_SCHEME + "://").append(rmhost).append(":").append(rmport)
                    .append(RM_APPS_GET_ENDPOINT);

            if (urlParams != null && urlParams.size() != 0) {
                urlBuilder.append("?");
                for (String key : urlParams.keySet()) {
                    urlBuilder.append(key + "=" + urlParams.get(key) + "&");
                }
                urlBuilder.delete(urlBuilder.length() - 1, urlBuilder.length());
            }

            url = urlBuilder.toString();
            LOG.info("Attempt to get running/submitted jobs information from RM URL = " + url);

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

        LOG.info("Result of attempt to get running/submitted jobs from RM - URL=" + url
                + ",ResponseCode=" + con.getResponseCode() + ",Response=" + response);

        return response;
    }

    public static String getTextContent(InputStream is) throws IOException {
        BufferedReader in = null;
        StringBuilder response = null;
        try {
            in = new BufferedReader(new InputStreamReader(is));
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
        LOG.debug("Shutdown called");
        pool.awaitTermination(200, TimeUnit.MILLISECONDS);
        LOG.debug("Await termination called to wait for 200 msec");
        if (!pool.isShutdown()) {
            pool.shutdownNow();
            LOG.debug("Await termination called to wait for 200 msec");
            pool.awaitTermination(100, TimeUnit.MILLISECONDS);
        }
        if (!pool.isShutdown()) {
            LOG.warn("Pool did not shutdown");
        }
    }

    public static int getRMPort(Configuration conf) throws IOException {
        String rmHostPortStr = conf.get(YarnConfiguration.RM_WEBAPP_ADDRESS);
        String[] rmHostPort = rmHostPortStr.split(":");
        if (rmHostPort == null || rmHostPort.length != 2) {
            throw new IOException("Invalid value for property "
                    + YarnConfiguration.RM_WEBAPP_ADDRESS + " = " + rmHostPortStr);
        }
        int rmPort = Integer.parseInt(rmHostPort[1]);
        return rmPort;
    }

    public static void updateTimeoutsToFailFast(Configuration conf) {
        conf.set("hbase.client.retries.number", String.valueOf(CLIENT_RETRIES_NUMBER));
        conf.set("zookeeper.recovery.retry", String.valueOf(ZOOKEEPER_RECOVERY_RETRY_COUNT));
        conf.set("hbase.client.pause", String.valueOf(CLIENT_PAUSE_TIME));
    }

    /**
     * This method set the configuration values for Capacity scheduler.
     * @param conf - Configuration to which Capacity Queue information to be added
     */
    public static void updateCapacityQueueInfo(Configuration conf) {
        conf.set(QUEUE_NAME,
            conf.get(PHOENIX_INDEX_MR_QUEUE_NAME_PROPERTY, DEFAULT_QUEUE_NAME));
        int mapMemoryMB = conf.getInt(PHOENIX_INDEX_MR_MAP_MEMORY_PROPERTY, DEFAULT_MAP_MEMROY_MB);

        conf.setInt(MAP_MEMORY_MB, mapMemoryMB);
        conf.set(MAP_JAVA_OPTS, XMX_OPT + ((int) (mapMemoryMB * 0.9)) + "m");

        LOG.info("Queue Name=" + conf.get(QUEUE_NAME) + ";" + "Map Meory MB="
                + conf.get(MAP_MEMORY_MB) + ";" + "Map Java Opts="
                + conf.get(MAP_JAVA_OPTS));
    }
}
