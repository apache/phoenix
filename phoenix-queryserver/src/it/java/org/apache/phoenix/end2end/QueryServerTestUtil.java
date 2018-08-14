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
package org.apache.phoenix.end2end;

import java.io.File;
import java.security.PrivilegedAction;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.LocalHBaseCluster;
import org.apache.hadoop.hbase.util.FSUtils;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.phoenix.query.ConfigurationFactory;
import org.apache.phoenix.queryserver.client.ThinClientUtil;
import org.apache.phoenix.queryserver.server.QueryServer;
import org.apache.phoenix.util.InstanceResolver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Strings;

public class QueryServerTestUtil {
    private static final Logger LOG = LoggerFactory.getLogger(QueryServerTestUtil.class);

    private final Configuration conf;
    private final HBaseTestingUtility util;
    private LocalHBaseCluster hbase;

    private final QueryServer pqs;
    private int port;
    private String url;

    private String principal;
    private File keytab;

    private ExecutorService executor;

    public QueryServerTestUtil(Configuration conf) {
        this.conf = Objects.requireNonNull(conf);
        this.util = new HBaseTestingUtility(conf);
        this.pqs = new QueryServer(new String[0], conf);
    }

    public QueryServerTestUtil(Configuration conf, String principal, File keytab) {
        this.conf = Objects.requireNonNull(conf);
        this.principal = principal;
        this.keytab = keytab;
        this.util = new HBaseTestingUtility(conf);
        this.pqs = new QueryServer(new String[0], conf);
    }

    public void startLocalHBaseCluster(Class testClass) throws Exception {
        startLocalHBaseCluster(testClass.getCanonicalName());
    }

    public void startLocalHBaseCluster(String uniqueName) throws Exception {
        LOG.debug("Starting local HBase cluster for '{}'", uniqueName);
        // Start ZK
        util.startMiniZKCluster();
        // Start HDFS
        util.startMiniDFSCluster(1);
        // Start HBase
        Path rootdir = util.getDataTestDirOnTestFS(uniqueName);
        FSUtils.setRootDir(conf, rootdir);
        hbase = new LocalHBaseCluster(conf, 1);
        hbase.startup();
    }

    public void stopLocalHBaseCluster() throws Exception {
        LOG.debug("Stopping local HBase cluster");
        if (hbase != null) {
            hbase.shutdown();
            hbase.join();
        }
        if (util != null) {
            util.shutdownMiniDFSCluster();
            util.shutdownMiniZKCluster();
        }
    }

    public void startQueryServer() throws Exception {
        setupQueryServerConfiguration(conf);
        executor = Executors.newSingleThreadExecutor();
        if (!Strings.isNullOrEmpty(principal) && null != keytab) {
            // Get the PQS ident for PQS to use
            final UserGroupInformation ugi = UserGroupInformation
                    .loginUserFromKeytabAndReturnUGI(principal, keytab.getAbsolutePath());
            // Launch PQS, doing in the Kerberos login instead of letting PQS do it itself (which would
            // break the HBase/HDFS logins also running in the same test case).
            executor.submit(new Runnable() {
                @Override
                public void run() {
                    ugi.doAs(new PrivilegedAction<Void>() {
                        @Override
                        public Void run() {
                            pqs.run();
                            return null;
                        }
                    });
                }
            });
        } else {
            // Launch PQS without a login
            executor.submit(new Runnable() {
                @Override
                public void run() {
                    pqs.run();
                }
            });
        }
        pqs.awaitRunning();
        port = pqs.getPort();
        url = ThinClientUtil.getConnectionUrl("localhost", port);
    }

    public void stopQueryServer() throws Exception {
        if (pqs != null) {
            pqs.stop();
        }
        if (executor != null) {
            executor.shutdown();
            if (!executor.awaitTermination(5, TimeUnit.SECONDS)) {
                LOG.info("PQS didn't exit in 5 seconds, proceeding anyways.");
            }
        }
    }

    public static void setupQueryServerConfiguration(final Configuration conf) {
        // Make sure the ConnectionInfo doesn't try to pull a default Configuration
        InstanceResolver.getSingleton(ConfigurationFactory.class, new ConfigurationFactory() {
            @Override
            public Configuration getConfiguration() {
                return conf;
            }
            @Override
            public Configuration getConfiguration(Configuration confToClone) {
                Configuration copy = new Configuration(conf);
                copy.addResource(confToClone);
                return copy;
            }
        });
    }

    public int getPort() {
        return port;
    }

    public String getUrl() {
        return url;
    }

    /**
     * Returns the query server URL with the specified URL params
     * @param params URL params
     * @return URL with params
     */
    public String getUrl(Map<String, String> params) {
        if (params == null || params.size() == 0) {
            return url;
        }
        StringBuilder urlParams = new StringBuilder();
        for (Map.Entry<String, String> param : params.entrySet()) {
            urlParams.append(";").append(param.getKey()).append("=").append(param.getValue());
        }
        return url + urlParams;
    }
}
