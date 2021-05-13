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

import com.google.protobuf.RpcController;
import com.google.protobuf.ServiceException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.CoordinatedStateManager;
import org.apache.hadoop.hbase.MiniHBaseCluster;
import org.apache.hadoop.hbase.protobuf.generated.ClientProtos;
import org.apache.hadoop.hbase.regionserver.HRegionServer;
import org.apache.hadoop.hbase.regionserver.RSRpcServices;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/**
 * This is a extended MiniHbaseCluster Region Server whcih allows developer/tester to inject
 * delay into specific server side operations for testing.
 */
public class DelayedRegionServer extends MiniHBaseCluster.MiniHBaseClusterRegionServer {

    private static final Logger LOGGER = LoggerFactory.getLogger(DelayedRegionServer.class);

    static boolean doDelay = false;
    // Activate the delays after table creation to test get/scan/put
    private static int DELAY_GET = 0;
    private static int DELAY_SCAN = 30000;
    private static int DELAY_MUTATE = 0;
    private static int DELAY_MULTI_OP = 0;

    public static void setDelayEnabled(boolean delay) {
        doDelay = delay;
    }

    public static void setDelayGet(int delayGet) {
        DELAY_GET = delayGet;
    }

    public static void setDelayScan(int delayScan) {
        DELAY_SCAN = delayScan;
    }

    public static void setDelayMutate(int delayMutate) {
        DELAY_MUTATE = delayMutate;
    }

    public static void setDelayMultiOp(int delayMultiOp) {
        DELAY_MULTI_OP = delayMultiOp;
    }

    public DelayedRegionServer(Configuration conf, CoordinatedStateManager cp)
            throws IOException, InterruptedException {
        super(conf, cp);
    }

    @Override protected RSRpcServices createRpcServices() throws IOException {
        return new DelayedRSRpcServices(this);
    }

    /**
     * This class injects delay for Rpc calls and after executes super methods is delay is set.
     */
    public static class DelayedRSRpcServices extends RSRpcServices {

        DelayedRSRpcServices(HRegionServer rs) throws IOException {
            super(rs);
        }

        @Override public ClientProtos.GetResponse get(RpcController controller,
                ClientProtos.GetRequest request) throws ServiceException {
            try {
                if (doDelay) {
                    Thread.sleep(DELAY_GET);
                }
            } catch (InterruptedException e) {
                LOGGER.error("Sleep interrupted during get operation", e);
            }
            return super.get(controller, request);
        }

        @Override public ClientProtos.MutateResponse mutate(RpcController rpcc,
                ClientProtos.MutateRequest request) throws ServiceException {
            try {
                if (doDelay) {
                    Thread.sleep(DELAY_MUTATE);
                }
            } catch (InterruptedException e) {
                LOGGER.error("Sleep interrupted during mutate operation", e);
            }
            return super.mutate(rpcc, request);
        }

        @Override
        public ClientProtos.MultiResponse multi(RpcController rpcc,
                ClientProtos.MultiRequest request) throws ServiceException {
            try {
                if (doDelay) {
                    Thread.sleep(DELAY_MULTI_OP);
                }
            } catch (InterruptedException e) {
                LOGGER.error("Sleep interrupted during multi operation", e);
            }
            return super.multi(rpcc, request);
        }

        @Override public ClientProtos.ScanResponse scan(RpcController controller,
                ClientProtos.ScanRequest request) throws ServiceException {
            try {
                if (doDelay) {
                    Thread.sleep(DELAY_SCAN);
                }
            } catch (InterruptedException e) {
                LOGGER.error("Sleep interrupted during scan operation", e);
            }
            return super.scan(controller, request);
        }
    }
}