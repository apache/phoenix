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

import java.io.IOException;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HRegionLocation;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.client.ClusterConnection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.ipc.HBaseRpcController;
import org.apache.hadoop.hbase.shaded.protobuf.generated.AdminProtos.AdminService;
import org.apache.hadoop.ipc.RemoteException;
import org.apache.phoenix.schema.PTable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class ServerMetaDataUtil extends MetaDataUtil {
    private static final Logger LOGGER = LoggerFactory.getLogger(ServerMetaDataUtil.class);

    /**
     * This function checks if all regions of a table is online
     * @param conf
     * @param table
     * @return true when all regions of a table are online
     */
    public static boolean tableRegionsOnline(Configuration conf, PTable table) {
        try (ClusterConnection hcon =
                (ClusterConnection) ConnectionFactory.createConnection(conf)) {
            List<HRegionLocation> locations = hcon.locateRegions(
              org.apache.hadoop.hbase.TableName.valueOf(table.getPhysicalName().getBytes()));

            for (HRegionLocation loc : locations) {
                try {
                    ServerName sn = loc.getServerName();
                    if (sn == null) continue;

                    AdminService.BlockingInterface admin = hcon.getAdmin(sn);
                    HBaseRpcController controller = hcon.getRpcControllerFactory().newController();
                    org.apache.hadoop.hbase.shaded.protobuf.ProtobufUtil.getRegionInfo(controller,
                        admin, loc.getRegion().getRegionName());
                } catch (RemoteException e) {
                    LOGGER.debug("Cannot get region " + loc.getRegion().getEncodedName() + " info due to error:" + e);
                    return false;
                }
            }
        } catch (IOException ex) {
            LOGGER.warn("tableRegionsOnline failed due to:", ex);
            return false;
        }
        return true;
    }
}
