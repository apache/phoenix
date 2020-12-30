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
package org.apache.phoenix.end2end;

import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.HBaseAdmin;
//import org.apache.phoenix.coprocessor.SyscatRegionObserver;
import org.apache.phoenix.jdbc.PhoenixConnection;
import org.apache.phoenix.jdbc.PhoenixDatabaseMetaData;
import org.apache.phoenix.query.QueryServices;
import org.apache.phoenix.query.QueryServicesOptions;
import org.apache.phoenix.util.ReadOnlyProps;
import org.apache.phoenix.util.SchemaUtil;
import org.junit.Test;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;

public class Dummy {
    @Test
    public void test() throws Exception {
        try (Connection conn = DriverManager.getConnection("jdbc:phoenix:localhost")) {
//
//            try (HBaseAdmin admin = conn.unwrap(PhoenixConnection.class)
//                    .getQueryServices().getAdmin()) {
//                HTableDescriptor htd;
//                TableName syscatPhysicalTableName = SchemaUtil.getPhysicalTableName(
//                        PhoenixDatabaseMetaData.SYSTEM_CATALOG_NAME, ReadOnlyProps.EMPTY_PROPS);
//                htd = admin.getTableDescriptor(syscatPhysicalTableName);
//
//                if (!htd.hasCoprocessor(SyscatRegionObserver.class.getName())) {
////                    admin.disableTable(syscatPhysicalTableName);
////                    admin.getTableRegions(syscatPhysicalTableName).get(0).setOffline(true);
//                    int priority = ReadOnlyProps.EMPTY_PROPS.getInt(QueryServices.COPROCESSOR_PRIORITY_ATTRIB,
//                            QueryServicesOptions.DEFAULT_COPROCESSOR_PRIORITY);
//                    htd.addCoprocessor(SyscatRegionObserver.class.getName(), null, priority-2, null);
//                    admin.modifyTable(syscatPhysicalTableName, htd);
////                    pollForUpdatedTableDescriptor(admin, htd, syscatPhysicalTableName.getName());
////                    admin.enableTable(syscatPhysicalTableName);
////                    admin.getTableRegions(syscatPhysicalTableName).get(0).setOffline(false);
//                }
//            }

//            ResultSet rs = conn.createStatement().executeQuery("SELECT * FROM SYSTEM.CATALOG WHERE TABLE_NAME=''");
//            while (rs.next()) {
//
//            }
//
//
//            String ddl =
//                    "SELECT TABLE_NAME,VIEW_INDEX_ID,VIEW_INDEX_ID_DATA_TYPE " +
//                            "FROM SYSTEM.CATALOG WHERE COLUMN_COUNT IS NOT NULL";
//            rs = conn.createStatement().executeQuery(ddl);
//            while (rs.next()) {
//                String tablename = rs.getString(1);
//                int viewType = rs.getInt(3);
//                long viewId;
//                String type;
//                try  {
//                    viewId = rs.getShort(2);
//                    type = "short";
//                }  catch (Exception e) {
//                    viewId = rs.getLong(2);
//                    type = "long";
//                }
//                System.out.println(tablename + "," + viewId + "," + viewType  +","+ type);
//            }
        }
    }
}
