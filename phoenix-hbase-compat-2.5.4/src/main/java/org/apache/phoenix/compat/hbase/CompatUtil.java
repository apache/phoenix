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
package org.apache.phoenix.compat.hbase;

import java.io.IOException;
import java.util.List;
import org.apache.hadoop.hbase.MetaTableAccessor;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.client.PackagePrivateFieldAccessor;
import org.apache.hadoop.hbase.client.RegionInfo;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.protobuf.ProtobufUtil;
import org.apache.hadoop.hbase.protobuf.generated.ClientProtos;
import org.apache.hadoop.hbase.protobuf.generated.ClientProtos.MutationProto;
import org.apache.hadoop.hbase.protobuf.generated.ClientProtos.MutationProto.MutationType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CompatUtil {

  private static final Logger LOGGER = LoggerFactory.getLogger(CompatUtil.class);

  private CompatUtil() {
    // Not to be instantiated
  }

  public static List<RegionInfo> getMergeRegions(Connection conn, RegionInfo regionInfo)
    throws IOException {
    return MetaTableAccessor.getMergeRegions(conn, regionInfo);
  }

  public static RegionInfo getRegionInfo(Result result) throws IOException {
    return MetaTableAccessor.getRegionInfo(result);
  }

  public static Mutation toMutation(MutationProto mProto) throws IOException {
    return ProtobufUtil.toMutation(mProto);
  }

  public static MutationProto toMutation(MutationType type, Mutation mutation) throws IOException {
    return ProtobufUtil.toMutation(type, mutation);
  }

  public static Scan toScan(final ClientProtos.Scan proto) throws IOException {
    return ProtobufUtil.toScan(proto);
  }

  public static ClientProtos.Scan toScan(Scan scan) throws IOException {
    return ProtobufUtil.toScan(scan);
  }

  public static void setMvccReadPoint(Scan scan, long mvccReadPoint) {
    PackagePrivateFieldAccessor.setMvccReadPoint(scan, mvccReadPoint);
  }

  public static long getMvccReadPoint(Scan scan) {
    return PackagePrivateFieldAccessor.getMvccReadPoint(scan);
  }

  public static void closeAdminAndLog(Admin admin, Logger logger) {
    try {
      admin.close();
    } catch (IOException e) {
      logger.error("Closing the admin failed: ", e);
    }
  }
}
