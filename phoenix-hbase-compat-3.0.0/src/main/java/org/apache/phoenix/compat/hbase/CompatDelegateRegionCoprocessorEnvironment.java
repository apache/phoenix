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
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.quotas.OperationQuota;
import org.apache.hadoop.hbase.quotas.OperationQuota.OperationType;
import org.apache.hadoop.hbase.quotas.RpcQuotaManager;
import org.apache.hadoop.hbase.quotas.RpcThrottlingException;
import org.apache.hadoop.hbase.regionserver.Region;

public abstract class CompatDelegateRegionCoprocessorEnvironment
  implements RegionCoprocessorEnvironment {
  protected RegionCoprocessorEnvironment delegate;

  public CompatDelegateRegionCoprocessorEnvironment(RegionCoprocessorEnvironment delegate) {
    super();
    this.delegate = delegate;
  }

  @Override
  public OperationQuota checkScanQuota(Scan scan, long maxBlockBytesScanned,
    long prevBlockBytesScannedDifference) throws IOException, RpcThrottlingException {
    return delegate.checkScanQuota(scan, maxBlockBytesScanned, prevBlockBytesScannedDifference);
  }

  @Override
  public OperationQuota checkBatchQuota(Region region, int numWrites, int numReads)
    throws IOException, RpcThrottlingException {
    return delegate.checkBatchQuota(region, numWrites, numReads);
  }

  @Override
  public OperationQuota checkBatchQuota(Region arg0, OperationType arg1)
    throws IOException, RpcThrottlingException {
    return delegate.checkBatchQuota(arg0, arg1);
  }

  @Override
  public RpcQuotaManager getRpcQuotaManager() {
    return delegate.getRpcQuotaManager();
  }
}
