/*
 * Licensed to the Apache Software Foundation (ASF) under one
 *   or more contributor license agreements.  See the NOTICE file
 *   distributed with this work for additional information
 *   regarding copyright ownership.  The ASF licenses this file
 *   to you under the Apache License, Version 2.0 (the
 *   "License"); you may not use this file except in compliance
 *   with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 *   Unless required by applicable law or agreed to in writing, software
 *   distributed under the License is distributed on an "AS IS" BASIS,
 *   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *   See the License for the specific language governing permissions and
 *   limitations under the License.
 */

package org.apache.phoenix.pherf.configuration;

import javax.xml.bind.annotation.XmlType;
import java.util.List;

@XmlType
public class LoadProfile {
    public static int MIN_BATCH_SIZE = 1;

    private int batchSize;
    private long numOperations;
    List<TenantGroup> tenantDistribution;
    List<OperationGroup> opDistribution;

    public LoadProfile() {
        this.batchSize = MIN_BATCH_SIZE;
        this.numOperations = Long.MAX_VALUE;
    }

    public int getBatchSize() {
        return batchSize;
    }

    public void setBatchSize(int batchSize) {
        this.batchSize = batchSize;
    }

    public long getNumOperations() {
        return numOperations;
    }

    public void setNumOperations(long numOperations) {
        this.numOperations = numOperations;
    }

    public List<TenantGroup> getTenantDistribution() {
        return tenantDistribution;
    }

    public void setTenantDistribution(List<TenantGroup> tenantDistribution) {
        this.tenantDistribution = tenantDistribution;
    }

    public List<OperationGroup> getOpDistribution() {
        return opDistribution;
    }

    public void setOpDistribution(List<OperationGroup> opDistribution) {
        this.opDistribution = opDistribution;
    }
}
