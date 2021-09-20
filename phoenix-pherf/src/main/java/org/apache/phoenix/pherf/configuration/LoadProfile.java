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
    private static final int MIN_BATCH_SIZE = 1;
    private static final String DEFAULT_TENANT_ID_FMT = "T%s%08d";
    private static final int DEFAULT_GROUP_ID_LEN = 6;
    private static final int DEFAULT_TENANT_ID_LEN = 15;

    // Holds the batch size to be used in upserts.
    private int batchSize;
    // Holds the number of operations to be generated.
    private long numOperations;
    /**
     * Holds the format to be used when generating tenantIds.
     * TenantId format should typically have 2 parts -
     * 1. string fmt - that hold the tenant group id.
     * 2. int fmt - that holds a random number between 1 and max tenants
     * for e.g DEFAULT_TENANT_ID_FMT = "T%s%08d";
     *
     * When the Tenant Group is configured to use a global connection,
     * for now this is modelled as a special tenant whose id will translate to "TGLOBAL00000001"
     * since the group id => "GLOBAL" and num tenants = 1.
     * For now this is a hack/temporary workaround.
     *
     * TODO :
     * Ideally it needs to be built into the framework and injected during event generation.
     */
    private String tenantIdFormat;
    private int groupIdLength;
    private int tenantIdLength;
    // Holds the desired tenant distribution for this load.
    private List<TenantGroup> tenantDistribution;
    // Holds the desired operation distribution for this load.
    private List<OperationGroup> opDistribution;

    public LoadProfile() {
        this.batchSize = MIN_BATCH_SIZE;
        this.numOperations = Long.MAX_VALUE;
        this.tenantIdFormat = DEFAULT_TENANT_ID_FMT;
        this.tenantIdLength = DEFAULT_TENANT_ID_LEN;
        this.groupIdLength = DEFAULT_GROUP_ID_LEN;
    }

    public String getTenantIdFormat() {
        return tenantIdFormat;
    }

    public void setTenantIdFormat(String tenantIdFormat) {
        this.tenantIdFormat = tenantIdFormat;
    }

    public int getTenantIdLength() {
        return tenantIdLength;
    }

    public void setTenantIdLength(int tenantIdLength) {
        this.tenantIdLength = tenantIdLength;
    }

    public int getGroupIdLength() {
        return groupIdLength;
    }

    public void setGroupIdLength(int groupIdLength) {
        this.groupIdLength = groupIdLength;
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
