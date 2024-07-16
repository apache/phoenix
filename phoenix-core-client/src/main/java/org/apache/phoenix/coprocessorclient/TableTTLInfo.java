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

package org.apache.phoenix.coprocessorclient;

import org.apache.hadoop.hbase.util.Bytes;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;

/**
 * Simple POJO class to hold TTL info
 */
public class TableTTLInfo implements Comparable {
    private final byte[] physicalTableName;
    private final byte[] tenantId;
    private final byte[] entityName;
    private final byte[] matchPattern;
    private final int ttl;

    public TableTTLInfo(String physicalTableName, String tenantId, String entityName, String matchPattern, int ttl) {
        super();
        this.physicalTableName = physicalTableName.getBytes(StandardCharsets.UTF_8);
        this.tenantId = tenantId.getBytes(StandardCharsets.UTF_8);
        this.entityName = entityName.getBytes(StandardCharsets.UTF_8);
        this.matchPattern = matchPattern.getBytes(StandardCharsets.UTF_8);
        this.ttl = ttl;
    }

    public TableTTLInfo(byte[] physicalTableName, byte[] tenantId, byte[] entityName, byte[] matchPattern, int ttl) {
        super();
        this.physicalTableName = physicalTableName;
        this.tenantId = tenantId;
        this.matchPattern = matchPattern;
        this.entityName = entityName;
        this.ttl = ttl;
    }

    public int getTTL() {
        return ttl;
    }
    public byte[] getTenantId() {
        return tenantId;
    }

    public byte[] getEntityName() {
        return entityName;
    }

    public byte[] getMatchPattern() {
        return matchPattern;
    }
    public byte[] getPhysicalTableName() {
        return physicalTableName;
    }


    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        TableTTLInfo that = (TableTTLInfo) o;
        return Arrays.equals(physicalTableName, that.physicalTableName) &&
                Arrays.equals(tenantId, that.tenantId) &&
                Arrays.equals(entityName, that.entityName);
    }

    @Override
    public int hashCode() {
        return Arrays.hashCode(tenantId) +  Arrays.hashCode(entityName);
    }
    @Override
    public int compareTo(Object obj) {
        if (this == obj)
            return 0;
        if (obj == null)
            throw new NullPointerException();
        TableTTLInfo other = (TableTTLInfo) obj;
        int result = Bytes.BYTES_COMPARATOR.compare(this.physicalTableName,other.physicalTableName);
        if (result == 0) {
            result = Bytes.BYTES_COMPARATOR.compare(this.entityName,other.entityName);
        }
        if (result == 0)  {
            result = Bytes.BYTES_COMPARATOR.compare(this.tenantId, other.tenantId);
        }
        return result;
    }

    @Override
    public String toString() {
        return "TableTTLInfo { " +
                "physicalTableName=" + Bytes.toString(physicalTableName) +
                ", tenantId=" + Bytes.toString(tenantId) +
                ", entityName=" + Bytes.toString(entityName) +
                ", matchPattern=" + Bytes.toStringBinary(matchPattern) +
                ", ttl=" + ttl +
                " }";
    }

}
