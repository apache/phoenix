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
package org.apache.phoenix.schema;

import org.apache.hadoop.hbase.util.Bytes;
import org.apache.phoenix.query.QueryConstants;
import org.apache.phoenix.util.ByteUtil;


public class SequenceKey implements Comparable<SequenceKey> {
    private final String tenantId;
    private final String schemaName;
    private final String sequenceName;
    private final byte[] key;
    
    public SequenceKey(String tenantId, String schemaName, String sequenceName, int nBuckets) {
        this.tenantId = tenantId;
        this.schemaName = schemaName;
        this.sequenceName = sequenceName;
        this.key = ByteUtil.concat((nBuckets <= 0 ? ByteUtil.EMPTY_BYTE_ARRAY : QueryConstants.SEPARATOR_BYTE_ARRAY), tenantId == null  ? ByteUtil.EMPTY_BYTE_ARRAY : Bytes.toBytes(tenantId), QueryConstants.SEPARATOR_BYTE_ARRAY, schemaName == null ? ByteUtil.EMPTY_BYTE_ARRAY : Bytes.toBytes(schemaName), QueryConstants.SEPARATOR_BYTE_ARRAY, Bytes.toBytes(sequenceName));
        if (nBuckets > 0) {
            key[0] = SaltingUtil.getSaltingByte(key, SaltingUtil.NUM_SALTING_BYTES, key.length - SaltingUtil.NUM_SALTING_BYTES, nBuckets);
        }
    }

    public byte[] getKey() {
        return key;

    }
    public String getTenantId() {
        return tenantId;
    }

    public String getSchemaName() {
        return schemaName;
    }

    public String getSequenceName() {
        return sequenceName;
    }

    @Override
    public int compareTo(SequenceKey that) {
        int c = this.tenantId == that.getTenantId() ? 0 : this.tenantId == null ? -1 : that.getTenantId() == null ? 1 : this.tenantId.compareTo(that.getTenantId());
        if (c == 0) {
            c = this.schemaName == that.getSchemaName() ? 0 : this.schemaName == null ? -1 : that.getSchemaName() == null ? 1 : this.schemaName.compareTo(that.getSchemaName());
            if (c == 0) {
                return sequenceName.compareTo(that.getSequenceName());
            }
        }
        return c;
    }
    
    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + ((tenantId == null) ? 0 : tenantId.hashCode());
        result = prime * result + ((schemaName == null) ? 0 : schemaName.hashCode());
        result = prime * result + sequenceName.hashCode();
        return result;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) return true;
        if (obj == null) return false;
        if (getClass() != obj.getClass()) return false;
        SequenceKey other = (SequenceKey)obj;
        return this.compareTo(other) == 0;
    }
}