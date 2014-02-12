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


public class SequenceKey implements Comparable<SequenceKey> {
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

    private final String tenantId;
    private final String schemaName;
    private final String sequenceName;
    
    public SequenceKey(String tenantId, String schemaName, String sequenceName) {
        this.tenantId = tenantId;
        this.schemaName = schemaName;
        this.sequenceName = sequenceName;
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
}