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

import org.apache.phoenix.query.QueryConstants;

import com.google.common.base.Preconditions;

public class PTableKey {
    private final PName tenantId;
    private final String name;
    
    public PTableKey(PName tenantId, String name) {
        Preconditions.checkNotNull(name);
        this.tenantId = tenantId;
        if (name.indexOf(QueryConstants.NAMESPACE_SEPARATOR) != -1) {
            this.name = name.replace(QueryConstants.NAMESPACE_SEPARATOR, QueryConstants.NAME_SEPARATOR);
        } else {
            this.name = name;
        }
    }

    public PName getTenantId() {
        return tenantId;
    }

    public String getName() {
        return name;
    }
    
    @Override
    public String toString() {
        return name + ((tenantId == null || tenantId.getBytes().length==0) ? "" : " for " + tenantId.getString());
    }
    
    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + ((tenantId == null || tenantId.getBytes().length==0) ? 0 : tenantId.hashCode());
        result = prime * result + name.hashCode();
        return result;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) return true;
        if (obj == null) return false;
        if (getClass() != obj.getClass()) return false;
        PTableKey other = (PTableKey)obj;
        if (!name.equals(other.name)) return false;
        if (tenantId == null) {
            if (other.tenantId != null) return false;
        } else if (!tenantId.equals(other.tenantId)) return false;
        return true;
    }

}
