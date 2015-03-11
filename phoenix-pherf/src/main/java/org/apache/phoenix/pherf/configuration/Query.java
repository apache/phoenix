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

import javax.xml.bind.annotation.XmlAttribute;
import javax.xml.bind.annotation.XmlType;

@XmlType
public class Query {

    private String statement;
    private Long expectedAggregateRowCount;
    private String tenantId;
    private String ddl;
    private String queryGroup;
    private String id;

    /**
     * SQL statement
     *
     * @return
     */
    @XmlAttribute
    public String getStatement() {
        return statement;
    }

    public void setStatement(String statement) {
        // normalize statement - merge all consecutive spaces into one
        this.statement = statement.replaceAll("\\s+", " ");
    }

    /**
     * Tenant Id used by connection of this query
     *
     * @return
     */
    @XmlAttribute
    public String getTenantId() {
        return tenantId;
    }

    public void setTenantId(String tenantId) {
        this.tenantId = tenantId;
    }

    /**
     * Expected aggregate row count is matched if specified
     *
     * @return
     */
    @XmlAttribute
    public Long getExpectedAggregateRowCount() {
        return expectedAggregateRowCount;
    }

    public void setExpectedAggregateRowCount(Long expectedAggregateRowCount) {
        this.expectedAggregateRowCount = expectedAggregateRowCount;
    }

    /**
     * DDL is executed only once. If tenantId is specified then DDL is executed with tenant
     * specific connection.
     *
     * @return
     */
    @XmlAttribute
    public String getDdl() {
        return ddl;
    }

    public void setDdl(String ddl) {
        this.ddl = ddl;
    }

    /**
     * queryGroup attribute is just a string value to help correlate queries across sets or files.
     * This helps to make sense of reporting results.
     *
     * @return the group id
     */
    @XmlAttribute
    public String getQueryGroup() {
        return queryGroup;
    }

    public void setQueryGroup(String queryGroup) {
        this.queryGroup = queryGroup;
    }

    /**
     * Set hint to query
     *
     * @param queryHint
     */
    public void setHint(String queryHint) {
        if (null != queryHint) {
            this.statement =
                    this.statement.toUpperCase()
                            .replace("SELECT ", "SELECT /*+ " + queryHint + "*/ ");
        }
    }

    /**
     * Query ID, Use UUID if none specified
     *
     * @return
     */
    @XmlAttribute
    public String getId() {
        if (null == this.id) {
            this.id = java.util.UUID.randomUUID().toString();
        }
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }
}
