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

import org.apache.phoenix.pherf.rules.RulesApplier;

import javax.xml.bind.annotation.XmlAttribute;
import javax.xml.bind.annotation.XmlType;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

@XmlType
public class Query {

    private String id;
    private String queryGroup;
    private String tenantId;
    private String statement;
    private Long expectedAggregateRowCount;
    private String ddl;
    private boolean useGlobalConnection;
    private Pattern pattern;
    private long timeoutDuration = Long.MAX_VALUE;

    public Query() {
    	pattern = Pattern.compile("\\[.*?\\]");
    }
    
    /**
     * SQL statement
     *
     * @return
     */
    @XmlAttribute
    public String getStatement() {
        return statement;
    }

    public String getDynamicStatement(RulesApplier ruleApplier, Scenario scenario)
            throws Exception {
        String ret = this.statement;
        String needQuotes = "";
        Matcher m = pattern.matcher(ret);
        while (m.find()) {
            String dynamicField = m.group(0).replace("[", "").replace("]", "");
            Column dynamicColumn = ruleApplier.getRule(dynamicField, scenario);
            needQuotes =
                    (dynamicColumn.getType() == DataTypeMapping.CHAR
                            || dynamicColumn.getType() == DataTypeMapping.VARCHAR) ? "'" : "";
            ret =
                    ret.replace("[" + dynamicField + "]",
                            needQuotes + ruleApplier.getDataValue(dynamicColumn).getValue()
                                    + needQuotes);
        }
        return ret;
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

    @XmlAttribute
    public boolean isUseGlobalConnection() {
        return useGlobalConnection;
    }

    public void setUseGlobalConnection(boolean useGlobalConnection) {
        this.useGlobalConnection = useGlobalConnection;
    }

    @XmlAttribute
    public long getTimeoutDuration() {
        return this.timeoutDuration;
    }

    public void setTimeoutDuration(long timeoutDuration) {
        this.timeoutDuration = timeoutDuration;
    }
}
