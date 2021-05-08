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
import org.apache.phoenix.thirdparty.com.google.common.collect.Lists;

import javax.xml.bind.annotation.XmlAttribute;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class Upsert {

    private String id;
    private String upsertGroup;
    private String statement;
    private List<Column> column;
    private boolean useGlobalConnection;
    private Pattern pattern;
    private long timeoutDuration = Long.MAX_VALUE;

    public Upsert() {
    	pattern = Pattern.compile("\\[.*?\\]");
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
            ret = ret.replace("[" + dynamicField + "]",
                    needQuotes + ruleApplier.getDataValue(dynamicColumn).getValue()
                                    + needQuotes);
        }
        return ret;
    }

    /**
     * upsertGroup attribute is just a string value to help correlate upserts across sets or files.
     * This helps to make sense of reporting results.
     *
     * @return the group id
     */
    @XmlAttribute
    public String getUpsertGroup() {
        return upsertGroup;
    }

    public void setUpsertGroup(String upsertGroup) {
        this.upsertGroup = upsertGroup;
    }


    /**
     * Upsert ID, Use UUID if none specified
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

    public List<Column> getColumn() {
        if (column == null) return Lists.newArrayList();
        return column;
    }

    public void setColumn(List<Column> column) {
        this.column = column;
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

    public String getStatement() {
        return statement;
    }

    public void setStatement(String statement) {
        // normalize statement - merge all consecutive spaces into one
        this.statement = statement.replaceAll("\\s+", " ");
    }
}
