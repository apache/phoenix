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
package org.apache.phoenix.parse;

import static org.apache.phoenix.schema.stats.StatisticsCollectionScope.ALL;
import static org.apache.phoenix.schema.stats.StatisticsCollectionScope.COLUMNS;
import static org.apache.phoenix.schema.stats.StatisticsCollectionScope.INDEX;

import java.util.Map;

import org.apache.phoenix.schema.stats.StatisticsCollectionScope;

import com.sun.istack.NotNull;


public class UpdateStatisticsStatement extends SingleTableStatement {
    private final StatisticsCollectionScope scope;
    private final Map<String,Object> props;
    
    public UpdateStatisticsStatement(NamedTableNode table, @NotNull StatisticsCollectionScope scope, Map<String,Object> props) {
        super(table, 0);
        this.scope = scope;
        this.props = props;
    }

    public boolean updateColumns() {
        return scope == COLUMNS || scope == ALL;
    }

    public boolean updateIndex() {
        return scope == INDEX || scope == ALL;
    }

    public boolean updateAll() {
        return scope == ALL;
    }

    public Map<String,Object> getProps() {
        return props;
    };
}
