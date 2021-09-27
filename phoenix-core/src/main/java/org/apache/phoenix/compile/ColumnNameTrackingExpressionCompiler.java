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
package org.apache.phoenix.compile;

import java.sql.SQLException;
import java.util.List;

import org.apache.phoenix.parse.ColumnParseNode;
import org.apache.phoenix.parse.StatelessTraverseAllParseNodeVisitor;

import org.apache.phoenix.thirdparty.com.google.common.collect.Lists;

public class ColumnNameTrackingExpressionCompiler extends StatelessTraverseAllParseNodeVisitor {

    private List<String> dataColumnNames = Lists.newArrayListWithExpectedSize(10);

    public void reset() {
        this.getDataColumnNames().clear();
    }

    @Override
    public Void visit(ColumnParseNode node) throws SQLException {
        getDataColumnNames().add(node.getName());
        return null;
    }

    public List<String> getDataColumnNames() {
        return dataColumnNames;
    }

}
