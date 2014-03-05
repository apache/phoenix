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

import java.sql.SQLException;


public class SequenceValueParseNode extends TerminalParseNode {
    public enum Op {NEXT_VALUE, CURRENT_VALUE};
	private final TableName tableName;
	private final Op op;

	public SequenceValueParseNode(TableName tableName, Op op) {
		this.tableName = tableName;
		this.op = op;
	}

	@Override
	public <T> T accept(ParseNodeVisitor<T> visitor) throws SQLException {
		return visitor.visit(this);
	}

	public TableName getTableName() {
		return tableName;
	}

    @Override
    public boolean isStateless() {
        return true;
    }

    public Op getOp() {
        return op;
    }
}