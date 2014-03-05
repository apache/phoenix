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

public class CreateSequenceStatement extends MutableStatement {

    public static CreateSequenceStatement create(TableName sequenceName) {
        return new CreateSequenceStatement(sequenceName, null, null, null, true, 0);
    }
    
	private final TableName sequenceName;
	private final ParseNode startWith;
	private final ParseNode incrementBy;
    private final ParseNode cacheSize;
    private final boolean ifNotExists;
	private final int bindCount;

	protected CreateSequenceStatement(TableName sequenceName, ParseNode startsWith, ParseNode incrementBy, ParseNode cacheSize, boolean ifNotExists, int bindCount) {
		this.sequenceName = sequenceName;
		this.startWith = startsWith == null ? LiteralParseNode.ONE : startsWith;
		this.incrementBy = incrementBy == null ? LiteralParseNode.ONE : incrementBy;
        this.cacheSize = cacheSize == null ? null : cacheSize;
		this.ifNotExists = ifNotExists;
		this.bindCount = bindCount;
	}

	@Override
	public int getBindCount() {
		return this.bindCount;
	}
	
	public ParseNode getIncrementBy() {
		return incrementBy;
	}

	public TableName getSequenceName() {
		return sequenceName;
	}

    public ParseNode getCacheSize() {
        return cacheSize;
    }

	public ParseNode getStartWith() {
		return startWith;
	}

    public boolean ifNotExists() {
        return ifNotExists;
    }
}