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
        return new CreateSequenceStatement(sequenceName, null, null, null, null, null, false, true,
                0);
    }
    
	private final TableName sequenceName;
	private final ParseNode startWith;
	private final ParseNode incrementBy;
    private final ParseNode cacheSize;
    private final ParseNode minValue;
    private final ParseNode maxValue;
    private final boolean cycle;
    private final boolean ifNotExists;
	private final int bindCount;

    protected CreateSequenceStatement(TableName sequenceName, ParseNode startWith,
            ParseNode incrementBy, ParseNode cacheSize, ParseNode minValue, ParseNode maxValue,
            boolean cycle, boolean ifNotExists, int bindCount) {
        this.sequenceName = sequenceName;
        // if MINVALUE, MAXVALUE and START WITH are not specified, set START WITH to 1 in order to
        // maintain backward compatibility
        this.startWith =
                (minValue == null && maxValue == null && startWith == null) ? LiteralParseNode.ONE
                        : startWith;
        this.minValue = minValue == null ? new LiteralParseNode(Long.MIN_VALUE) : minValue;
        this.maxValue = maxValue == null ? new LiteralParseNode(Long.MAX_VALUE) : maxValue;
        this.incrementBy = incrementBy == null ? LiteralParseNode.ONE : incrementBy;
        this.cacheSize = cacheSize;
        this.cycle = cycle;
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

	public ParseNode getMinValue() {
        return minValue;
    }

    public ParseNode getMaxValue() {
        return maxValue;
    }

    public boolean getCycle() {
        return cycle;
    }

    public ParseNode getStartWith() {
		return startWith;
	}

    public boolean ifNotExists() {
        return ifNotExists;
    }
}