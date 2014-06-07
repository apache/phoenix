/*
 * Copyright 2010 The Apache Software Foundation
 *
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

import org.apache.hadoop.hbase.filter.CompareFilter.CompareOp;

import com.google.common.base.Preconditions;

/**
 * Specifies the sort order on disk of row key columns. The default is ASC.</p>
 * 
 * HBase always stores row keys in ascending order resulting in scans to also be
 * sorted by ascending row keys. This enum is used to associate a sort order
 * with each row key column to allow storing row key columns in descending
 * order.</p>
 * 
 * The often cited example of when you may want to do this is a row key that has
 * a date component. If all other parts of the row key are equal, a scan would
 * return the data from least recent to most recent; to get the scan to return
 * the most recent data first, the time component of the row key can be marked
 * as "desc".</p>
 * 
 * Internally, the bits of values for columns marked as "desc" are inverted before handing
 * them to HBase to persist; they are inverted again when read back out.
 * </p>
 * 
 * Example DDL:</p>
 * 
 * CREATE TABLE Events(event_type INTEGER NOT NULL, event_date DATE NOT NULL, event_name VARCHAR NOT NULL 
 * CONSTRAINT PK PRIMARY KEY (event_type, event_date DESC))</p>
 * 
 * @since 1.2
 */

public enum SortOrder {

	ASC(2) {
		@Override
		public CompareOp transform(CompareOp op) {
			return op;
		}
	},

	DESC(1) {
		@Override
		public CompareOp transform(CompareOp op) {
			switch (op) {
				case EQUAL: return op;
				case NOT_EQUAL: return op;
				case NO_OP: return op;
				case GREATER: return CompareOp.LESS;
				case GREATER_OR_EQUAL: return CompareOp.LESS_OR_EQUAL;
				case LESS: return CompareOp.GREATER;
				case LESS_OR_EQUAL: return CompareOp.GREATER_OR_EQUAL;
			}
			throw new IllegalArgumentException("Add the missing case statement!");
		}
	};
	
	/**
	 * The default order that row keys are stored in.
	 */
	public static SortOrder getDefault() {
		return ASC;
	}
	
	public static byte[] invert(byte[] src, int srcOffset, byte[] dest, int dstOffset, int length) {
		Preconditions.checkNotNull(src);
		Preconditions.checkNotNull(dest);
		for (int i = 0; i < length; i++) {
			dest[dstOffset + i] = (byte) (src[srcOffset + i] ^ 0xFF);
		}
		return dest;
	}

	public static byte[] invert(byte[] src, int srcOffset, int length) {
		return invert(src, srcOffset, new byte[length], 0, length);
	}

	public static byte invert(byte b) {
		return (byte) (b ^ 0xFF);
	}
	
	/**
	 * Returns the SortOrder instance for the specified DDL stmt keyword.
	 */
	public static SortOrder fromDDLValue(String sortOrder) {
		Preconditions.checkArgument(sortOrder != null);
		if (sortOrder.equalsIgnoreCase("ASC")) {
			return ASC;
		} else if (sortOrder.equalsIgnoreCase("DESC")) {
			return DESC;
		} else {
			throw new IllegalArgumentException("Unknown SortOrder: " + sortOrder);
		}
	}

	/**
	 * Returns the SortOrder instance for the specified internal value.
	 */
	public static SortOrder fromSystemValue(int value) {
		for (SortOrder mod : SortOrder.values()) {
			if (mod.getSystemValue() == value) {
				return mod;
			}
		}
		return getDefault();
	}

	private final int serializationId;

	private SortOrder(int serializationId) {
		this.serializationId = serializationId;
	}

	/**
	 * Returns an internal value representing the specified SortOrder.
	 */
	public int getSystemValue() {
		return serializationId;
	}

	public abstract CompareOp transform(CompareOp op);
}
