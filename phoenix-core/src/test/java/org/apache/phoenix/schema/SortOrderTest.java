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

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;

import org.apache.hadoop.hbase.filter.CompareFilter.CompareOp;
import org.apache.phoenix.schema.types.PBoolean;
import org.apache.phoenix.schema.types.PDataType;
import org.junit.Test;

/**
 * @since 3.0
 */
public class SortOrderTest {
	
	@Test
	public void ascSortOrderDoesNotTransformOp() {
	    for (CompareOp op : CompareOp.values()) {
	    	assertSame(op, SortOrder.ASC.transform(op));
	    }
	}
	
	@Test
	public void booleanLogic() {
	    assertTrue(PBoolean.INSTANCE.toObject(PDataType.TRUE_BYTES, SortOrder.ASC) == PBoolean.INSTANCE.toObject(
          PDataType.FALSE_BYTES, SortOrder.DESC));
        assertTrue(
            PBoolean.INSTANCE.toObject(PBoolean.INSTANCE.toBytes(true), SortOrder.ASC) == PBoolean.INSTANCE.toObject(
                PBoolean.INSTANCE.toBytes(false), SortOrder.DESC));
        assertTrue(
            PBoolean.INSTANCE.toObject(PBoolean.INSTANCE.toBytes(true,SortOrder.ASC)) == PBoolean.INSTANCE.toObject(
                PBoolean.INSTANCE.toBytes(false,SortOrder.DESC)));

        assertFalse(PBoolean.INSTANCE.toObject(PDataType.FALSE_BYTES, SortOrder.ASC) == PBoolean.INSTANCE.toObject(PDataType.FALSE_BYTES, SortOrder.DESC));
        assertFalse(
            PBoolean.INSTANCE.toObject(PBoolean.INSTANCE.toBytes(false), SortOrder.ASC) == PBoolean.INSTANCE.toObject(
                PBoolean.INSTANCE.toBytes(false), SortOrder.DESC));
        assertFalse(
            PBoolean.INSTANCE.toObject(PBoolean.INSTANCE.toBytes(false,SortOrder.ASC)) == PBoolean.INSTANCE.toObject(
                PBoolean.INSTANCE.toBytes(false,SortOrder.DESC)));
	}

	@Test
	public void descSortOrderTransformsOp() {
	    for (CompareOp op : CompareOp.values()) {
	    	CompareOp oppositeOp = SortOrder.DESC.transform(op);
	    	switch (op) {
			case EQUAL:
				assertSame(CompareOp.EQUAL, oppositeOp);
				break;
			case GREATER:
				assertSame(CompareOp.LESS, oppositeOp);
				break;
			case GREATER_OR_EQUAL:
				assertSame(CompareOp.LESS_OR_EQUAL, oppositeOp);
				break;
			case LESS:
				assertSame(CompareOp.GREATER, oppositeOp);
				break;
			case LESS_OR_EQUAL:
				assertSame(CompareOp.GREATER_OR_EQUAL, oppositeOp);
				break;
			case NOT_EQUAL:
				assertSame(CompareOp.NOT_EQUAL, oppositeOp);
				break;
			case NO_OP:
				assertSame(CompareOp.NO_OP, oppositeOp);
				break;
	    	}
	    }		
	}
	
	@Test
    public void defaultIsAsc() {
		assertSame(SortOrder.ASC, SortOrder.getDefault());
	}
	
	@Test
	public void ddlValue() {
		assertSame(SortOrder.ASC, SortOrder.fromDDLValue("ASC"));
		assertSame(SortOrder.ASC, SortOrder.fromDDLValue("asc"));
		assertSame(SortOrder.ASC, SortOrder.fromDDLValue("aSc"));
		assertSame(SortOrder.DESC, SortOrder.fromDDLValue("DESC"));
		assertSame(SortOrder.DESC, SortOrder.fromDDLValue("desc"));
		assertSame(SortOrder.DESC, SortOrder.fromDDLValue("DesC"));		
		
		try {
			SortOrder.fromDDLValue("foo");
		} catch (IllegalArgumentException expected) {
			
		}
	}
	
	@Test
	public void systemValue() {
		assertSame(SortOrder.ASC, SortOrder.fromSystemValue(SortOrder.ASC.getSystemValue()));
		assertSame(SortOrder.DESC, SortOrder.fromSystemValue(SortOrder.DESC.getSystemValue()));		
		assertSame(SortOrder.ASC, SortOrder.fromSystemValue(0));
	}
	
	@Test
	public void invertByte() {
		byte b = 42;
		assertNotEquals(b, SortOrder.invert(b));
		assertEquals(b, SortOrder.invert(SortOrder.invert(b)));
	}
	
	@Test
	public void invertByteArray() {
		byte[] b = new byte[]{1, 2, 3, 4};
		assertArrayEquals(b, SortOrder.invert(SortOrder.invert(b, 0, b.length), 0, b.length));
	}
}
