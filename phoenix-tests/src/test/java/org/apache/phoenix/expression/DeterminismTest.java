/*
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements. See the NOTICE
 * file distributed with this work for additional information regarding copyright ownership. The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with the
 * License. You may obtain a copy of the License at http://www.apache.org/licenses/LICENSE-2.0 Unless required by
 * applicable law or agreed to in writing, software distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the specific language
 * governing permissions and limitations under the License.
 */
package org.apache.phoenix.expression;

import static org.junit.Assert.assertEquals;

import org.junit.Test;

public class DeterminismTest {
	@Test
    public void testCombine() {
		// combining a determinism enum with ALWAYS should always return the
		// other determinism
		assertEquals("Unexpected result ", Determinism.PER_ROW,
				Determinism.ALWAYS.combine(Determinism.PER_ROW));
		assertEquals("Unexpected result ", Determinism.PER_STATEMENT,
				Determinism.ALWAYS.combine(Determinism.PER_STATEMENT));
		assertEquals("Unexpected result ", Determinism.PER_STATEMENT,
				Determinism.PER_STATEMENT.combine(Determinism.ALWAYS));
		assertEquals("Unexpected result ", Determinism.PER_ROW,
				Determinism.PER_ROW.combine(Determinism.ALWAYS));
		
		// combining PER_STATEMENT and PER_ROW should return PER_ROW
		assertEquals("Unexpected result ", Determinism.PER_ROW,
				Determinism.PER_STATEMENT.combine(Determinism.PER_ROW));
		assertEquals("Unexpected result ", Determinism.PER_ROW,
				Determinism.PER_ROW.combine(Determinism.PER_STATEMENT));

	}
}
