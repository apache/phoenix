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
package org.apache.phoenix.filter;

import java.util.BitSet;
import java.util.HashSet;
import org.apache.phoenix.schema.PTable.QualifierEncodingScheme;
import junit.framework.TestCase;

public class EncodedQualifiersColumnProjectionFilterTest extends TestCase {

    private final String someEmptyCFName = "cfName1";
    private final String someConditionalCFName1 = "conditionalCfName1";
    private final String someConditionalCFName2 = "conditionalCfName2";
    private final QualifierEncodingScheme someQualifiedEncodingScheme = QualifierEncodingScheme.ONE_BYTE_QUALIFIERS;
    private final BitSet someBitSet;
    private EncodedQualifiersColumnProjectionFilter filter;

    public EncodedQualifiersColumnProjectionFilterTest() {
        HashSet<byte[]> conditionalCFNames = new HashSet<byte[]>(2);
        conditionalCFNames.add(someConditionalCFName1.getBytes());
        conditionalCFNames.add(someConditionalCFName2.getBytes());

        this.someBitSet = new BitSet();
        this.someBitSet.xor(new BitSet(0)); // All 1s

        this.filter = new EncodedQualifiersColumnProjectionFilter(
                this.someEmptyCFName.getBytes(),
                someBitSet,
                conditionalCFNames,
                this.someQualifiedEncodingScheme);
    }

    public void testToString() {
        String outputString = this.filter.toString();

        assertTrue(outputString.contains("EmptyCFName"));
        assertTrue(outputString.contains("EncodingScheme"));
        assertTrue(outputString.contains("TrackedColumns"));
        assertTrue(outputString.contains("ConditionOnlyCfs"));
        assertTrue(outputString.contains(this.someEmptyCFName));
        assertTrue(outputString.contains(this.someConditionalCFName1));
        assertTrue(outputString.contains(this.someConditionalCFName2));
        assertTrue(outputString.contains(this.someBitSet.toString()));
    }
}
