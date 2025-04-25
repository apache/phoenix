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

import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class DecodeViewIndexIdFunctionTest {

    @Test
    public void testExpressionWithDecodeViewIndexIdFunction() throws Exception {
        ParseNode parseNode = SQLParser.parseCondition("DECODE_VIEW_INDEX_ID(VIEW_INDEX_ID, VIEW_INDEX_ID_DATA_TYPE) = 32768");
        boolean hasGetViewIndexIdParseNode = false;
        for (ParseNode childNode : parseNode.getChildren()) {
            if (childNode.getClass().isAssignableFrom(DecodeViewIndexIdParseNode.class)) {
                assertEquals(2, childNode.getChildren().size());
                hasGetViewIndexIdParseNode = true;
            }
        }
        assertTrue(hasGetViewIndexIdParseNode);
    }

    @Test
    public void testValidationForDecodeViewIndexIdFunction() throws Exception {
        boolean hasGetViewIndexIdParseNode = false;
        try {
            ParseNode parseNode = SQLParser.parseCondition("DECODE_VIEW_INDEX_ID(VIEW_INDEX_ID, b) = 32768");
            for (ParseNode childNode : parseNode.getChildren()) {
                if (childNode.getClass().isAssignableFrom(DecodeViewIndexIdParseNode.class)) {
                    assertEquals(2, childNode.getChildren().size());
                    hasGetViewIndexIdParseNode = true;
                }
            }
        } catch (Exception e) {
            hasGetViewIndexIdParseNode = false;

        }
        assertFalse(hasGetViewIndexIdParseNode);
    }

}
