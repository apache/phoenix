/*
 * Copyright 2010 The Apache Software Foundation
 *
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 *distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you maynot use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicablelaw or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.phoenix.execute;

import static org.apache.phoenix.execute.MutationState.joinSortedIntArrays;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;

import org.junit.Test;

public class MutationStateTest {

    @Test
    public void testJoinIntArrays() {
        // simple case
        int[] a = new int[] {1};
        int[] b = new int[] {2};
        int[] result = joinSortedIntArrays(a, b);
        
        assertEquals(2, result.length);
        assertArrayEquals(new int[] {1,2}, result);
        
        // empty arrays
        a = new int[0];
        b = new int[0];
        result = joinSortedIntArrays(a, b);
        
        assertEquals(0, result.length);
        assertArrayEquals(new int[] {}, result);
        
        // dupes between arrays
        a = new int[] {1,2,3};
        b = new int[] {1,2,4};
        result = joinSortedIntArrays(a, b);
        
        assertEquals(4, result.length);
        assertArrayEquals(new int[] {1,2,3,4}, result);
        
        // dupes within arrays
        a = new int[] {1,2,2,3};
        b = new int[] {1,2,4};
        result = joinSortedIntArrays(a, b);
        
        assertEquals(4, result.length);
        assertArrayEquals(new int[] {1,2,3,4}, result);
    }
}
