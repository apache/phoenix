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

package org.apache.phoenix.query;

import static org.apache.phoenix.query.KeyRange.EMPTY_RANGE;
import static org.apache.phoenix.query.KeyRange.EVERYTHING_RANGE;
import static org.apache.hadoop.hbase.util.Bytes.toBytes;

import java.util.Arrays;
import java.util.Collection;

import junit.framework.TestCase;

import org.apache.phoenix.schema.types.PChar;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

@RunWith(Parameterized.class)
public class KeyRangeUnionTest extends TestCase {
    private final KeyRange a, b, union;

    public KeyRangeUnionTest(KeyRange a, KeyRange b, KeyRange union) {
        this.a = a;
        this.b = b;
        this.union = union;
    }

    @Parameters(name="union of {0} and {1} is {2}")
    public static Collection<?> data() {
        return Arrays.asList(new Object[][] {
                {
                    PChar.INSTANCE.getKeyRange(toBytes("C"), true, toBytes("E"), true),
                    PChar.INSTANCE.getKeyRange(toBytes("D"), true, toBytes("F"), true),
                    PChar.INSTANCE.getKeyRange(toBytes("C"), true, toBytes("F"), true)
                },
                {
                    PChar.INSTANCE.getKeyRange(toBytes("C"), false, toBytes("E"), false),
                    PChar.INSTANCE.getKeyRange(toBytes("D"), true, toBytes("F"), true),
                    PChar.INSTANCE.getKeyRange(toBytes("C"), false, toBytes("F"), true)
                },
                {
                    PChar.INSTANCE.getKeyRange(toBytes("C"), false, toBytes("E"), false),
                    PChar.INSTANCE.getKeyRange(toBytes("D"), true, toBytes("E"), true),
                    PChar.INSTANCE.getKeyRange(toBytes("C"), false, toBytes("E"), true)
                },
                {
                    PChar.INSTANCE.getKeyRange(toBytes("C"), false, toBytes("E"), false),
                    PChar.INSTANCE.getKeyRange(toBytes("C"), true, toBytes("E"), true),
                    PChar.INSTANCE.getKeyRange(toBytes("C"), true, toBytes("E"), true)
                },
                {
                    PChar.INSTANCE.getKeyRange(toBytes("C"), true, toBytes("E"), false),
                    EMPTY_RANGE,
                    PChar.INSTANCE.getKeyRange(toBytes("C"), true, toBytes("E"), false),
                },
                {
                    EVERYTHING_RANGE,
                    PChar.INSTANCE.getKeyRange(toBytes("E"), false, toBytes("F"), true),
                    EVERYTHING_RANGE,
                },
                {
                    EVERYTHING_RANGE,
                    EVERYTHING_RANGE,
                    EVERYTHING_RANGE,
                },
                {
                    EMPTY_RANGE,
                    EVERYTHING_RANGE,
                    EVERYTHING_RANGE,
                },
        });
    }
    @Test
    public void union() {
        assertEquals(union, a.union(b));
        assertEquals(union, b.union(a));
    }
}
