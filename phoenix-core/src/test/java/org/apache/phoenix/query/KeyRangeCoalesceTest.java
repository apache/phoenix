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
import static java.util.Arrays.asList;
import static org.apache.hadoop.hbase.util.Bytes.toBytes;

import java.util.*;

import junit.framework.TestCase;

import org.apache.phoenix.schema.types.PChar;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

@RunWith(Parameterized.class)
public class KeyRangeCoalesceTest extends TestCase {
    private static final Random RANDOM = new Random(1);
    private final List<KeyRange> expected, input;

    public KeyRangeCoalesceTest(List<KeyRange> expected, List<KeyRange> input) {
        this.expected = expected;
        this.input = input;
    }

    @Parameters(name="{0} coalesces to {1}")
    public static Collection<?> data() {
        return Arrays.asList(new Object[][] {
                {expect(
                    EMPTY_RANGE
                ),
                input(
                )},
                {expect(
                        PChar.INSTANCE.getKeyRange(toBytes("C"), true, toBytes("E"), true)
                ),
                input(
                        PChar.INSTANCE.getKeyRange(toBytes("C"), true, toBytes("E"), true)
                )},
                {expect(
                        PChar.INSTANCE.getKeyRange(toBytes("C"), true, toBytes("E"), true)
                ),
                input(
                        PChar.INSTANCE.getKeyRange(toBytes("C"), true, toBytes("D"), true),
                        PChar.INSTANCE.getKeyRange(toBytes("D"), true, toBytes("E"), true)
                )},
                {expect(
                        PChar.INSTANCE.getKeyRange(toBytes("C"), true, toBytes("Z"), true)
                ),
                input(
                        PChar.INSTANCE.getKeyRange(toBytes("C"), true, toBytes("D"), true),
                        PChar.INSTANCE.getKeyRange(toBytes("D"), true, toBytes("E"), true),
                        PChar.INSTANCE.getKeyRange(toBytes("D"), true, toBytes("Z"), true)
                )},
                {expect(
                        PChar.INSTANCE.getKeyRange(toBytes("B"), true, toBytes("Z"), true)
                ),
                input(
                        PChar.INSTANCE.getKeyRange(toBytes("C"), true, toBytes("D"), true),
                        PChar.INSTANCE.getKeyRange(toBytes("B"), true, toBytes("E"), true),
                        PChar.INSTANCE.getKeyRange(toBytes("D"), true, toBytes("Z"), true)
                )},
                {expect(
                        PChar.INSTANCE.getKeyRange(toBytes("B"), true, toBytes("Z"), true)
                ),
                input(
                        PChar.INSTANCE.getKeyRange(toBytes("C"), true, toBytes("D"), true),
                        PChar.INSTANCE.getKeyRange(toBytes("B"), true, toBytes("Z"), false),
                        PChar.INSTANCE.getKeyRange(toBytes("D"), true, toBytes("Z"), true)
                )},
                {expect(
                        PChar.INSTANCE.getKeyRange(toBytes("A"), true, toBytes("A"), true),
                        PChar.INSTANCE.getKeyRange(toBytes("B"), true, toBytes("Z"), false)
                ),
                input(
                        PChar.INSTANCE.getKeyRange(toBytes("A"), true, toBytes("A"), true),
                        PChar.INSTANCE.getKeyRange(toBytes("B"), true, toBytes("Z"), false)
                )},
                {expect(
                        PChar.INSTANCE.getKeyRange(toBytes("A"), true, toBytes("B"), false),
                        PChar.INSTANCE.getKeyRange(toBytes("B"), false, toBytes("Z"), false)
                ),
                input(
                        PChar.INSTANCE.getKeyRange(toBytes("A"), true, toBytes("B"), false),
                        PChar.INSTANCE.getKeyRange(toBytes("B"), false, toBytes("Z"), false)
                )},
                {expect(
                        PChar.INSTANCE.getKeyRange(toBytes("A"), true, toBytes("Z"), false)
                ),
                input(
                        PChar.INSTANCE.getKeyRange(toBytes("A"), true, toBytes("B"), false),
                        PChar.INSTANCE.getKeyRange(toBytes("B"), true, toBytes("Z"), false)
                )},
                {expect(
                    EVERYTHING_RANGE
                ),
                input(
                    EVERYTHING_RANGE,
                    EVERYTHING_RANGE
                )},
                {expect(
                    EVERYTHING_RANGE
                ),
                input(
                    EVERYTHING_RANGE
                )},
                {expect(
                    EVERYTHING_RANGE
                ),
                input(
                    EMPTY_RANGE,
                    EVERYTHING_RANGE,
                    EVERYTHING_RANGE
                )},
                {expect(
                    EMPTY_RANGE
                ),
                input(
                    EMPTY_RANGE
                )}
        });
    }
    @Test
    public void coalesce() {
        assertEquals(expected, KeyRange.coalesce(input));
        List<KeyRange> tmp = new ArrayList<KeyRange>(input);
        Collections.reverse(tmp);
        assertEquals(expected, KeyRange.coalesce(input));
        Collections.shuffle(tmp, RANDOM);
        assertEquals(expected, KeyRange.coalesce(input));
    }
    
    private static final List<KeyRange> expect(KeyRange... kr) {
        return asList(kr);
    }
    
    private static final List<KeyRange> input(KeyRange... kr) {
        return asList(kr);
    }
}
