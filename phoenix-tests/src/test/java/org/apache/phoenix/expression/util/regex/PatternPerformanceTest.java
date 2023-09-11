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
package org.apache.phoenix.expression.util.regex;

import static org.junit.Assert.assertTrue;

import java.sql.SQLException;

import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.phoenix.schema.types.PBoolean;
import org.apache.phoenix.schema.types.PVarchar;
import org.apache.phoenix.schema.types.PVarcharArray;
import org.apache.phoenix.schema.types.PhoenixArray;
import org.junit.Test;

public class PatternPerformanceTest {

    static private class Timer {
        private long startTimeStamp;

        public void reset() {
            startTimeStamp = System.currentTimeMillis();
        }

        public double currentTime() {
            return (System.currentTimeMillis() - startTimeStamp) / 1000.0;
        }

        public void printTime(String hint) {
            System.out.println(hint + " Time=" + currentTime());
        }
    }

    private String[] data = new String[] { "ONE:TWO:THREE", "ABC:DEF", "PKU:THU:FDU" };
    private ImmutableBytesWritable[] dataPtr = new ImmutableBytesWritable[] { getPtr(data[0]),
            getPtr(data[1]), getPtr(data[2]) };
    private String patternString;
    private ImmutableBytesWritable resultPtr = new ImmutableBytesWritable();
    private int maxTimes = 10000000;
    private Timer timer = new Timer();
    private final boolean ENABLE_ASSERT = false;

    private static ImmutableBytesWritable getPtr(String str) {
        return new ImmutableBytesWritable(PVarchar.INSTANCE.toBytes(str));
    }

    private void testReplaceAll(ImmutableBytesWritable replacePtr, AbstractBasePattern pattern,
            String name) {
        timer.reset();
        for (int i = 0; i < maxTimes; ++i) {
            ImmutableBytesWritable ptr = dataPtr[i % 3];
            resultPtr.set(ptr.get(), ptr.getOffset(), ptr.getLength());
            pattern.replaceAll(resultPtr, replacePtr.get(), replacePtr.getOffset(),
                replacePtr.getLength());
            if (ENABLE_ASSERT) {
                String result = (String) PVarchar.INSTANCE.toObject(resultPtr);
                assertTrue((i % 3 == 1 && ":".equals(result))
                        || (i % 3 != 1 && "::".equals(result)));
            }
        }
        timer.printTime(name);
    }

    public void testReplaceAll() {
        patternString = "[A-Z]+";
        ImmutableBytesWritable replacePtr = getPtr("");
        testReplaceAll(replacePtr, new JavaPattern(patternString), "Java replaceAll");
        testReplaceAll(replacePtr, new JONIPattern(patternString), "JONI replaceAll");
    }

    private void testLike(AbstractBasePattern pattern, String name) {
        timer.reset();
        for (int i = 0; i < maxTimes; ++i) {
            ImmutableBytesWritable ptr = dataPtr[i % 3];
            resultPtr.set(ptr.get(), ptr.getOffset(), ptr.getLength());
            pattern.matches(resultPtr);
            if (ENABLE_ASSERT) {
                Boolean b = (Boolean) PBoolean.INSTANCE.toObject(resultPtr);
                assertTrue(i % 3 != 2 || b.booleanValue());
            }
        }
        timer.printTime(name);
    }

    public void testLike() {
        patternString = "\\Q\\E.*\\QU\\E.*\\QU\\E.*\\QU\\E.*\\Q\\E";
        testLike(new JavaPattern(patternString), "Java Like");
        testLike(new JONIPattern(patternString), "JONI Like");
    }

    private void testSubstr(AbstractBasePattern pattern, String name) {
        timer.reset();
        for (int i = 0; i < maxTimes; ++i) {
            ImmutableBytesWritable ptr = dataPtr[i % 3];
            resultPtr.set(ptr.get(),ptr.getOffset(),ptr.getLength());
            pattern.substr(resultPtr, 0);
            if (ENABLE_ASSERT) {
                assertTrue((i % 3 != 2 || ":THU".equals(PVarchar.INSTANCE.toObject(resultPtr))));
            }
        }
        timer.printTime(name);
    }

    public void testSubstr() {
        patternString = "\\:[A-Z]+";
        testSubstr(new JavaPattern(patternString), "Java Substr");
        testSubstr(new JONIPattern(patternString), "JONI Substr");
    }

    private void testSplit(AbstractBaseSplitter pattern, String name) throws SQLException {
        timer.reset();
        for (int i = 0; i < maxTimes; ++i) {
            ImmutableBytesWritable ptr = dataPtr[i % 3];
            resultPtr.set(ptr.get(), ptr.getOffset(), ptr.getLength());
            boolean ret = pattern.split(resultPtr);
            if (ENABLE_ASSERT) {
                PhoenixArray array = (PhoenixArray) PVarcharArray.INSTANCE.toObject(resultPtr);
                assertTrue(ret && (i % 3 != 1 || ((String[]) array.getArray()).length == 2));
            }
        }
        timer.printTime(name);
    }

    public void testSplit() throws SQLException {
        patternString = "\\:";
        testSplit(new GuavaSplitter(patternString), "GuavaSplit");
        testSplit(new JONIPattern(patternString), "JONI Split");
    }

    @Test
    public void test() throws Exception {
        // testLike();
        // testReplaceAll();
        // testSubstr();
        // testSplit();
    }
}
