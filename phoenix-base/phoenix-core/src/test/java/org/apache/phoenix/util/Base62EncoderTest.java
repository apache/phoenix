/**
 *  Licensed to the Apache Software Foundation (ASF) under one or more
 *  contributor license agreements.  See the NOTICE file distributed with
 *  this work for additional information regarding copyright ownership.
 *  The ASF licenses this file to You under the Apache License, Version 2.0
 *  (the "License"); you may not use this file except in compliance with
 *  the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */
package org.apache.phoenix.util;

import static org.junit.Assert.assertEquals;

import org.junit.Test;

public class Base62EncoderTest {

    @Test
    public final void testPowersBase62() {
        // add 1 since pow62 doesn't contain it
        long[] pow62 = new long[Base62Encoder.pow62.length + 1];
        pow62[0] = 1;
        System.arraycopy(Base62Encoder.pow62, 0, pow62, 1, Base62Encoder.pow62.length);
        long input = 0l;
        // test 0
        assertEquals("Base 62 encoded value for " + input + "  is incorrect ", "0", Base62Encoder.toString(input));
        StringBuilder expectedValBuilder = new StringBuilder("1");
        for (int i = 0; i < pow62.length; ++i) {
            input = pow62[i];
            assertEquals("Base 62 encoded value for " + input + "  is incorrect ", expectedValBuilder.toString(),
                Base62Encoder.toString(input));
            assertEquals("Base 62 encoded value for " + input + "  is incorrect ", "-" + expectedValBuilder.toString(),
                Base62Encoder.toString(-input));
            expectedValBuilder.append("0");
        }
    }

}
