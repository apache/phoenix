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

/**
 * Utility for converting a base 10 number to string that represents a base 62 number 
 */
public class Base62Encoder {

    // All possible chars for representing a number as a base 62 encoded String
    public static final char[] digits = "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz".toCharArray();

    private static final char[] DigitTens = new char[3844];
    private static final char[] DigitOnes = new char[3844];

    static {
        for (byte i = 0; i < 62; ++i) {
            for (byte j = 0; j < 62; ++j) {
                DigitTens[i * 62 + j] = digits[i];
                DigitOnes[i * 62 + j] = digits[j];
            }
        }
    }

    final static long[] pow62 = { 62, 3844, 238328, 14776336, 916132832, 56800235584L, 3521614606208L,
        218340105584896L, 13537086546263552L, 839299365868340224L };

    /**
     * Returns the length of the base 62 encoded string required to represent num
     * 
     * @param num
     *            must be a positive number
     */
    static int stringSize(long num) {
        for (int i = 0; i < 10; i++) {
            if (num < pow62[i])
                return i + 1;
        }
        return 11;
    }

    /**
     * Fills the given buffer with a string representing the given number in base 62. The characters are placed into the
     * buffer backwards starting with the least significant digit and working backwards from there.
     * 
     * @param num
     *            number to convert, should be > Long.MIN_VALUE
     * @param size
     *            size of the buffer
     * @param buf
     *            buffer to place encoded string
     */
    static void getChars(long num, int size, char[] buf) {
        long q;
        int r;
        int charPos = size;
        char sign = 0;

        if (num < 0) {
            sign = '-';
            num = -num;
        }

        // Get 2 digits per iteration using longs until quotient fits into an int
        while (num > Integer.MAX_VALUE) {
            q = num / 3844;
            r = (int) (num - (q * 3844));
            num = q;
            buf[--charPos] = DigitOnes[r];
            buf[--charPos] = DigitTens[r];
        }

        // Get 2 digits per iteration using ints
        int q2;
        int i2 = (int) num;
        while (i2 >= 65536) {
            q2 = i2 / 3844;
            r = i2 - (q2 * 3844);
            i2 = q2;
            buf[--charPos] = DigitOnes[r];
            buf[--charPos] = DigitTens[r];
        }

        // Fall through to fast mode for smaller numbers
        // assert(i2 <= 65536, i2);
        for (;;) {
            // this evaluates to i2/62
            // see "How to optimize for the Pentium family of microprocessors", Agner Fog, section 18.7
            q2 = ((i2 + 1) * 33825) >>> (16 + 5);
            r = i2 - (q2 * 62);
            buf[--charPos] = digits[r];
            i2 = q2;
            if (i2 == 0)
                break;
        }
        if (sign != 0) {
            buf[--charPos] = sign;
        }
    }

    /**
     * Returns a String object representing the specified long encoded in base 62.
     * 
     * @param num
     *            number to be converted
     * @return a string representation of the number encoded in base 62
     */
    public static String toString(long num) {
        if (num == Long.MIN_VALUE)
            return "-AzL8n0Y58m8";
        int size = (num < 0) ? stringSize(-num) + 1 : stringSize(num);
        char[] buf = new char[size];
        getChars(num, size, buf);
        return new String(buf);
    }
}
