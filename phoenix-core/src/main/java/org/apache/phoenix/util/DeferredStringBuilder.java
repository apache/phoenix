/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to you under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.phoenix.util;

/**
 * This utility class was partially copied from Salesforce's internationalization utility library
 * (com.salesforce.i18n:i18n-util:1.0.4), which was released under the 3-clause BSD License.
 * The i18n-util library is not maintained anymore, and it was using vulnerable dependencies.
 * For more info, see: https://issues.apache.org/jira/browse/PHOENIX-6818
 *
 * This class implements a StringBuilder that is incrementally copied from a source String.
 * Actual creation the new buffer is deferred until a character differs from a character at
 * the same position in the source String.  This class is useful for reducing garbage creation
 * when doing operations like escaping a String, when most Strings are not expected to contain
 * any escapable characters.  In that case, no additional memory is used (as the original
 * String is not actually copied).
 */
public final class DeferredStringBuilder implements Appendable, CharSequence {

    private StringBuilder buf;
    private int pos;
    private final CharSequence source;

    public DeferredStringBuilder(CharSequence source) {
        if (source == null) {
            this.buf = new StringBuilder(16);
        }
        this.source = source;
    }

    public DeferredStringBuilder append(char c) {
        if (this.buf == null) {
            if (this.pos < this.source.length() && c == this.source.charAt(this.pos)) {
                // characters match - just move ahead
                ++this.pos;
            } else {
                // doh - character mismatch - now we need to allocate a real StringBuilder
                this.buf = new StringBuilder(this.source.length() + 16);
                this.buf.append(this.source.subSequence(0, this.pos));
                this.buf.append(c);
            }
        } else {
            // we've already got the buf - just add this character
            this.buf.append(c);
        }
        return this;
    }

    public DeferredStringBuilder append(CharSequence csq) {
        if (csq == null) {
            return this;
        }
        return append(csq, 0, csq.length());
    }

    public DeferredStringBuilder append(CharSequence csq, int start, int end) {
        if (csq != null) {
            if (buf == null) {
                int chars = end - start;
                // For small strings or overflow, do it char by char.
                if (chars < 10 || (this.pos + chars > this.source.length())) {
                    for (int i = start; i < end; ++i) {
                        append(csq.charAt(i));
                    }
                } else {
                    CharSequence subSeq = csq.subSequence(start, end);
                    //String.equals seems to get optimized a lot quicker than the
                    // chartA + length + loop method. I don't think this will matter at all,
                    // but between this and OptimizedURLEncoder, this made these classes
                    // disappear from my profiler
                    if (this.source.subSequence(this.pos, this.pos + chars).equals(subSeq)) {
                        this.pos += chars;
                    } else {
                        this.buf = new StringBuilder(this.source.length() + 16);
                        this.buf.append(this.source.subSequence(0, this.pos));
                        this.buf.append(subSeq);
                    }
                }
            } else {
                // We know it's different, so just append the whole string.
                buf.append(csq, start, end);
            }
        }
        return this;
    }

    public char charAt(int index) {
        if (this.buf != null) {
            return this.buf.charAt(index);
        } else if (index < pos) {
            return this.source.charAt(index);
        } else {
            throw new StringIndexOutOfBoundsException(index);
        }
    }

    public CharSequence subSequence(int start, int end) {
        if (this.buf != null) {
            return this.buf.subSequence(start, end);
        } else if (end <= pos) {
            return this.source.subSequence(start, end);
        } else {
            throw new StringIndexOutOfBoundsException(end);
        }
    }

    @Override
    public String toString() {
        if (this.buf != null) {
            return this.buf.toString();
        }
        if (this.pos == this.source.length()) {
            return this.source.toString();
        }
        return this.source.subSequence(0, this.pos).toString();
    }

    public int length() {
        return this.buf != null ? this.buf.length() : this.pos;
    }
}
