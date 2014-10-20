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
package org.apache.phoenix.coprocessor;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.hbase.filter.FilterBase;
import org.apache.hadoop.hbase.util.Bytes;

/**
 * 
 * Matches rows that end with a given byte array suffix
 *
 * 
 * @since 3.0
 */
public class SuffixFilter extends FilterBase {
    protected byte[] suffix = null;

    public SuffixFilter(final byte[] suffix) {
        this.suffix = suffix;
    }
    
    @Override
    public void readFields(DataInput input) throws IOException {
        suffix = Bytes.readByteArray(input);
    }

    @Override
    public void write(DataOutput output) throws IOException {
        Bytes.writeByteArray(output, suffix);
    }

    @Override
    public boolean filterRowKey(byte[] buffer, int offset, int length) {
        if (buffer == null || this.suffix == null) return true;
        if (length < suffix.length) return true;
        // if they are equal, return false => pass row
        // else return true, filter row
        // if we are passed the suffix, set flag
        int cmp = Bytes.compareTo(buffer, offset + (length - this.suffix.length),
                this.suffix.length, this.suffix, 0, this.suffix.length);
        return cmp != 0;
    }
}
