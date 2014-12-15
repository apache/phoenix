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

import java.io.IOException;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.exceptions.DeserializationException;
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
    public byte[] toByteArray() throws IOException {
        return suffix;
    }

    @Override
    public ReturnCode filterKeyValue(Cell ignored) throws IOException {
      return ReturnCode.INCLUDE;
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

    
    public static SuffixFilter parseFrom(final byte [] pbBytes) throws DeserializationException {
        return new SuffixFilter(pbBytes);
    }
}