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
package org.apache.phoenix.hbase.index.util;

import org.apache.hadoop.hbase.io.ImmutableBytesWritable;

public class ReadOnlyImmutableBytesPtr extends ImmutableBytesPtr {
    
    private static final String ERROR_MESSAGE = "Read-only bytes pointer may not be changed";

    public ReadOnlyImmutableBytesPtr() {
    }

    public ReadOnlyImmutableBytesPtr(byte[] bytes) {
        super(bytes);
    }

    public ReadOnlyImmutableBytesPtr(ImmutableBytesWritable ibw) {
        super(ibw.get(), ibw.getOffset(), ibw.getLength());
    }

    public ReadOnlyImmutableBytesPtr(ImmutableBytesPtr ibp) {
        super(ibp.get(), ibp.getOffset(), ibp.getLength());
    }

    public ReadOnlyImmutableBytesPtr(byte[] bytes, int offset, int length) {
        super(bytes, offset, length);
    }

    @Override
    public void set(byte[] b) {
        throw new UnsupportedOperationException(ERROR_MESSAGE);
    }

    @Override
    public void set(ImmutableBytesWritable ptr) {
        throw new UnsupportedOperationException(ERROR_MESSAGE);
    }

    @Override
    public void set(byte[] b, int offset, int length) {
        throw new UnsupportedOperationException(ERROR_MESSAGE);
    }
}
