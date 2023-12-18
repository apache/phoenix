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

import java.io.DataInput;
import java.io.IOException;

import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.util.Bytes;

public class ImmutableBytesPtr extends ImmutableBytesWritable {
    private int hashCode;
    
    public ImmutableBytesPtr() {
    }

    public ImmutableBytesPtr(byte[] bytes) {
        super(bytes);
        hashCode = super.hashCode();
    }

    public ImmutableBytesPtr(ImmutableBytesWritable ibw) {
        super(ibw.get(), ibw.getOffset(), ibw.getLength());
        hashCode = super.hashCode();
    }

    public ImmutableBytesPtr(ImmutableBytesPtr ibp) {
        super(ibp.get(), ibp.getOffset(), ibp.getLength());
        hashCode = ibp.hashCode;
    }

    public ImmutableBytesPtr(byte[] bytes, int offset, int length) {
        super(bytes, offset, length);
        hashCode = super.hashCode();
    }

    @Override
    public int hashCode() {
        return hashCode;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) return true;
        if (obj == null) return false;
        if (getClass() != obj.getClass()) return false;
        ImmutableBytesPtr that = (ImmutableBytesPtr)obj;
        if (this.hashCode != that.hashCode) return false;
        if (Bytes.compareTo(this.get(), this.getOffset(), this.getLength(), that.get(), that.getOffset(), that.getLength()) != 0) return false;
        return true;
    }

    public void set(ImmutableBytesWritable ptr) {
        set(ptr.get(),ptr.getOffset(),ptr.getLength());
      }

    /**
     * @param b Use passed bytes as backing array for this instance.
     */
    @Override
    public void set(final byte [] b) {
      super.set(b);
      hashCode = super.hashCode();
    }

    /**
     * @param b Use passed bytes as backing array for this instance.
     * @param offset
     * @param length
     */
    @Override
    public void set(final byte [] b, final int offset, final int length) {
        super.set(b,offset,length);
        hashCode = super.hashCode();
    }

    @Override
    public void readFields(final DataInput in) throws IOException {
        super.readFields(in);
        hashCode = super.hashCode();
    }
    
    /**
     * @return the backing byte array, copying only if necessary
     */
    public byte[] copyBytesIfNecessary() {
    return copyBytesIfNecessary(this);
    }

  public static byte[] copyBytesIfNecessary(ImmutableBytesWritable ptr) {
    if (ptr.getOffset() == 0 && ptr.getLength() == ptr.get().length) {
      return ptr.get();
    }
    return ptr.copyBytes();
  }
}
