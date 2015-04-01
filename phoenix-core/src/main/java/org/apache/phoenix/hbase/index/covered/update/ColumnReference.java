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
package org.apache.phoenix.hbase.index.covered.update;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.phoenix.hbase.index.util.ImmutableBytesPtr;
import org.apache.phoenix.hbase.index.util.ReadOnlyImmutableBytesPtr;

/**
 * 
 */
public class ColumnReference implements Comparable<ColumnReference> {
    
  public static final byte[] ALL_QUALIFIERS = new byte[0];
  
  private static int calcHashCode(ImmutableBytesWritable familyPtr, ImmutableBytesWritable qualifierPtr) {
    final int prime = 31;
    int result = 1;
    result = prime * result + familyPtr.hashCode();
    result = prime * result + qualifierPtr.hashCode();
    return result;
  }

    private final int hashCode;
    protected volatile byte[] family;
    protected volatile byte[] qualifier;
    private final ImmutableBytesPtr familyPtr;
    private final ImmutableBytesPtr qualifierPtr;

    public ColumnReference(byte[] family, byte[] qualifier) {
        this.familyPtr = new ReadOnlyImmutableBytesPtr(family);
        this.qualifierPtr = new ReadOnlyImmutableBytesPtr(qualifier);
        this.hashCode = calcHashCode(this.familyPtr, this.qualifierPtr);
    }

    public ColumnReference(byte[] family, int familyOffset, int familyLength, byte[] qualifier,
            int qualifierOffset, int qualifierLength) {
        this.familyPtr = new ReadOnlyImmutableBytesPtr(family, familyOffset, familyLength);
        this.qualifierPtr = new ReadOnlyImmutableBytesPtr(qualifier, qualifierOffset, qualifierLength);
        this.hashCode = calcHashCode(this.familyPtr, this.qualifierPtr);
    }
  
    public byte[] getFamily() {
        if (this.family == null) {
            synchronized (this.familyPtr) {
                if (this.family == null) {
                    this.family = this.familyPtr.copyBytesIfNecessary();
                }
            }
        }
        return this.family;
    }

    public byte[] getQualifier() {
        if (this.qualifier == null) {
            synchronized (this.qualifierPtr) {
                if (this.qualifier == null) {
                    this.qualifier = this.qualifierPtr.copyBytesIfNecessary();
                }
            }
        }
        return this.qualifier;
    }

    public ImmutableBytesPtr getFamilyWritable() {
        return this.familyPtr;
    }

    public ImmutableBytesPtr getQualifierWritable() {
        return this.qualifierPtr;
    }

  public boolean matches(Cell kv) {
    if (matchesFamily(kv.getFamilyArray(), kv.getFamilyOffset(), kv.getFamilyLength())) {
      return matchesQualifier(kv.getQualifierArray(), kv.getQualifierOffset(), kv.getQualifierLength());
    }
    return false;
  }

  /**
   * @param qual to check against
   * @return <tt>true</tt> if this column covers the given qualifier.
   */
  public boolean matchesQualifier(byte[] qual) {
    return matchesQualifier(qual, 0, qual.length);
  }

    public boolean matchesQualifier(byte[] bytes, int offset, int length) {
        return allColumns() ? true : match(bytes, offset, length, qualifierPtr.get(),
            qualifierPtr.getOffset(), qualifierPtr.getLength());
    }

  /**
   * @param family to check against
   * @return <tt>true</tt> if this column covers the given family.
   */
  public boolean matchesFamily(byte[] family) {
    return matchesFamily(family, 0, family.length);
  }

  public boolean matchesFamily(byte[] bytes, int offset, int length) {
    return match(bytes, offset, length, familyPtr.get(), familyPtr.getOffset(), familyPtr.getLength());
  }

  /**
   * @return <tt>true</tt> if this should include all column qualifiers, <tt>false</tt> otherwise
   */
  public boolean allColumns() {
    return getQualifier() == ALL_QUALIFIERS;
  }

    /**
     * Check to see if the passed bytes match the stored bytes
     * @param first
     * @param storedKey the stored byte[], should never be <tt>null</tt>
     * @return <tt>true</tt> if they are byte-equal
     */
    private boolean match(byte[] first, int offset1, int length1, byte[] storedKey, int offset2,
            int length2) {
        return first == null ? false : Bytes.equals(first, offset1, length1, storedKey, offset2,
            length2);
    }

    public KeyValue getFirstKeyValueForRow(byte[] row) {
        return KeyValue.createFirstOnRow(row, getFamily(), getQualifier() == ALL_QUALIFIERS ? null
                : getQualifier());
    }

  @Override
  public int compareTo(ColumnReference o) {
    int c = familyPtr.compareTo(o.familyPtr);
    if (c == 0) {
      // matching families, compare qualifiers
      c = qualifierPtr.compareTo(o.qualifierPtr);
    }
    return c;
  }

  @Override
  public boolean equals(Object o) {
    if (o instanceof ColumnReference) {
      ColumnReference other = (ColumnReference) o;
      if (hashCode == other.hashCode && familyPtr.equals(other.familyPtr)) {
        return qualifierPtr.equals(other.qualifierPtr);
      }
    }
    return false;
  }

  @Override
  public int hashCode() {
    return hashCode;
  }

  @Override
  public String toString() {
    return "ColumnReference - " + Bytes.toString(getFamily()) + ":" + Bytes.toString(getQualifier());
  }
}