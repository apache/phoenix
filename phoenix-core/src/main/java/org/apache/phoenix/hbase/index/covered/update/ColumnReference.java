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

import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.phoenix.hbase.index.util.ImmutableBytesPtr;

/**
 * 
 */
public class ColumnReference implements Comparable<ColumnReference> {
    
  public static final byte[] ALL_QUALIFIERS = new byte[0];
  
  private static int calcHashCode(byte[] family, byte[] qualifier) {
    final int prime = 31;
    int result = 1;
    result = prime * result + Bytes.hashCode(family);
    result = prime * result + Bytes.hashCode(qualifier);
    return result;
  }

  private final int hashCode;
  protected final byte[] family;
  protected final byte[] qualifier;
    private volatile ImmutableBytesWritable familyPtr;
    private volatile ImmutableBytesWritable qualifierPtr;

  public ColumnReference(byte[] family, byte[] qualifier) {
    this.family = family;
    this.qualifier = qualifier;
    this.hashCode = calcHashCode(family, qualifier);
  }

  public byte[] getFamily() {
    return this.family;
  }

  public byte[] getQualifier() {
    return this.qualifier;
  }
  
    public ImmutableBytesWritable getFamilyWritable() {
        if (this.familyPtr == null) {
            synchronized (this.family) {
                if (this.familyPtr == null) {
                    this.familyPtr = new ImmutableBytesPtr(this.family);
                }
            }
        }
        return this.familyPtr;
    }

    public ImmutableBytesWritable getQualifierWritable() {
        if (this.qualifierPtr == null) {
            synchronized (this.qualifier) {
                if (this.qualifierPtr == null) {
                    this.qualifierPtr = new ImmutableBytesPtr(this.qualifier);
                }
            }
        }
        return this.qualifierPtr;
    }

  @SuppressWarnings("deprecation")
  public boolean matches(KeyValue kv) {
    if (matchesFamily(kv.getBuffer(), kv.getFamilyOffset(), kv.getFamilyLength())) {
      return matchesQualifier(kv.getBuffer(), kv.getQualifierOffset(), kv.getQualifierLength());
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
    return allColumns() ? true : match(bytes, offset, length, qualifier);
  }

  /**
   * @param family to check against
   * @return <tt>true</tt> if this column covers the given family.
   */
  public boolean matchesFamily(byte[] family) {
    return matchesFamily(family, 0, family.length);
  }

  public boolean matchesFamily(byte[] bytes, int offset, int length) {
    return match(bytes, offset, length, family);
  }

  /**
   * @return <tt>true</tt> if this should include all column qualifiers, <tt>false</tt> otherwise
   */
  public boolean allColumns() {
    return this.qualifier == ALL_QUALIFIERS;
  }

  /**
   * Check to see if the passed bytes match the stored bytes
   * @param first
   * @param storedKey the stored byte[], should never be <tt>null</tt>
   * @return <tt>true</tt> if they are byte-equal
   */
  private boolean match(byte[] first, int offset, int length, byte[] storedKey) {
    return first == null ? false : Bytes.equals(first, offset, length, storedKey, 0,
      storedKey.length);
  }

  public KeyValue getFirstKeyValueForRow(byte[] row) {
    return KeyValue.createFirstOnRow(row, family, qualifier == ALL_QUALIFIERS ? null : qualifier);
  }

  @Override
  public int compareTo(ColumnReference o) {
    int c = Bytes.compareTo(family, o.family);
    if (c == 0) {
      // matching families, compare qualifiers
      c = Bytes.compareTo(qualifier, o.qualifier);
    }
    return c;
  }

  @Override
  public boolean equals(Object o) {
    if (o instanceof ColumnReference) {
      ColumnReference other = (ColumnReference) o;
      if (hashCode == other.hashCode && Bytes.equals(family, other.family)) {
        return Bytes.equals(qualifier, other.qualifier);
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
    return "ColumnReference - " + Bytes.toString(family) + ":" + Bytes.toString(qualifier);
  }
}