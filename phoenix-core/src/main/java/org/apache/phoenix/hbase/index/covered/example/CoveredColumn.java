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

package org.apache.phoenix.hbase.index.covered.example;

import org.apache.hadoop.hbase.util.Bytes;

import org.apache.phoenix.hbase.index.covered.update.ColumnReference;

/**
 * A single Column (either a Column Family or a full Family:Qualifier pair) in a {@link ColumnGroup}
 * . If no column qualifier is specified (null), matches all known qualifiers of the family.
 */
public class CoveredColumn extends ColumnReference {

  public static final String SEPARATOR = ":";
  String familyString;
  private final int hashCode;

  public CoveredColumn(byte[] family, byte[] qualifier){
    this(Bytes.toString(family), qualifier);
  }

  public CoveredColumn(String family, byte[] qualifier) {
    super(Bytes.toBytes(family), qualifier == null ? ColumnReference.ALL_QUALIFIERS : qualifier);
    this.familyString = family;
    this.hashCode = calcHashCode(family, qualifier);
  }

  public static CoveredColumn parse(String spec) {
    int sep = spec.indexOf(SEPARATOR);
    if (sep < 0) {
      throw new IllegalArgumentException(spec + " is not a valid specifier!");
    }
    String family = spec.substring(0, sep);
    String qual = spec.substring(sep + 1);
    byte[] column = qual.length() == 0 ? null : Bytes.toBytes(qual);
    return new CoveredColumn(family, column);
  }

  public String serialize() {
    return CoveredColumn.serialize(familyString, getQualifier());
  }

  public static String serialize(String first, byte[] second) {
    String nextValue = first + CoveredColumn.SEPARATOR;
    if (second != null) {
      nextValue += Bytes.toString(second);
    }
    return nextValue;
  }

  /**
   * @param family2 to check
   * @return <tt>true</tt> if the passed family matches the family this column covers
   */
  public boolean matchesFamily(String family2) {
    return this.familyString.equals(family2);
  }

  @Override
  public int hashCode() {
    return hashCode;
  }

  private static int calcHashCode(String familyString, byte[] qualifier) {
    final int prime = 31;
    int result = 1;
    result = prime * result + familyString.hashCode();
    if (qualifier != null) {
      result = prime * result + Bytes.hashCode(qualifier);
    }
    return result;
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj) return true;
    if (!super.equals(obj)) return false;
    if (getClass() != obj.getClass()) return false;
    CoveredColumn other = (CoveredColumn) obj;
    if (hashCode != other.hashCode) return false;
    if (!familyString.equals(other.familyString)) return false;
    return Bytes.equals(getQualifier(), other.getQualifier());
  }

  @Override
  public String toString() {
    String qualString = getQualifier() == null ? "null" : Bytes.toString(getQualifier());
    return "CoveredColumn:[" + familyString + ":" + qualString + "]";
  }
}