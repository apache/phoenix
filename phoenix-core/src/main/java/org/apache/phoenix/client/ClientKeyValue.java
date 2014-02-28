/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.phoenix.client;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Arrays;

import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.util.Bytes;

/**
 * {@link KeyValue} that should only be used from the client side. Enables clients to be more
 * flexible with the byte arrays they use when building a {@link KeyValue}, but still wire
 * compatible.
 * <p>
 * All<tt> byte[]</tt> (or {@link ImmutableBytesWritable}) passed into the constructor are only ever
 * read once - when writing <tt>this</tt> onto the wire. They are never copied into another array or
 * reused. This has the advantage of being much more efficient than the usual {@link KeyValue}
 * <p>
 * The down side is that we no longer can support some of the usual methods like
 * {@link #getBuffer()} or {@link #getKey()} since its is backed with multiple <tt>byte[]</tt> and
 * <i>should only be used by the client to <b>send</b> information</i>
 * <p>
 * <b>WARNING:</b> should only be used by advanced users who know how to construct their own
 * KeyValues
 */
public class ClientKeyValue extends KeyValue {

  private static ImmutableBytesWritable NULL = new ImmutableBytesWritable(new byte[0]);
  private ImmutableBytesWritable row;
  private ImmutableBytesWritable family;
  private ImmutableBytesWritable qualifier;
  private Type type;
  private long ts;
  private ImmutableBytesWritable value;

  /**
   * @param row must not be <tt>null</tt>
   * @param type must not be <tt>null</tt>
   */
  public ClientKeyValue(ImmutableBytesWritable row, ImmutableBytesWritable family,
      ImmutableBytesWritable qualifier, long ts, Type type, ImmutableBytesWritable value) {
    this.row = row;
    this.family = family == null ? NULL : family;
    this.qualifier = qualifier == null ? NULL : qualifier;
    this.type = type;
    this.ts = ts;
    this.value = value == null ? NULL : value;
  }

  /**
   * Convenience constructor that just wraps all the bytes in {@link ImmutableBytesWritable}
   */
  public ClientKeyValue(byte[] row, byte[] family, byte[] qualifier, long ts, Type t, byte[] value) {
    this(wrap(row), wrap(family), wrap(qualifier), ts, t, wrap(value));
  }

  /**
   * Convenience constructor that just wraps all the bytes in {@link ImmutableBytesWritable}
   */
  public ClientKeyValue(byte[] row, byte[] family, byte[] qualifier, long ts, Type t) {
    this(wrap(row), wrap(family), wrap(qualifier), ts, t, null);
  }

  private static ImmutableBytesWritable wrap(byte[] b) {
    return b == null ? NULL : new ImmutableBytesWritable(b);
  }

  @Override
  public KeyValue clone() {
    return new ClientKeyValue(copy(row), copy(family), copy(qualifier), ts, type, copy(value));
  }

  private ImmutableBytesWritable copy(ImmutableBytesWritable bytes) {
    return new ImmutableBytesWritable(bytes.copyBytes());
  }

  private static byte[] copyIfNecessary(ImmutableBytesWritable bytes) {
    byte[] b = bytes.get();
    if (bytes.getLength() == b.length && bytes.getOffset() == 0) {
      return b;
    }
    return Arrays.copyOfRange(b, bytes.getOffset(), bytes.getOffset() + bytes.getLength());
  }

  @Override
  public KeyValue shallowCopy() {
    return new ClientKeyValue(row, family, qualifier, ts, type, value);
  }

  @Override
  public int getValueOffset() {
    return value.getOffset();
  }

  @Override
  public int getValueLength() {
    return value.getLength();
  }

  @Override
  public int getRowOffset() {
    return row.getOffset();
  }

  @Override
  public short getRowLength() {
    return (short) row.getLength();
  }

  @Override
  public int getFamilyOffset() {
    return family.getOffset();
  }

  @Override
  public byte getFamilyLength() {
    return (byte) family.getLength();
  }

  @Override
  public byte getFamilyLength(int foffset) {
    return this.getFamilyLength();
  }

  @Override
  public int getQualifierOffset() {
    return qualifier.getOffset();
  }

  @Override
  public int getQualifierLength() {
    return qualifier.getLength();
  }

  @Override
  public int getQualifierLength(int rlength, int flength) {
    return this.getQualifierLength();
  }

  @Override
  public int getTotalColumnLength(int rlength, int foffset) {
    return this.getFamilyLength() + this.getQualifierLength();
  }

  @Override
  public int getTotalColumnLength() {
    return qualifier.getLength() + family.getLength();
  }

  @Override
  public byte[] getValue() {
    return copyIfNecessary(value);
  }

  @Override
  public byte[] getRow() {
    return copyIfNecessary(row);
  }

  @Override
  public long getTimestamp() {
    return ts;
  }

  @Override
  public byte[] getFamily() {
    return copyIfNecessary(family);
  }

  @Override
  public byte[] getQualifier() {
    return copyIfNecessary(qualifier);
  }

  @Override
  public byte getType() {
    return this.type.getCode();
  }

  @Override
  public boolean matchingFamily(byte[] family) {
    if (family == null) {
      if (this.family.getLength() == 0) {
        return true;
      }
      return false;
    }
    return matchingFamily(family, 0, family.length);
  }

  @Override
  public boolean matchingFamily(byte[] family, int offset, int length) {
    if (family == null) {
      if (this.family.getLength() == 0) {
        return true;
      }
      return false;
    }
    return matches(family, offset, length, this.family);
  }

  @Override
  public boolean matchingFamily(KeyValue other) {
    if(other == null) {
      return false;
    }
    if(other instanceof ClientKeyValue) {
      ClientKeyValue kv = (ClientKeyValue)other;
      return this.family.compareTo(kv.family) == 0;
    }
    return matchingFamily(other.getBuffer(), other.getFamilyOffset(), other.getFamilyLength());
  }

  private boolean matches(byte[] b, int offset, int length, ImmutableBytesWritable bytes) {
    return Bytes.equals(b, offset, length, bytes.get(), bytes.getOffset(), bytes.getLength());
  }

  @Override
  public boolean matchingQualifier(byte[] qualifier) {
    if (qualifier == null) {
      if (this.qualifier.getLength() == 0) {
        return true;
      }
      return false;
    }
    return matchingQualifier(qualifier, 0, qualifier.length);
  }

  @Override
  public boolean matchingQualifier(byte[] qualifier, int offset, int length) {
    if (qualifier == null) {
      if (this.qualifier.getLength() == 0) {
        return true;
      }
      return false;
    }
    return matches(qualifier, offset, length, this.qualifier);
  }

  @Override
  public boolean matchingQualifier(KeyValue other) {
    if (other == null) {
      return false;
    }
    if (other instanceof ClientKeyValue) {
      ClientKeyValue kv = (ClientKeyValue) other;
      return this.row.compareTo(kv.row) == 0;
    }
    return matchingQualifier(other.getBuffer(), other.getQualifierOffset(),
      other.getQualifierLength());
  }

  @Override
  public boolean matchingRow(byte[] row){
    if (row == null) {
      return false;
    }
    return matches(row, 0, row.length, this.row);
  }

  @Override
  public boolean matchingRow(byte[] row, int offset, int length) {
    if (row == null) {
      return false;
    }
    return matches(row, offset, length, this.row);
  }

  @Override
  public boolean matchingRow(KeyValue other) {
    return matchingRow(other.getBuffer(), other.getRowOffset(), other.getRowLength());
  }

  @Override
  public boolean matchingColumnNoDelimiter(byte[] column) {
    // match both the family and qualifier
    if (matchingFamily(column, 0, this.family.getLength())) {
      return matchingQualifier(column, family.getLength(), column.length - family.getLength());
    }
    return false;
  }

  @Override
  public boolean matchingColumn(byte[] family, byte[] qualifier) {
    return this.matchingFamily(family) && matchingQualifier(qualifier);
  }

  @Override
  public boolean nonNullRowAndColumn() {
    return (this.row != null && row.getLength() > 0) && !isEmptyColumn();
  }

  @Override
  public boolean isEmptyColumn() {
    return this.qualifier != null && this.qualifier.getLength() > 0;
  }


  @Override
  public void write(DataOutput out) throws IOException {
    // we need to simulate the keyvalue writing, but actually step through each buffer.
    //start with keylength
    long longkeylength = KeyValue.KEY_INFRASTRUCTURE_SIZE + row.getLength() + family.getLength()
        + qualifier.getLength();
    if (longkeylength > Integer.MAX_VALUE) {
      throw new IllegalArgumentException("keylength " + longkeylength + " > " + Integer.MAX_VALUE);
    }
    // need to figure out the max length before we start
    int length = this.getLength();
    out.writeInt(length);

    // write the actual data
    int keylength = (int) longkeylength;
    out.writeInt(keylength);
    int vlength = value == null ? 0 : value.getLength();
    out.writeInt(vlength);
    out.writeShort((short) (row.getLength() & 0x0000ffff));
    out.write(this.row.get(), this.row.getOffset(), this.row.getLength());
    out.writeByte((byte) (family.getLength() & 0x0000ff));
    if (family.getLength() != 0) {
      out.write(this.family.get(), this.family.getOffset(), this.family.getLength());
    }
    if (qualifier != NULL) {
      out.write(this.qualifier.get(), this.qualifier.getOffset(), this.qualifier.getLength());
    }
    out.writeLong(ts);
    out.writeByte(this.type.getCode());
    if (this.value != NULL) {
      out.write(this.value.get(), this.value.getOffset(), this.value.getLength());
    }
  }

  @Override
  public int getLength() {
    return KEYVALUE_INFRASTRUCTURE_SIZE + KeyValue.ROW_LENGTH_SIZE + row.getLength()
        + KeyValue.FAMILY_LENGTH_SIZE + family.getLength() + qualifier.getLength()
        + KeyValue.TIMESTAMP_SIZE + KeyValue.TYPE_SIZE + value.getLength();
  }

  @Override
  public String toString() {
    return keyToString() + "/vlen=" + getValueLength() + "/ts=" + getMemstoreTS();
  }

  private String keyToString() {
    String row = Bytes.toStringBinary(this.row.get(), this.row.getOffset(), this.row.getLength());
    String family = this.family.getLength() == 0 ? "" : Bytes.toStringBinary(this.family.get(),
      this.family.getOffset(), this.family.getLength());
    String qualifier = this.qualifier.getLength() == 0 ? "" : Bytes.toStringBinary(
      this.qualifier.get(), this.qualifier.getOffset(), this.qualifier.getLength());
    String timestampStr = Long.toString(ts);
    byte type = this.type.getCode();
    return row + "/" + family + (family != null && family.length() > 0 ? ":" : "") + qualifier
        + "/" + timestampStr + "/" + Type.codeToType(type);
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj) return true;
    if (!super.equals(obj)) return false;
    if (getClass() != obj.getClass()) return false;
    ClientKeyValue other = (ClientKeyValue) obj;
    if (family == null) {
      if (other.family != null) return false;
    } else if (!family.equals(other.family)) return false;
    if (qualifier == null) {
      if (other.qualifier != null) return false;
    } else if (!qualifier.equals(other.qualifier)) return false;
    if (row == null) {
      if (other.row != null) return false;
    } else if (!row.equals(other.row)) return false;
    if (ts != other.ts) return false;
    if (type != other.type) return false;
    if (value == null) {
      if (other.value != null) return false;
    } else if (!value.equals(other.value)) return false;
    return true;
  }

  @Override
  public int hashCode() {
    // TODO do we need to keep the same hashcode logic as KeyValue? Everywhere else we don't keep
    // them by reference, but presumably clients might hash them.
    final int prime = 31;
    int result = super.hashCode();
    result = prime * result + family.hashCode();
    result = prime * result + qualifier.hashCode();
    result = prime * result + row.hashCode();
    result = prime * result + (int) (ts ^ (ts >>> 32));
    result = prime * result + type.hashCode();
    result = prime * result + value.hashCode();
    return result;
  }

  @Override
  public void readFields(int length, DataInput in) throws IOException {
    throw new UnsupportedOperationException(ClientKeyValue.class.getSimpleName()
        + " should not be used for server-side operations");
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    throw new UnsupportedOperationException(ClientKeyValue.class.getSimpleName()
        + " should not be used for server-side operations");
  }

  @Override
  public int getKeyOffset() {
    return 0;
  }


  @Override
  public int getFamilyOffset(int rlength) {
    return 0;
  }

  @Override
  public int getQualifierOffset(int foffset) {
    return 0;
  }

  @Override
  public int getTimestampOffset() {
    return 0;
  }

  @Override
  public int getTimestampOffset(int keylength) {
    return 0;
  }

  @Override
  public int getOffset() {
    return 0;
  }

  @Override
  public boolean updateLatestStamp(byte[] now) {
    if (this.isLatestTimestamp()) {
      // unfortunately, this is a bit slower than the usual kv, but we don't expect this to happen
      // all that often on the client (unless users are updating the ts this way), as it generally
      // happens on the server
      this.ts = Bytes.toLong(now);
      return true;
    }
    return false;
  }

  @Override
  public boolean isLatestTimestamp() {
    return this.ts == HConstants.LATEST_TIMESTAMP;
  }

  @Override
  public int getKeyLength() {
    return KEY_INFRASTRUCTURE_SIZE + getRowLength() + getFamilyLength() + getQualifierLength();
  }

  @Override
  public byte[] getKey() {
    throw new UnsupportedOperationException(ClientKeyValue.class.getSimpleName()
        + " does not support a single backing buffer.");
  }

  @Override
  public String getKeyString() {
    throw new UnsupportedOperationException(ClientKeyValue.class.getSimpleName()
        + " does not support a single backing buffer.");
  }

  @Override
  public SplitKeyValue split() {
    throw new UnsupportedOperationException(ClientKeyValue.class.getSimpleName()
        + " should not be used for server-side operations");
  }

  @Override
  public byte[] getBuffer() {
    throw new UnsupportedOperationException(ClientKeyValue.class.getSimpleName()
        + " does not support a single backing buffer.");
  }

  public ImmutableBytesWritable getRawRow() {
    return this.row;
  }

  public ImmutableBytesWritable getRawFamily() {
    return this.family;
  }

  public ImmutableBytesWritable getRawQualifier() {
    return this.qualifier;
  }

  public ImmutableBytesWritable getRawValue() {
    return this.value;
  }
}