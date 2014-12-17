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
package org.apache.phoenix.schema.types;

import com.google.common.base.Preconditions;
import com.google.common.primitives.Booleans;
import org.apache.phoenix.schema.SortOrder;

import java.sql.Types;

public class PBoolean extends PDataType<Boolean> {

  public static final PBoolean INSTANCE = new PBoolean();

  private PBoolean() {
    super("BOOLEAN", Types.BOOLEAN, Boolean.class, null, 21);
  }

  @Override
  public byte[] toBytes(Object object) {
    if (object == null) {
      // TODO: review - return null?
      throw newIllegalDataException(this + " may not be null");
    }
    return ((Boolean) object).booleanValue() ? TRUE_BYTES : FALSE_BYTES;
  }

  @Override
  public int toBytes(Object object, byte[] bytes, int offset) {
    if (object == null) {
      // TODO: review - return null?
      throw newIllegalDataException(this + " may not be null");
    }
    bytes[offset] = ((Boolean) object).booleanValue() ? TRUE_BYTE : FALSE_BYTE;
    return BOOLEAN_LENGTH;
  }

  @Override
  public byte[] toBytes(Object object, SortOrder sortOrder) {
    if (object == null) {
      // TODO: review - return null?
      throw newIllegalDataException(this + " may not be null");
    }
    return ((Boolean) object).booleanValue() ^ sortOrder == SortOrder.ASC ?
        TRUE_BYTES :
        FALSE_BYTES;
  }

  @Override
  public Boolean toObject(byte[] bytes, int offset, int length, PDataType actualType,
      SortOrder sortOrder, Integer maxLength, Integer scale) {
    Preconditions.checkNotNull(sortOrder);
    if (length == 0) {
      return null;
    }
    if (actualType == this) {
      if (length > 1) {
        throw newIllegalDataException("BOOLEAN may only be a single byte");
      }
      return ((bytes[offset] == FALSE_BYTE ^ sortOrder == SortOrder.DESC) ?
          Boolean.FALSE :
          Boolean.TRUE);
    } else if (actualType == PDecimal.INSTANCE) {
      // false translated to the ZERO_BYTE
      return ((bytes[offset] == ZERO_BYTE ^ sortOrder == SortOrder.DESC) ?
          Boolean.FALSE :
          Boolean.TRUE);
    }
    throwConstraintViolationException(actualType, this);
    return null;
  }

  @Override
  public boolean isCoercibleTo(PDataType targetType) {
    return super.isCoercibleTo(targetType) || targetType.equals(PBinary.INSTANCE);
  }

  @Override
  public boolean isCastableTo(PDataType targetType) {
    // Allow cast to BOOLEAN so it can be used in an index or group by
    return super.isCastableTo(targetType) || targetType.equals(PDecimal.INSTANCE);
  }

  @Override
  public boolean isFixedWidth() {
    return true;
  }

  @Override
  public Integer getByteSize() {
    return BOOLEAN_LENGTH;
  }

  @Override
  public int estimateByteSize(Object o) {
    return BOOLEAN_LENGTH;
  }

  @Override
  public int compareTo(Object lhs, Object rhs, PDataType rhsType) {
    return Booleans.compare((Boolean) lhs, (Boolean) rhs);
  }

  @Override
  public Object toObject(String value) {
    return Boolean.parseBoolean(value);
  }

  @Override
  public Object toObject(Object object, PDataType actualType) {
    if (actualType == this || object == null) {
      return object;
    }
    if (actualType == PVarbinary.INSTANCE || actualType == PBinary.INSTANCE) {
      byte[] bytes = (byte[]) object;
      return toObject(bytes, 0, bytes.length);
    }
    return throwConstraintViolationException(actualType, this);
  }

  @Override
  public Object getSampleValue(Integer maxLength, Integer arrayLength) {
    return RANDOM.get().nextBoolean();
  }
}
