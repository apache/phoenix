/*
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
package org.apache.phoenix.schema.types;

import java.sql.SQLException;
import org.apache.phoenix.exception.SQLExceptionCode;
import org.apache.phoenix.exception.SQLExceptionInfo;
import org.apache.phoenix.schema.IllegalDataException;

/**
 * Phoenix data type for protobuf-encoded payloads (the {@code PROTOBUF<my.org.Message>} SQL type).
 */
public final class PProtobuf extends PVarbinary {

  public static final PProtobuf INSTANCE = new PProtobuf();

  private PProtobuf() {
    super("PROTOBUF", PDataType.PROTOBUF_TYPE, byte[].class, null, 51);
  }

  @Override
  public boolean canBePrimaryKey() {
    return false;
  }

  @Override
  public boolean isComparisonSupported() {
    return false;
  }

  @Override
  public boolean isCoercibleTo(@SuppressWarnings("rawtypes") PDataType targetType) {
    return equalsAny(targetType, this, PBinary.INSTANCE, PVarbinary.INSTANCE);
  }

  @Override
  public boolean isBytesComparableWith(@SuppressWarnings("rawtypes") PDataType otherType) {
    return otherType == PVarbinary.INSTANCE || otherType == PProtobuf.INSTANCE;
  }

  @Override
  public int toBytes(Object object, byte[] bytes, int offset) {
    if (object == null) {
      return 0;
    }
    byte[] o = (byte[]) object;
    System.arraycopy(o, 0, bytes, offset, o.length);
    return o.length;
  }

  @Override
  public Object toObject(String value) {
    if (value == null || value.length() == 0) {
      return null;
    }
    SQLException cause =
      new SQLExceptionInfo.Builder(SQLExceptionCode.PROTOBUF_FROM_STRING_NOT_ALLOWED).build()
        .buildException();
    throw new IllegalDataException(cause);
  }
}
