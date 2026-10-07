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
package org.apache.phoenix.schema;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import org.apache.hadoop.hbase.HConstants;
import org.apache.phoenix.parse.ColumnDef;
import org.apache.phoenix.parse.ColumnName;
import org.apache.phoenix.parse.ParseNodeFactory;
import org.apache.phoenix.parse.PrimaryKeyConstraint;
import org.apache.phoenix.query.QueryConstants;
import org.apache.phoenix.schema.types.PDataType;
import org.apache.phoenix.schema.types.PProtobuf;
import org.apache.phoenix.schema.types.PVarchar;
import org.junit.Test;

public class PColumnImplProtobufTest {
  private static final ParseNodeFactory FACTORY = new ParseNodeFactory();

  private static PName name(String s) {
    return PNameFactory.newName(s);
  }

  @Test
  public void protoMessageNameDefaultsToNullOnLegacyConstructors() {
    PColumn a = new PColumnImpl(name("c1"), name(QueryConstants.DEFAULT_COLUMN_FAMILY),
      PVarchar.INSTANCE, null, null, true, 0, SortOrder.ASC, null, null, false, null, false, false,
      null, HConstants.LATEST_TIMESTAMP);
    assertNull(a.getProtoMessageName());

    PColumn b = new PColumnImpl(name("c1"), name(QueryConstants.DEFAULT_COLUMN_FAMILY),
      PVarchar.INSTANCE, null, null, true, 0, SortOrder.ASC, null, null, false, null, false, false,
      null, HConstants.LATEST_TIMESTAMP, false);
    assertNull(b.getProtoMessageName());
  }

  @Test
  public void pColumnInterfaceDefaultsProtoMessageNameToNull() {
    PColumn stub = new PColumn() {
      @Override
      public PName getName() {
        return name("stub");
      }

      @Override
      public PName getFamilyName() {
        return null;
      }

      @Override
      public int getPosition() {
        return 0;
      }

      @Override
      public Integer getArraySize() {
        return null;
      }

      @Override
      public byte[] getViewConstant() {
        return null;
      }

      @Override
      public boolean isViewReferenced() {
        return false;
      }

      @Override
      public int getEstimatedSize() {
        return 0;
      }

      @Override
      public String getExpressionStr() {
        return null;
      }

      @Override
      public long getTimestamp() {
        return HConstants.LATEST_TIMESTAMP;
      }

      @Override
      public boolean isDerived() {
        return false;
      }

      @Override
      public boolean isExcluded() {
        return false;
      }

      @Override
      public boolean isRowTimestamp() {
        return false;
      }

      @Override
      public boolean isDynamic() {
        return false;
      }

      @Override
      public byte[] getColumnQualifierBytes() {
        return null;
      }

      @Override
      public PDataType getDataType() {
        return PVarchar.INSTANCE;
      }

      @Override
      public Integer getMaxLength() {
        return null;
      }

      @Override
      public Integer getScale() {
        return null;
      }

      @Override
      public boolean isNullable() {
        return true;
      }

      @Override
      public SortOrder getSortOrder() {
        return SortOrder.ASC;
      }
    };
    assertNull("PColumn interface default should be null", stub.getProtoMessageName());
  }

  @Test
  public void equalsAndHashCodeIgnoreProtoMessageName() {
    PColumn withProto = new PColumnImpl(name("payload"), name(QueryConstants.DEFAULT_COLUMN_FAMILY),
      PProtobuf.INSTANCE, null, null, true, 0, SortOrder.ASC, null, null, false, null, false, false,
      null, HConstants.LATEST_TIMESTAMP, false, "my.org.Order");
    PColumn withDifferentProto =
      new PColumnImpl(name("payload"), name(QueryConstants.DEFAULT_COLUMN_FAMILY),
        PProtobuf.INSTANCE, null, null, true, 0, SortOrder.ASC, null, null, false, null, false,
        false, null, HConstants.LATEST_TIMESTAMP, false, "some.other.Message");
    PColumn withNullProto =
      new PColumnImpl(name("payload"), name(QueryConstants.DEFAULT_COLUMN_FAMILY),
        PProtobuf.INSTANCE, null, null, true, 0, SortOrder.ASC, null, null, false, null, false,
        false, null, HConstants.LATEST_TIMESTAMP, false, null);
    assertEquals(withProto, withDifferentProto);
    assertEquals(withProto, withNullProto);
    assertEquals(withProto.hashCode(), withDifferentProto.hashCode());
    assertEquals(withProto.hashCode(), withNullProto.hashCode());
  }

  @Test
  public void copyConstructorPreservesProtoMessageName() {
    PColumn original = new PColumnImpl(name("payload"), name(QueryConstants.DEFAULT_COLUMN_FAMILY),
      PProtobuf.INSTANCE, null, null, true, 0, SortOrder.ASC, null, null, false, null, false, false,
      null, HConstants.LATEST_TIMESTAMP, false, "my.org.Order");

    PColumn copy = new PColumnImpl(original, 3);
    assertEquals("my.org.Order", copy.getProtoMessageName());
    assertEquals(3, copy.getPosition());

    PColumn viewCopy = new PColumnImpl(original, null, false);
    assertEquals("my.org.Order", viewCopy.getProtoMessageName());
  }

  @Test
  public void newColumnLeavesProtoMessageNameNullForVarchar() throws Exception {
    ColumnDef def = FACTORY.columnDef(ColumnName.caseSensitiveColumnName("name"),
      PVarchar.INSTANCE.getSqlTypeName(), true, 64, null, false, SortOrder.getDefault(), null,
      false);
    PColumn column = ColumnMetaDataOps.newColumn(2, def, PrimaryKeyConstraint.EMPTY,
      QueryConstants.DEFAULT_COLUMN_FAMILY, false, null, false);
    assertNotNull(column);
    assertNull("Non-PROTOBUF column must not carry a message name", column.getProtoMessageName());

    assertEquals(PVarchar.INSTANCE, column.getDataType());
    assertTrue(column.getMaxLength() != null && column.getMaxLength() == 64);
  }
}
