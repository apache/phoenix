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
package org.apache.phoenix.schema;

import org.apache.hadoop.hbase.util.Pair;
import org.apache.phoenix.exception.SQLExceptionCode;
import org.apache.phoenix.exception.SQLExceptionInfo;
import org.apache.phoenix.jdbc.PhoenixConnection;
import org.apache.phoenix.parse.ColumnDef;
import org.apache.phoenix.parse.ColumnName;
import org.apache.phoenix.parse.PrimaryKeyConstraint;
import org.apache.phoenix.query.QueryConstants;
import org.apache.phoenix.schema.types.PVarbinary;
import org.apache.phoenix.util.EnvironmentEdgeManager;
import org.apache.phoenix.util.SchemaUtil;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Types;

import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.ARRAY_SIZE;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.COLUMN_DEF;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.COLUMN_FAMILY;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.COLUMN_NAME;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.COLUMN_QUALIFIER;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.COLUMN_SIZE;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.DATA_TABLE_NAME;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.DATA_TYPE;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.DECIMAL_DIGITS;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.IS_ROW_TIMESTAMP;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.IS_VIEW_REFERENCED;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.KEY_SEQ;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.NULLABLE;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.ORDINAL_POSITION;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.PK_NAME;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.SORT_ORDER;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.SYSTEM_CATALOG_SCHEMA;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.SYSTEM_CATALOG_TABLE;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.TABLE_NAME;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.TABLE_SCHEM;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.TENANT_ID;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.VIEW_CONSTANT;
import static org.apache.phoenix.schema.MetaDataClient.ALTER_SYSCATALOG_TABLE_UPGRADE;

public class ColumnMetaDataOps {
    public static final String UPSERT_COLUMN =
            "UPSERT INTO " + SYSTEM_CATALOG_SCHEMA + ".\"" + SYSTEM_CATALOG_TABLE + "\"( " +
                    TENANT_ID + "," +
                    TABLE_SCHEM + "," +
                    TABLE_NAME + "," +
                    COLUMN_NAME + "," +
                    COLUMN_FAMILY + "," +
                    DATA_TYPE + "," +
                    NULLABLE + "," +
                    COLUMN_SIZE + "," +
                    DECIMAL_DIGITS + "," +
                    ORDINAL_POSITION + "," +
                    SORT_ORDER + "," +
                    DATA_TABLE_NAME + "," + // write this both in the column and table rows for access by metadata APIs
                    ARRAY_SIZE + "," +
                    VIEW_CONSTANT + "," +
                    IS_VIEW_REFERENCED + "," +
                    PK_NAME + "," +  // write this both in the column and table rows for access by metadata APIs
                    KEY_SEQ + "," +
                    COLUMN_DEF + "," +
                    COLUMN_QUALIFIER + ", " +
                    IS_ROW_TIMESTAMP +
                    ") VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";

    public static void addColumnMutation(PhoenixConnection connection, String tenantId, String schemaName, String tableName, PColumn column, String parentTableName, String pkName, Short keySeq, boolean isSalted) throws SQLException {
        addColumnMutationInternal(connection, tenantId, schemaName, tableName, column, parentTableName, pkName, keySeq, isSalted);
    }

    public static void addColumnMutation(PhoenixConnection connection, String schemaName, String tableName, PColumn column, String parentTableName, String pkName, Short keySeq, boolean isSalted) throws SQLException {
        addColumnMutationInternal(connection, connection.getTenantId() == null ? null : connection.getTenantId().getString()
                , schemaName, tableName, column, parentTableName, pkName, keySeq, isSalted);
    }

    private static void addColumnMutationInternal(PhoenixConnection connection, String tenantId, String schemaName, String tableName, PColumn column, String parentTableName, String pkName, Short keySeq, boolean isSalted) throws SQLException {
        String addColumnSqlToUse = connection.isRunningUpgrade()
                && tableName.equals(SYSTEM_CATALOG_TABLE)
                && schemaName.equals(SYSTEM_CATALOG_SCHEMA) ? ALTER_SYSCATALOG_TABLE_UPGRADE
                : UPSERT_COLUMN;

        try (PreparedStatement colUpsert = connection.prepareStatement(addColumnSqlToUse)) {
            colUpsert.setString(1, tenantId);
            colUpsert.setString(2, schemaName);
            colUpsert.setString(3, tableName);
            colUpsert.setString(4, column.getName().getString());
            colUpsert.setString(5, column.getFamilyName() == null ? null : column.getFamilyName().getString());
            colUpsert.setInt(6, column.getDataType().getSqlType());
            colUpsert.setInt(7, SchemaUtil.getIsNullableInt(column.isNullable()));
            if (column.getMaxLength() == null) {
                colUpsert.setNull(8, Types.INTEGER);
            } else {
                colUpsert.setInt(8, column.getMaxLength());
            }
            if (column.getScale() == null) {
                colUpsert.setNull(9, Types.INTEGER);
            } else {
                colUpsert.setInt(9, column.getScale());
            }
            colUpsert.setInt(10, column.getPosition() + (isSalted ? 0 : 1));
            colUpsert.setInt(11, column.getSortOrder().getSystemValue());
            colUpsert.setString(12, parentTableName);
            if (column.getArraySize() == null) {
                colUpsert.setNull(13, Types.INTEGER);
            } else {
                colUpsert.setInt(13, column.getArraySize());
            }
            colUpsert.setBytes(14, column.getViewConstant());
            colUpsert.setBoolean(15, column.isViewReferenced());
            colUpsert.setString(16, pkName);
            if (keySeq == null) {
                colUpsert.setNull(17, Types.SMALLINT);
            } else {
                colUpsert.setShort(17, keySeq);
            }
            if (column.getExpressionStr() == null) {
                colUpsert.setNull(18, Types.VARCHAR);
            } else {
                colUpsert.setString(18, column.getExpressionStr());
            }
            //Do not try to set extra columns when using ALTER_SYSCATALOG_TABLE_UPGRADE
            if (colUpsert.getParameterMetaData().getParameterCount() > 18) {
                if (column.getColumnQualifierBytes() == null) {
                    colUpsert.setNull(19, Types.VARBINARY);
                } else {
                    colUpsert.setBytes(19, column.getColumnQualifierBytes());
                }
                colUpsert.setBoolean(20, column.isRowTimestamp());
            }
            colUpsert.execute();
        }
    }

    public static PColumn newColumn(int position, ColumnDef def, PrimaryKeyConstraint pkConstraint, String defaultColumnFamily,
                                    boolean addingToPK, byte[] columnQualifierBytes, boolean isImmutableRows) throws SQLException {
        try {
            ColumnName columnDefName = def.getColumnDefName();
            SortOrder sortOrder = def.getSortOrder();
            boolean isPK = def.isPK();
            boolean isRowTimestamp = def.isRowTimestamp();
            if (pkConstraint != null) {
                Pair<ColumnName, SortOrder> pkSortOrder = pkConstraint.getColumnWithSortOrder(columnDefName);
                if (pkSortOrder != null) {
                    isPK = true;
                    sortOrder = pkSortOrder.getSecond();
                    isRowTimestamp = pkConstraint.isColumnRowTimestamp(columnDefName);
                }
            }
            String columnName = columnDefName.getColumnName();
            if (isPK && sortOrder == SortOrder.DESC && def.getDataType() == PVarbinary.INSTANCE) {
                throw new SQLExceptionInfo.Builder(SQLExceptionCode.DESC_VARBINARY_NOT_SUPPORTED)
                        .setColumnName(columnName)
                        .build().buildException();
            }

            PName familyName = null;
            if (def.isPK() && !pkConstraint.getColumnNames().isEmpty() ) {
                throw new SQLExceptionInfo.Builder(SQLExceptionCode.PRIMARY_KEY_ALREADY_EXISTS)
                        .setColumnName(columnName).build().buildException();
            }
            boolean isNull = def.isNull();
            if (def.getColumnDefName().getFamilyName() != null) {
                String family = def.getColumnDefName().getFamilyName();
                if (isPK) {
                    throw new SQLExceptionInfo.Builder(SQLExceptionCode.PRIMARY_KEY_WITH_FAMILY_NAME)
                            .setColumnName(columnName).setFamilyName(family).build().buildException();
                } else if (!def.isNull() && !isImmutableRows) {
                    throw new SQLExceptionInfo.Builder(SQLExceptionCode.KEY_VALUE_NOT_NULL)
                            .setColumnName(columnName).setFamilyName(family).build().buildException();
                }
                familyName = PNameFactory.newName(family);
            } else if (!isPK) {
                familyName = PNameFactory.newName(defaultColumnFamily == null ? QueryConstants.DEFAULT_COLUMN_FAMILY : defaultColumnFamily);
            }

            if (isPK && !addingToPK && pkConstraint.getColumnNames().size() <= 1) {
                if (def.isNull() && def.isNullSet()) {
                    throw new SQLExceptionInfo.Builder(SQLExceptionCode.SINGLE_PK_MAY_NOT_BE_NULL)
                            .setColumnName(columnName).build().buildException();
                }
                isNull = false;
            }
            PColumn column = new PColumnImpl(PNameFactory.newName(columnName), familyName, def.getDataType(),
                    def.getMaxLength(), def.getScale(), isNull, position, sortOrder, def.getArraySize(),
                    null, false, def.getExpression(), isRowTimestamp,
                    false, columnQualifierBytes, EnvironmentEdgeManager.currentTimeMillis());
            return column;
        } catch (IllegalArgumentException e) { // Based on precondition check in constructor
            throw new SQLException(e);
        }
    }
}
