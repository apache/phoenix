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

package org.apache.phoenix.end2end;

import org.apache.commons.lang3.RandomStringUtils;
import org.apache.hadoop.hbase.CompareOperator;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.KeyValueUtil;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.filter.BinaryComparator;
import org.apache.hadoop.hbase.filter.BinaryComponentComparator;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.filter.PrefixFilter;
import org.apache.hadoop.hbase.filter.RowFilter;
import org.apache.hadoop.hbase.filter.SubstringComparator;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Pair;
import org.apache.phoenix.compile.ColumnResolver;
import org.apache.phoenix.compile.CreateTableCompiler;
import org.apache.phoenix.compile.ExpressionCompiler;
import org.apache.phoenix.compile.FromCompiler;
import org.apache.phoenix.compile.StatementContext;
import org.apache.phoenix.compile.WhereOptimizer;
import org.apache.phoenix.coprocessorclient.TableInfo;
import org.apache.phoenix.expression.Expression;
import org.apache.phoenix.expression.RowKeyColumnExpression;
import org.apache.phoenix.jdbc.PhoenixConnection;
import org.apache.phoenix.jdbc.PhoenixPreparedStatement;
import org.apache.phoenix.parse.ParseNode;
import org.apache.phoenix.parse.SQLParser;
import org.apache.phoenix.parse.SelectStatement;
import org.apache.phoenix.parse.TableName;
import org.apache.phoenix.query.BaseTest;
import org.apache.phoenix.query.ConnectionQueryServices;
import org.apache.phoenix.schema.PColumn;
import org.apache.phoenix.schema.PName;
import org.apache.phoenix.schema.PTable;
import org.apache.phoenix.schema.RowKeyValueAccessor;
import org.apache.phoenix.schema.SortOrder;
import org.apache.phoenix.schema.tuple.ResultTuple;
import org.apache.phoenix.schema.types.PChar;
import org.apache.phoenix.schema.types.PDataType;
import org.apache.phoenix.schema.types.PDate;
import org.apache.phoenix.schema.types.PDecimal;
import org.apache.phoenix.schema.types.PInteger;
import org.apache.phoenix.schema.types.PLong;
import org.apache.phoenix.schema.types.PSmallint;
import org.apache.phoenix.schema.types.PTimestamp;
import org.apache.phoenix.schema.types.PTinyint;
import org.apache.phoenix.schema.types.PVarchar;
import org.apache.phoenix.thirdparty.com.google.common.collect.Maps;
import org.apache.phoenix.util.ByteUtil;
import org.apache.phoenix.util.PhoenixRuntime;
import org.apache.phoenix.util.PropertiesUtil;
import org.apache.phoenix.util.ScanUtil;
import org.apache.phoenix.util.SchemaUtil;
import org.apache.phoenix.util.ViewUtil;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.Date;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;
import java.sql.Types;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.BitSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Random;
import java.util.stream.Collectors;

import static org.apache.phoenix.exception.SQLExceptionCode.VIEW_CANNOT_EXTEND_PK_WITH_PARENT_INDEXES;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.SYSTEM_LINK_HBASE_TABLE_NAME;
import static org.apache.phoenix.util.ByteUtil.EMPTY_BYTE_ARRAY;
import static org.apache.phoenix.util.PhoenixRuntime.TENANT_ID_ATTRIB;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public abstract class BaseRowKeyMatcherTestIT extends ParallelStatsDisabledIT {

    private static final Logger LOGGER = LoggerFactory.getLogger(BaseRowKeyMatcherTestIT.class);

    public static final String TENANT_URL_FMT = "%s;%s=%s";
    public static final String ORG_ID_PREFIX = "00D0x0000";
    public static final String ORG_ID_FMT = "%s%06d";
    public static final String PARTITION_FMT = "%03d";
    public static final String BASE_TABLE_NAME_FMT = "TEST_ENTITY.%s";
    public static final String INDEX_TABLE_NAME_FMT = "_IDX_TEST_ENTITY.%s";

    public static final String GLOBAL_VIEW_NAME_FMT = "TEST_ENTITY.G1_%s";
    public static final String TENANT_VIEW_NAME_FMT = "TEST_ENTITY.TV_%s_%d";
    public static final String LEAF_VIEW_NAME_FMT = "TEST_ENTITY.Z%02d_%02d_%s";
    public static final String ROW_ID_FMT = "00R0x000%07d";
    public static final String ZID_FMT = "00Z0x000%07d";
    public static final String COL1_FMT = "a%05d";
    public static final String COL2_FMT = "b%05d";
    public static final String COL3_FMT = "b%05d";
    public static final Random RANDOM_GEN = new Random();
    public static final int MAX_ROWS = 10000;

    // Returns the String form of the data type
    private String getType(PDataType pkType) {
        String pkTypeStr = "VARCHAR(25)";
        switch (pkType.getSqlType()) {
        case Types.VARCHAR:
            pkTypeStr = "VARCHAR(25)";
            break;
        case Types.CHAR:
            pkTypeStr = "CHAR(15)";
            break;
        case Types.DECIMAL:
            pkTypeStr = "DECIMAL(8,2)";
            break;
        case Types.INTEGER:
            pkTypeStr = "INTEGER";
            break;
        case Types.BIGINT:
            pkTypeStr = "BIGINT";
            break;
        case Types.DATE:
            pkTypeStr = "DATE";
            break;
        case Types.TIMESTAMP:
            pkTypeStr = "TIMESTAMP";
            break;
        default:
            pkTypeStr = "VARCHAR(25)";
        }
        return pkTypeStr;
    }

    // Returns random data for given data type
    private Object getData(PDataType type) {
        Random rnd = new Random();
        switch (type.getSqlType()) {
        case Types.VARCHAR:
            return RandomStringUtils.randomAlphanumeric(25);
        case Types.CHAR:
            //pkTypeStr = "CHAR(15)";
            return RandomStringUtils.randomAlphanumeric(15);
        case Types.DECIMAL:
            //pkTypeStr = "DECIMAL(8,2)";
            return Math.floor(rnd.nextInt(50000) * rnd.nextDouble());
        case Types.INTEGER:
            //pkTypeStr = "INTEGER";
            return rnd.nextInt(50000);
        case Types.BIGINT:
            //pkTypeStr = "BIGINT";
            return rnd.nextLong();
        case Types.DATE:
            //pkTypeStr = "DATE";
            return new Date(System.currentTimeMillis() + rnd.nextInt(50000));
        case Types.TIMESTAMP:
            //pkTypeStr = "TIMESTAMP";
            return new Timestamp(System.currentTimeMillis() + rnd.nextInt(50000));
        default:
            // pkTypeStr = "VARCHAR(25)";
            return RandomStringUtils.randomAlphanumeric(25);
        }
    }

    // Returns random data for given data type
    private Object getPKData(String testPKTypes) {
        Random rnd = new Random();
        switch (testPKTypes) {
        case "VARCHAR": {
            return RandomStringUtils.randomAlphanumeric(25);
        }
        case "CHAR(15)": {
            //pkTypeStr = "CHAR(15)";
            return RandomStringUtils.randomAlphanumeric(15);
        }
        case "CHAR(3)": {
            //pkTypeStr = "CHAR(3)";
            return RandomStringUtils.randomAlphanumeric(3);
        }
        case "DECIMAL":
            //pkTypeStr = "DECIMAL(8,2)";
            return Math.floor(rnd.nextInt(50000) * rnd.nextDouble());
        case "INTEGER":
            //pkTypeStr = "INTEGER";
            return rnd.nextInt(50000);
        case "BIGINT":
            //pkTypeStr = "BIGINT";
            return rnd.nextInt(50000);
        case "DATE":
            //pkTypeStr = "DATE";
            return new Date(System.currentTimeMillis() + rnd.nextInt(50000));
        case "TIMESTAMP":
            //pkTypeStr = "TIMESTAMP";
            return new Timestamp(System.currentTimeMillis() + rnd.nextInt(50000));
        default:
            // pkTypeStr = "VARCHAR(25)";
            return RandomStringUtils.randomAlphanumeric(25);

        }
    }

    // Helper method to create a base table
    private void createBaseTable(String tableName, boolean isMultiTenant, PDataType tenantDataType)
            throws SQLException {
        String baseTableName = String.format(BASE_TABLE_NAME_FMT, tableName);

        try (Connection globalConnection = DriverManager.getConnection(getUrl())) {
            try (Statement cstmt = globalConnection.createStatement()) {
                String
                        CO_BASE_TBL_TEMPLATE =
                        "CREATE TABLE IF NOT EXISTS %s(" +
                                "OID %s NOT NULL,KP CHAR(3) NOT NULL, " +
                                "COL1 VARCHAR,CREATED_DATE DATE,CREATED_BY CHAR(15)," +
                                "LAST_UPDATE DATE,LAST_UPDATE_BY CHAR(15),SYSTEM_MODSTAMP DATE " +
                                "CONSTRAINT pk PRIMARY KEY (OID,KP)) COLUMN_ENCODED_BYTES=0 %s";
                cstmt.execute(String.format(CO_BASE_TBL_TEMPLATE, baseTableName,
                        getType(tenantDataType),
                        isMultiTenant ? ", MULTI_TENANT=true" : ""));
            }
        }
    }

    // Helper method to create a global view
    // Return a pair [view-key, row-key-matcher for the view]
    private Pair<String, byte[]> createGlobalView(String tableName, int partition,
            PDataType[] pkTypes, SortOrder[] pkOrders, boolean hasGlobalViewIndexes)
            throws SQLException {

        String pkType1Str = getType(pkTypes[0]);
        String pkType2Str = getType(pkTypes[1]);
        String pkType3Str = getType(pkTypes[2]);

        String baseTableName = String.format(BASE_TABLE_NAME_FMT, tableName);
        String partitionName = String.format(PARTITION_FMT, partition);
        String globalViewName = String.format(GLOBAL_VIEW_NAME_FMT, partitionName);
        try (PhoenixConnection globalConnection = DriverManager.getConnection(getUrl())
                .unwrap(PhoenixConnection.class)) {
            try (Statement cstmt = globalConnection.createStatement()) {
                String
                        VIEW_TEMPLATE =
                        "CREATE VIEW IF NOT EXISTS %s(" +
                                "ID1 %s not null,ID2 %s not null,ID3 %s not null, " +
                                "ROW_ID CHAR(15) not null, COL2 VARCHAR " +
                                "CONSTRAINT pk PRIMARY KEY (ID1 %s, ID2 %s, ID3 %s, ROW_ID)) " +
                                "AS SELECT * FROM %s WHERE KP = '%s'";

                cstmt.execute(String.format(VIEW_TEMPLATE, globalViewName, pkType1Str, pkType2Str,
                        pkType3Str, pkOrders[0].name(), pkOrders[1].name(), pkOrders[2].name(),
                        baseTableName, partitionName));
                if (hasGlobalViewIndexes) {
                    String indexNamePrefix = String.format("G%s", partition);
                    String
                            GLOBAL_INDEX_TEMPLATE =
                            "CREATE INDEX IF NOT EXISTS %s_COL2_INDEX ON %s (COL2) " +
                                    "INCLUDE(SYSTEM_MODSTAMP)";
                    cstmt.execute(
                            String.format(GLOBAL_INDEX_TEMPLATE, indexNamePrefix, globalViewName));
                }

                return getRowKeyMatchersFromView(globalConnection.unwrap(PhoenixConnection.class),
                        globalViewName);
            }
        }
    }

    // Helper method to create a tenant view
    // Return a pair [view-key, row-key-matcher for the view]

    private Pair<String, byte[]> createTenantView(boolean extendPK, int partition,
            PDataType tenantIdType, int tenant,
            int tenantViewNum, String[] pkNames, PDataType[] pkTypes) throws SQLException {

        String partitionName = String.format(PARTITION_FMT, partition);
        String globalViewName = String.format(GLOBAL_VIEW_NAME_FMT, partitionName);

        String tenantId = "";
        if (tenantIdType.getSqlType() == Types.VARCHAR || tenantIdType.getSqlType() == Types.CHAR) {
            tenantId = String.format(ORG_ID_FMT, ORG_ID_PREFIX, tenant);
        } else {
            tenantId = String.format("%015d", tenant);
        }
        String
                tenantConnectionUrl =
                String.format(TENANT_URL_FMT, getUrl(), TENANT_ID_ATTRIB, tenantId);
        String tenantViewName = String.format(TENANT_VIEW_NAME_FMT, partitionName, tenantViewNum);
        String tenantViewOptions = "";
        try (Connection tenantConnection = DriverManager.getConnection(tenantConnectionUrl)) {
            tenantConnection.setAutoCommit(true);
            try (Statement cstmt = tenantConnection.createStatement()) {
                String
                        VIEW_WITH_PK_TEMPLATE =
                        "CREATE VIEW IF NOT EXISTS %s(ZID CHAR(15) NOT NULL,COL3 VARCHAR " +
                                "CONSTRAINT pk PRIMARY KEY (ZID)) " + "AS SELECT * FROM %s %s %s";
                String VIEW_WO_PK_TEMPLATE = "CREATE VIEW IF NOT EXISTS %s AS SELECT * from %s %s";
                if (extendPK) {
                    cstmt.execute(
                            String.format(VIEW_WITH_PK_TEMPLATE, tenantViewName, globalViewName,
                                    getWhereClause(pkNames, pkTypes), tenantViewOptions));
                } else {
                    cstmt.execute(String.format(VIEW_WO_PK_TEMPLATE, tenantViewName, globalViewName,
                            tenantViewOptions));
                }
                return getRowKeyMatchersFromView(tenantConnection.unwrap(PhoenixConnection.class),
                        tenantViewName);
            }
        }
    }

    // Helper method to create rows for a given tenant view
    private void upsertTenantViewRows(boolean isMultiTenant, boolean extendPK, int partition,
            PDataType tenantIdType, int tenant, int tenantViewNum, int rowIndex,
            String[] pkNames, PDataType[] pkTypes)
            throws SQLException {

        String rid = String.format(ROW_ID_FMT, rowIndex);
        String zid = String.format(ZID_FMT, rowIndex);
        String col1 = String.format(COL1_FMT, rowIndex, RANDOM_GEN.nextInt(MAX_ROWS));
        String col2 = String.format(COL2_FMT, rowIndex, RANDOM_GEN.nextInt(MAX_ROWS));
        String col3 = String.format(COL3_FMT, rowIndex, RANDOM_GEN.nextInt(MAX_ROWS));
        Object pk1 = null;
        Object pk2 = null;
        Object pk3 = null;

        String partitionName = String.format(PARTITION_FMT, partition);

        String tenantId = "";
        if (tenantIdType.getSqlType() == Types.VARCHAR || tenantIdType.getSqlType() == Types.CHAR) {
            tenantId = String.format(ORG_ID_FMT, ORG_ID_PREFIX, tenant);
        } else {
            tenantId = String.format("%015d", tenant);
        }
        String
                tenantConnectionUrl =
                String.format(TENANT_URL_FMT, getUrl(), TENANT_ID_ATTRIB, tenantId);
        String tenantViewName = String.format(TENANT_VIEW_NAME_FMT, partitionName, tenantViewNum);
        try (PhoenixConnection tenantConnection = DriverManager.getConnection(tenantConnectionUrl)
                .unwrap(PhoenixConnection.class)) {
            tenantConnection.setAutoCommit(true);
            String
                    TENANT_VIEW_WITH_PK =
                    String.format(
                            "UPSERT INTO %s(ROW_ID, ZID, COL1, COL2, COL3, SYSTEM_MODSTAMP) " +
                                    "VALUES(?, ?, ?, ?, ?, ?)",
                            tenantViewName);
            String
                    TENANT_VIEW_WO_PK =
                    String.format(
                            "UPSERT INTO %s(ID1, ID2, ID3, ROW_ID, COL1, COL2, SYSTEM_MODSTAMP) " +
                                    "VALUES(?, ?, ?, ?, ?, ?, ?)",
                            tenantViewName);
            String
                    NON_MULTI_TENANT_VIEW_WO_PK =
                    String.format(
                            "UPSERT INTO %s(OID, ID1, ID2, ID3, ROW_ID, COL1, COL2, " +
                                    "SYSTEM_MODSTAMP) " +
                                    "VALUES(?, ?, ?, ?, ?, ?, ?, ?)",
                            tenantViewName);
            String
                    NON_MULTI_TENANT_VIEW_WITH_PK =
                    String.format(
                            "UPSERT INTO %s(OID, ROW_ID, ZID, COL1, COL2, COL3, SYSTEM_MODSTAMP) " +
                                    "VALUES(?, ?, ?, ?, ?, ?, ?)",
                            tenantViewName);

            if (isMultiTenant) {
                if (extendPK) {
                    // Case: MultiTenant and ExtendedPK
                    try (PreparedStatement pstmt = tenantConnection.prepareStatement(
                            TENANT_VIEW_WITH_PK)) {
                        pstmt.setObject(1, rid);
                        pstmt.setObject(2, zid);
                        pstmt.setObject(3, col1);
                        pstmt.setObject(4, col2);
                        pstmt.setObject(5, col3);
                        pstmt.setObject(6, new Date(System.currentTimeMillis()));
                        pstmt.execute();
                    }
                } else {
                    // Case: MultiTenant and Non ExtendedPK
                    pk1 = getData(pkTypes[0]);
                    pk2 = getData(pkTypes[1]);
                    pk3 = getData(pkTypes[2]);
                    try (PreparedStatement pstmt = tenantConnection.prepareStatement(
                            TENANT_VIEW_WO_PK)) {
                        pstmt.setObject(1, pk1);
                        pstmt.setObject(2, pk2);
                        pstmt.setObject(3, pk3);
                        pstmt.setObject(4, rid);
                        pstmt.setObject(5, col1);
                        pstmt.setObject(6, col2);
                        pstmt.setObject(7, new Date(System.currentTimeMillis()));
                        pstmt.execute();
                    }
                }

            } else {
                if (extendPK) {
                    // Case: Non MultiTenant and ExtendedPK
                    try (PreparedStatement pstmt = tenantConnection.prepareStatement(
                            NON_MULTI_TENANT_VIEW_WITH_PK)) {
                        pstmt.setObject(1, tenantId);
                        pstmt.setObject(2, rid);
                        pstmt.setObject(3, zid);
                        pstmt.setObject(4, col1);
                        pstmt.setObject(5, col2);
                        pstmt.setObject(6, col3);
                        pstmt.setObject(7, new Date(System.currentTimeMillis()));
                        pstmt.execute();
                    }
                } else {
                    // Case: Non MultiTenant and Non ExtendedPK
                    pk1 = getData(pkTypes[0]);
                    pk2 = getData(pkTypes[1]);
                    pk3 = getData(pkTypes[2]);
                    try (PreparedStatement pstmt = tenantConnection.prepareStatement(
                            NON_MULTI_TENANT_VIEW_WO_PK)) {
                        pstmt.setObject(1, tenantId);
                        pstmt.setObject(2, pk1);
                        pstmt.setObject(3, pk2);
                        pstmt.setObject(4, pk3);
                        pstmt.setObject(5, rid);
                        pstmt.setObject(6, col1);
                        pstmt.setObject(7, col2);
                        pstmt.setObject(8, new Date(System.currentTimeMillis()));
                        pstmt.execute();
                    }
                }

            }

        } finally {
            LOGGER.debug(String.format(
                    "Upsert values " +
                            "tenantId = %s, pk1 = %s, pk2 = %s, pk3 = %s, " +
                            "rid = %s, col1 = %s, col2 = %s",
                    tenantId, pk1, pk2, pk3, rid, col1, col2));
        }
    }

    // Helper to get rowKeyMatcher from Metadata.
    private Pair<String, byte[]> getRowKeyMatchersFromView(PhoenixConnection connection,
            String viewName) throws SQLException {

        PName tenantId = connection.getTenantId();
        PTable view = PhoenixRuntime.getTable(connection, viewName);
        String tenantViewKey = String.format("%s.%s", tenantId, viewName);
        byte[] rowKeyMatcher = view.getRowKeyMatcher();
        return new Pair(tenantViewKey, rowKeyMatcher);

    }

    // Helper to get rowKeyMatcher from Metadata.
    private Pair<String, byte[]> getRowKeyMatchersFromView(PhoenixConnection connection,
            PTable view) throws SQLException {
        return getRowKeyMatchersFromView(connection, view.getName().getString());
    }

    // Helper to assert that row key matcher generated by the 2 WhereOptimizer methods are the same
    private byte[] assertRowKeyMatcherForView(PhoenixConnection connection, PTable view,
            Pair<String, byte[]> rowKeyInfo) throws SQLException {

        String viewStatement = view.getViewStatement();
        SelectStatement viewSelectStatement = new SQLParser(viewStatement).parseQuery();

        PhoenixPreparedStatement
                preparedViewStatement =
                connection.prepareStatement(viewStatement).unwrap(PhoenixPreparedStatement.class);

        ColumnResolver resolver = FromCompiler.getResolverForQuery(viewSelectStatement, connection);
        StatementContext
                viewStatementContext =
                new StatementContext(preparedViewStatement, resolver);

        PTable viewStatementTable = viewStatementContext.getCurrentTable().getTable();

        int nColumns = viewStatementTable.getColumns().size();
        BitSet isViewColumnReferencedToBe = new BitSet(nColumns);
        // Used to track column references in a view
        ExpressionCompiler
                expressionCompiler =
                new CreateTableCompiler.ColumnTrackingExpressionCompiler(viewStatementContext,
                        isViewColumnReferencedToBe);
        ParseNode whereNode = viewSelectStatement.getWhere();

        Expression whereExpression = whereNode.accept(expressionCompiler);

        byte[][] viewColumnConstantsToBe = new byte[nColumns][];
        CreateTableCompiler.ViewWhereExpressionVisitor
                visitor =
                new CreateTableCompiler.ViewWhereExpressionVisitor(viewStatementTable,
                        viewColumnConstantsToBe);
        whereExpression.accept(visitor);

        TableName
                tableName =
                TableName.createNormalized(view.getSchemaName().getString(),
                        view.getTableName().getString());
        byte[]
                rowKeyMatcher1 =
                WhereOptimizer.getRowKeyMatcher(viewStatementContext, tableName, viewStatementTable,
                        whereExpression);
        byte[]
                rowKeyMatcher2 =
                WhereOptimizer.getRowKeyMatcher(connection, tableName, viewStatementTable,
                        viewColumnConstantsToBe, isViewColumnReferencedToBe);
        LOGGER.debug(String.format(
                "target-view-name = %s, physical = %s, stmt-table = %s\n, " +
                        "row-matcher-0 = %s (syscat)\n, row-matcher-1 = %s\n, row-matcher-2 = %s\n",
                view.getName().getString(), viewStatementTable.getPhysicalName().getString(),
                viewStatementTable.getName().getString(),
                Bytes.toStringBinary(rowKeyInfo.getSecond()), Bytes.toStringBinary(rowKeyMatcher1),
                Bytes.toStringBinary(rowKeyMatcher2)));
        assertTrue("RowKey matcher pattern do not match",
                Bytes.compareTo(rowKeyInfo.getSecond(), rowKeyMatcher1) == 0);
        assertTrue("RowKey matcher patterns do not match",
                Bytes.compareTo(rowKeyInfo.getSecond(), rowKeyMatcher2) == 0);
        return rowKeyMatcher1;
    }

    // Helper method to return the row-key matcher patterns
    // for the whole view hierarchy under the table.
    private Map<String, byte[]> assertRowKeyMatchersForTable(String url, String parentSchemaName,
            String parentTableName) {
        Map<String, byte[]> viewToRowKeyMap = Maps.newHashMap();
        Properties tenantProps = PropertiesUtil.deepCopy(new Properties());
        try (Connection globalConnection = DriverManager.getConnection(url)) {
            ConnectionQueryServices
                    cqs =
                    globalConnection.unwrap(PhoenixConnection.class).getQueryServices();
            try (Table childLinkTable = cqs.getTable(
                    SchemaUtil.getPhysicalName(SYSTEM_LINK_HBASE_TABLE_NAME.toBytes(),
                            cqs.getProps()).getName())) {
                Pair<List<PTable>, List<TableInfo>>
                        allDescendants =
                        ViewUtil.findAllDescendantViews(childLinkTable, cqs.getConfiguration(),
                                EMPTY_BYTE_ARRAY, parentSchemaName.getBytes(),
                                parentTableName.getBytes(),
                                HConstants.LATEST_TIMESTAMP, false);
                for (PTable view : allDescendants.getFirst()) {
                    PName tenantId = view.getTenantId();
                    String viewName = view.getName().getString();

                    Connection
                            stmtConnection =
                            tenantId == null ?
                                    globalConnection :
                                    DriverManager.getConnection(
                                            String.format("%s;%s=%s", url, TENANT_ID_ATTRIB,
                                                    tenantId.getString()), tenantProps);

                    Pair<String, byte[]>
                            rowKeyInfo =
                            getRowKeyMatchersFromView(
                                    stmtConnection.unwrap(PhoenixConnection.class), view);
                    assertRowKeyMatcherForView(stmtConnection.unwrap(PhoenixConnection.class), view,
                            rowKeyInfo);
                    viewToRowKeyMap.put(rowKeyInfo.getFirst(), rowKeyInfo.getSecond());
                }
            }
            ;
        } catch (SQLException | IOException e) {
            throw new RuntimeException(e);
        }
        return viewToRowKeyMap;

    }

    private String getWhereClause(String[] pkNames, PDataType[] testPKTypes) {

        StringBuilder builder = new StringBuilder("WHERE ");
        Random rnd = new Random();

        for (int b = 0; b < testPKTypes.length; b++) {
            if (b > 0) builder.append(" AND ");
            switch (testPKTypes[b].getSqlType()) {
            case Types.VARCHAR: {
                // pkTypeStr = "VARCHAR(25)";
                builder.append(pkNames[b]).append(" = ").append("'")
                        .append(RandomStringUtils.randomAlphanumeric(25)).append("'");
                break;
            }
            case Types.CHAR: {
                //pkTypeStr = "CHAR(15)";
                builder.append(pkNames[b]).append(" = ").append("'")
                        .append(RandomStringUtils.randomAlphanumeric(15)).append("'");
                break;
            }
            case Types.DECIMAL:
                //pkTypeStr = "DECIMAL(8,2)";
                builder.append(pkNames[b]).append(" = ").append(rnd.nextDouble());
                break;
            case Types.INTEGER:
                //pkTypeStr = "INTEGER";
                builder.append(pkNames[b]).append(" = ").append(rnd.nextInt(500000));
                break;
            case Types.BIGINT:
                //pkTypeStr = "BIGINT";
                builder.append(pkNames[b]).append(" = ").append(rnd.nextLong());
                break;
            case Types.DATE:
                //pkTypeStr = "DATE";
                builder.append(pkNames[b]).append(" = ")
                        .append(" TO_DATE('2022-03-21T15:03:57+00:00') ");
                break;
            case Types.TIMESTAMP:
                //pkTypeStr = "TIMESTAMP";
                builder.append(pkNames[b]).append(" = ")
                        .append(" TO_TIMESTAMP('2019-10-27T16:17:57+00:00') ");
                break;
            default:
                // pkTypeStr = "VARCHAR(25)";
                builder.append(pkNames[b]).append("=").append("'")
                        .append(RandomStringUtils.randomAlphanumeric(15)).append("'");
            }
        }
        return builder.toString();
    }

    // Asserts that the row matched by the rowId and tenantId matches the prefix
    private void assertHBaseRowKeyMatchesPrefix(PhoenixConnection connection, byte[] hbaseTableName,
            int rowId, byte[] prefix) throws IOException, SQLException {

        byte[] rowkey = ByteUtil.EMPTY_BYTE_ARRAY;
        String rid = String.format(ROW_ID_FMT, rowId);

        try (Table tbl = connection.getQueryServices().getTable(hbaseTableName)) {

            PName tenantId = connection.getTenantId();
            Scan allRows = new Scan();
            // Add tenant as the prefix filter
            FilterList andFilter = new FilterList();
            andFilter.addFilter(new RowFilter(CompareOperator.EQUAL, new SubstringComparator(rid)));
            allRows.setFilter(andFilter);
            ResultScanner scanner = tbl.getScanner(allRows);
            int numMatchingRows = 0;
            for (Result result = scanner.next(); result != null; result = scanner.next()) {
                rowkey = result.getRow();
                numMatchingRows++;
            }
            assertEquals(String.format("Expected rows do match for table = %s, rowId = %s",
                    Bytes.toString(hbaseTableName), rowId), 1, numMatchingRows);

            PrefixFilter matchFilter = new PrefixFilter(prefix);
            LOGGER.debug(String.format("row-key = %s, tenantId = %s, prefix = %s, matched = %s",
                    Bytes.toStringBinary(rowkey), tenantId, Bytes.toStringBinary(prefix),
                    !matchFilter.filterRowKey(KeyValueUtil.createFirstOnRow(rowkey))));
        }
    }

    // Asserts that the row matching the tenantId and rowId matches with the viewIndexId
    private void assertIndexTableRowKeyMatchesPrefix(PhoenixConnection connection, PTable viewIndex,
            byte[] hbaseIndexTableName, int rowId) throws IOException, SQLException {

        byte[] rowkey = ByteUtil.EMPTY_BYTE_ARRAY;
        String rid = String.format(ROW_ID_FMT, rowId);

        try (Table tbl = connection.getQueryServices().getTable(hbaseIndexTableName)) {

            Scan allRows = new Scan();
            FilterList andFilter = new FilterList();
            andFilter.addFilter(new RowFilter(CompareOperator.EQUAL, new SubstringComparator(rid)));
            allRows.setFilter(andFilter);
            ResultScanner scanner = tbl.getScanner(allRows);
            int numMatchingRows = 0;
            for (Result result = scanner.next(); result != null; result = scanner.next()) {
                PColumn viewIndexIdPKColumn = viewIndex.getPKColumns().get(0);
                RowKeyColumnExpression
                        viewIndexIdColExpr =
                        new RowKeyColumnExpression(viewIndexIdPKColumn,
                                new RowKeyValueAccessor(viewIndex.getPKColumns(), 0));
                ImmutableBytesWritable ptr = new ImmutableBytesWritable();
                viewIndexIdColExpr.evaluate(new ResultTuple(result), ptr);
                long actualViewIndexID;
                if (hasLongViewIndexEnabled()) {
                    actualViewIndexID = PLong.INSTANCE.getCodec().decodeLong(ptr, SortOrder.ASC);
                } else {
                    actualViewIndexID =
                            PSmallint.INSTANCE.getCodec().decodeShort(ptr, SortOrder.ASC);
                }

                assertTrue("ViewIndexId's not match",
                        viewIndex.getViewIndexId() == actualViewIndexID);
                rowkey = result.getRow();
                numMatchingRows++;
            }
            assertEquals(String.format(
                            "Expected rows do match for index table = %s, row-key = %s, rowId = %s",
                            Bytes.toString(hbaseIndexTableName),
                            Bytes.toStringBinary(rowkey), rowId), 1,
                    numMatchingRows);

        }
    }

    protected abstract boolean hasLongViewIndexEnabled();

    private SortOrder[][] getSortOrders() {
        SortOrder[][]
                sortOrders =
                new SortOrder[][] { { SortOrder.ASC, SortOrder.ASC, SortOrder.ASC },
                        { SortOrder.ASC, SortOrder.ASC, SortOrder.DESC },
                        { SortOrder.ASC, SortOrder.DESC, SortOrder.ASC },
                        { SortOrder.ASC, SortOrder.DESC, SortOrder.DESC },
                        { SortOrder.DESC, SortOrder.ASC, SortOrder.ASC },
                        { SortOrder.DESC, SortOrder.ASC, SortOrder.DESC },
                        { SortOrder.DESC, SortOrder.DESC, SortOrder.ASC },
                        { SortOrder.DESC, SortOrder.DESC, SortOrder.DESC } };
        return sortOrders;
    }

    private List<PDataType[]> getTestCases() {

        List<PDataType[]> testCases = new ArrayList<>();
        // Test Case 1: PK1 = Integer, PK2 = Integer, PK3 = Integer
        testCases.add(new PDataType[] { PInteger.INSTANCE, PInteger.INSTANCE, PInteger.INSTANCE });
        // Test Case 2: PK1 = Long, PK2 = Long, PK3 = Long
        testCases.add(new PDataType[] { PLong.INSTANCE, PLong.INSTANCE, PLong.INSTANCE });
        // Test Case 3: PK1 = Timestamp, PK2 = Timestamp, PK3 = Timestamp
        testCases.add(
                new PDataType[] { PTimestamp.INSTANCE, PTimestamp.INSTANCE, PTimestamp.INSTANCE });
        // Test Case 4: PK1 = Char, PK2 = Char, PK3 = Char
        testCases.add(new PDataType[] { PChar.INSTANCE, PChar.INSTANCE, PChar.INSTANCE });
        // Test Case 5: PK1 = Decimal, PK2 = Decimal, PK3 = Integer
        // last PK cannot be of variable length when creating a view on top of it
        testCases.add(new PDataType[] { PDecimal.INSTANCE, PDecimal.INSTANCE, PInteger.INSTANCE });
        // Test Case 6: PK1 = Date, PK2 = Date, PK3 = Date
        testCases.add(new PDataType[] { PDate.INSTANCE, PDate.INSTANCE, PDate.INSTANCE });
        // Test Case 7: PK1 = Varchar, PK2 = Varchar, PK3 = Integer
        // last PK cannot be of variable length when creating a view on top of it
        testCases.add(new PDataType[] { PVarchar.INSTANCE, PVarchar.INSTANCE, PInteger.INSTANCE });

        return testCases;
    }

    @Test
    public void testViewsWithExtendedPK() {
        try {
            List<PDataType[]> testCases = getTestCases();
            SortOrder[][] sortOrders = getSortOrders();

            String tableName = "";
            tableName = createViewHierarchy(
                    testCases, sortOrders, 500, 5000, 3,
                    true, true, false);
            assertRowKeyMatchersForTable(getUrl(), SchemaUtil.getSchemaNameFromFullName(tableName),
                    SchemaUtil.getTableNameFromFullName(tableName));
            tableName =
                    createViewHierarchy(
                            testCases, sortOrders, 600, 6000, 3,
                            false, true, false);
            assertRowKeyMatchersForTable(getUrl(), SchemaUtil.getSchemaNameFromFullName(tableName),
                    SchemaUtil.getTableNameFromFullName(tableName));

        } catch (Exception e) {
            e.printStackTrace();
            LOGGER.error(e.getMessage());
        }
    }

    @Test
    public void testViewsWithVariousTenantIdTypes() {
        try {
            List<PDataType[]> testCases = new ArrayList<>();
            // Test Case 1: PK1 = Integer, PK2 = Integer, PK3 = Integer
            testCases.add(new PDataType[] { PInteger.INSTANCE, PInteger.INSTANCE, PInteger.INSTANCE });
            SortOrder[][]
                    sortOrders =
                    new SortOrder[][] { { SortOrder.ASC, SortOrder.ASC, SortOrder.ASC } };

            String tableName = "";
            tableName = createViewHierarchy( PInteger.INSTANCE,
                    testCases, sortOrders, 700, 7000, 3,
                    true, true, false);
            assertRowKeyMatchersForTable(getUrl(), SchemaUtil.getSchemaNameFromFullName(tableName),
                    SchemaUtil.getTableNameFromFullName(tableName));
            assertRowKeyMatchersForTable(getUrl(), SchemaUtil.getSchemaNameFromFullName(tableName),
                    SchemaUtil.getTableNameFromFullName(tableName));

            tableName = createViewHierarchy( PLong.INSTANCE,
                    testCases, sortOrders, 710, 7100, 3,
                    true, true, false);
            assertRowKeyMatchersForTable(getUrl(), SchemaUtil.getSchemaNameFromFullName(tableName),
                    SchemaUtil.getTableNameFromFullName(tableName));
            assertRowKeyMatchersForTable(getUrl(), SchemaUtil.getSchemaNameFromFullName(tableName),
                    SchemaUtil.getTableNameFromFullName(tableName));

            tableName = createViewHierarchy( PSmallint.INSTANCE,
                    testCases, sortOrders, 720, 7200, 3,
                    true, true, false);
            assertRowKeyMatchersForTable(getUrl(), SchemaUtil.getSchemaNameFromFullName(tableName),
                    SchemaUtil.getTableNameFromFullName(tableName));
            assertRowKeyMatchersForTable(getUrl(), SchemaUtil.getSchemaNameFromFullName(tableName),
                    SchemaUtil.getTableNameFromFullName(tableName));

            tableName = createViewHierarchy( PTinyint.INSTANCE,
                    testCases, sortOrders, 730, 7300, 3,
                    true, true, false);
            assertRowKeyMatchersForTable(getUrl(), SchemaUtil.getSchemaNameFromFullName(tableName),
                    SchemaUtil.getTableNameFromFullName(tableName));
            assertRowKeyMatchersForTable(getUrl(), SchemaUtil.getSchemaNameFromFullName(tableName),
                    SchemaUtil.getTableNameFromFullName(tableName));

        } catch (Exception e) {
            e.printStackTrace();
            LOGGER.error(e.getMessage());
        }
    }

    @Test
    public void testViewsWithoutExtendedPK() {
        try {
            List<PDataType[]> testCases = getTestCases();
            SortOrder[][] sortOrders = getSortOrders();
            String tableName = "";
            tableName =
                    createViewHierarchy(
                            testCases, sortOrders, 100, 1000, 3,
                            true, false, false);
            assertRowKeyMatchersForTable(getUrl(), SchemaUtil.getSchemaNameFromFullName(tableName),
                    SchemaUtil.getTableNameFromFullName(tableName));
            tableName =
                    createViewHierarchy(
                            testCases, sortOrders, 200, 2000, 3,
                            false, false, false);
            assertRowKeyMatchersForTable(getUrl(), SchemaUtil.getSchemaNameFromFullName(tableName),
                    SchemaUtil.getTableNameFromFullName(tableName));
            tableName = createViewHierarchy(
                    testCases, sortOrders, 300, 3000, 3,
                    true, false, true);
            assertRowKeyMatchersForTable(getUrl(), SchemaUtil.getSchemaNameFromFullName(tableName),
                    SchemaUtil.getTableNameFromFullName(tableName));

            tableName =
                    createViewHierarchy(
                            testCases, sortOrders, 400, 4000, 3,
                            false, false, true);
            assertRowKeyMatchersForTable(getUrl(), SchemaUtil.getSchemaNameFromFullName(tableName),
                    SchemaUtil.getTableNameFromFullName(tableName));

        } catch (Exception e) {
            e.printStackTrace();
            LOGGER.error(e.getMessage());
        }

    }

    @Test
    public void testNonMultiTenantExtendedViewsWithViewIndexesFail() {
        try {
            List<PDataType[]> testCases = new ArrayList<>();
            // Test Case 1: PK1 = Integer, PK2 = Integer, PK3 = Integer
            testCases.add(
                    new PDataType[] { PInteger.INSTANCE, PInteger.INSTANCE, PInteger.INSTANCE });

            SortOrder[][]
                    sortOrders =
                    new SortOrder[][] { { SortOrder.ASC, SortOrder.ASC, SortOrder.ASC } };
            createViewHierarchy(
                    testCases, sortOrders, 900, 9000, 3,
                    false, true, true);
            fail();
        } catch (SQLException sqle) {
            assertEquals(VIEW_CANNOT_EXTEND_PK_WITH_PARENT_INDEXES.getErrorCode(),
                    sqle.getErrorCode());
        } catch (Exception e) {
            fail("SQLException expected: " + VIEW_CANNOT_EXTEND_PK_WITH_PARENT_INDEXES);
        }

    }

    @Test
    public void testMultiTenantExtendedViewsWithViewIndexesFail() {
        try {
            List<PDataType[]> testCases = new ArrayList<>();
            // Test Case 1: PK1 = Integer, PK2 = Integer, PK3 = Integer
            testCases.add(
                    new PDataType[] { PInteger.INSTANCE, PInteger.INSTANCE, PInteger.INSTANCE });

            SortOrder[][]
                    sortOrders =
                    new SortOrder[][] { { SortOrder.ASC, SortOrder.ASC, SortOrder.ASC } };
            String tableName = "";
            tableName = createViewHierarchy(
                    testCases, sortOrders, 910, 9100, 3,
                    true, true, true);
            assertRowKeyMatchersForTable(getUrl(), SchemaUtil.getSchemaNameFromFullName(tableName),
                    SchemaUtil.getTableNameFromFullName(tableName));
            fail();
        } catch (SQLException sqle) {
            assertEquals(VIEW_CANNOT_EXTEND_PK_WITH_PARENT_INDEXES.getErrorCode(),
                    sqle.getErrorCode());
        } catch (Exception e) {
            fail("SQLException expected: " + VIEW_CANNOT_EXTEND_PK_WITH_PARENT_INDEXES);
        }

    }
    private String createViewHierarchy(List<PDataType[]> testCases, SortOrder[][] sortOrders,
            int startPartition, int startRowId, int numTenants, boolean isMultiTenant,
            boolean extendPK, boolean hasGlobalViewIndexes) throws Exception {
        return createViewHierarchy(PChar.INSTANCE, testCases, sortOrders, startPartition,
                startRowId, numTenants,isMultiTenant, extendPK,hasGlobalViewIndexes);
    }


    private String createViewHierarchy(PDataType tenantIdType, List<PDataType[]> testCases, SortOrder[][] sortOrders,
            int startPartition, int startRowId, int numTenants, boolean isMultiTenant,
            boolean extendPK, boolean hasGlobalViewIndexes) throws Exception {

        Map<String, byte[]> actualViewToRowKeyMap = Maps.newHashMap();
        String tableName = BaseTest.generateUniqueName();

        // Create a base table
        createBaseTable(tableName, isMultiTenant, tenantIdType);
        String baseTableName = String.format(BASE_TABLE_NAME_FMT, tableName);
        String indexTableName = String.format(INDEX_TABLE_NAME_FMT, tableName);

        int partition = startPartition;
        int rowId = startRowId;

        // Create the global view
        for (int testCase = 0; testCase < testCases.size(); testCase++) {
            for (int index = 0; index < sortOrders.length; index++) {
                partition++;
                Pair<String, byte[]>
                        gvRowKeyInfo =
                        createGlobalView(tableName, partition, testCases.get(testCase),
                                sortOrders[index], hasGlobalViewIndexes);
                actualViewToRowKeyMap.put(gvRowKeyInfo.getFirst(), gvRowKeyInfo.getSecond());
                LOGGER.debug(String.format("Created global view %s with partition = %d",
                        gvRowKeyInfo.getFirst(), partition));
            }
        }

        partition = startPartition;
        String[] globalViewPKNames = new String[] { "ID1", "ID2", "ID3" };
        // Create the tenant view for each partition
        for (int testCase = 0; testCase < testCases.size(); testCase++) {
            for (int index = 0; index < sortOrders.length; index++) {
                partition++;
                for (int tenant = 1; tenant <= numTenants; tenant++) {
                    Pair<String, byte[]>
                            tvRowKeyInfo =
                            createTenantView(
                                    extendPK, partition, tenantIdType, tenant, 1, globalViewPKNames,
                                    testCases.get(testCase));
                    actualViewToRowKeyMap.put(tvRowKeyInfo.getFirst(), tvRowKeyInfo.getSecond());
                    LOGGER.debug(String.format("Created tenant view %s [partition = %d]",
                            tvRowKeyInfo.getFirst(), partition));
                }
            }
        }

        partition = startPartition;
        // Upsert rows into the tenant view for each partition
        for (int testCase = 0; testCase < testCases.size(); testCase++) {
            for (int index = 0; index < sortOrders.length; index++) {
                partition++;
                for (int tenant = 1; tenant <= numTenants; tenant++) {
                    rowId++;
                    try {
                        upsertTenantViewRows(
                                isMultiTenant, extendPK, partition, tenantIdType, tenant,
                                1, rowId, globalViewPKNames, testCases.get(testCase));
                    } catch (Exception ex) {
                        String
                                testInfo =
                                Arrays.stream(testCases.get(testCase)).map(String::valueOf)
                                        .collect(Collectors.joining(","));
                        String
                                sortInfo =
                                Arrays.stream(sortOrders[index]).map(String::valueOf)
                                        .collect(Collectors.joining(","));
                        String
                                pkInfo =
                                Arrays.stream(globalViewPKNames).map(String::valueOf)
                                        .collect(Collectors.joining(","));
                        LOGGER.error(ex.getMessage());
                        ex.printStackTrace();
                        fail(String.format(
                                "isMultiTenant(%s), extendPK(%s), partition(%d), tenant(%s), " +
                                        "rowId(%s), pkInfo(%s), testInfo(%s), sortInfo(%s)",
                                isMultiTenant, extendPK, partition, tenant,
                                rowId, pkInfo, testInfo, sortInfo));
                    }
                }
            }
        }

        partition = startPartition;
        rowId = startRowId;
        // Validate the matcher pattern from SYSTEM.CATALOG matches the rowkey from HBase
        // for each tenant view
        // actualViewToRowKeyMap holds matcher pattern for each view (global and tenant specific)
        for (int testCase = 0; testCase < testCases.size(); testCase++) {
            for (int index = 0; index < sortOrders.length; index++) {
                partition++;
                for (int tenant = 1; tenant <= numTenants; tenant++) {
                    rowId++;
                    String partitionName = String.format(PARTITION_FMT, partition);

                    String tenantId = "";
                    if (tenantIdType.getSqlType() == Types.VARCHAR || tenantIdType.getSqlType() == Types.CHAR) {
                        tenantId = String.format(ORG_ID_FMT, ORG_ID_PREFIX, tenant);
                    } else {
                        tenantId = String.format("%015d", tenant);
                    }
                    String
                            tenantConnectionUrl =
                            String.format(TENANT_URL_FMT, getUrl(), TENANT_ID_ATTRIB, tenantId);
                    String tenantViewName = String.format(TENANT_VIEW_NAME_FMT, partitionName, 1);
                    String tenantViewKey = String.format("%s.%s", tenantId, tenantViewName);
                    try (PhoenixConnection tenantConnection = DriverManager.getConnection(
                            tenantConnectionUrl).unwrap(PhoenixConnection.class)) {
                        assertHBaseRowKeyMatchesPrefix(tenantConnection,
                                baseTableName.getBytes(StandardCharsets.UTF_8), rowId,
                                actualViewToRowKeyMap.get(tenantViewKey));
                        if (hasGlobalViewIndexes) {
                            PTable view = PhoenixRuntime.getTable(tenantConnection, tenantViewName);
                            assertIndexTableRowKeyMatchesPrefix(tenantConnection,
                                    view.getIndexes().get(0),
                                    indexTableName.getBytes(StandardCharsets.UTF_8), rowId);
                        }
                    }
                }
            }
        }
        return baseTableName;
    }
}
