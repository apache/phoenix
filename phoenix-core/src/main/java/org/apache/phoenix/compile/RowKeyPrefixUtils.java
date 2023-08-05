package org.apache.phoenix.compile;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Pair;
import org.apache.phoenix.coprocessor.TableInfo;
import org.apache.phoenix.prefix.table.TableTTLInfo;
import org.apache.phoenix.query.ConnectionQueryServices;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.phoenix.expression.Expression;
import org.apache.phoenix.jdbc.PhoenixConnection;
import org.apache.phoenix.jdbc.PhoenixPreparedStatement;
import org.apache.phoenix.parse.ParseNode;
import org.apache.phoenix.parse.SQLParser;
import org.apache.phoenix.parse.SelectStatement;
import org.apache.phoenix.query.KeyRange;
import org.apache.phoenix.schema.*;
import org.apache.phoenix.util.*;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.sql.*;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;

import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.TTL_NOT_DEFINED;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.SYSTEM_LINK_HBASE_TABLE_NAME;
import static org.apache.phoenix.util.ByteUtil.EMPTY_BYTE_ARRAY;
import static org.apache.phoenix.util.PhoenixRuntime.TENANT_ID_ATTRIB;

public class RowKeyPrefixUtils {
    public static final String ORG_ID_FMT = "00D0x0000";

    public static List<TableTTLInfo> getRowKeyPrefixesForTable(String url, String parentSchemaName, String parentTableName) {
        List<TableTTLInfo> tableList = new ArrayList<TableTTLInfo>();

        Properties tenantProps = PropertiesUtil.deepCopy(new Properties());
        try (Connection globalConnection = DriverManager.getConnection(url)) {
            ConnectionQueryServices cqs = globalConnection.unwrap(PhoenixConnection.class).getQueryServices();
            try (Table childLinkTable = cqs.getTable(SchemaUtil.getPhysicalName(
                    SYSTEM_LINK_HBASE_TABLE_NAME.toBytes(), cqs.getProps()).getName())) {
                Pair<List<PTable>, List<TableInfo>> allDescendants =
                        ViewUtil.findAllDescendantViews(childLinkTable, cqs.getConfiguration(),
                                EMPTY_BYTE_ARRAY, parentSchemaName.getBytes(),
                                parentTableName.getBytes(), HConstants.LATEST_TIMESTAMP, false);
                for (PTable view : allDescendants.getFirst()) {
                    String physicalTableName = view.getPhysicalName().getString();
                    PName tenantId = view.getTenantId();
                    String tableSchema = view.getSchemaName().getString();
                    String viewName = view.getName().getString();
                    String viewStatement = view.getViewStatement();
                    int ttl = (int) view.getTTL();
                    String viewFullName = String.format("%s.%s", tableSchema, viewName);
                    byte[] tenantIdBytes = tenantId == null ? ByteUtil.EMPTY_BYTE_ARRAY : tenantId.getString().getBytes(StandardCharsets.UTF_8);
                    SelectStatement viewSelectStatement = new SQLParser(viewStatement).parseQuery();

                    Connection stmtConnection = tenantId == null?
                            globalConnection :  DriverManager.getConnection(
                            String.format("%s;%s=%s", url, TENANT_ID_ATTRIB, tenantId.getString()), tenantProps);


                    StatementContext viewStatementContext =  getViewStatementContext(stmtConnection, viewStatement, viewSelectStatement);
                    PTable viewStatementTable = viewStatementContext.getCurrentTable().getTable();
                    // Where clause with INs and ORs
                    ParseNode whereNode = viewSelectStatement.getWhere();
                    Expression whereExpression = whereNode.accept(new WhereCompiler.WhereExpressionCompiler(viewStatementContext));

                    WhereOptimizer.KeyExpressionVisitor visitor = new WhereOptimizer.KeyExpressionVisitor(viewStatementContext,
                            viewStatementTable);
                    WhereOptimizer.KeyExpressionVisitor.KeySlots keySlots = whereExpression.accept(visitor);
                    List<PColumn> pkColumns = viewStatementTable.getPKColumns();
                    System.out.println("**********************************************************************");
                    System.out.println("************************ Prefix info *********************************");
                    System.out.println("**********************************************************************");

                    System.out.println(String.format("t=%s,p=%s,v=%s,ttl=%d",
                            tenantId == null ? "" : tenantId, physicalTableName, viewFullName, ttl));

                    byte[] rowKeyPrefix = getRowKeyPrefix(tenantIdBytes, viewFullName, viewStatement, viewStatementTable, keySlots);
                    TableTTLInfo tableTTLInfo = new TableTTLInfo(
                            viewStatementTable.getPhysicalName().toString().getBytes(StandardCharsets.UTF_8),
                            tenantIdBytes,
                            viewFullName.getBytes(StandardCharsets.UTF_8),
                            rowKeyPrefix, ttl);
                    if (ttl != TTL_NOT_DEFINED) {
                        tableList.add(tableTTLInfo);
                    }

                }
            };
        } catch (SQLException | IOException e) {
            throw new RuntimeException(e);
        }
        return tableList;

    }


    public static List<TableTTLInfo> getRowKeyPrefixesForTable(String fullTableName, Configuration config) {
        List<TableTTLInfo> tableList = new ArrayList<TableTTLInfo>();

        try (Connection globalConnection = QueryUtil.getConnectionOnServer(new Properties(), config)) {
            ConnectionQueryServices cqs = globalConnection.unwrap(PhoenixConnection.class).getQueryServices();
            String schemaName = SchemaUtil.getSchemaNameFromFullName(fullTableName);
            String tableName = SchemaUtil.getTableNameFromFullName(fullTableName);
            try (Table childLinkTable = cqs.getTable(SchemaUtil.getPhysicalName(
                    SYSTEM_LINK_HBASE_TABLE_NAME.toBytes(), cqs.getProps()).getName())) {
                Pair<List<PTable>, List<TableInfo>> allDescendants =
                        ViewUtil.findAllDescendantViews(childLinkTable, cqs.getConfiguration(),
                                EMPTY_BYTE_ARRAY, schemaName.getBytes(),
                                tableName.getBytes(), HConstants.LATEST_TIMESTAMP, false);
                for (PTable view : allDescendants.getFirst()) {
                    String physicalTableName = view.getPhysicalName().getString();
                    PName tenantId = view.getTenantId();
                    String tableSchema = view.getSchemaName().getString();
                    String viewName = view.getName().getString();
                    String viewStatement = view.getViewStatement();
                    int ttl = (int) view.getTTL();
                    String viewFullName = String.format("%s.%s", tableSchema, viewName);
                    byte[] tenantIdBytes = tenantId == null ? ByteUtil.EMPTY_BYTE_ARRAY : tenantId.getString().getBytes(StandardCharsets.UTF_8);
                    SelectStatement viewSelectStatement = new SQLParser(viewStatement).parseQuery();

                    Properties tenantProps = new Properties();
                    if (tenantId != null) {
                        tenantProps.setProperty(PhoenixRuntime.TENANT_ID_ATTRIB, tenantId.getString());
                    }

                    Connection stmtConnection = tenantId == null?
                            globalConnection : QueryUtil.getConnectionOnServer(tenantProps, config);

                    StatementContext viewStatementContext =  getViewStatementContext(stmtConnection, viewStatement, viewSelectStatement);
                    PTable viewStatementTable = viewStatementContext.getCurrentTable().getTable();
                    // Where clause with INs and ORs
                    ParseNode whereNode = viewSelectStatement.getWhere();
                    Expression whereExpression = whereNode.accept(new WhereCompiler.WhereExpressionCompiler(viewStatementContext));

                    WhereOptimizer.KeyExpressionVisitor visitor = new WhereOptimizer.KeyExpressionVisitor(viewStatementContext,
                            viewStatementTable);
                    WhereOptimizer.KeyExpressionVisitor.KeySlots keySlots = whereExpression.accept(visitor);
                    List<PColumn> pkColumns = viewStatementTable.getPKColumns();
                    System.out.println("**********************************************************************");
                    System.out.println("************************ Prefix info *********************************");
                    System.out.println("**********************************************************************");

                    System.out.println(String.format("t=%s,p=%s,v=%s,ttl=%d",
                            tenantId == null ? "" : tenantId, physicalTableName, viewFullName, ttl));

                    byte[] rowKeyPrefix = getRowKeyPrefix(tenantIdBytes, viewFullName, viewStatement, viewStatementTable, keySlots);
                    TableTTLInfo tableTTLInfo = new TableTTLInfo(
                            viewStatementTable.getPhysicalName().toString().getBytes(StandardCharsets.UTF_8),
                            tenantIdBytes,
                            viewFullName.getBytes(StandardCharsets.UTF_8),
                            rowKeyPrefix, ttl);
                    tableList.add(tableTTLInfo);

                }
            };
        } catch (SQLException | IOException e) {
            throw new RuntimeException(e);
        }
        return tableList;

    }

    private static StatementContext getViewStatementContext(
            Connection tenantConnection,
            String viewStatement,
            SelectStatement viewSelectStatement) throws SQLException {

        PhoenixPreparedStatement preparedViewStatement =
                tenantConnection.prepareStatement(viewStatement).unwrap(PhoenixPreparedStatement.class);

        ColumnResolver resolver = FromCompiler.getResolverForQuery(viewSelectStatement,
                tenantConnection.unwrap(PhoenixConnection.class));
        StatementContext viewStatementContext = new StatementContext(preparedViewStatement, resolver);
        return viewStatementContext;

    }

    private static byte[] getRowKeyPrefix(byte[] tenantIdBytes, String viewName, String viewStatement, PTable viewStmtTable, WhereOptimizer.KeyExpressionVisitor.KeySlots keySlots) {
        List<PColumn> pks = viewStmtTable.getPKColumns();
        RowKeySchema.RowKeySchemaBuilder builder = new RowKeySchema.RowKeySchemaBuilder(pks.size());
        for (final PColumn pk : pks) {
            builder.addField(pk, pk.isNullable(), pk.getSortOrder());
        }
        System.out.println(String.format("Physical table = %s, View Stmt table = %s", viewStmtTable.getPhysicalName().toString(), viewStmtTable.getName().toString()));
        System.out.println(String.format("PK  : %s", builder.build().toString()));
        System.out.println(String.format("SQL : %s", viewStatement));

        System.out.println("*********************");
        System.out.println("**** Slot info ******");
        System.out.println("*********************");
        List<List<KeyRange>> rowKeySlotRangesList = new ArrayList<>();
        if (tenantIdBytes.length != 0) {
            rowKeySlotRangesList.add(Arrays.asList(KeyRange.POINT.apply(tenantIdBytes)));
        }
        for (WhereOptimizer.KeyExpressionVisitor.KeySlot slot : keySlots.getSlots()) {
            //System.out.println(String.format("Key Range : %s", (slot == null) ? "null" : slot.getKeyRanges().toString()));
            if (slot != null) {
                rowKeySlotRangesList.add(slot.getKeyRanges());
            }
        }
        System.out.println(String.format("pks = %d, ranges = %d", pks.size(), rowKeySlotRangesList.size()));
        System.out.println(String.format("View = %s, View Stmt table = %s, Key Range : %s", viewName, viewStmtTable.getName().toString(),
                (rowKeySlotRangesList.isEmpty()) ? "null" : rowKeySlotRangesList.toString()));

        int[] rowKeySlotSpans = new int[rowKeySlotRangesList.size()];
        Arrays.fill(rowKeySlotSpans, 0, rowKeySlotRangesList.size(), 0);
        byte[] rowKey = new byte[1024];
        int[] rowKeySlotRangesIndexes = new int[rowKeySlotRangesList.size()];
        Arrays.fill(rowKeySlotRangesIndexes, 0, rowKeySlotRangesList.size(), 0);
        int rowKeyLength = ScanUtil.setKey(
                builder.build(),
                rowKeySlotRangesList,
                rowKeySlotSpans,
                rowKeySlotRangesIndexes,
                KeyRange.Bound.LOWER,
                rowKey,
                0,
                0,
                rowKeySlotRangesList.size());
        byte[] rowKeyPrefix = Arrays.copyOf(rowKey, rowKeyLength);
        String rowKeyPrefixStr = Bytes.toStringBinary(rowKeyPrefix);
        String rowKeyPrefixHex = Bytes.toHex(rowKeyPrefix);
        byte[] rowKeyPrefixFromHex = Bytes.fromHex(rowKeyPrefixHex);
        assert(Bytes.compareTo(rowKeyPrefix, rowKeyPrefixFromHex) == 0);

        System.out.println("************************");
        System.out.println("***** RowKey info ******");
        System.out.println("************************");
        System.out.println(String.format("In Hex : %s", rowKeyPrefixHex));
        System.out.println(String.format("In StringBinary: %s", rowKeyPrefixStr));
        return rowKeyPrefix;
    }

}
