package org.apache.phoenix.spark.datasource.v2.writer;

import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.List;
import java.util.Properties;
import java.util.stream.Collectors;

import org.apache.phoenix.util.PhoenixRuntime;
import org.apache.phoenix.util.QueryUtil;
import org.apache.phoenix.util.SchemaUtil;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.execution.datasources.SparkJdbcUtil;
import org.apache.spark.sql.execution.datasources.jdbc.PhoenixJdbcDialect$;
import org.apache.spark.sql.sources.v2.writer.DataWriter;
import org.apache.spark.sql.sources.v2.writer.WriterCommitMessage;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import com.google.common.collect.Lists;

public class PhoenixDataWriter implements DataWriter<InternalRow> {

    private final StructType schema;
    private final Connection conn;
    private final PreparedStatement statement;

    public PhoenixDataWriter(PhoenixDataSourceWriteOptions options) {
        String scn = options.getScn();
        String tenantId = options.getTenantId();
        String zkUrl = options.getZkUrl();
        Properties overridingProps = new Properties();
        if (scn != null) {
            overridingProps.put(PhoenixRuntime.CURRENT_SCN_ATTRIB, scn);
        }
        if (tenantId != null) {
            overridingProps.put(PhoenixRuntime.TENANT_ID_ATTRIB, tenantId);
        }
        this.schema = options.getSchema();
        try {
            this.conn = DriverManager.getConnection("jdbc:phoenix:" + zkUrl, overridingProps);
            List<String> colNames = Lists.newArrayList(options.getSchema().names());
            if (!options.skipNormalizingIdentifier()){
                colNames = colNames.stream().map(colName -> SchemaUtil.normalizeIdentifier(colName)).collect(Collectors.toList());
            }
            String upsertSql = QueryUtil.constructUpsertStatement(options.getTableName(), colNames, null);
            this.statement = this.conn.prepareStatement(upsertSql);
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void write(InternalRow internalRow) throws IOException {
        try {
            int i=0;
            for (StructField field : schema.fields()) {
                DataType dataType = field.dataType();
                if (internalRow.isNullAt(i)) {
                    statement.setNull(i + 1, SparkJdbcUtil.getJdbcType(dataType,
                            PhoenixJdbcDialect$.MODULE$).jdbcNullType());
                } else {
                    Row row = SparkJdbcUtil.toRow(schema, internalRow);
                    SparkJdbcUtil.makeSetter(conn, PhoenixJdbcDialect$.MODULE$, dataType).apply(statement, row, i);
                }
                ++i;
            }
            statement.execute();
        } catch (SQLException e) {
            throw new IOException("Exception while executing Phoenix prepared statement", e);
        }
    }

    @Override
    public WriterCommitMessage commit() throws IOException {
        try {
            conn.commit();
        } catch (SQLException e) {
            throw new RuntimeException(e);
        } finally {
            try {
                statement.close();
                conn.close();
            }
            catch (SQLException ex) {
                throw new RuntimeException(ex);
            }
        }
        return null;
    }

    @Override
    public void abort() throws IOException {
    }
}
