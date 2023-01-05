package org.apache.phoenix.coprocessor;

import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.RegionInfo;
import org.apache.hadoop.hbase.coprocessor.MasterCoprocessor;
import org.apache.hadoop.hbase.coprocessor.MasterCoprocessorEnvironment;
import org.apache.hadoop.hbase.coprocessor.MasterObserver;
import org.apache.hadoop.hbase.coprocessor.ObserverContext;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.phoenix.schema.SaltingUtil;
import org.apache.phoenix.util.QueryUtil;
import org.apache.phoenix.util.SchemaUtil;

public class PhoenixMasterObserver implements MasterObserver, MasterCoprocessor {

    @Override
    public Optional<MasterObserver> getMasterObserver() {
        return Optional.of(this);
    }

    @Override
    public void preMergeRegions(final ObserverContext<MasterCoprocessorEnvironment> ctx, RegionInfo[] regionsToMerge) throws IOException {
        try {
            if (blockMergeForSaltedTables(ctx, regionsToMerge)) {
                //different salt so block merge
                throw new IOException("Don't merge regions with different salt bits");
            }
        } catch (SQLException e) {
            throw new IOException("SQLException while fetching data from phoenix", e);
        }
    }

    private boolean blockMergeForSaltedTables(ObserverContext<MasterCoprocessorEnvironment> ctx,
            RegionInfo[] regionsToMerge) throws SQLException {
        TableName table = regionsToMerge[0].getTable();
        int saltBuckets = getTableSalt(ctx, table.toString());
        if(saltBuckets > 0 ) {
            System.out.println("Number of buckets="+saltBuckets);
            return !regionsHaveSameSalt(regionsToMerge);
        }
        // table is not salted so don't block merge
        return false;
    }

    private int getTableSalt(ObserverContext<MasterCoprocessorEnvironment> ctx, String table) throws SQLException {
        int saltBucket = 0;
        String schemaName = SchemaUtil.getSchemaNameFromFullName(table);
        String tableName = SchemaUtil.getTableNameFromFullName(table);
        String sql =
                "SELECT salt_buckets FROM system.catalog WHERE table_name = ? "
                        + "AND (table_schem = ? OR table_schem IS NULL)";

        if (schemaName == null || schemaName.isEmpty()) {
            sql = sql + "   and table_schem is null";
        } else {
            sql = sql + String.format(" and table_schem=%s", schemaName);
        }

        Configuration conf = ctx.getEnvironment().getConfiguration();
        PreparedStatement stmt;
        ResultSet resultSet;
        try (Connection conn = QueryUtil.getConnectionOnServer(conf)) {
            stmt = conn.prepareStatement(sql);
            stmt.setString(1, tableName);
            stmt.setString(2, schemaName);
            resultSet = stmt.executeQuery();
            if (resultSet.next()) {
                saltBucket = resultSet.getInt("salt_buckets");
            }
            resultSet.close();
            stmt.close();
        } 
        return saltBucket;
    }

    private boolean regionsHaveSameSalt(RegionInfo[] regionsToMerge) {
        Collection<Integer> saltKeys = new HashSet<>();
        List<RegionInfo> filteredRegionsToMerge= new ArrayList<>();
        for (final RegionInfo region : regionsToMerge) {
            saltKeys.add(extractSaltByteFromRegionName(region));
        }
        return saltKeys.size() == 1;
    }

    private int extractSaltByteFromRegionName(RegionInfo rInfo) {
        String[] split = rInfo.getRegionNameAsString().split(",");
        String s = split[1];
        System.out.println(s);
        int salt = 0;
        if (s == null || s.isEmpty()) {
            salt = 0;
        } else {
            byte[] bytes = Bytes.toBytesBinary(s);
            salt = bytes[0];
        }

        System.out.println("salt is : " + salt);
        System.out.println();
        return salt;
    }

}
