package org.apache.phoenix.coprocessor;

import java.io.IOException;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.Collection;
import java.util.HashSet;
import java.util.Optional;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.RegionInfo;
import org.apache.hadoop.hbase.coprocessor.MasterCoprocessor;
import org.apache.hadoop.hbase.coprocessor.MasterCoprocessorEnvironment;
import org.apache.hadoop.hbase.coprocessor.MasterObserver;
import org.apache.hadoop.hbase.coprocessor.ObserverContext;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.phoenix.jdbc.PhoenixConnection;
import org.apache.phoenix.schema.PTable;
import org.apache.phoenix.schema.PTableKey;
import org.apache.phoenix.util.QueryUtil;

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
        System.out.println("Number of buckets="+saltBuckets);
        if(saltBuckets > 0 ) {
            System.out.println("Number of buckets="+saltBuckets);
            return !regionsHaveSameSalt(regionsToMerge);
        }
        // table is not salted so don't block merge
        return false;
    }

    private int getTableSalt(ObserverContext<MasterCoprocessorEnvironment> ctx, String table) throws SQLException {
        int saltBucket = 0;
        Configuration conf = ctx.getEnvironment().getConfiguration();
        try (Connection conn = QueryUtil.getConnectionOnServer(conf)) {
            PhoenixConnection pConn = conn.unwrap(PhoenixConnection.class);
            PTable pTable = pConn.getTable(new PTableKey(pConn.getTenantId(), table));
            saltBucket = pTable.getBucketNum();
        } 
        return saltBucket;
    }

    private boolean regionsHaveSameSalt(RegionInfo[] regionsToMerge) {
        Collection<Integer> saltKeys = new HashSet<>();
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
