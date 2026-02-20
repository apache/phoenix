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
import org.apache.phoenix.schema.PTable;
import org.apache.phoenix.schema.TableNotFoundException;
import org.apache.phoenix.util.PhoenixRuntime;
import org.apache.phoenix.util.QueryUtil;

public class PhoenixMasterObserver implements MasterObserver, MasterCoprocessor {

    @Override
    public Optional<MasterObserver> getMasterObserver() {
        return Optional.of(this);
    }

    @Override
    public void preMergeRegions(final ObserverContext<MasterCoprocessorEnvironment> ctx, RegionInfo[] regionsToMerge) throws IOException {
        // proceed if it is a phoenix table
        try {
            if (shouldBlockMerge(ctx, regionsToMerge)) {
                //different salt so block merge
                throw new IOException("Don't merge regions with different salt bits");
            }
        } catch (SQLException e) {
            // should we block here??
            // this exception can be because of something wrong on cluster
            // but to be on safe side we block merge here assuming blocking of merge would have no impact
            // but merging wrong regions can have data consistency issue
            throw new IOException("SQLException while fetching data from phoenix", e);
        }
    }

    private boolean shouldBlockMerge(ObserverContext<MasterCoprocessorEnvironment> ctx,
                                     RegionInfo[] regionsToMerge) throws SQLException {
        TableName tableName = regionsToMerge[0].getTable();
        PTable phoenixTable = getPhoenixTable(ctx, tableName);
        if(phoenixTable == null) {
            // it is not a phoenix table
            return false;
        }
        int saltBuckets = phoenixTable.getBucketNum();
        System.out.println("Number of buckets="+saltBuckets);
        if(saltBuckets <= 0 ) {
            // table is not salted so don't block merge
            return false;
        }
        // if regions have same salt they can be merged
        return !regionsHaveSameSalt(regionsToMerge);
    }

    private PTable getPhoenixTable(ObserverContext<MasterCoprocessorEnvironment> ctx, TableName tableName) throws SQLException {
        Configuration conf = ctx.getEnvironment().getConfiguration();
        PTable pTable;
        try (Connection hbaseConn= QueryUtil.getConnectionOnServer(conf)) {
            try {
                pTable = PhoenixRuntime.getTable(hbaseConn, tableName.toString());
            } catch (TableNotFoundException e) {
                // assuming this is not a phoenix table
                return null;
            }
        }
        return pTable;
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
