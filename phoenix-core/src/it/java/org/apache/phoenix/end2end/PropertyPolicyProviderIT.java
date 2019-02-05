package org.apache.phoenix.end2end;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HConstants;
import org.apache.phoenix.mapreduce.util.ConnectionUtil;
import org.junit.Test;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.Properties;

public class PropertyPolicyProviderIT  extends ParallelStatsDisabledIT {

    @Test
    public void testUsingDefaultHBaseConfigs() throws SQLException {
        Configuration config = HBaseConfiguration.create();
        config.set(HConstants.ZOOKEEPER_QUORUM, getUrl());
        Properties properties=new Properties();
        properties.put("allowedProperty","value");
        try(
                Connection conn = ConnectionUtil.getInputConnection(config, properties)
        ){}
    }

}
