package org.apache.hadoop.hbase.ipc.controller;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.ipc.PhoenixRpcSchedulerFactory;
import org.apache.hadoop.hbase.ipc.ServerRpcController;
import org.apache.phoenix.jdbc.PhoenixDatabaseMetaData;
import org.apache.phoenix.thirdparty.com.google.common.collect.ImmutableList;
import org.apache.phoenix.util.SchemaUtil;

import java.util.List;

public class ServerToServerRpcControllerImpl extends ServerRpcController implements
        ServerToServerRpcController {

    private int priority;
    // list of system tables that can possibly have server-server rpc's
    private static final List<String> SYSTEM_TABLE_NAMES = new ImmutableList.Builder<String>()
            .add(PhoenixDatabaseMetaData.SYSTEM_CATALOG_NAME)
            .add(PhoenixDatabaseMetaData.SYSTEM_CHILD_LINK_NAME)
            .add(PhoenixDatabaseMetaData.SYSTEM_TASK_NAME)
            .add(SchemaUtil.getPhysicalTableName(PhoenixDatabaseMetaData.SYSTEM_CATALOG_NAME_BYTES, true)
                    .getNameAsString())
            .add(SchemaUtil.getPhysicalTableName(PhoenixDatabaseMetaData.SYSTEM_CHILD_LINK_NAME_BYTES, true)
                    .getNameAsString())
            .add(SchemaUtil.getPhysicalTableName(PhoenixDatabaseMetaData.SYSTEM_TASK_NAME_BYTES, true)
                    .getNameAsString())
            .build();

    public ServerToServerRpcControllerImpl(
            Configuration conf) {
        super();
        this.priority = PhoenixRpcSchedulerFactory.getMetadataPriority(conf);
    }

    @Override
    public void setPriority(final TableName tn) {
        if (SYSTEM_TABLE_NAMES.contains(tn.getNameAsString())) {
            setPriority(this.priority);
        }
    }


    @Override public void setPriority(int priority) {
        this.priority = priority;
    }


    @Override public int getPriority() {
        return this.priority;
    }
}
