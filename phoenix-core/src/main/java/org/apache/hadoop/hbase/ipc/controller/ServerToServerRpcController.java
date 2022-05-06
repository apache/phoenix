package org.apache.hadoop.hbase.ipc.controller;

import com.google.protobuf.RpcController;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.TableName;

public interface ServerToServerRpcController extends RpcController {

    /**
     * @param priority Priority for this request; should fall roughly in the range
     *          {@link HConstants#NORMAL_QOS} to {@link HConstants#HIGH_QOS}
     */
    void setPriority(int priority);

    /**
     * @param tn Set priority based off the table we are going against.
     */
    void setPriority(final TableName tn);

    /**
     * @return The priority of this request
     */
    int getPriority();
}
