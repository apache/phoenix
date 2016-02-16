/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.phoenix.pig.udf;

import java.io.IOException;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HConstants;
import org.apache.phoenix.mapreduce.util.ConnectionUtil;
import org.apache.phoenix.util.PhoenixRuntime;
import org.apache.pig.EvalFunc;
import org.apache.pig.data.Tuple;
import org.apache.pig.impl.util.UDFContext;

import com.google.common.base.Preconditions;
import com.google.common.base.Strings;

import edu.umd.cs.findbugs.annotations.NonNull;
import edu.umd.cs.findbugs.annotations.Nullable;

/**
 * UDF to Reserve a chunk of numbers for a given sequence
 * 
 * @note The way this UDF is invoked we open a new connection for every tuple row. The UDF will not perform well on
 *       large datasets as it involves creating a new connection for every tuple row
 */
public class ReserveNSequence extends EvalFunc<Long> {

    public static final String INVALID_TUPLE_MESSAGE = "Tuple should have correct fields(NumtoReserve,SequenceName).";
    public static final String EMPTY_SEQUENCE_NAME_MESSAGE = "Sequence name should be not null";
    public static final String EMPTY_ZK_MESSAGE = "ZKQuorum should be not null";
    public static final String INVALID_NUMBER_MESSAGE = "Number of Sequences to Reserve should be greater than 0";

    private final String zkQuorum;
    private final String tenantId;
    private Configuration configuration;
    Connection connection;
    
    public ReserveNSequence(@NonNull String zkQuorum, @Nullable String tenantId) {
        Preconditions.checkNotNull(zkQuorum, EMPTY_ZK_MESSAGE);
        this.zkQuorum = zkQuorum;
        this.tenantId = tenantId;
    }
    /**
     * Reserve N next sequences for a sequence name. N is the first field in the tuple. Sequence name is the second
     * field in the tuple zkquorum is the third field in the tuple
     */
    @Override
    public Long exec(Tuple input) throws IOException {
        Preconditions.checkArgument(input != null && input.size() >= 2, INVALID_TUPLE_MESSAGE);
        Long numToReserve = (Long)(input.get(0));
        Preconditions.checkArgument(numToReserve > 0, INVALID_NUMBER_MESSAGE);
        String sequenceName = (String)input.get(1);
        Preconditions.checkNotNull(sequenceName, EMPTY_SEQUENCE_NAME_MESSAGE);
        // It will create a connection when called for the first Tuple per task.
        // The connection gets cleaned up in finish() method
        if (connection == null) {
            initConnection();
        }
        ResultSet rs = null;
        try {
            String sql = getNextNSequenceSelectStatement(Long.valueOf(numToReserve), sequenceName);
            rs = connection.createStatement().executeQuery(sql);
            Preconditions.checkArgument(rs.next());
            Long startIndex = rs.getLong(1);
            rs.close();
            connection.commit();
            return startIndex;
        } catch (SQLException e) {
            throw new IOException("Caught exception while processing row." + e.getMessage(), e);
        }
    }
    
    /**
     * Cleanup to be performed at the end.
     * Close connection
     */
    @Override
    public void finish() {
        if (connection != null) {
            try {
                connection.close();
            } catch (SQLException e) {
                throw new RuntimeException("Caught exception while closing connection", e);
            }
        }
    }
    
    private void initConnection() throws IOException {
        // Create correct configuration to be used to make phoenix connections
        UDFContext context = UDFContext.getUDFContext();
        configuration = new Configuration(context.getJobConf());
        configuration.set(HConstants.ZOOKEEPER_QUORUM, this.zkQuorum);
        if (Strings.isNullOrEmpty(tenantId)) {
            configuration.unset(PhoenixRuntime.TENANT_ID_ATTRIB);
        } else {
            configuration.set(PhoenixRuntime.TENANT_ID_ATTRIB, tenantId);
        }
        try {
            connection = ConnectionUtil.getOutputConnection(configuration);
        } catch (SQLException e) {
            throw new IOException("Caught exception while creating connection", e);
        }
    }

    private String getNextNSequenceSelectStatement(Long numToReserve, String sequenceName) {
        return new StringBuilder().append("SELECT NEXT " + numToReserve + " VALUES" + " FOR ").append(sequenceName)
                .toString();
    }

}
