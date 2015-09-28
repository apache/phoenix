/**
 * 
 */
package org.apache.phoenix.pig.udf;

/*
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements. See the NOTICE
 * file distributed with this work for additional information regarding copyright ownership. The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with the
 * License. You may obtain a copy of the License at http://www.apache.org/licenses/LICENSE-2.0 Unless required by
 * applicable law or agreed to in writing, software distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the specific language
 * governing permissions and limitations under the License.
 */
import java.io.IOException;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HConstants;
import org.apache.phoenix.mapreduce.util.ConnectionUtil;
import org.apache.pig.EvalFunc;
import org.apache.pig.data.Tuple;
import org.apache.pig.impl.util.UDFContext;

import com.google.common.base.Preconditions;

/**
 * UDF to Reserve a chunk of numbers for a given sequence
 * 
 * @note The way this UDF is invoked we open a new connection for every tuple row. The UDF will not perform well on
 *       large datasets as it involves creating a new connection for every tuple row
 */
public class ReserveNSequence extends EvalFunc<Long> {

    public static final String INVALID_TUPLE_MESSAGE = "Tuple should have correct fields(NumtoReserve,SequenceName,zkquorum.";
    public static final String EMPTY_SEQUENCE_NAME_MESSAGE = "Sequence name should be not null";
    public static final String EMPTY_ZK_MESSAGE = "ZKQuorum should be not null";
    public static final String INVALID_NUMBER_MESSAGE = "Number of Sequences to Reserve should be greater than 0";
    public static final String SEQUENCE_NAME_CONF_KEY = "phoenix.sequence.name";

    /**
     * Reserve N next sequences for a sequence name. N is the first field in the tuple. Sequence name is the second
     * field in the tuple zkquorum is the third field in the tuple
     */
    @Override
    public Long exec(Tuple input) throws IOException {
        Preconditions.checkArgument(input != null && input.size() == 3, INVALID_TUPLE_MESSAGE);
        Long numToReserve = (Long)(input.get(0));
        Preconditions.checkArgument(numToReserve > 0, INVALID_NUMBER_MESSAGE);
        String sequenceName = (String)input.get(1);
        Preconditions.checkNotNull(sequenceName, EMPTY_SEQUENCE_NAME_MESSAGE);
        String zkquorum = (String)input.get(2);
        Preconditions.checkNotNull(zkquorum, EMPTY_ZK_MESSAGE);
        UDFContext context = UDFContext.getUDFContext();
        Configuration configuration = context.getJobConf();
        configuration.set(HConstants.ZOOKEEPER_QUORUM, zkquorum);
        Connection connection = null;
        ResultSet rs = null;
        try {
            connection = ConnectionUtil.getOutputConnection(configuration);
            String sql = getNextNSequenceSelectStatement(Long.valueOf(numToReserve), sequenceName);
            rs = connection.createStatement().executeQuery(sql);
            Preconditions.checkArgument(rs.next());
            Long startIndex = rs.getLong(1);
            rs.close();
            connection.commit();
            return startIndex;
        } catch (SQLException e) {
            throw new IOException("Caught exception while processing row." + e.getMessage(), e);
        } finally {
            try {
                if (connection != null) {
                    connection.close();
                }
            } catch (SQLException e) {
                throw new IOException("Caught exception while closing connection", e);
            }
        }
    }

    private String getNextNSequenceSelectStatement(Long numToReserve, String sequenceName) {
        return new StringBuilder().append("SELECT NEXT " + numToReserve + " VALUES" + " FOR ").append(sequenceName)
                .toString();
    }

}
