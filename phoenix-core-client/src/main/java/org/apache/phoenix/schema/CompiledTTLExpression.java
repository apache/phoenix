/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.phoenix.schema;

import java.io.IOException;
import java.sql.SQLException;
import java.util.List;

import org.apache.hadoop.hbase.Cell;
import org.apache.phoenix.coprocessor.generated.PTableProtos;

public interface CompiledTTLExpression {

    /**
     * Serialize the TTL expression as a protobuf byte[]
     * @return protobuf for the TTL expression
     * @throws SQLException
     */
    default byte[] serialize() throws SQLException {
        try {
            PTableProtos.TTLExpression proto = toProto();
            return proto != null ? proto.toByteArray() : null;
        } catch (IOException e) {
            throw new SQLException(
                    String.format("Error serializing %s as scan attribute", this), e);
        }
    }

    /**
     * Returns the representation of the ttl expression as specified in the DDL
     * @return string representation
     */
    String getTTLExpression();

    /**
     * Returns the TTL value to be used for masking in TTLRegionScanner on a given row
     * @param result Input row
     * @param isRaw true when the row is from a raw scan like Index verification
     * @return ttl value in seconds
     */
    long getRowTTLForMasking(List<Cell> result, boolean isRaw);

    /**
     * Returns the TTL value to be used during compaction in CompactionScanner on a given row
     * @param result Input row
     * @return ttl value in seconds
     */
    long getRowTTLForCompaction(List<Cell> result);

    String toString();

    /**
     * Serialize the TTLExpression to protobuf
     * @return
     * @throws SQLException
     * @throws IOException
     */
    PTableProtos.TTLExpression toProto() throws SQLException, IOException;
}
