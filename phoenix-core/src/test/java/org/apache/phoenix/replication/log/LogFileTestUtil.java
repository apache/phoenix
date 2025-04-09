/*
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
package org.apache.phoenix.replication.log;

import java.io.IOException;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellBuilderFactory;
import org.apache.hadoop.hbase.CellBuilderType;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;

public interface LogFileTestUtil {

    static LogFile.Record newPutRecord(String table, long commitId, String rowKey, long ts,
            int numCols) {
        return new LogFileRecord().setMutation(newPut(rowKey, ts, numCols))
            .setHBaseTableName(table).setCommitId(commitId);
    }

    static Put newPut(String rowKey, long ts, int numCols) {
        byte[] qualifier = Bytes.toBytes("q");
        Put put = new Put(Bytes.toBytes(rowKey));
        put.setTimestamp(ts);
        for (int i = 0; i < numCols; i++) {
            put.addColumn(Bytes.toBytes("col" + i), qualifier, ts,
                Bytes.toBytes("v" + i + "_" + rowKey));
        }
        return put;
    }

    static LogFile.Record newDeleteRecord(String table, long commitId, String rowKey, long ts,
            int numCols) {
        return new LogFileRecord().setMutation(newDelete(rowKey, ts, numCols))
            .setHBaseTableName(table).setCommitId(commitId);
    }

    static Delete newDelete(String rowKey, long ts, int numCols) {
        byte[] qualifier = Bytes.toBytes("q");
        Delete delete = new Delete(Bytes.toBytes(rowKey));
        delete.setTimestamp(ts);
        for (int i = 0; i < numCols; i++) {
            delete.addColumn(Bytes.toBytes("col" + i), qualifier);
        }
        return delete;
    }

    static LogFile.Record newDeleteColumnRecord(String table, long commitId, String rowKey,
            long ts, int numCols) throws IOException {
        byte[] qualifier = Bytes.toBytes("q");
        Delete delete = new Delete(Bytes.toBytes(rowKey));
        delete.setTimestamp(ts);
        for (int i = 0; i < numCols; i++) {
            byte[] column = Bytes.toBytes("col" + i);
            delete.add(CellBuilderFactory.create(CellBuilderType.SHALLOW_COPY)
                .setRow(Bytes.toBytes(rowKey)).setFamily(column).setQualifier(qualifier)
                .setTimestamp(ts).setType(Cell.Type.DeleteColumn).build());
        }
        LogFile.Record record = new LogFileRecord().setMutation(delete).setHBaseTableName(table)
            .setCommitId(commitId);
        return record;
    }

    static LogFile.Record newDeleteFamilyRecord(String table, long commitId, String rowKey,
            long ts, int numCols) throws IOException {
        Delete delete = new Delete(Bytes.toBytes(rowKey));
        delete.setTimestamp(ts);
        for (int i = 0; i < numCols; i++) {
            byte[] column = Bytes.toBytes("col" + i);
            delete.addFamily(column);
        }
        LogFile.Record record = new LogFileRecord().setMutation(delete).setHBaseTableName(table)
            .setCommitId(commitId);
        return record;
    }

    static LogFile.Record newDeleteFamilyVersionRecord(String table, long commitId, String rowKey,
            long ts, int numCols) throws IOException {
        Delete delete = new Delete(Bytes.toBytes(rowKey));
        delete.setTimestamp(ts);
        for (int i = 0; i < numCols; i++) {
            byte[] column = Bytes.toBytes("col" + i);
            delete.addFamilyVersion(column, ts);
        }
        LogFile.Record record = new LogFileRecord().setMutation(delete).setHBaseTableName(table)
            .setCommitId(commitId);
        return record;
    }

    static void assertRecordEquals(String message, LogFile.Record r1, LogFile.Record r2)
            throws AssertionError {
        try {
            if (!r1.getMutation().toJSON().equals(r2.getMutation().toJSON())
                    || !r1.getHBaseTableName().equals(r2.getHBaseTableName())
                    || r1.getCommitId() != r2.getCommitId()) {
                throw new AssertionError(message + ": left=" + r1 + ", right=" + r2);
            }
        } catch (IOException e) {
            throw new AssertionError(e.getMessage());
        }
    }

    static void assertMutationEquals(String message, Mutation m1, Mutation m2) {
        try {
            if (!m1.toJSON().equals(m2.toJSON())) {
                throw new AssertionError(message + ": left=" + m1 + ", right=" + m2);
            }
        } catch (IOException e) {
            throw new AssertionError(e.getMessage());
        }
    }

}
