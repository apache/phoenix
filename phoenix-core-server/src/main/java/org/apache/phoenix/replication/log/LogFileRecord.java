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

import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.client.Put;

@edu.umd.cs.findbugs.annotations.SuppressWarnings(value = { "EI_EXPOSE_REP", "EI_EXPOSE_REP2" },
    justification = "Intentional")
public class LogFileRecord implements LogFile.Record {

  private String tableName;
  private long commitId;
  private Mutation mutation;
  private int serializedLength;

  public LogFileRecord() {
  }

  @Override
  public String getHBaseTableName() {
    return tableName;
  }

  @Override
  public LogFile.Record setHBaseTableName(String tableName) {
    this.tableName = tableName;
    return this;
  }

  @Override
  public long getCommitId() {
    return commitId;
  }

  @Override
  public LogFile.Record setCommitId(long commitId) {
    this.commitId = commitId;
    return this;
  }

  @Override
  public Mutation getMutation() {
    return this.mutation;
  }

  @Override
  public LogFile.Record setMutation(Mutation mutation) {
    this.mutation = mutation;
    return this;
  }

  @Override
  public int getSerializedLength() {
    // NOTE: Should be set by the Codec using setSerializedLength after reading or writing
    // the record.
    return this.serializedLength;
  }

  @Override
  public LogFile.Record setSerializedLength(int serializedLength) {
    this.serializedLength = serializedLength;
    return this;
  }

  @Override
  public int hashCode() {
    int code = tableName.hashCode();
    code ^= Long.hashCode(commitId);
    code ^= mutation.toString().hashCode();
    return code;
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj) {
      return true;
    }
    if (obj == null) {
      return false;
    }
    if (getClass() != obj.getClass()) {
      return false;
    }
    LogFileRecord other = (LogFileRecord) obj;
    return tableName.equals(other.tableName) && commitId == other.commitId
      && mutation.toString().equals(other.mutation.toString());
  }

  @Override
  public String toString() {
    return "LogFileRecord [mutation=" + mutation.toString() + ", tableName=" + tableName
      + ", commitId=" + commitId + " ]";
  }

  // Internals only below. Not for LogFile interface consumer use.

  protected enum MutationType {
    PUT(1),
    DELETE(2);

    private int code;

    MutationType(int code) {
      this.code = code;
    }

    int getCode() {
      return code;
    }

    static MutationType get(Mutation mutation) {
      if (mutation instanceof Put) {
        return PUT;
      } else if (mutation instanceof Delete) {
        return DELETE;
      }
      throw new UnsupportedOperationException("Unsupported mutation type: " + mutation);
    }

    static MutationType codeToType(int code) {
      for (MutationType type : MutationType.values()) {
        if (type.code == code) {
          return type;
        }
      }
      throw new UnsupportedOperationException("Unsupported mutation code: " + code);
    }

  }

}
