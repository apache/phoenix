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
package org.apache.phoenix.hbase.index.exception;

import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.hbase.util.Bytes;
import org.apache.phoenix.hbase.index.table.HTableInterfaceReference;
import org.apache.phoenix.hbase.index.util.ImmutableBytesPtr;
import org.apache.phoenix.thirdparty.com.google.common.base.MoreObjects;
import org.apache.phoenix.thirdparty.com.google.common.collect.Lists;

/**
 * Indicate a failure to write to multiple index tables.
 */
@SuppressWarnings("serial")
public class MultiIndexWriteFailureException extends IndexWriteException {

  public static final String FAILURE_MSG = "Failed to write to multiple index tables: ";
  private List<HTableInterfaceReference> failures;

  /**
   * @param failures the tables to which the index write did not succeed
   */
  public MultiIndexWriteFailureException(List<HTableInterfaceReference> failures,
          boolean disableIndexOnFailure) {
      super(disableIndexOnFailure);
      this.failures = failures;
  }

  public MultiIndexWriteFailureException(List<HTableInterfaceReference> failures,
          boolean disableIndexOnFailure, Throwable cause) {
      super(cause, disableIndexOnFailure);
      this.failures = failures;
  }

  /**
   * This constructor used to rematerialize this exception when receiving an rpc exception from the
   * server
   * @param message detail message
   */
  public MultiIndexWriteFailureException(String message) {
      super(IndexWriteException.parseDisableIndexOnFailure(message));
      Pattern p = Pattern.compile(FAILURE_MSG + "\\[(.*)\\]");
      Matcher m = p.matcher(message);
      if (m.find()) {
          failures = Lists.newArrayList();
          String tablesStr = m.group(1);
          for (String tableName : tablesStr.split(",\\s")) {
            HTableInterfaceReference tableRef = new HTableInterfaceReference(new ImmutableBytesPtr(Bytes.toBytes(tableName)));
            failures.add(tableRef);
        }
      }
  }

  public List<HTableInterfaceReference> getFailedTables() {
    return this.failures;
  }

  public void setFailedTables(List<HTableInterfaceReference> failedTables) {
      this.failures = failedTables;
  }

  @Override
    public String getMessage() {
        return MoreObjects.firstNonNull(super.getMessage(),"") + " " + FAILURE_MSG + failures;
    }
}