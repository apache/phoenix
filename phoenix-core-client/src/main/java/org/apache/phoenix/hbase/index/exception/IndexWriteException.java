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

import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.apache.hadoop.hbase.HBaseIOException;
import org.apache.phoenix.query.QueryServicesOptions;

import org.apache.phoenix.thirdparty.com.google.common.base.MoreObjects;

/**
 * Generic {@link Exception} that an index write has failed
 */
@SuppressWarnings("serial")
public class IndexWriteException extends HBaseIOException {

    /*
     * We pass this message back to the client so that the config only needs to be set on the
     * server side.
     */
    private static final String DISABLE_INDEX_ON_FAILURE_MSG = "disableIndexOnFailure=";
    private boolean disableIndexOnFailure = QueryServicesOptions.DEFAULT_INDEX_FAILURE_DISABLE_INDEX;

  public IndexWriteException() {
    super();
  }

    /**
     * Used for the case where we cannot reach the index, but not sure of the table or the mutations
     * that caused the failure
     * @param message
     * @param cause
     */
  public IndexWriteException(String message, Throwable cause) {
      super(message, cause);
  }

  public IndexWriteException(Throwable cause, boolean disableIndexOnFailure) {
    super(cause);
    this.disableIndexOnFailure = disableIndexOnFailure;
  }

  public IndexWriteException(boolean disableIndexOnFailure) {
    this.disableIndexOnFailure = disableIndexOnFailure;
  }

public IndexWriteException(Throwable cause) {
    super(cause);
  }

    public static boolean parseDisableIndexOnFailure(String message) {
        Pattern p =
                Pattern.compile(DISABLE_INDEX_ON_FAILURE_MSG + "(true|false)",
                    Pattern.CASE_INSENSITIVE);
        Matcher m = p.matcher(message);
        if (m.find()) {
            boolean disableIndexOnFailure = Boolean.parseBoolean(m.group(1));
            return disableIndexOnFailure;
        }
        return QueryServicesOptions.DEFAULT_INDEX_FAILURE_DISABLE_INDEX;
    }

    public boolean isDisableIndexOnFailure() {
        return disableIndexOnFailure;
    }

    @Override
    public String getMessage() {
        return MoreObjects.firstNonNull(super.getMessage(), "") + " "
                + DISABLE_INDEX_ON_FAILURE_MSG + disableIndexOnFailure + ",";
    }
}