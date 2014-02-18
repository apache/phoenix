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
package org.apache.phoenix.hbase.index.builder;

import java.io.IOException;

import org.apache.hadoop.hbase.DoNotRetryIOException;

/**
 * Unexpected failure while building index updates that wasn't caused by an {@link IOException}.
 * This should be used if there is some basic issue with indexing - and no matter of retries will
 * fix it.
 */
@SuppressWarnings("serial")
public class IndexBuildingFailureException extends DoNotRetryIOException {

  /**
   * Constructor for over the wire propagation. Generally, shouldn't be used since index failure
   * should have an underlying cause to propagate.
   * @param msg reason for the failure
   */
  public IndexBuildingFailureException(String msg) {
    super(msg);
  }

  /**
   * @param msg reason
   * @param cause underlying cause for the failure
   */
  public IndexBuildingFailureException(String msg, Throwable cause) {
    super(msg, cause);
  }
}