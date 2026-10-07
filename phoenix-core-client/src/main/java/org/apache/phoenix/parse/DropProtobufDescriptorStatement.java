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
package org.apache.phoenix.parse;

import org.apache.phoenix.jdbc.PhoenixStatement.Operation;

/**
 * Parsed form of:
 *
 * <pre>
 * DROP PROTOBUF DESCRIPTOR [ IF EXISTS ] qualified_proto_name
 * </pre>
 * <p>
 * The in-use check at execution time queries {@code SYSTEM.PROTO_DESC_LINK} via a single-region
 * prefix scan keyed by the descriptor name; {@code SYSTEM.CATALOG} is never scanned (the
 * multi-minute cost of full {@code SYSTEM.CATALOG} scans at production scale is what motivated the
 * auxiliary link table).
 */
public class DropProtobufDescriptorStatement extends MutableStatement {

  private final String protoMessageName;
  private final boolean ifExists;

  public DropProtobufDescriptorStatement(String protoMessageName, boolean ifExists) {
    this.protoMessageName = protoMessageName;
    this.ifExists = ifExists;
  }

  @Override
  public int getBindCount() {
    return 0;
  }

  /**
   * Overrides {@link MutableStatement}'s default {@code Operation.UPSERT} to match the convention
   * every other Phoenix {@code DROP *} statement uses ({@code DropTableStatement},
   * {@code DropSchemaStatement}, {@code DropIndexStatement}, {@code DropSequenceStatement} all
   * return {@code Operation.DELETE}). The DROP-as-delete classification matters for Phoenix's
   * mutation routing and metrics; without this override the statement would be mis-bucketed as an
   * UPSERT-class mutation.
   */
  @Override
  public Operation getOperation() {
    return Operation.DELETE;
  }

  public String getProtoMessageName() {
    return protoMessageName;
  }

  public boolean isIfExists() {
    return ifExists;
  }
}
