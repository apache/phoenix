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

import java.util.Collections;
import java.util.List;

/**
 * Parsed form of:
 *
 * <pre>
 * CREATE PROTOBUF BUNDLE [ IF NOT EXISTS ] (
 *   qualified_proto_name [, qualified_proto_name ]*
 * )
 *   FROM url_string_literal
 * ;
 * </pre>
 * <p>
 * Multi-message registration in a single DDL statement — a syntactic convenience over N individual
 * {@code CREATE PROTOBUF DESCRIPTOR} statements that all share the same source URL. The URL points
 * at a single {@code FileDescriptorSet} (typically a
 * {@code protoc --include_imports --descriptor_set_out=...} output containing many `.proto` files);
 * the statement registers one Phoenix descriptor per name in the parenthesized list, all sharing
 * that source.
 * <p>
 * Semantics:
 * <ul>
 * <li>Every name in the list must resolve to a top-level message in the loaded
 * {@code FileDescriptorSet}. If any name fails resolution the whole bundle is rejected with
 * {@code PROTOBUF_DESCRIPTOR_TYPE_NOT_FOUND_IN_FILE} naming the offending entry; no partial
 * registration occurs.</li>
 * <li>{@code IF NOT EXISTS} is per-name: existing descriptors with the same name are skipped
 * without overwrite; new names are registered. The statement is therefore idempotent over the union
 * of (already-registered, newly-registered) names.</li>
 * <li>Duplicate names within the list are a parse error (caught by the grammar's unique-list
 * check).</li>
 * </ul>
 * Validation (descriptor compilation, name-resolution, {@code FileDescriptorSet} sanity) happens
 * downstream in {@code MetaDataClient.createProtobufBundle}; this class is a pure parse-time data
 * carrier.
 */
public class CreateProtobufBundleStatement extends MutableStatement {

  private final List<String> protoMessageNames;
  private final boolean ifNotExists;
  /** URL of a serialized {@code FileDescriptorSet}. Always non-null in a parsed statement. */
  private final String sourceUrl;

  public CreateProtobufBundleStatement(List<String> protoMessageNames, boolean ifNotExists,
    String sourceUrl) {
    this.protoMessageNames = protoMessageNames == null
      ? Collections.emptyList()
      : Collections.unmodifiableList(protoMessageNames);
    this.ifNotExists = ifNotExists;
    this.sourceUrl = sourceUrl;
  }

  @Override
  public int getBindCount() {
    return 0;
  }

  /**
   * Returns the immutable list of fully-qualified proto message names being registered. List order
   * matches the source-text order in the DDL (used for deterministic error messages and audit
   * logging); registration is not order-sensitive.
   */
  public List<String> getProtoMessageNames() {
    return protoMessageNames;
  }

  public boolean isIfNotExists() {
    return ifNotExists;
  }

  /** Returns the source URL of the serialized {@code FileDescriptorSet}. */
  public String getSourceUrl() {
    return sourceUrl;
  }
}
