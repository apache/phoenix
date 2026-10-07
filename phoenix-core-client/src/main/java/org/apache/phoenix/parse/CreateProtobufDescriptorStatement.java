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

/**
 * Parsed form of:
 *
 * <pre>
 * CREATE PROTOBUF DESCRIPTOR [ IF NOT EXISTS ] qualified_proto_name
 *   FROM url_string_literal       -- URL of a serialized FileDescriptorSet
 * </pre>
 * <p>
 * The URL points at a {@code FileDescriptorSet} binary produced by
 * {@code protoc --include_imports --descriptor_set_out=...}. There is no inline {@code .proto} text
 * form — the binary-only design avoids the maintenance cost of carrying a proto grammar in Phoenix.
 * Validation (descriptor compilation, name-resolution, {@code FileDescriptorSet} sanity) happens
 * downstream in {@code MetaDataClient.createProtobufDescriptor}; this class is a pure parse-time
 * data carrier.
 * <p>
 * The proto message name preserves the source case (proto identifiers are case-sensitive) via the
 * {@code proto_message_name_part} grammar rule's wildcard capture — see PhoenixSQL.g for the
 * case-preservation mechanism.
 */
public class CreateProtobufDescriptorStatement extends MutableStatement {

  private final String protoMessageName;
  private final boolean ifNotExists;
  /** URL of a serialized {@code FileDescriptorSet}. Always non-null in a parsed statement. */
  private final String sourceUrl;

  public CreateProtobufDescriptorStatement(String protoMessageName, boolean ifNotExists,
    String sourceUrl) {
    this.protoMessageName = protoMessageName;
    this.ifNotExists = ifNotExists;
    this.sourceUrl = sourceUrl;
  }

  @Override
  public int getBindCount() {
    return 0;
  }

  public String getProtoMessageName() {
    return protoMessageName;
  }

  public boolean isIfNotExists() {
    return ifNotExists;
  }

  /** Returns the source URL of the serialized {@code FileDescriptorSet}. */
  public String getSourceUrl() {
    return sourceUrl;
  }
}
