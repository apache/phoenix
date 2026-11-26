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
 * ALTER PROTOBUF DESCRIPTOR [ IF EXISTS ] qualified_proto_name
 *   FROM url_string_literal       -- URL of a serialized FileDescriptorSet
 * </pre>
 * <p>
 * {@code IF EXISTS} matches the convention of {@code ALTER INDEX (IF EXISTS)?} — when present, the
 * statement silently no-ops if the descriptor does not exist (useful for idempotent migration
 * scripts that may run on databases where prior versions weren't installed). When absent, a missing
 * descriptor raises {@code PROTOBUF_DESCRIPTOR_NOT_FOUND}.
 * <p>
 * {@code IF NOT EXISTS} is deliberately NOT supported on ALTER (it would mean "alter something that
 * doesn't exist"). Use {@code CREATE PROTOBUF DESCRIPTOR IF NOT EXISTS} for the create-if-missing
 * pattern.
 * <p>
 * Unlike {@code CREATE}, the new descriptor must pass the evolution-envelope check against the
 * previously-registered version before it replaces it. Validation is descriptor-local: no
 * {@code SYSTEM.CATALOG} scan. Evolution rule enforcement happens downstream in
 * {@code ProtoEvolutionValidator}.
 */
public class AlterProtobufDescriptorStatement extends MutableStatement {

  private final String protoMessageName;
  private final boolean ifExists;
  /** URL of a serialized {@code FileDescriptorSet}. Always non-null in a parsed statement. */
  private final String sourceUrl;

  public AlterProtobufDescriptorStatement(String protoMessageName, boolean ifExists,
    String sourceUrl) {
    this.protoMessageName = protoMessageName;
    this.ifExists = ifExists;
    this.sourceUrl = sourceUrl;
  }

  @Override
  public int getBindCount() {
    return 0;
  }

  public String getProtoMessageName() {
    return protoMessageName;
  }

  public boolean isIfExists() {
    return ifExists;
  }

  /** Returns the source URL of the serialized {@code FileDescriptorSet}. */
  public String getSourceUrl() {
    return sourceUrl;
  }
}
