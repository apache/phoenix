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
package org.apache.phoenix.schema.types;

/**
 * Enum representing the consistency level for Phoenix secondary indexes.
 * <p>
 * <b>STRONG</b>: Synchronous index maintenance where index mutations are applied synchronously with
 * data table mutations. This provides immediate consistency but may impact write performance and
 * availability.
 * </p>
 * <p>
 * <b>EVENTUAL</b>: Asynchronous index maintenance where index mutations are applied in the
 * background. This provides better write performance and availability but with eventual consistency
 * guarantees.
 * </p>
 */
public enum IndexConsistency {
  /**
   * Strong consistency - synchronous index maintenance (default behavior). Index updates are
   * applied synchronously with data table updates.
   */
  STRONG('s'),

  /**
   * Eventual consistency - asynchronous index maintenance using CDC. Index updates are applied in
   * the background, providing eventual consistency.
   */
  EVENTUAL('e');

  private final char code;

  IndexConsistency(char code) {
    this.code = code;
  }

  /**
   * Returns the single character code for the consistency.
   * @return 's' for STRONG, 'e' for EVENTUAL
   */
  public char getCode() {
    return code;
  }

  /**
   * Returns the string representation of the code for storage.
   * @return single character string for storage
   */
  public String getCodeAsString() {
    return String.valueOf(code);
  }

  /**
   * Parses an IndexConsistency from its stored code.
   * @param code the stored code.
   * @return the corresponding IndexConsistency, or null if code is null/empty/invalid.
   */
  public static IndexConsistency fromCode(String code) {
    if (code == null || code.isEmpty()) {
      return null;
    }
    char c = code.charAt(0);
    for (IndexConsistency ic : values()) {
      if (ic.code == c) {
        return ic;
      }
    }
    return null;
  }

  /**
   * Returns the default consistency level for indexes.
   * @return STRONG consistency as the default
   */
  public static IndexConsistency getDefault() {
    return STRONG;
  }

  /**
   * Checks if this consistency level requires synchronous index maintenance.
   * @return true if synchronous maintenance is required, false otherwise
   */
  public boolean isSynchronous() {
    return this == STRONG;
  }

  /**
   * Checks if this consistency level uses asynchronous index maintenance.
   * @return true if asynchronous maintenance is used, false otherwise
   */
  public boolean isAsynchronous() {
    return this == EVENTUAL;
  }
}
