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
package org.apache.phoenix.query.explain;

import com.fasterxml.jackson.databind.JsonNode;
import java.util.List;

/**
 * The {@link ExplainOracle} freezes today's EXPLAIN output as a set of golden fixtures. Future
 * EXPLAIN-design PRs that intentionally change the output register an {@code ExplainChangeRule}
 * that rewrites the (frozen) form into the new expected form, so every change to the grammar is
 * explicit and reviewable.
 */
public interface ExplainChangeRule {

  /**
   * Transform the golden plan steps text for the named case. The default implementation returns
   * {@code goldenText} unchanged.
   * @param caseId     the corpus case identifier (e.g. {@code "pointLookup"})
   * @param goldenText the line-oriented golden as currently expected (already transformed by any
   *                   earlier rule in the chain)
   * @return the next-expected golden text (may be a new list or the same instance)
   */
  default List<String> applyText(String caseId, List<String> goldenText) {
    return goldenText;
  }

  /**
   * Transform the golden JSON attributes tree for the named case. The default implementation
   * returns {@code goldenJson} unchanged.
   * @param caseId     the corpus case identifier
   * @param goldenJson the JSON attributes tree as currently expected (already transformed by any
   *                   earlier rule in the chain)
   * @return the next-expected JSON tree (may be a mutated input or a new node)
   */
  default JsonNode applyJson(String caseId, JsonNode goldenJson) {
    return goldenJson;
  }
}
