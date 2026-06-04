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

import static org.junit.Assert.fail;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectWriter;
import com.fasterxml.jackson.databind.SerializationFeature;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import org.apache.phoenix.compile.ExplainPlan;
import org.apache.phoenix.compile.ExplainPlanAttributes;

/**
 * Backward compatibility test for Phoenix EXPLAIN output.
 * <p>
 * For each case, the test compares the current EXPLAIN output against an expected baseline supplied
 * by the caller (embedded directly in the test).
 * <p>
 * The {@link ExplainChangeRule} chain is applied to the expected side before comparison, so a
 * future EXPLAIN change appends one rule transforming the embedded baseline into its new expected
 * form.
 */
public final class ExplainOracle {

  private final List<ExplainChangeRule> rules;
  private final ExplainTextNormalizer textNormalizer;
  private final ExplainJsonNormalizer jsonNormalizer;
  private final ObjectMapper mapper;
  private final ObjectWriter prettyWriter;

  public ExplainOracle() {
    this(Collections.<ExplainChangeRule> emptyList());
  }

  public ExplainOracle(List<ExplainChangeRule> rules) {
    this.rules = new ArrayList<>(rules);
    this.textNormalizer = new ExplainTextNormalizer();
    this.jsonNormalizer = new ExplainJsonNormalizer();
    this.mapper = new ObjectMapper();
    this.mapper.setSerializationInclusion(JsonInclude.Include.ALWAYS);
    this.mapper.configure(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS, false);
    this.mapper.configure(SerializationFeature.FAIL_ON_EMPTY_BEANS, false);
    this.prettyWriter = mapper.writerWithDefaultPrettyPrinter();
  }

  /** Test-side {@link ObjectMapper} for building expected JSON */
  public ObjectMapper mapper() {
    return mapper;
  }

  /**
   * Verify the given plan against the embedded expected baseline for {@code caseId}.
   * @param caseId       the corpus case identifier (used in diff messages)
   * @param plan         the plan under test
   * @param expectedText the embedded expected, post-normalization plan-steps text
   * @param expectedJson the embedded expected, post-normalization JSON attributes tree
   */
  public void verify(String caseId, ExplainPlan plan, List<String> expectedText,
    JsonNode expectedJson) throws IOException {
    List<String> textCurrent = textNormalizer.normalize(plan.getPlanSteps());
    JsonNode jsonCurrent = serializeNormalized(plan.getPlanStepsAsAttributes());

    List<String> textExpected = new ArrayList<>(expectedText);
    JsonNode jsonExpected = expectedJson == null ? null : expectedJson.deepCopy();

    for (ExplainChangeRule rule : rules) {
      textExpected = rule.applyText(caseId, textExpected);
      if (jsonExpected != null) {
        jsonExpected = rule.applyJson(caseId, jsonExpected);
      }
    }

    if (!textExpected.equals(textCurrent)) {
      fail(textDiffMessage(caseId, textExpected, textCurrent));
    }
    if (jsonExpected != null && !jsonExpected.equals(jsonCurrent)) {
      fail(jsonDiffMessage(caseId, jsonExpected, jsonCurrent));
    }
  }

  /**
   * Serialize the given attributes to JSON and apply the cluster/connection normalizer in one step.
   * Exposed so the test can render and inspect the normalized JSON.
   */
  public JsonNode serializeNormalized(ExplainPlanAttributes attributes) throws IOException {
    String raw = mapper.writeValueAsString(attributes);
    JsonNode node = mapper.readTree(raw);
    jsonNormalizer.normalize(node);
    return node;
  }

  /** Normalize plan-steps text. Exposed so the test can render and inspect normalized output. */
  public List<String> normalizeText(List<String> raw) {
    return textNormalizer.normalize(raw);
  }

  /** Pretty-print a JSON node using the oracle's mapper config. */
  public String prettyJson(JsonNode node) throws JsonProcessingException {
    return prettyWriter.writeValueAsString(node);
  }

  private static String textDiffMessage(String caseId, List<String> expected, List<String> actual) {
    StringBuilder sb = new StringBuilder();
    sb.append("Text mismatch for case '").append(caseId).append("'.\n");
    sb.append("--- expected (").append(expected.size()).append(" lines)\n");
    for (String l : expected) {
      sb.append("  ").append(l).append('\n');
    }
    sb.append("--- actual (").append(actual.size()).append(" lines)\n");
    for (String l : actual) {
      sb.append("  ").append(l).append('\n');
    }
    sb.append("--- line-by-line diff\n");
    int n = Math.max(expected.size(), actual.size());
    for (int i = 0; i < n; i++) {
      String e = i < expected.size() ? expected.get(i) : "<missing>";
      String a = i < actual.size() ? actual.get(i) : "<missing>";
      if (!e.equals(a)) {
        sb.append(" @").append(i).append(":\n");
        sb.append("  - expected: ").append(e).append('\n');
        sb.append("  - actual:   ").append(a).append('\n');
      }
    }
    return sb.toString();
  }

  private String jsonDiffMessage(String caseId, JsonNode expected, JsonNode actual) {
    StringBuilder sb = new StringBuilder();
    sb.append("JSON mismatch for case '").append(caseId).append("'.\n");
    try {
      sb.append("--- expected\n").append(prettyJson(expected)).append('\n');
      sb.append("--- actual\n").append(prettyJson(actual)).append('\n');
    } catch (JsonProcessingException e) {
      sb.append("(failed to pretty-print: ").append(e.getMessage()).append(")\n");
    }
    sb.append("--- pointer diff\n");
    appendPointerDiff(sb, "", expected, actual);
    return sb.toString();
  }

  private static void appendPointerDiff(StringBuilder sb, String pointer, JsonNode expected,
    JsonNode actual) {
    if (expected == null && actual == null) {
      return;
    }
    if (expected == null || actual == null) {
      sb.append("  ").append(pointer.isEmpty() ? "/" : pointer).append(" expected=")
        .append(expected).append(" actual=").append(actual).append('\n');
      return;
    }
    if (expected.equals(actual)) {
      return;
    }
    if (expected.isObject() && actual.isObject()) {
      List<String> fields = new ArrayList<>();
      expected.fieldNames().forEachRemaining(fields::add);
      actual.fieldNames().forEachRemaining(f -> {
        if (!fields.contains(f)) {
          fields.add(f);
        }
      });
      Collections.sort(fields);
      for (String f : fields) {
        appendPointerDiff(sb, pointer + "/" + f, expected.get(f), actual.get(f));
      }
      return;
    }
    if (expected.isArray() && actual.isArray()) {
      int n = Math.max(expected.size(), actual.size());
      for (int i = 0; i < n; i++) {
        appendPointerDiff(sb, pointer + "/" + i, i < expected.size() ? expected.get(i) : null,
          i < actual.size() ? actual.get(i) : null);
      }
      return;
    }
    sb.append("  ").append(pointer.isEmpty() ? "/" : pointer).append(" expected=").append(expected)
      .append(" actual=").append(actual).append('\n');
  }

  /** Assemble an ExplainOracle with the given rule chain. */
  public static ExplainOracle forRules(ExplainChangeRule... rules) {
    return new ExplainOracle(Arrays.asList(rules));
  }
}
