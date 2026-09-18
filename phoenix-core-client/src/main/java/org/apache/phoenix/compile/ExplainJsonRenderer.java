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
package org.apache.phoenix.compile;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.util.DefaultIndenter;
import com.fasterxml.jackson.core.util.DefaultPrettyPrinter;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectWriter;
import com.fasterxml.jackson.databind.SerializationFeature;
import java.sql.SQLException;

/**
 * Serializes an {@link ExplainPlanAttributes} tree to a JSON document for the
 * {@code EXPLAIN (FORMAT JSON) <stmt>} statement.
 * <p>
 * The output is pretty-printed with two space indentation for both objects and arrays.
 * <p>
 * The JSON layout tracks the Java field names and structure of {@link ExplainPlanAttributes}. It is
 * deliberately not a stable contract and carries no version field. It is an opt-in view onto an
 * internal structure, useful for tooling and assertions.
 * <p>
 * This class intentionally does not reuse the shared {@link org.apache.phoenix.util.JacksonUtil}
 * mapper so that the general-purpose mapper configuration can change without affecting the EXPLAIN
 * JSON contract.
 */
public final class ExplainJsonRenderer {

  private static final ObjectWriter WRITER = buildWriter();

  private static ObjectWriter buildWriter() {
    ObjectMapper mapper = new ObjectMapper();
    // Emit every field, with an explicit null for any unset value, so the JSON view is a faithful
    // projection of the attributes tree.
    mapper.setSerializationInclusion(JsonInclude.Include.ALWAYS);
    mapper.configure(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS, false);
    mapper.configure(SerializationFeature.FAIL_ON_EMPTY_BEANS, false);
    // Jackson's stock DefaultPrettyPrinter indents object fields but uses a single space
    // FixedSpaceIndenter for array elements. Set the same two space indenter on both so nested
    // objects and array elements each start on their own indented line.
    DefaultIndenter indenter = new DefaultIndenter("  ", "\n");
    DefaultPrettyPrinter printer =
      new DefaultPrettyPrinter().withObjectIndenter(indenter).withArrayIndenter(indenter);
    return mapper.writer(printer);
  }

  private ExplainJsonRenderer() {
  }

  /**
   * Serialize the given attributes to a pretty-printed JSON document.
   * @param attributes the plan attributes to serialize
   * @return the JSON document
   * @throws SQLException if serialization fails
   */
  public static String render(ExplainPlanAttributes attributes) throws SQLException {
    try {
      return WRITER.writeValueAsString(attributes);
    } catch (JsonProcessingException e) {
      throw new SQLException("Failed to serialize EXPLAIN attributes as JSON", e);
    }
  }
}
