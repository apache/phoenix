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

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.SerializerProvider;
import com.fasterxml.jackson.databind.ser.std.StdSerializer;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import org.apache.phoenix.schema.PColumn;

/**
 * Jackson serializer for {@code Set<PColumn>} as it appears on
 * {@link ExplainPlanAttributes#getServerMergeColumns()}. {@code PColumn} is an interface backed by
 * implementations that are not Jackson-friendly.
 */
public class ServerMergeColumnsSerializer extends StdSerializer<Set<PColumn>> {

  private static final long serialVersionUID = 1L;

  @SuppressWarnings("unchecked")
  public ServerMergeColumnsSerializer() {
    super((Class<Set<PColumn>>) (Class<?>) Set.class);
  }

  @Override
  public void serialize(Set<PColumn> value, JsonGenerator gen, SerializerProvider provider)
    throws IOException {
    List<String> names = new ArrayList<>(value.size());
    for (PColumn column : value) {
      names.add(column == null ? null : column.toString());
    }
    Collections.sort(names, (a, b) -> {
      if (a == null && b == null) return 0;
      if (a == null) return -1;
      if (b == null) return 1;
      return a.compareTo(b);
    });
    gen.writeStartArray();
    for (String name : names) {
      gen.writeString(name);
    }
    gen.writeEndArray();
  }
}
