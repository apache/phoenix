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
import java.util.List;
import org.apache.hadoop.hbase.HRegionLocation;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.client.RegionInfo;
import org.apache.hadoop.hbase.util.Bytes;

/**
 * Jackson serializer for {@code List<HRegionLocation>} as it appears on
 * {@link ExplainPlanAttributes#getRegionLocations()}. The HBase {@code HRegionLocation} bean is not
 * cleanly serializable by Jackson's default introspection.
 */
public class RegionLocationsListSerializer extends StdSerializer<List<HRegionLocation>> {

  private static final long serialVersionUID = 1L;

  @SuppressWarnings("unchecked")
  public RegionLocationsListSerializer() {
    super((Class<List<HRegionLocation>>) (Class<?>) List.class);
  }

  @Override
  public void serialize(List<HRegionLocation> value, JsonGenerator gen, SerializerProvider provider)
    throws IOException {
    gen.writeStartArray();
    for (HRegionLocation loc : value) {
      gen.writeStartObject();
      RegionInfo region = loc == null ? null : loc.getRegion();
      gen.writeStringField("startKey",
        region == null ? null : Bytes.toStringBinary(region.getStartKey()));
      gen.writeStringField("endKey",
        region == null ? null : Bytes.toStringBinary(region.getEndKey()));
      ServerName sn = loc == null ? null : loc.getServerName();
      gen.writeStringField("server", sn == null ? null : sn.toString());
      gen.writeEndObject();
    }
    gen.writeEndArray();
  }
}
