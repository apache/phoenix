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
package org.apache.phoenix.end2end.json.index;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Random;
import org.bson.BsonDocument;

/**
 * Deterministic 100-row fixture used by the four JSON/BSON index ITs.
 * Same logical content surfaced as both BSON documents and JSON strings so a single
 * ground-truth set drives all four ITs.
 */
public final class JsonBsonTestDataset {

  public static final long SEED = 0xC0FFEEL;
  public static final int ROW_COUNT = 100;
  public static final int SPARSE_ROW_COUNT = 15;     // rows missing the indexed path
  public static final int EDGE_ROW_COUNT = 5;        // edge values

  public static final class Row {
    public final String pk;
    public final String name;       // null when row is sparse
    public final String email;      // null when row is sparse
    public final Long score;        // null when row is sparse
    public final String city;       // always present
    public final String zip;        // null when row is sparse

    Row(String pk, String name, String email, Long score, String city, String zip) {
      this.pk = pk;
      this.name = name;
      this.email = email;
      this.score = score;
      this.city = city;
      this.zip = zip;
    }
  }

  private JsonBsonTestDataset() {}

  /** 100 deterministic rows. The same call always returns the same list. */
  public static List<Row> rows() {
    List<Row> out = new ArrayList<>(ROW_COUNT);
    Random rng = new Random(SEED);
    String[] names = { "alice", "bob", "carol", "dave", "eve", "frank", "grace",
        "heidi", "ivan", "judy", "ken", "lara", "mallory", "nina", "olivia",
        "peggy", "quinn", "rita", "sam", "trent", "ursula", "victor", "wendy",
        "xavier", "yvonne", "zara" };
    String[] cities = { "ny", "sf", "la", "sea", "chi", "bos", "atl", "den",
        "phx", "dal" };
    for (int i = 0; i < ROW_COUNT; i++) {
      String pk = String.format("k%03d", i);
      boolean sparse = i >= ROW_COUNT - SPARSE_ROW_COUNT - EDGE_ROW_COUNT
          && i < ROW_COUNT - EDGE_ROW_COUNT;
      boolean edge = i >= ROW_COUNT - EDGE_ROW_COUNT;
      String city = cities[rng.nextInt(cities.length)];
      if (sparse) {
        out.add(new Row(pk, null, null, null, city, null));
      } else if (edge) {
        // edge rows: empty-string name, zero score, negative score, big string, decimals-as-long
        switch (i - (ROW_COUNT - EDGE_ROW_COUNT)) {
          case 0:
            out.add(new Row(pk, "", "empty@example.com", 0L, city, "00000"));
            break;
          case 1:
            out.add(new Row(pk, "neg", "neg@example.com", -42L, city, "11111"));
            break;
          case 2:
            out.add(new Row(pk, repeat("a", 256), "long@example.com", 1L, city, "22222"));
            break;
          case 3:
            out.add(new Row(pk, "big", "big@example.com", 9_000_000_000L, city, "33333"));
            break;
          default:
            out.add(new Row(pk, "edge", "edge@example.com", 1L, city, "44444"));
            break;
        }
      } else {
        String name = names[rng.nextInt(names.length)];
        long score = (long) rng.nextInt(1000);
        String email = name + "@example.com";
        String zip = String.format("%05d", rng.nextInt(99999));
        out.add(new Row(pk, name, email, score, city, zip));
      }
    }
    return Collections.unmodifiableList(out);
  }

  /** BSON-flat shape: {"name":..., "email":..., "city":...}. Null rows omit name+email. */
  public static BsonDocument toBsonFlat(Row r) {
    StringBuilder sb = new StringBuilder("{");
    if (r.name != null) sb.append("\"name\":").append(jsonStr(r.name)).append(",");
    if (r.email != null) sb.append("\"email\":").append(jsonStr(r.email)).append(",");
    sb.append("\"city\":").append(jsonStr(r.city));
    sb.append("}");
    return BsonDocument.parse(sb.toString());
  }

  /** BSON-nested shape: {"profile":{"score":...,"city":...},"name":...}. Sparse rows omit profile. */
  public static BsonDocument toBsonNested(Row r) {
    StringBuilder sb = new StringBuilder("{");
    if (r.score != null) {
      sb.append("\"profile\":{\"score\":").append(r.score)
          .append(",\"city\":").append(jsonStr(r.city)).append("},");
    }
    if (r.name != null) sb.append("\"name\":").append(jsonStr(r.name)).append(",");
    sb.append("\"city\":").append(jsonStr(r.city));
    sb.append("}");
    return BsonDocument.parse(sb.toString());
  }

  /** JSON-flat (string) — same shape as BSON-flat. */
  public static String toJsonFlat(Row r) {
    return toBsonFlat(r).toJson();
  }

  /** JSON-nested (string) — same shape as BSON-nested. */
  public static String toJsonNested(Row r) {
    return toBsonNested(r).toJson();
  }

  private static String jsonStr(String s) {
    StringBuilder sb = new StringBuilder("\"");
    for (int i = 0; i < s.length(); i++) {
      char c = s.charAt(i);
      if (c == '"' || c == '\\') sb.append('\\');
      sb.append(c);
    }
    sb.append('"');
    return sb.toString();
  }

  private static String repeat(String s, int n) {
    StringBuilder sb = new StringBuilder(s.length() * n);
    for (int i = 0; i < n; i++) sb.append(s);
    return sb.toString();
  }
}
