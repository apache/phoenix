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
package org.apache.phoenix.end2end.tpcds;

import java.sql.Connection;
import java.sql.DriverManager;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Developer only tool that prints the embedded {@code <LABEL>_EXPECTED} arrays for every query in
 * {@link TPCDSLikeSingleChannelIT} and {@link TPCDSLikeCrossChannelIT}.
 */
public final class TPCDSLikeExpectedRegenerator {

  private TPCDSLikeExpectedRegenerator() {
  }

  public static void main(String[] args) throws Exception {
    if (args.length == 0) {
      System.out.println(usage());
      return;
    }
    String url = args[0];
    Map<String, String> all = new LinkedHashMap<>();
    all.putAll(TPCDSLikeSingleChannelIT.queries());
    all.putAll(TPCDSLikeCrossChannelIT.queries());
    try (Connection conn = DriverManager.getConnection(url)) {
      TPCDSLikeFixture.load(conn);
      System.out.println("// ===== TpcdsLikeSingleChannelIT / TpcdsLikeCrossChannelIT expected"
        + " arrays (regenerated) =====");
      for (Map.Entry<String, String> e : all.entrySet()) {
        // Queries are written against TpcdsLikeFixture.SCHEMA, which is what load() populates.
        System.out.println(TPCDSLikeAssertions.captureLiteral(conn, e.getKey(), e.getValue()));
      }
    }
  }

  private static String usage() {
    return "TPCDSLikeExpectedRegenerator: prints paste-ready *_EXPECTED arrays.\n\n"
      + "Recommended (uses the test mini-cluster, no external setup):\n"
      + "  mvn -pl phoenix-core verify \\\n"
      + "      -Dit.test=TPCDSLikeSingleChannelIT,TPCDSLikeCrossChannelIT \\\n" + "      -D"
      + TPCDSLikeAssertions.REGENERATE_PROPERTY + "=true\n\n"
      + "Against a standalone Phoenix cluster:\n" + "  java ... "
      + TPCDSLikeExpectedRegenerator.class.getName() + " <jdbc-url>\n";
  }
}
