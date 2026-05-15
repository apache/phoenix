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
 */
package org.apache.phoenix.end2end.index.dyncol;

import static org.apache.phoenix.util.TestUtil.TEST_PROPERTIES;
import static org.junit.Assert.assertEquals;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.Random;

import org.apache.phoenix.end2end.ParallelStatsDisabledIT;
import org.apache.phoenix.end2end.ParallelStatsDisabledTest;
import org.apache.phoenix.util.PropertiesUtil;
import org.junit.Test;
import org.junit.experimental.categories.Category;

/**
 * Side-by-side correctness: across a 200-row randomized fixture with
 * sparse virtual-column population, the same SELECT must return identical
 * results with the index ENABLED vs. DISABLED.
 */
@Category(ParallelStatsDisabledTest.class)
public class DynamicColumnIndexConsistencyIT extends ParallelStatsDisabledIT {

  private static final int N_ROWS = 200;
  private static final int N_QUERIES = 50;
  private static final long SEED = 0xC0FFEEL;

  private Connection conn() throws Exception {
    Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
    return DriverManager.getConnection(getUrl(), props);
  }

  @Test
  public void resultsMatchWithAndWithoutIndex() throws Exception {
    String table = generateUniqueName();
    String index = generateUniqueName();
    Random rng = new Random(SEED);

    try (Connection c = conn()) {
      c.createStatement().execute(
          "CREATE TABLE " + table + " (pk VARCHAR PRIMARY KEY, regular VARCHAR) "
              + "COLUMN_ENCODED_BYTES=0");
      c.createStatement().execute(
          "CREATE INDEX " + index + " ON " + table + " (extra VARCHAR DYNAMIC)");

      for (int i = 0; i < N_ROWS; i++) {
        String pk = String.format("p%05d", i);
        if (rng.nextInt(100) < 60) {
          String v = "v" + rng.nextInt(20);
          PreparedStatement ps = c.prepareStatement(
              "UPSERT INTO " + table + " (pk, extra) VALUES (?, ?)");
          ps.setString(1, pk);
          ps.setString(2, v);
          ps.executeUpdate();
        } else {
          PreparedStatement ps = c.prepareStatement(
              "UPSERT INTO " + table + " (pk, regular) VALUES (?, ?)");
          ps.setString(1, pk);
          ps.setString(2, "r" + i);
          ps.executeUpdate();
        }
      }
      c.commit();

      for (int q = 0; q < N_QUERIES; q++) {
        String v = "v" + rng.nextInt(20);
        String withIndex = "SELECT pk FROM " + table + " WHERE extra = ?";
        String noIndex = "SELECT /*+ NO_INDEX */ pk FROM " + table + " WHERE extra = ?";

        List<String> a = collect(c, withIndex, v);
        List<String> b = collect(c, noIndex, v);
        assertEquals("query " + q + " (extra='" + v + "')", b, a);
      }
    }
  }

  private static List<String> collect(Connection c, String sql, String v) throws Exception {
    PreparedStatement ps = c.prepareStatement(sql);
    ps.setString(1, v);
    ResultSet rs = ps.executeQuery();
    List<String> out = new ArrayList<>();
    while (rs.next()) out.add(rs.getString(1));
    java.util.Collections.sort(out);
    return out;
  }
}
