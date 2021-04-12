/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.phoenix.end2end;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.security.MessageDigest;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;

import org.junit.Test;


public class MD5FunctionIT extends ParallelStatsDisabledIT {
  
  @Test
  public void testRetrieve() throws Exception {
      String testString = "mwalsh";
      
      Connection conn = DriverManager.getConnection(getUrl());
      String md5_retrieve_test = generateUniqueName();
      String ddl =
          "CREATE TABLE IF NOT EXISTS " + md5_retrieve_test + " (pk VARCHAR NOT NULL PRIMARY KEY)";
      conn.createStatement().execute(ddl);
      String dml = String.format("UPSERT INTO " + md5_retrieve_test + " VALUES('%s')", testString);
      conn.createStatement().execute(dml);
      conn.commit();
      
      ResultSet rs = conn.createStatement().executeQuery("SELECT MD5(pk) FROM " + md5_retrieve_test);
      assertTrue(rs.next());
      byte[] first = MessageDigest.getInstance("MD5").digest(testString.getBytes());
      byte[] second = rs.getBytes(1);
      assertArrayEquals(first, second);
      assertFalse(rs.next());
  }      
  
  @Test
  public void testRetrieveCompositeKey() throws Exception {
      String testString = "FOOBAR";
      
      Connection conn = DriverManager.getConnection(getUrl());
      String md5_retrieve_test = generateUniqueName();
      String ddl = "CREATE TABLE IF NOT EXISTS " + md5_retrieve_test
          + " (k1 CHAR(3) NOT NULL, k2 CHAR(3) NOT NULL, CONSTRAINT PK PRIMARY KEY (K1,K2))";
      conn.createStatement().execute(ddl);
      String dml = "UPSERT INTO " + md5_retrieve_test + " VALUES('FOO','BAR')";
      conn.createStatement().execute(dml);
      conn.commit();
      
      ResultSet rs = conn.createStatement().executeQuery(
          "SELECT MD5((K1,K2)) FROM " + md5_retrieve_test);
      assertTrue(rs.next());
      byte[] first = MessageDigest.getInstance("MD5").digest(testString.getBytes());
      byte[] second = rs.getBytes(1);
      assertArrayEquals(first, second);
      assertFalse(rs.next());
  }      
  
  @Test
  public void testUpsert() throws Exception {
      String testString1 = "mwalsh1";
      String testString2 = "mwalsh2";
      
      Connection conn = DriverManager.getConnection(getUrl());
      String md5_upsert_test = generateUniqueName();
      String ddl = "CREATE TABLE IF NOT EXISTS " + md5_upsert_test
          + " (k1 binary(16) NOT NULL,k2 binary(16) NOT NULL  CONSTRAINT pk PRIMARY KEY (k1, k2))";
      conn.createStatement().execute(ddl);
      String dml = String.format("UPSERT INTO " + md5_upsert_test + " VALUES(md5('%s'),md5('%s'))", testString1, testString2);
      conn.createStatement().execute(dml);
      conn.commit();
      
      ResultSet rs = conn.createStatement().executeQuery("SELECT k1,k2 FROM " + md5_upsert_test);
      assertTrue(rs.next());
      byte[] pk1 = MessageDigest.getInstance("MD5").digest(testString1.getBytes());
      byte[] pk2 = MessageDigest.getInstance("MD5").digest(testString2.getBytes());
      assertArrayEquals(pk1, rs.getBytes(1));
      assertArrayEquals(pk2, rs.getBytes(2));
      assertFalse(rs.next());
      PreparedStatement stmt = conn.prepareStatement(
          "SELECT k1,k2 FROM " + md5_upsert_test + " WHERE k1=md5(?)");
      stmt.setString(1, testString1);
      rs = stmt.executeQuery();
      assertTrue(rs.next());
      byte[] second1 = rs.getBytes(1);
      byte[] second2 = rs.getBytes(2);
      assertArrayEquals(pk1, second1);
      assertArrayEquals(pk2, second2);
      assertFalse(rs.next());
  }                                                           

}
