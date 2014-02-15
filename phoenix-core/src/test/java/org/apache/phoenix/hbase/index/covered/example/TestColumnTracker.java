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
package org.apache.phoenix.hbase.index.covered.example;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.util.ArrayList;
import java.util.Collection;

import org.junit.Test;

import org.apache.phoenix.hbase.index.covered.update.ColumnReference;
import org.apache.phoenix.hbase.index.covered.update.ColumnTracker;

public class TestColumnTracker {

  @Test
  public void testEnsureGuarranteedMinValid() {
    assertFalse("Guarranted min wasn't recognized as having newer timestamps!",
      ColumnTracker.isNewestTime(ColumnTracker.GUARANTEED_NEWER_UPDATES));
  }

  @Test
  public void testOnlyKeepsOlderTimestamps() {
    Collection<ColumnReference> columns = new ArrayList<ColumnReference>();
    ColumnTracker tracker = new ColumnTracker(columns);
    tracker.setTs(10);
    assertEquals("Column tracker didn't set original TS", 10, tracker.getTS());
    tracker.setTs(12);
    assertEquals("Column tracker allowed newer timestamp to be set.", 10, tracker.getTS());
    tracker.setTs(9);
    assertEquals("Column tracker didn't decrease set timestamp for smaller value", 9,
      tracker.getTS());
  }

  @Test
  public void testHasNewerTimestamps() throws Exception {
    Collection<ColumnReference> columns = new ArrayList<ColumnReference>();
    ColumnTracker tracker = new ColumnTracker(columns);
    assertFalse("Tracker has newer timestamps when no ts set", tracker.hasNewerTimestamps());
    tracker.setTs(10);
    assertTrue("Tracker doesn't have newer timetamps with set ts", tracker.hasNewerTimestamps());
  }
}