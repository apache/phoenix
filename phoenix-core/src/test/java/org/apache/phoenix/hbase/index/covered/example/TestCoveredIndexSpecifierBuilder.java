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

import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.phoenix.hbase.index.covered.example.ColumnGroup;
import org.apache.phoenix.hbase.index.covered.example.CoveredColumn;
import org.apache.phoenix.hbase.index.covered.example.CoveredColumnIndexSpecifierBuilder;
import org.junit.Test;

public class TestCoveredIndexSpecifierBuilder {
  private static final String FAMILY = "FAMILY";
  private static final String FAMILY2 = "FAMILY2";
  private static final String INDEX_TABLE = "INDEX_TABLE";
  private static final String INDEX_TABLE2 = "INDEX_TABLE2";


  @Test
  public void testSimpleSerialziationDeserialization() throws Exception {
    byte[] indexed_qualifer = Bytes.toBytes("indexed_qual");

    //setup the index 
    CoveredColumnIndexSpecifierBuilder builder = new CoveredColumnIndexSpecifierBuilder();
    ColumnGroup fam1 = new ColumnGroup(INDEX_TABLE);
    // match a single family:qualifier pair
    CoveredColumn col1 = new CoveredColumn(FAMILY, indexed_qualifer);
    fam1.add(col1);
    // matches the family2:* columns
    CoveredColumn col2 = new CoveredColumn(FAMILY2, null);
    fam1.add(col2);
    builder.addIndexGroup(fam1);
    ColumnGroup fam2 = new ColumnGroup(INDEX_TABLE2);
    // match a single family2:qualifier pair
    CoveredColumn col3 = new CoveredColumn(FAMILY2, indexed_qualifer);
    fam2.add(col3);
    builder.addIndexGroup(fam2);
    
    Configuration conf = new Configuration(false);
    //convert the map that HTableDescriptor gets into the conf the coprocessor receives
    Map<String, String> map = builder.convertToMap();
    for(Entry<String, String> entry: map.entrySet()){
      conf.set(entry.getKey(), entry.getValue());
    }

    List<ColumnGroup> columns = CoveredColumnIndexSpecifierBuilder.getColumns(conf);
    assertEquals("Didn't deserialize the expected number of column groups", 2, columns.size());
    ColumnGroup group = columns.get(0);
    assertEquals("Didn't deserialize expected column in first group", col1, group.getColumnForTesting(0));
    assertEquals("Didn't deserialize expected column in first group", col2, group.getColumnForTesting(1));
    group = columns.get(1);
    assertEquals("Didn't deserialize expected column in second group", col3, group.getColumnForTesting(0));
  }
}