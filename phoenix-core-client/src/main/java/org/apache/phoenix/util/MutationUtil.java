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
package org.apache.phoenix.util;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.client.Put;

public class MutationUtil {

  private MutationUtil() {
  }

  /**
   * Creates a true deep copy of the Put Mutation, including deep copies of all cells if the cells
   * are backed by off-heap. The Mutation(Mutation source) constructor always does a shallow copy of
   * the cells.
   * @param original The original Put Mutation to copy
   * @return A new Put Mutation with deep copies of all fields and Cells
   */
  public static Put copyPut(Put original) throws IOException {
    return copyPut(original, false);
  }

  /**
   * Creates a true deep copy of the Put Mutation, including deep copies of all cells if the cells
   * are backed by off-heap. The Mutation(Mutation source) constructor always does a shallow copy of
   * the cells.
   * @param original       The original Put Mutation to copy
   * @param skipAttributes If true, the attributes are not copied
   * @return A new Put Mutation with deep copies of all fields and Cells
   */
  public static Put copyPut(Put original, boolean skipAttributes) throws IOException {
    // Copies the bytes internally
    Put copy = new Put(original.getRow());

    // Copy the fields in Mutation class
    // Copy timestamp
    copy.setTimestamp(original.getTimestamp());
    // Copy durability
    copy.setDurability(original.getDurability());

    // Copy the fields in OperationWithAttributes class
    // Copy attributes
    if (!skipAttributes) {
      for (Map.Entry<String, byte[]> entry : original.getAttributesMap().entrySet()) {
        copy.setAttribute(entry.getKey(), entry.getValue().clone());
      }
    }
    // copy priority
    copy.setPriority(original.getPriority());

    for (List<Cell> cells : original.getFamilyCellMap().values()) {
      for (Cell cell : cells) {
        // copy cell if needed
        copy.add(CellUtil.cloneIfNecessary(cell));
      }
    }
    return copy;
  }
}
