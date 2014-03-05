/**
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
package org.apache.phoenix.hbase.index.covered.filter;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.filter.BinaryComparator;
import org.apache.hadoop.hbase.filter.ByteArrayComparable;
import org.apache.hadoop.hbase.filter.FamilyFilter;

/**
 * Similar to the {@link FamilyFilter} but stops when the end of the family is reached and only
 * supports equality
 */
public class FamilyOnlyFilter extends FamilyFilter {

  boolean done = false;
  private boolean previousMatchFound;

  /**
   * Filter on exact binary matches to the passed family
   * @param family to compare against
   */
  public FamilyOnlyFilter(final byte[] family) {
    this(new BinaryComparator(family));
  }

  public FamilyOnlyFilter(final ByteArrayComparable familyComparator) {
    super(CompareOp.EQUAL, familyComparator);
  }


  @Override
  public boolean filterAllRemaining() {
    return done;
  }

  @Override
  public void reset() {
    done = false;
    previousMatchFound = false;
  }

  @Override
  public ReturnCode filterKeyValue(Cell v) {
    if (done) {
      return ReturnCode.SKIP;
    }
    ReturnCode code = super.filterKeyValue(v);
    if (previousMatchFound) {
      // we found a match before, and now we are skipping the key because of the family, therefore
      // we are done (no more of the family).
      if (code.equals(ReturnCode.SKIP)) {
      done = true;
      }
    } else {
      // if we haven't seen a match before, then it doesn't matter what we see now, except to mark
      // if we've seen a match
      if (code.equals(ReturnCode.INCLUDE)) {
        previousMatchFound = true;
      }
    }
    return code;
  }

}
