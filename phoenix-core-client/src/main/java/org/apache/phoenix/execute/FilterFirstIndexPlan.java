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
package org.apache.phoenix.execute;

import org.apache.phoenix.compile.QueryPlan;
import org.apache.phoenix.schema.TableRef;

/**
 * A filter-first query plan for vector search where an uncovered secondary index evaluates the
 * relational filter before projecting columns from the data table.
 * <p>
 * The driving secondary index is exposed via {@link #getIndexTableRef()} so the optimizer can rank
 * the candidate plan against vector index plans, while {@link #getTableRef()} returns the
 * underlying data table scanned during execution.
 */
public class FilterFirstIndexPlan extends DelegateQueryPlan {

  private final TableRef indexTableRef;

  public FilterFirstIndexPlan(QueryPlan delegate, TableRef indexTableRef) {
    super(delegate);
    this.indexTableRef = indexTableRef;
  }

  /** Returns the secondary index evaluated for the relational filter. */
  public TableRef getIndexTableRef() {
    return indexTableRef;
  }

  @Override
  public String toString() {
    return "FilterFirstIndexPlan [index=" + indexTableRef.getTable().getName().getString()
      + ", delegate=" + delegate + "]";
  }
}
