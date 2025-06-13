/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to you under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.phoenix.jdbc;

import java.sql.SQLException;
import java.util.List;

import org.apache.phoenix.compile.RowProjector;
import org.apache.phoenix.compile.StatementContext;
import org.apache.phoenix.schema.tuple.Tuple;

/**
 * Phoenix ResultSet implementation with prefetched rows. It is expected that the implementation
 * does not need to use any iterators to make server side RPC call.
 */
public class PhoenixPrefetchedResultSet extends PhoenixResultSet {

  private final List<Tuple> prefetchedRows;
  private int prefetchedRowsIndex;

  public PhoenixPrefetchedResultSet(RowProjector rowProjector,
      StatementContext ctx, List<Tuple> prefetchedRows) throws SQLException {
    super(null, rowProjector, ctx);
    this.prefetchedRows = prefetchedRows;
    this.prefetchedRowsIndex = 0;
  }

  @Override
  protected Tuple getCurrentRowImpl() {
    return prefetchedRows.size() > prefetchedRowsIndex ? prefetchedRows.get(prefetchedRowsIndex++)
            : null;
  }

}
