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
package org.apache.phoenix.compile.keyspace.oracle;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Collections;
import java.util.List;
import java.util.Properties;

import org.apache.phoenix.compile.ColumnResolver;
import org.apache.phoenix.compile.FromCompiler;
import org.apache.phoenix.compile.QueryPlan;
import org.apache.phoenix.compile.ScanRanges;
import org.apache.phoenix.compile.StatementContext;
import org.apache.phoenix.compile.WhereCompiler;
import org.apache.phoenix.expression.Expression;
import org.apache.phoenix.jdbc.PhoenixConnection;
import org.apache.phoenix.jdbc.PhoenixPreparedStatement;
import org.apache.phoenix.jdbc.PhoenixStatement;
import org.apache.phoenix.parse.ParseNode;
import org.apache.phoenix.parse.SQLParser;
import org.apache.phoenix.parse.SelectStatement;
import org.apache.phoenix.schema.PTable;

/**
 * End-to-end harness: given a CREATE TABLE and a SELECT query, run both V2 and the oracle
 * over a shared enumerated row domain and report any soundness or widening divergences.
 * <p>
 * The runner:
 * <ol>
 * <li>Creates the table via the supplied DDL statements.</li>
 * <li>Compiles the SELECT — V2 runs as part of compilation and populates
 *     {@link ScanRanges}.</li>
 * <li>Re-parses the SELECT to grab its WHERE {@link ParseNode}, then runs just
 *     {@link WhereCompiler#compile(StatementContext, ParseNode)} to get a raw
 *     {@link Expression} tree (no V2 rewrite). This is the input the oracle operates on.</li>
 * <li>Converts via {@link ExpressionAdapter} to {@link AbstractExpression}; unsupported
 *     shapes become {@link AbstractExpression#unknown(String)} leaves.</li>
 * <li>Builds an enumeration grid via {@link EnumerationGrid#build(AbstractExpression, int)}.</li>
 * <li>Decodes {@link ScanRanges} via {@link ScanRangesDecoder}. If the decoder can't handle
 *     the shape (salted, slotSpan issues, etc.), the result is {@link Report#skipped}.</li>
 * <li>Runs the oracle on the abstract expression.</li>
 * <li>Calls {@link HarnessAssertions#evaluate} to get the soundness report.</li>
 * </ol>
 */
public final class HarnessRunner {

  private HarnessRunner() {}

  /** A final report from one run. */
  public static final class Report {
    public final String query;
    public final boolean skipped;
    public final String skipReason;
    public final AbstractExpression expr;
    public final AbstractKeySpaceList oracleView;
    public final AbstractKeySpaceList v2View;
    public final HarnessAssertions.Report assertions;

    public Report(String query, boolean skipped, String skipReason, AbstractExpression expr,
      AbstractKeySpaceList oracleView, AbstractKeySpaceList v2View,
      HarnessAssertions.Report assertions) {
      this.query = query;
      this.skipped = skipped;
      this.skipReason = skipReason;
      this.expr = expr;
      this.oracleView = oracleView;
      this.v2View = v2View;
      this.assertions = assertions;
    }

    public static Report skip(String query, String reason) {
      return new Report(query, true, reason, null, null, null, null);
    }

    @Override
    public String toString() {
      if (skipped) {
        return "Report[SKIPPED, query=" + query + ", reason=" + skipReason + "]";
      }
      return "Report[query=" + query + ", expr=" + expr
        + ", oracle=" + oracleView + ", v2=" + v2View + ", " + assertions + "]";
    }
  }

  /**
   * Runs the harness for a single query.
   * @param jdbcUrl         Phoenix JDBC URL for a connectionless or real instance
   * @param ddlStatements   list of CREATE TABLE (or related) statements to run before the
   *                        query; may be empty if the table already exists
   * @param query           the SELECT query to inspect
   * @param gridSizeCap     maximum enumeration grid size; exceeding this causes a SKIP
   */
  public static Report run(String jdbcUrl, List<String> ddlStatements, String query,
    long gridSizeCap) {
    Properties props = new Properties();
    try (Connection conn = DriverManager.getConnection(jdbcUrl, props)) {
      for (String ddl : ddlStatements) {
        conn.createStatement().execute(ddl);
      }
      PhoenixConnection pconn = conn.unwrap(PhoenixConnection.class);

      // 1. Compile the query (V2 runs here) to get ScanRanges.
      PhoenixPreparedStatement pstmt = new PhoenixPreparedStatement(pconn, query);
      QueryPlan plan = pstmt.compileQuery();
      ScanRanges sr = plan.getContext().getScanRanges();
      PTable table = plan.getContext().getCurrentTable().getTable();
      if (table.getBucketNum() != null) {
        return Report.skip(query, "salted table");
      }
      if (table.isMultiTenant() && pconn.getTenantId() != null) {
        return Report.skip(query, "multi-tenant connection");
      }
      if (table.getViewIndexId() != null) {
        return Report.skip(query, "view-index table");
      }
      if (table.getPKColumns() == null || table.getPKColumns().isEmpty()) {
        return Report.skip(query, "no PK columns");
      }

      // 2. Re-parse to grab the raw WHERE expression (pre-V2-rewrite).
      //
      // Production compiles a query through StatementNormalizer first — that pass lowers
      // shapes like BETWEEN into their AND/OR expansions so downstream compilers see only
      // primitive ops. Our harness takes a shortcut to get an Expression tree, but that
      // shortcut skips StatementNormalizer. Pre-normalize the parse node here so BETWEEN
      // and similar transforms are applied before WhereCompiler.compile.
      Expression whereExpr;
      try {
        SelectStatement select = (SelectStatement) new SQLParser(query).parseStatement();
        ParseNode whereNode = select.getWhere();
        if (whereNode == null) {
          return Report.skip(query, "no WHERE clause");
        }
        PhoenixStatement freshStmt = new PhoenixStatement(pconn);
        ColumnResolver resolver = FromCompiler.getResolverForQuery(select, pconn);
        ParseNode normalizedWhere =
          org.apache.phoenix.compile.StatementNormalizer.normalize(whereNode, resolver);
        StatementContext ctx = new StatementContext(freshStmt, resolver, new org.apache.hadoop
          .hbase.client.Scan(), new org.apache.phoenix.compile.SequenceManager(freshStmt));
        whereExpr = WhereCompiler.compile(ctx, normalizedWhere);
      } catch (Exception e) {
        return Report.skip(query, "could not re-compile WHERE: " + e.getMessage());
      }
      if (whereExpr == null) {
        return Report.skip(query, "WHERE compiled to null");
      }

      // 3. Convert to AbstractExpression.
      ExpressionAdapter adapter = new ExpressionAdapter(table);
      AbstractExpression abstractExpr = adapter.convert(whereExpr);

      // 4. Build enumeration grid.
      int nPk = table.getPKColumns().size();
      List<List<Object>> grid = EnumerationGrid.build(abstractExpr, nPk);
      long gridSize = EnumerationGrid.estimateSize(grid);
      if (gridSize > gridSizeCap) {
        return Report.skip(query,
          "grid size " + gridSize + " exceeds cap " + gridSizeCap);
      }

      // 5. Decode ScanRanges.
      AbstractKeySpaceList v2View;
      try {
        v2View = ScanRangesDecoder.decode(sr, table);
      } catch (ScanRangesDecoder.UnsupportedEncodingShape e) {
        return Report.skip(query, "decoder: " + e.getMessage());
      }

      // 6. Run oracle.
      AbstractKeySpaceList oracleView = Oracle.extract(abstractExpr, nPk);

      // 7. Evaluate.
      List<HarnessAssertions.Row> rows = HarnessAssertions.enumerateRows(grid);
      HarnessAssertions.Report rpt =
        HarnessAssertions.evaluate(abstractExpr, oracleView, v2View, rows);
      return new Report(query, false, null, abstractExpr, oracleView, v2View, rpt);
    } catch (SQLException e) {
      return Report.skip(query, "setup failure: " + e.getMessage());
    }
  }

  /** Convenience: single DDL + query. */
  public static Report run(String jdbcUrl, String ddl, String query, long gridSizeCap) {
    return run(jdbcUrl, Collections.singletonList(ddl), query, gridSizeCap);
  }
}
