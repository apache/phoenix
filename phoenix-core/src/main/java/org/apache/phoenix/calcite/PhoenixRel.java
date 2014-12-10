package org.apache.phoenix.calcite;

import java.util.List;

import org.apache.calcite.plan.Convention;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rex.RexNode;
import org.apache.phoenix.compile.QueryPlan;
import org.apache.phoenix.compile.RowProjector;
import org.apache.phoenix.expression.ColumnExpression;
import org.apache.phoenix.jdbc.PhoenixConnection;
import org.apache.phoenix.schema.PTable;

/**
 * Relational expression in Phoenix.
 *
 * <p>Phoenix evaluates relational expressions using {@link java.util.Iterator}s
 * over streams of {@link org.apache.phoenix.schema.tuple.Tuple}s.</p>
 */
public interface PhoenixRel extends RelNode {
  /** Calling convention for relational operations that occur in Phoenix. */
  Convention CONVENTION = new Convention.Impl("PHOENIX", PhoenixRel.class);

  /** Relative cost of Phoenix versus Enumerable convention.
   *
   * <p>Multiply by the value (which is less than unity), and you will get a cheaper cost.
   * Phoenix is cheaper.
   */
  double PHOENIX_FACTOR = 0.5;

  void implement(Implementor implementor, PhoenixConnection conn);

  /** Holds context for an traversal over a tree of relational expressions
   * to convert it to an executable plan. */
  interface Implementor {
    void visitInput(int i, PhoenixRel input);
    ColumnExpression newColumnExpression(int index);
    void setContext(PhoenixConnection conn, PTable pTable, RexNode filter);
    void setProjects(List<? extends RexNode> projects);
    QueryPlan makePlan();
  }
}
