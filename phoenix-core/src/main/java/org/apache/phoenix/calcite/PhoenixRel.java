package org.apache.phoenix.calcite;

import java.util.List;

import org.apache.calcite.plan.Convention;
import org.apache.calcite.rel.RelNode;
import org.apache.phoenix.compile.QueryPlan;
import org.apache.phoenix.compile.RowProjector;
import org.apache.phoenix.execute.TupleProjector;
import org.apache.phoenix.expression.ColumnExpression;
import org.apache.phoenix.expression.Expression;
import org.apache.phoenix.schema.PTable;
import org.apache.phoenix.schema.TableRef;

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

  /** Relative cost of server plan versus client plan.
   *
   * <p>Multiply by the value (which is less than unity), and you will get a cheaper cost.
   * Server is cheaper.
   */
  double SERVER_FACTOR = 0.2;

  QueryPlan implement(Implementor implementor);
  
  class ImplementorContext {
      private boolean retainPKColumns;
      private boolean forceProject;
      
      public ImplementorContext(boolean retainPKColumns, boolean forceProject) {
          this.retainPKColumns = retainPKColumns;
          this.forceProject = forceProject;
      }
      
      public boolean isRetainPKColumns() {
          return this.retainPKColumns;
      }
      
      public boolean forceProject() {
          return this.forceProject;
      }
  }

  /** Holds context for an traversal over a tree of relational expressions
   * to convert it to an executable plan. */
  interface Implementor {
    QueryPlan visitInput(int i, PhoenixRel input);
    ColumnExpression newColumnExpression(int index);
    void setTableRef(TableRef tableRef);
    TableRef getTableRef();
    void pushContext(ImplementorContext context);
    ImplementorContext popContext();
    ImplementorContext getCurrentContext();
    PTable createProjectedTable();
    RowProjector createRowProjector();
    TupleProjector project(List<Expression> exprs);
  }
}
