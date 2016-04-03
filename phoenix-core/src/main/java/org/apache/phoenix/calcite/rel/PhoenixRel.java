package org.apache.phoenix.calcite.rel;

import java.util.List;

import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.metadata.RelMetadataProvider;
import org.apache.calcite.util.ImmutableIntList;
import org.apache.phoenix.calcite.PhoenixSequence;
import org.apache.phoenix.calcite.TableMapping;
import org.apache.phoenix.calcite.metadata.PhoenixRelMetadataProvider;
import org.apache.phoenix.compile.QueryPlan;
import org.apache.phoenix.compile.SequenceManager;
import org.apache.phoenix.compile.SequenceValueExpression;
import org.apache.phoenix.execute.RuntimeContext;
import org.apache.phoenix.execute.TupleProjector;
import org.apache.phoenix.expression.ColumnExpression;
import org.apache.phoenix.expression.Expression;
import org.apache.phoenix.parse.SequenceValueParseNode;
import org.apache.phoenix.schema.types.PDataType;

/**
 * Relational expression in Phoenix.
 *
 * <p>Phoenix evaluates relational expressions using {@link java.util.Iterator}s
 * over streams of {@link org.apache.phoenix.schema.tuple.Tuple}s.</p>
 */
public interface PhoenixRel extends RelNode {

  /** Metadata Provider for PhoenixRel */
  RelMetadataProvider METADATA_PROVIDER = new PhoenixRelMetadataProvider();
  
  /** For test purpose */
  String ROW_COUNT_FACTOR = "phoenix.calcite.metadata.rowcount.factor";

  /** Relative cost of Phoenix versus Enumerable convention.
   *
   * <p>Multiply by the value (which is less than unity), and you will get a cheaper cost.
   * Phoenix is cheaper.
   */
  double PHOENIX_FACTOR = 0.0001;

  /** Relative cost of server plan versus client plan.
   *
   * <p>Multiply by the value (which is less than unity), and you will get a cheaper cost.
   * Server is cheaper.
   */
  double SERVER_FACTOR = 0.2;

  QueryPlan implement(Implementor implementor);
  
  class ImplementorContext {
      public final boolean retainPKColumns;
      public final boolean forceProject;
      public final ImmutableIntList columnRefList;
      
      public ImplementorContext(boolean retainPKColumns, boolean forceProject, ImmutableIntList columnRefList) {
          this.retainPKColumns = retainPKColumns;
          this.forceProject = forceProject;
          this.columnRefList = columnRefList;
      }
      
      public ImplementorContext withColumnRefList(ImmutableIntList columnRefList) {
          return new ImplementorContext(this.retainPKColumns, this.forceProject, columnRefList);
      }
  }

  /** Holds context for an traversal over a tree of relational expressions
   * to convert it to an executable plan. */
  interface Implementor {
    QueryPlan visitInput(int i, PhoenixRel input);
    ColumnExpression newColumnExpression(int index);
    @SuppressWarnings("rawtypes")
    Expression newFieldAccessExpression(String variableId, int index, PDataType type);
    SequenceValueExpression newSequenceExpression(PhoenixSequence seq, SequenceValueParseNode.Op op);
    RuntimeContext getRuntimeContext();
    void setTableMapping(TableMapping tableMapping);
    TableMapping getTableMapping();
    void setSequenceManager(SequenceManager sequenceManager);
    void pushContext(ImplementorContext context);
    ImplementorContext popContext();
    ImplementorContext getCurrentContext();
    TupleProjector project(List<Expression> exprs);
  }
}
