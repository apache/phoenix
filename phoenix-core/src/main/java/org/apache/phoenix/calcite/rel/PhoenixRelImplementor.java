package org.apache.phoenix.calcite.rel;

import java.util.List;

import org.apache.calcite.util.ImmutableIntList;
import org.apache.phoenix.calcite.PhoenixSequence;
import org.apache.phoenix.calcite.TableMapping;
import org.apache.phoenix.compile.QueryPlan;
import org.apache.phoenix.compile.SequenceManager;
import org.apache.phoenix.compile.SequenceValueExpression;
import org.apache.phoenix.compile.StatementContext;
import org.apache.phoenix.execute.RuntimeContext;
import org.apache.phoenix.execute.TupleProjector;
import org.apache.phoenix.expression.Expression;
import org.apache.phoenix.parse.SequenceValueParseNode;
import org.apache.phoenix.schema.types.PDataType;

/** Holds context for an traversal over a tree of relational expressions
 * to convert it to an executable plan. */
public interface PhoenixRelImplementor {
    QueryPlan visitInput(int i, PhoenixQueryRel input);
    Expression newColumnExpression(int index);
    @SuppressWarnings("rawtypes")
    Expression newBindParameterExpression(int index, PDataType type, Integer maxLength);
    @SuppressWarnings("rawtypes")
    Expression newFieldAccessExpression(String variableId, int index, PDataType type);
    SequenceValueExpression newSequenceExpression(PhoenixSequence seq, SequenceValueParseNode.Op op);
    StatementContext getStatementContext();
    RuntimeContext getRuntimeContext();
    void setTableMapping(TableMapping tableMapping);
    TableMapping getTableMapping();
    void setSequenceManager(SequenceManager sequenceManager);
    void pushContext(ImplementorContext context);
    ImplementorContext popContext();
    ImplementorContext getCurrentContext();
    TupleProjector project(List<Expression> exprs);
    
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
}
