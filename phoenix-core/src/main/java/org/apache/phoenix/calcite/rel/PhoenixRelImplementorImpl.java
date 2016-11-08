package org.apache.phoenix.calcite.rel;

import java.sql.SQLException;
import java.util.Collections;
import java.util.List;
import java.util.Stack;

import org.apache.phoenix.calcite.PhoenixSequence;
import org.apache.phoenix.calcite.TableMapping;
import org.apache.phoenix.compile.QueryPlan;
import org.apache.phoenix.compile.SequenceManager;
import org.apache.phoenix.compile.SequenceValueExpression;
import org.apache.phoenix.compile.StatementContext;
import org.apache.phoenix.coprocessor.MetaDataProtocol;
import org.apache.phoenix.execute.RuntimeContext;
import org.apache.phoenix.execute.TupleProjector;
import org.apache.phoenix.expression.BindParameterExpression;
import org.apache.phoenix.expression.CorrelateVariableFieldAccessExpression;
import org.apache.phoenix.expression.Expression;
import org.apache.phoenix.parse.ParseNodeFactory;
import org.apache.phoenix.parse.SequenceValueParseNode;
import org.apache.phoenix.parse.TableName;
import org.apache.phoenix.schema.KeyValueSchema;
import org.apache.phoenix.schema.PColumn;
import org.apache.phoenix.schema.PColumnImpl;
import org.apache.phoenix.schema.PName;
import org.apache.phoenix.schema.PNameFactory;
import org.apache.phoenix.schema.PTable;
import org.apache.phoenix.schema.PTableImpl;
import org.apache.phoenix.schema.PTableType;
import org.apache.phoenix.schema.types.PDataType;
import com.google.common.collect.Lists;

public class PhoenixRelImplementorImpl implements PhoenixRelImplementor {
    private final StatementContext statementContext;
    private final RuntimeContext runtimeContext;
	private Stack<ImplementorContext> contextStack;
	private SequenceManager sequenceManager;
	private TableMapping tableMapping;
	
	public PhoenixRelImplementorImpl(
	        StatementContext statementContext, RuntimeContext runtimeContext) {
	    this.statementContext = statementContext;
	    this.runtimeContext = runtimeContext;
	    this.contextStack = new Stack<ImplementorContext>();
	}
	
    @Override
    public QueryPlan visitInput(int i, PhoenixQueryRel input) {
        return input.implement(this);
    }

	@Override
	public Expression newColumnExpression(int index) {
		return tableMapping.newColumnExpression(index);
	}
    
    @SuppressWarnings("rawtypes")
    @Override
    public Expression newBindParameterExpression(int index, PDataType type, Integer maxLength) {
        return new BindParameterExpression(index, type, maxLength, runtimeContext);
    }
    
    @SuppressWarnings("rawtypes")
    @Override
    public Expression newFieldAccessExpression(String variableId, int index, PDataType type) {
        Expression fieldAccessExpr = runtimeContext.getCorrelateVariable(variableId).newExpression(index);
        return new CorrelateVariableFieldAccessExpression(runtimeContext, variableId, fieldAccessExpr);
    }
    
    @Override
    public SequenceValueExpression newSequenceExpression(PhoenixSequence seq, SequenceValueParseNode.Op op) {
        PName tenantName = seq.pc.getTenantId();
        TableName tableName = TableName.create(seq.schemaName, seq.sequenceName);
        try {
            return sequenceManager.newSequenceReference(tenantName, tableName, null, op);
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public StatementContext getStatementContext() {
        return statementContext;
    }
    
    @Override
    public RuntimeContext getRuntimeContext() {
        return runtimeContext;
    }

    @Override
	public void setTableMapping(TableMapping tableMapping) {
		this.tableMapping = tableMapping;
	}
    
    @Override
    public TableMapping getTableMapping() {
        return this.tableMapping;
    }
    
    @Override
    public void setSequenceManager(SequenceManager sequenceManager) {
        this.sequenceManager = sequenceManager;
    }

    @Override
    public void pushContext(ImplementorContext context) {
        this.contextStack.push(context);
    }

    @Override
    public ImplementorContext popContext() {
        return contextStack.pop();
    }

    @Override
    public ImplementorContext getCurrentContext() {
        return contextStack.peek();
    }
    
    @Override
    public TupleProjector project(List<Expression> exprs) {
        KeyValueSchema.KeyValueSchemaBuilder builder = new KeyValueSchema.KeyValueSchemaBuilder(0);
        List<PColumn> columns = Lists.<PColumn>newArrayList();
        for (int i = 0; i < exprs.size(); i++) {
            String name = ParseNodeFactory.createTempAlias();
            Expression expr = exprs.get(i);
            builder.addField(expr);
            columns.add(new PColumnImpl(PNameFactory.newName(name), PNameFactory.newName(TupleProjector.VALUE_COLUMN_FAMILY),
                    expr.getDataType(), expr.getMaxLength(), expr.getScale(), expr.isNullable(),
                    i, expr.getSortOrder(), null, null, false, name, false, false));
        }
        try {
            PTable pTable = PTableImpl.makePTable(null, PName.EMPTY_NAME, PName.EMPTY_NAME,
                    PTableType.SUBQUERY, null, MetaDataProtocol.MIN_TABLE_TIMESTAMP, PTable.INITIAL_SEQ_NUM,
                    null, null, columns, null, null, Collections.<PTable>emptyList(),
                    false, Collections.<PName>emptyList(), null, null, false, false, false, null,
                    null, null, true, false, 0, 0, false, null, false);
            this.setTableMapping(new TableMapping(pTable));
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
        
        return new TupleProjector(builder.build(), exprs.toArray(new Expression[exprs.size()]));        
    }

}
