package org.apache.phoenix.calcite.rel;

import java.sql.SQLException;
import java.util.Collections;
import java.util.List;
import java.util.Stack;

import org.apache.hadoop.hbase.HConstants;
import org.apache.phoenix.calcite.CalciteUtils;
import org.apache.phoenix.calcite.PhoenixSequence;
import org.apache.phoenix.calcite.PhoenixTable;
import org.apache.phoenix.calcite.rel.PhoenixRel.ImplementorContext;
import org.apache.phoenix.compile.ColumnProjector;
import org.apache.phoenix.compile.ExpressionProjector;
import org.apache.phoenix.compile.QueryPlan;
import org.apache.phoenix.compile.RowProjector;
import org.apache.phoenix.compile.SequenceManager;
import org.apache.phoenix.compile.SequenceValueExpression;
import org.apache.phoenix.compile.TupleProjectionCompiler;
import org.apache.phoenix.coprocessor.MetaDataProtocol;
import org.apache.phoenix.execute.RuntimeContext;
import org.apache.phoenix.execute.TupleProjector;
import org.apache.phoenix.expression.ColumnExpression;
import org.apache.phoenix.expression.CorrelateVariableFieldAccessExpression;
import org.apache.phoenix.expression.Expression;
import org.apache.phoenix.parse.ParseNodeFactory;
import org.apache.phoenix.parse.SequenceValueParseNode;
import org.apache.phoenix.parse.TableName;
import org.apache.phoenix.schema.ColumnRef;
import org.apache.phoenix.schema.KeyValueSchema;
import org.apache.phoenix.schema.PColumn;
import org.apache.phoenix.schema.PColumnImpl;
import org.apache.phoenix.schema.PName;
import org.apache.phoenix.schema.PNameFactory;
import org.apache.phoenix.schema.PTable;
import org.apache.phoenix.schema.PTableImpl;
import org.apache.phoenix.schema.PTableType;
import org.apache.phoenix.schema.TableRef;
import org.apache.phoenix.schema.KeyValueSchema.KeyValueSchemaBuilder;
import org.apache.phoenix.schema.types.PDataType;
import org.apache.phoenix.util.SchemaUtil;

import com.google.common.collect.Lists;

public class PhoenixRelImplementorImpl implements PhoenixRel.Implementor {
    private final RuntimeContext runtimeContext;
	private TableRef tableRef;
	private List<PColumn> mappedColumns;
	private Stack<ImplementorContext> contextStack;
	private SequenceManager sequenceManager;
	
	public PhoenixRelImplementorImpl(RuntimeContext runtimeContext) {
	    this.runtimeContext = runtimeContext;
	    this.contextStack = new Stack<ImplementorContext>();
	}
	
    @Override
    public QueryPlan visitInput(int i, PhoenixRel input) {
        return input.implement(this);
    }

	@Override
	public ColumnExpression newColumnExpression(int index) {
		ColumnRef colRef = new ColumnRef(this.tableRef, this.mappedColumns.get(index).getPosition());
		return colRef.newColumnExpression();
	}
    
    @SuppressWarnings("rawtypes")
    @Override
    public Expression newFieldAccessExpression(String variableId, int index, PDataType type) {
        Expression fieldAccessExpr = runtimeContext.newCorrelateVariableReference(variableId, index);
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
    public RuntimeContext getRuntimeContext() {
        return runtimeContext;
    }

    @Override
	public void setTableRef(TableRef tableRef) {
		this.tableRef = tableRef;
		this.mappedColumns = PhoenixTable.getMappedColumns(tableRef.getTable());
	}
    
    @Override
    public TableRef getTableRef() {
        return this.tableRef;
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
    public PTable createProjectedTable() {
        List<ColumnRef> sourceColumnRefs = Lists.<ColumnRef> newArrayList();
        List<PColumn> columns = getCurrentContext().retainPKColumns ?
                  getTableRef().getTable().getColumns() : mappedColumns;
        for (PColumn column : columns) {
            sourceColumnRefs.add(new ColumnRef(getTableRef(), column.getPosition()));
        }
        
        try {
            return TupleProjectionCompiler.createProjectedTable(getTableRef(), sourceColumnRefs, getCurrentContext().retainPKColumns);
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }
    
    @Override
    public TupleProjector createTupleProjector() {
        KeyValueSchemaBuilder builder = new KeyValueSchemaBuilder(0);
        List<Expression> exprs = Lists.<Expression> newArrayList();
        for (PColumn column : mappedColumns) {
            if (!SchemaUtil.isPKColumn(column) || !getCurrentContext().retainPKColumns) {
                Expression expr = new ColumnRef(tableRef, column.getPosition()).newColumnExpression();
                exprs.add(expr);
                builder.addField(expr);                
            }
        }
        
        return new TupleProjector(builder.build(), exprs.toArray(new Expression[exprs.size()]));
    }
    
    @Override
    public RowProjector createRowProjector() {
        List<ColumnProjector> columnProjectors = Lists.<ColumnProjector>newArrayList();
        for (int i = 0; i < mappedColumns.size(); i++) {
            PColumn column = mappedColumns.get(i);
            Expression expr = newColumnExpression(i); // Do not use column.position() here.
            columnProjectors.add(new ExpressionProjector(column.getName().getString(), getTableRef().getTable().getName().getString(), expr, false));
        }
        // TODO get estimate row size
        return new RowProjector(columnProjectors, 0, false);        
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
                    null, null, true, false, 0, 0);
            this.setTableRef(new TableRef(CalciteUtils.createTempAlias(), pTable, HConstants.LATEST_TIMESTAMP, false));
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
        
        return new TupleProjector(builder.build(), exprs.toArray(new Expression[exprs.size()]));        
    }

}
