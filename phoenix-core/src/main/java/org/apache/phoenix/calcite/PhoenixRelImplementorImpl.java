package org.apache.phoenix.calcite;

import java.sql.SQLException;
import java.util.List;
import java.util.Stack;

import org.apache.phoenix.calcite.PhoenixRel.ImplementorContext;
import org.apache.phoenix.compile.ColumnProjector;
import org.apache.phoenix.compile.ExpressionProjector;
import org.apache.phoenix.compile.QueryPlan;
import org.apache.phoenix.compile.RowProjector;
import org.apache.phoenix.compile.TupleProjectionCompiler;
import org.apache.phoenix.expression.ColumnExpression;
import org.apache.phoenix.expression.Expression;
import org.apache.phoenix.schema.ColumnRef;
import org.apache.phoenix.schema.PColumn;
import org.apache.phoenix.schema.PTable;
import org.apache.phoenix.schema.TableRef;

import com.google.common.collect.Lists;

class PhoenixRelImplementorImpl implements PhoenixRel.Implementor {
	private TableRef tableRef;
	private Stack<ImplementorContext> contextStack;
	
	public PhoenixRelImplementorImpl() {
	    this.contextStack = new Stack<ImplementorContext>();
	    pushContext(new ImplementorContext(true));
	}
	
    @Override
    public QueryPlan visitInput(int i, PhoenixRel input) {
        return input.implement(this);
    }

	@Override
	public ColumnExpression newColumnExpression(int index) {
		ColumnRef colRef = new ColumnRef(this.tableRef, index);
		return colRef.newColumnExpression();
	}


    @Override
	public void setTableRef(TableRef tableRef) {
		this.tableRef = tableRef;
	}
    
    @Override
    public TableRef getTableRef() {
        return this.tableRef;
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
        for (PColumn column : getTableRef().getTable().getColumns()) {
            sourceColumnRefs.add(new ColumnRef(getTableRef(), column.getPosition()));
        }
        
        try {
            return TupleProjectionCompiler.createProjectedTable(getTableRef(), sourceColumnRefs, getCurrentContext().isRetainPKColumns());
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }
    
    @Override
    public RowProjector createRowProjector() {
        List<ColumnProjector> columnProjectors = Lists.<ColumnProjector>newArrayList();
        for (PColumn column : getTableRef().getTable().getColumns()) {
            Expression expr = newColumnExpression(column.getPosition());
            columnProjectors.add(new ExpressionProjector(column.getName().getString(), getTableRef().getTable().getName().getString(), expr, false));
        }
        // TODO get estimate row size
        return new RowProjector(columnProjectors, 0, false);        
    }

}
