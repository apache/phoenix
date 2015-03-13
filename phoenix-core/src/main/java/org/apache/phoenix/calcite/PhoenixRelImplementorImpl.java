package org.apache.phoenix.calcite;

import java.util.Stack;

import org.apache.phoenix.calcite.PhoenixRel.ImplementorContext;
import org.apache.phoenix.compile.QueryPlan;
import org.apache.phoenix.expression.ColumnExpression;
import org.apache.phoenix.schema.ColumnRef;
import org.apache.phoenix.schema.TableRef;

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

}
