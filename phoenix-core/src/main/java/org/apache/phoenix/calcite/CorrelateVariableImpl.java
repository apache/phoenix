package org.apache.phoenix.calcite;

import org.apache.phoenix.execute.RuntimeContext.CorrelateVariable;
import org.apache.phoenix.expression.Expression;
import org.apache.phoenix.schema.tuple.Tuple;

public class CorrelateVariableImpl implements CorrelateVariable {
    private final TableMapping tableMapping;
    private Tuple value;
    
    public CorrelateVariableImpl(TableMapping tableMapping) {
        this.tableMapping = tableMapping;
    }

    @Override
    public Expression newExpression(int index) {
        return tableMapping.newColumnExpression(index);
    }

    @Override
    public Tuple getValue() {
        return value;
    }

    @Override
    public void setValue(Tuple value) {
        this.value = value;
    }

}
