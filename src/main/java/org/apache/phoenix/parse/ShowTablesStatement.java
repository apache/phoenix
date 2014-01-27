package org.apache.phoenix.parse;

public class ShowTablesStatement implements BindableStatement {
    @Override
    public int getBindCount() {
        return 0;
    }
}
