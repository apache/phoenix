package org.apache.phoenix.parse;

import java.util.Set;

import org.apache.hadoop.hbase.util.Pair;

import org.apache.phoenix.schema.PTable;
import org.apache.phoenix.thirdparty.com.google.common.collect.ArrayListMultimap;
import org.apache.phoenix.thirdparty.com.google.common.collect.ListMultimap;

public class CreateCDCStatement extends MutableStatement {
    private final TableName cdcObjName;
    private final TableName dataTable;
    private final ColumnName timeIdxColumn;
    private final Set<PTable.CDCChangeScope> includeScopes;
    private final ListMultimap<String,Pair<String,Object>> props;
    private final boolean ifNotExists;
    private final int bindCount;

    public CreateCDCStatement(TableName cdcObjName, TableName dataTable, ColumnName timeIdxColumn,
                              Set<PTable.CDCChangeScope> includeScopes, ListMultimap<String, Pair<String, Object>> props, boolean ifNotExists, int bindCount) {
        this.cdcObjName = cdcObjName;
        this.dataTable = dataTable;
        this.timeIdxColumn = timeIdxColumn;
        this.includeScopes = includeScopes;
        this.props = props == null ? ArrayListMultimap.<String,Pair<String,Object>>create() : props;
        this.ifNotExists = ifNotExists;
        this.bindCount = bindCount;
    }

    public TableName getCdcObjName() {
        return cdcObjName;
    }

    public TableName getDataTable() {
        return dataTable;
    }

    public ColumnName getTimeIdxColumn() {
        return timeIdxColumn;
    }

    public Set<PTable.CDCChangeScope> getIncludeScopes() {
        return includeScopes;
    }

    public ListMultimap<String, Pair<String, Object>> getProps() {
        return props;
    }

    public boolean isIfNotExists() {
        return ifNotExists;
    }

    @Override
    public int getBindCount() {
        return bindCount;
    }
}
