package org.apache.phoenix.schema;

import org.apache.phoenix.exception.SQLExceptionCode;
import org.apache.phoenix.exception.SQLExceptionInfo;

import java.sql.SQLException;

public class TablesNotInSyncException extends SQLException {
    private static final long serialVersionUID = 1L;
    private static SQLExceptionCode code = SQLExceptionCode.TABLES_NOT_IN_SYNC;

    public TablesNotInSyncException(String table1, String table2, String diff) {
        super(new SQLExceptionInfo.Builder(code).setMessage("Table: " + table1 + " and Table: " + table2 + " differ in " + diff).build().toString(), code.getSQLState(), code.getErrorCode());
    }

}
