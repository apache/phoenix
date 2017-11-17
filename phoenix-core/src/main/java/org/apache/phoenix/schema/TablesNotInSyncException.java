package org.apache.phoenix.schema;

import org.apache.phoenix.exception.SQLExceptionCode;

import java.sql.SQLException;

public class TablesNotInSyncException extends SQLException {
    private static final long serialVersionUID = 1L;
    private static SQLExceptionCode code = SQLExceptionCode.TABLE_ALREADY_EXIST;

    public TablesNotInSyncException(String table1, String table2, String diff) {
        super("Table: " + table1 + " and Table: " + table2 + " differ in " + diff);
    }

}
