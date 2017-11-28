package org.apache.phoenix.schema;

import org.apache.phoenix.exception.SQLExceptionCode;
import org.apache.phoenix.exception.SQLExceptionInfo;

import java.sql.SQLException;

/**
 * Exception to raise when multiple tables differ in specified properties
 * This can happen since Apache Phoenix code doesn't work atomically for many parts
 * For example, Base table and index tables are inconsistent in namespace mapping
 * OR View Index table doesn't exist for multi-tenant base table
 */
public class TablesNotInSyncException extends SQLException {
    private static final long serialVersionUID = 1L;
    private static SQLExceptionCode code = SQLExceptionCode.TABLES_NOT_IN_SYNC;

    public TablesNotInSyncException(String table1, String table2, String diff) {
        super(new SQLExceptionInfo.Builder(code).setMessage("Table: " + table1 + " and Table: " + table2 + " differ in " + diff).build().toString(), code.getSQLState(), code.getErrorCode());
    }

}
