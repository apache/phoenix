package org.apache.phoenix.exception;

import java.sql.SQLException;

public class QueryOutOfScanRangeException extends SQLException {

    private static SQLExceptionCode code = SQLExceptionCode.VALUE_OUTSIDE_RANGE;

    public QueryOutOfScanRangeException(String message){
        super(new SQLExceptionInfo.Builder(code).setMessage(message).build().toString(), code.getSQLState(), code.getErrorCode());
    }
}
