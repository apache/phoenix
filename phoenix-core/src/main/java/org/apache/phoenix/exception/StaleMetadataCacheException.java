package org.apache.phoenix.exception;

import org.apache.hadoop.hbase.DoNotRetryIOException;

import java.sql.SQLException;

public class StaleMetadataCacheException extends SQLException {
    private static final long serialVersionUID = 1L;
    private static SQLExceptionCode code = SQLExceptionCode.STALE_METADATA_CACHE_EXCEPTION;

    public StaleMetadataCacheException(String  message) {
        super(message, code.getSQLState(), code.getErrorCode());
    }
}
