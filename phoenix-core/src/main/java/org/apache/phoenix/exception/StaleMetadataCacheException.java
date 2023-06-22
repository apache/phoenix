package org.apache.phoenix.exception;

import org.apache.hadoop.hbase.DoNotRetryIOException;

import java.sql.SQLException;

public class StaleMetadataCacheException extends SQLException {

    public StaleMetadataCacheException(Throwable e) {
        super(e);
    }
    public StaleMetadataCacheException(String  message) {
        super(message);
    }
}
