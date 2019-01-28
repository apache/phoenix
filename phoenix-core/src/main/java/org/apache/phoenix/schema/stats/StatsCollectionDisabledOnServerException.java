package org.apache.phoenix.schema.stats;

import org.apache.phoenix.exception.SQLExceptionCode;
import org.apache.phoenix.exception.SQLExceptionInfo;

import java.sql.SQLException;

public class StatsCollectionDisabledOnServerException extends SQLException {

    private static final long serialVersionUID = 1L;
    private static SQLExceptionCode code = SQLExceptionCode.STATS_COLLECTION_DISABLED_ON_SERVER;

    public StatsCollectionDisabledOnServerException() {
        super(new SQLExceptionInfo.Builder(code)
                .setMessage(code.getMessage()).build().toString(),
                code.getSQLState(), code.getErrorCode());
    }
}
