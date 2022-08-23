package org.apache.phoenix.jdbc;

import org.apache.phoenix.exception.SQLExceptionCode;
import org.apache.phoenix.exception.SQLExceptionInfo;

import java.sql.SQLException;

public class MalformedUrlException {
    public static SQLException getMalFormedUrlException(String url) {
        return new SQLExceptionInfo.Builder(SQLExceptionCode.MALFORMED_CONNECTION_URL)
                .setMessage(url).build().buildException();
    }

    public static SQLException getMalFormedUrlException(String message, String url) {
        return getMalFormedUrlException(message + "\n" + url);
    }
}
