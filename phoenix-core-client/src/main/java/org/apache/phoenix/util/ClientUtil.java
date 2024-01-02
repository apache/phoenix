/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.phoenix.util;

import java.io.IOException;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import com.google.protobuf.ServiceException;
import org.apache.hadoop.hbase.DoNotRetryIOException;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.NotServingRegionException;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.RetriesExhaustedWithDetailsException;
import org.apache.phoenix.coprocessorclient.HashJoinCacheNotFoundException;
import org.apache.phoenix.exception.PhoenixIOException;
import org.apache.phoenix.exception.SQLExceptionCode;
import org.apache.phoenix.exception.SQLExceptionInfo;
import org.apache.phoenix.schema.StaleRegionBoundaryCacheException;

public class ClientUtil {
    private static final String FORMAT = "ERROR %d (%s): %s";
    private static final Pattern PATTERN = Pattern.compile("ERROR (\\d+) \\((\\w+)\\): (.*)");
    private static final Pattern HASH_JOIN_EXCEPTION_PATTERN = Pattern.compile("joinId: (-?\\d+)");
    private static final Pattern PATTERN_FOR_TS = Pattern.compile(",serverTimestamp=(\\d+),");
    private static final Map<Class<? extends Exception>, SQLExceptionCode> errorcodeMap
            = new HashMap<Class<? extends Exception>, SQLExceptionCode>();
    static {
        // Map a normal exception into a corresponding SQLException.
        errorcodeMap.put(ArithmeticException.class, SQLExceptionCode.SERVER_ARITHMETIC_ERROR);
    }


    private static String constructSQLErrorMessage(SQLExceptionCode code, Throwable e, String message) {
        return constructSQLErrorMessage(code.getErrorCode(), code.getSQLState(), code.getMessage() + " " + e.getMessage() + " " + message);
    }

    private static String constructSQLErrorMessage(SQLException e, String message) {
        return constructSQLErrorMessage(e.getErrorCode(), e.getSQLState(), e.getMessage() + " " + message);
    }

    private static String constructSQLErrorMessage(int errorCode, String SQLState, String message) {
        return String.format(FORMAT, errorCode, SQLState, message);
    }

    public static SQLException parseServerException(Throwable t) {
        SQLException e = parseServerExceptionOrNull(t);
        if (e != null) {
            return e;
        }
        return new PhoenixIOException(t);
    }

    public static SQLException parseServerExceptionOrNull(Throwable t) {
        while (t.getCause() != null) {
            if (t instanceof NotServingRegionException) {
                return parseRemoteException(new StaleRegionBoundaryCacheException());
            }
            t = t.getCause();
        }
        return parseRemoteException(t);
    }

    /**
     * Return the first SQLException in the exception chain, otherwise parse it.
     * When we're receiving an exception locally, there's no need to string parse,
     * as the SQLException will already be part of the chain.
     * @param t
     * @return the SQLException, or null if none found
     */
    public static SQLException parseLocalOrRemoteServerException(Throwable t) {
        while (t.getCause() != null) {
            if (t instanceof NotServingRegionException) {
                return parseRemoteException(new StaleRegionBoundaryCacheException());
            } else if (t instanceof SQLException) {
                return (SQLException) t;
            }
            t = t.getCause();
        }
        return parseRemoteException(t);
    }

    public static SQLException parseRemoteException(Throwable t) {

        String message = t.getLocalizedMessage();
        if (message != null) {
            // If the message matches the standard pattern, recover the SQLException and throw it.
            Matcher matcher = PATTERN.matcher(t.getLocalizedMessage());
            if (matcher.find()) {
                int statusCode = Integer.parseInt(matcher.group(1));
                SQLExceptionCode code = SQLExceptionCode.fromErrorCode(statusCode);
                if(code.equals(SQLExceptionCode.HASH_JOIN_CACHE_NOT_FOUND)){
                    Matcher m = HASH_JOIN_EXCEPTION_PATTERN.matcher(t.getLocalizedMessage());
                    if (m.find()) { return new HashJoinCacheNotFoundException(Long.parseLong(m.group(1))); }
                }
                return new SQLExceptionInfo.Builder(code).setMessage(matcher.group()).setRootCause(t).build().buildException();
            }
        }
        return null;
    }

    public static long parseServerTimestamp(Throwable t) {
        while (t.getCause() != null) {
            t = t.getCause();
        }
        return parseTimestampFromRemoteException(t);
    }

    public static long parseTimestampFromRemoteException(Throwable t) {
        String message = t.getLocalizedMessage();
        if (message != null) {
            // If the message matches the standard pattern, recover the SQLException and throw it.
            Matcher matcher = PATTERN_FOR_TS.matcher(t.getLocalizedMessage());
            if (matcher.find()) {
                String tsString = matcher.group(1);
                if (tsString != null) {
                    return Long.parseLong(tsString);
                }
            }
        }
        return HConstants.LATEST_TIMESTAMP;
    }

    public static IOException createIOException(String msg, Throwable t) {
        // First unwrap SQLExceptions if it's root cause is an IOException.
        if (t instanceof SQLException) {
            Throwable cause = t.getCause();
            if (cause instanceof IOException) {
                t = cause;
            }
        }
        // Throw immediately if DoNotRetryIOException
        if (t instanceof DoNotRetryIOException) {
            return (DoNotRetryIOException) t;
        } else if (t instanceof IOException) {
            // If the IOException does not wrap any exception, then bubble it up.
            Throwable cause = t.getCause();
            if (cause instanceof RetriesExhaustedWithDetailsException)
                return new DoNotRetryIOException(t.getMessage(), cause);
            else if (cause == null || cause instanceof IOException) {
                return (IOException) t;
            }
            // Else assume it's been wrapped, so throw as DoNotRetryIOException to prevent client hanging while retrying
            return new DoNotRetryIOException(t.getMessage(), cause);
        } else if (t instanceof SQLException) {
            // If it's already an SQLException, construct an error message so we can parse and reconstruct on the client side.
            return new DoNotRetryIOException(constructSQLErrorMessage((SQLException) t, msg), t);
        } else {
            // Not a DoNotRetryIOException, IOException or SQLException. Map the exception type to a general SQLException
            // and construct the error message so it can be reconstruct on the client side.
            //
            // If no mapping exists, rethrow it as a generic exception.
            SQLExceptionCode code = errorcodeMap.get(t.getClass());
            if (code == null) {
                return new DoNotRetryIOException(msg + ": " + t.getMessage(), t);
            } else {
                return new DoNotRetryIOException(constructSQLErrorMessage(code, t, msg), t);
            }
        }
    }

    public static void throwIOException(String msg, Throwable t) throws IOException {
        throw createIOException(msg, t);
    }

    /**
     * Returns true if HBase namespace exists, else returns false
     * @param admin HbaseAdmin Object
     * @param schemaName Phoenix schema name for which we check existence of the HBase namespace
     * @return true if the HBase namespace exists, else returns false
     * @throws IOException If there is an exception checking the HBase namespace
     */
    public static boolean isHBaseNamespaceAvailable(Admin admin, String schemaName) throws IOException {
        String[] hbaseNamespaces = admin.listNamespaces();
        return Arrays.asList(hbaseNamespaces).contains(schemaName);
    }

    /**
     * Convert ServiceException into an IOException
     * @param se ServiceException
     * @return IOException
     */
    public static IOException parseServiceException(ServiceException se) {
        Throwable cause = se.getCause();
        if (cause != null && cause instanceof IOException) {
            return (IOException) cause;
        }
        return new IOException(se);
    }
}
