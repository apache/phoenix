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
import java.util.HashMap;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.hbase.DoNotRetryIOException;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.HTablePool;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.phoenix.exception.PhoenixIOException;
import org.apache.phoenix.exception.SQLExceptionCode;
import org.apache.phoenix.exception.SQLExceptionInfo;
import org.apache.phoenix.hbase.index.util.VersionUtil;


@SuppressWarnings("deprecation")
public class ServerUtil {
    private static final int COPROCESSOR_SCAN_WORKS = VersionUtil.encodeVersion("0.98.6");
    
    private static final String FORMAT = "ERROR %d (%s): %s";
    private static final Pattern PATTERN = Pattern.compile("ERROR (\\d+) \\((\\w+)\\): (.*)");
    private static final Map<Class<? extends Exception>, SQLExceptionCode> errorcodeMap
        = new HashMap<Class<? extends Exception>, SQLExceptionCode>();
    static {
        // Map a normal exception into a corresponding SQLException.
        errorcodeMap.put(ArithmeticException.class, SQLExceptionCode.SERVER_ARITHMETIC_ERROR);
    }

    public static void throwIOException(String msg, Throwable t) throws IOException {
        throw createIOException(msg, t);
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
            if (cause == null || cause instanceof IOException) {
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
            t = t.getCause();
        }
        return parseRemoteException(t);
    }

    private static SQLException parseRemoteException(Throwable t) {
        	String message = t.getLocalizedMessage();
        	if (message != null) {
            // If the message matches the standard pattern, recover the SQLException and throw it.
            Matcher matcher = PATTERN.matcher(t.getLocalizedMessage());
            if (matcher.find()) {
                int statusCode = Integer.parseInt(matcher.group(1));
                SQLExceptionCode code;
                try {
                    code = SQLExceptionCode.fromErrorCode(statusCode);
                } catch (SQLException e) {
                    return e;
                }
                return new SQLExceptionInfo.Builder(code).setMessage(matcher.group()).build().buildException();
            }
        	}
        return null;
    }

    private static boolean coprocessorScanWorks(RegionCoprocessorEnvironment env) {
        return (VersionUtil.encodeVersion(env.getHBaseVersion()) >= COPROCESSOR_SCAN_WORKS);
    }
    
    /*
     * This code works around HBASE-11837 which causes HTableInterfaces retrieved from
     * RegionCoprocessorEnvironment to not read local data.
     */
    private static HTableInterface getTableFromSingletonPool(RegionCoprocessorEnvironment env, byte[] tableName) throws IOException {
        // It's ok to not ever do a pool.close() as we're storing a single
        // table only. The HTablePool holds no other resources that this table
        // which will be closed itself when it's no longer needed.
        @SuppressWarnings("resource")
        HTablePool pool = new HTablePool(env.getConfiguration(),1);
        try {
            return pool.getTable(tableName);
        } catch (RuntimeException t) {
            // handle cases that an IOE is wrapped inside a RuntimeException like HTableInterface#createHTableInterface
            if(t.getCause() instanceof IOException) {
                throw (IOException)t.getCause();
            } else {
                throw t;
            }
        }
    }
    
    public static HTableInterface getHTableForCoprocessorScan (RegionCoprocessorEnvironment env, HTableInterface writerTable) throws IOException {
        if (coprocessorScanWorks(env)) {
            return writerTable;
        }
        return getTableFromSingletonPool(env, writerTable.getTableName());
    }
    
    public static HTableInterface getHTableForCoprocessorScan (RegionCoprocessorEnvironment env, byte[] tableName) throws IOException {
        if (coprocessorScanWorks(env)) {
            return env.getTable(TableName.valueOf(tableName));
        }
        return getTableFromSingletonPool(env, tableName);
    }
}
