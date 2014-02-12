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

import java.sql.SQLException;
import java.util.*;

import com.google.common.collect.Iterables;


/**
 * Utilities for operating on {@link SQLCloseable}s.
 * 
 * 
 * @since 0.1
 */
public class SQLCloseables {
    /** Not constructed */
    private SQLCloseables() { }
    
    /**
     * Allows you to close as many of the {@link SQLCloseable}s as possible.
     * 
     * If any of the close's fail with an IOException, those exception(s) will
     * be thrown after attempting to close all of the inputs.
     */
    public static void closeAll(Iterable<? extends SQLCloseable> iterable) throws SQLException {
        SQLException ex = closeAllQuietly(iterable);
        if (ex != null) throw ex;
    }
 
    public static SQLException closeAllQuietly(Iterable<? extends SQLCloseable> iterable) {
        if (iterable == null) return null;
        
        LinkedList<SQLException> exceptions = null;
        for (SQLCloseable closeable : iterable) {
            try {
                closeable.close();
            } catch (SQLException x) {
                if (exceptions == null) exceptions = new LinkedList<SQLException>();
                exceptions.add(x);
            }
        }
        
        SQLException ex = MultipleCausesSQLException.fromSQLExceptions(exceptions);
        return ex;
    }

    /**
     * A subclass of {@link SQLException} that allows you to chain multiple 
     * causes together.
     * 
     * 
     * @since 0.1
     * @see SQLCloseables
     */
    static private class MultipleCausesSQLException extends SQLException {
		private static final long serialVersionUID = 1L;

		static SQLException fromSQLExceptions(Collection<? extends SQLException> exceptions) {
            if (exceptions == null || exceptions.isEmpty()) return null;
            if (exceptions.size() == 1) return Iterables.getOnlyElement(exceptions);
            
            return new MultipleCausesSQLException(exceptions);
        }
        
        private final Collection<? extends SQLException> exceptions;
        private boolean hasSetStackTrace;
        
        /**
         * Use the {@link #fromIOExceptions(Collection) factory}.
         */
        private MultipleCausesSQLException(Collection<? extends SQLException> exceptions) {
            this.exceptions = exceptions;
        }

        @Override
        public String getMessage() {
            StringBuilder sb = new StringBuilder(this.exceptions.size() * 50);
            int exceptionNum = 0;
            for (SQLException ex : this.exceptions) {
                sb.append("Cause Number " + exceptionNum + ": " + ex.getMessage() + "\n");
                exceptionNum++;
            }
            return sb.toString();
        }
        
        @Override
        public StackTraceElement[] getStackTrace() {
            if (!this.hasSetStackTrace) {
                ArrayList<StackTraceElement> frames = new ArrayList<StackTraceElement>(this.exceptions.size() * 20);
                
                int exceptionNum = 0;
                for (SQLException exception : this.exceptions) {
                    StackTraceElement header = new StackTraceElement(MultipleCausesSQLException.class.getName(), 
                            "Exception Number " + exceptionNum, 
                            "<no file>",
                            0);
                    
                    frames.add(header);
                    for (StackTraceElement ste : exception.getStackTrace()) {
                        frames.add(ste);
                    }
                    exceptionNum++;
                }
                
                setStackTrace(frames.toArray(new StackTraceElement[frames.size()]));
                this.hasSetStackTrace = true;
            }        
            
            return super.getStackTrace();
        }

    }
    
}
