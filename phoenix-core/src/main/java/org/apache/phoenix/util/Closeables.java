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

import com.google.common.collect.Iterables;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedList;


/**
 * Utilities for operating on {@link Closeable}s.
 * 
 */
public class Closeables {

    private static final Logger LOGGER = LoggerFactory.getLogger(Closeables.class);

    /** Not constructed */
    private Closeables() { }
    
    /**
     * Close a {@code Closeable}, returning an {@code IOException} if it occurs while closing
     * instead of throwing it. This is nearly a clone of the Guava Closeables.closeQuietly method
     * which has long since been removed from Guava.
     *
     * Use of this method should be avoided -- quietly swallowing IOExceptions (particularly on
     * Closeables that are being written to) is a code smell. Use of the equivalent method in
     * Guava was done for this reason.
     *
     * @param closeable the Closeable to be closed, can be null
     * @return the IOException if one was thrown, otherwise {@code null}
     */
    public static IOException closeQuietly(@Nullable Closeable closeable) {
        if (closeable == null) {
            return null;
        }
        try {
            closeable.close();
            return null;
        } catch (IOException e) {
            LOGGER.error("Error closing " + closeable, e);
            return e;
        }
    }

    /**
     * Allows you to close as many of the {@link Closeable}s as possible.
     * 
     * If any of the close's fail with an IOException, those exception(s) will
     * be thrown after attempting to close all of the inputs.
     */
    public static void closeAll(Iterable<? extends Closeable> iterable) throws IOException {
        IOException ex = closeAllQuietly(iterable);
        if (ex != null) throw ex;
    }
 
    public static IOException closeAllQuietly(Iterable<? extends Closeable> iterable) {
        if (iterable == null) return null;
        
        LinkedList<IOException> exceptions = null;
        for (Closeable closeable : iterable) {
            IOException ioe = closeQuietly(closeable);
            if (ioe != null) {
                if (exceptions == null) exceptions = new LinkedList<IOException>();
                exceptions.add(ioe);
            }
        }
        
        IOException ex = MultipleCausesIOException.fromIOExceptions(exceptions);
        return ex;
    }

    static private class MultipleCausesIOException extends IOException {
    	private static final long serialVersionUID = 1L;

        static IOException fromIOExceptions(Collection<? extends IOException> exceptions) {
            if (exceptions == null || exceptions.isEmpty()) return null;
            if (exceptions.size() == 1) return Iterables.getOnlyElement(exceptions);
            
            return new MultipleCausesIOException(exceptions);
        }
        
        private final Collection<? extends IOException> exceptions;
        private boolean hasSetStackTrace;
        
        /**
         * Use the {@link #fromIOExceptions(Collection) factory}.
         */
        private MultipleCausesIOException(Collection<? extends IOException> exceptions) {
            this.exceptions = exceptions;
        }

        @Override
        public String getMessage() {
            StringBuilder sb = new StringBuilder(this.exceptions.size() * 50);
            int exceptionNum = 0;
            for (IOException ex : this.exceptions) {
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
                for (IOException exception : this.exceptions) {
                    StackTraceElement header = new StackTraceElement(MultipleCausesIOException.class.getName(), 
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
