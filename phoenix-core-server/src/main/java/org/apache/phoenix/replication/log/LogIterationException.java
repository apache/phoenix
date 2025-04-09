/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.phoenix.replication.log;

import java.io.IOException;

/**
 * Unchecked exception thrown when an IOException occurs during iteration over a replication log
 * using its Iterator interface.
 */
public class LogIterationException extends RuntimeException {

    private static final long serialVersionUID = 1L; // Recommended for RuntimeExceptions

    /**
     * Constructs a new LogIterationException with the specified cause.
     * A standard detail message incorporating the cause's message is generated.
     *
     * @param cause the cause (the {@code IOException} that occurred during iteration).
     *              (A {@code null} value is permitted, and indicates that the cause is
     *              nonexistent or unknown.)
     */
    public LogIterationException(IOException cause) {
        super("IOException occurred during Replication Log iteration: "
              + (cause == null ? "Unknown cause" : cause.getMessage()), cause);
    }

    /**
     * Constructs a new LogIterationException with the specified detail message and cause.
     *
     * @param message the detail message.
     * @param cause the cause (the {@code IOException} that occurred during iteration).
     *              (A {@code null} value is permitted, and indicates that the cause is
     *              nonexistent or unknown.)
     */
    public LogIterationException(String message, IOException cause) {
        super(message, cause);
    }

    /**
     * Returns the cause of this exception (the {@code IOException} that
     * occurred during iteration). Overrides the standard getCause to return
     * the more specific type directly.
     *
     * @return the {@code IOException} which is the cause of this exception,
     *         or {@code null} if the cause is nonexistent or unknown.
     */
    @Override
    public synchronized IOException getCause() {
        Throwable cause = super.getCause();
        return (cause instanceof IOException) ? (IOException) cause : null;
    }
}