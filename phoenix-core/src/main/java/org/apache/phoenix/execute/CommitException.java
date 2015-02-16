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
package org.apache.phoenix.execute;

import java.sql.SQLException;
import java.util.Set;

import org.apache.phoenix.jdbc.PhoenixConnection;

import com.google.common.collect.ImmutableSet;

public class CommitException extends SQLException {
    private static final long serialVersionUID = 2L;
    private final Set<Integer> failures;

    public CommitException(Exception e, Set<Integer> failures) {
        super(e);
        this.failures = ImmutableSet.copyOf(failures);
    }

    /**
     * Returns indexes of UPSERT and DELETE statements that have failed. Indexes returned
     * correspond to each failed statement's order of creation within a {@link PhoenixConnection} up to
     * commit/rollback.
     * <p>
     * Statements whose index is returned in this set correspond to one or more HBase mutations that have failed.
     * <p>
     * Statement indexes are maintained correctly for connections that mutate and query 
     * <b>data</b> (DELETE, UPSERT and SELECT) only. Statement (and their subsequent failure) order
     * is undefined for connections that execute metadata operations due to the fact that Phoenix rolls
     * back connections after metadata mutations.
     * 
     * @see PhoenixConnection#getStatementExecutionsCount()
     */
    public Set<Integer> getFailures() {
    	return failures;
    }
}
