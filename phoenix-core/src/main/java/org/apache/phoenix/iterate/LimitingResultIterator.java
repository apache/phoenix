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
package org.apache.phoenix.iterate;

import java.sql.SQLException;
import java.util.List;

import org.apache.phoenix.schema.tuple.Tuple;

/**
 * 
 * Iterates through tuples up to a limit
 *
 * 
 * @since 1.2
 */
public class LimitingResultIterator extends DelegateResultIterator {
    private int rowCount;
    private final int limit;
    
    public LimitingResultIterator(ResultIterator delegate, int limit) {
        super(delegate);
        this.limit = limit;
    }

    @Override
    public Tuple next() throws SQLException {
        if (rowCount++ >= limit) {
            close(); // Free resources early
            return null;
        }
        return super.next();
    }

    @Override
    public void explain(List<String> planSteps) {
        super.explain(planSteps);
        planSteps.add("CLIENT " + limit + " ROW LIMIT");
    }

	@Override
	public String toString() {
		return "LimitingResultIterator [rowCount=" + rowCount + ", limit="
				+ limit + "]";
	}
}
