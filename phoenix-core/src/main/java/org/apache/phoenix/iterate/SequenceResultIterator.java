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

import org.apache.phoenix.compile.SequenceManager;
import org.apache.phoenix.schema.tuple.Tuple;

/**
 * 
 * Iterates through tuples retrieving sequences from the server as needed
 *
 * 
 * @since 3.0
 */
public class SequenceResultIterator extends DelegateResultIterator {
    private final SequenceManager sequenceManager;
    
    public SequenceResultIterator(ResultIterator delegate, SequenceManager sequenceManager) throws SQLException {
        super(delegate);
        this.sequenceManager = sequenceManager;
    }
    
    @Override
    public Tuple next() throws SQLException {
        Tuple next = super.next();
        if (next == null) {
            return null;
        }
        next = sequenceManager.newSequenceTuple(next);
        return next;
    }

    @Override
    public void explain(List<String> planSteps) {
        super.explain(planSteps);
        int nSequences = sequenceManager.getSequenceCount();
        planSteps.add("CLIENT RESERVE VALUES FROM " + nSequences + " SEQUENCE" + (nSequences == 1 ? "" : "S"));
    }

	@Override
	public String toString() {
		return "SequenceResultIterator [sequenceManager=" + sequenceManager
				+ "]";
	}
}
