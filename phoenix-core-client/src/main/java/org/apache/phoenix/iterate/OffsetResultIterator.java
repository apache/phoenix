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

import static org.apache.phoenix.util.ScanUtil.getDummyTuple;
import static org.apache.phoenix.util.ScanUtil.isDummy;

import org.apache.phoenix.compile.ExplainPlanAttributes.ExplainPlanAttributesBuilder;
import org.apache.phoenix.schema.tuple.Tuple;
import org.apache.phoenix.util.EnvironmentEdgeManager;

/**
 * Iterates through tuples up to a limit
 *
 * @since 1.2
 */
public class OffsetResultIterator extends DelegateResultIterator {
    private int rowCount;
    private final int offset;
    private Tuple lastScannedTuple;
    private long pageSizeMs = Long.MAX_VALUE;
    private boolean isIncompatibleClient = false;

    public OffsetResultIterator(ResultIterator delegate, Integer offset) {
        super(delegate);
        this.offset = offset == null ? -1 : offset;
        this.lastScannedTuple = null;
    }

    public OffsetResultIterator(ResultIterator delegate, Integer offset, long pageSizeMs,
                                boolean isIncompatibleClient) {
        this(delegate, offset);
        this.pageSizeMs = pageSizeMs;
        this.isIncompatibleClient = isIncompatibleClient;
    }

    @Override
    public Tuple next() throws SQLException {
        long startTime = EnvironmentEdgeManager.currentTimeMillis();
        while (rowCount < offset) {
            Tuple tuple = super.next();
            if (tuple == null) {
                return null;
            }
            if (tuple.size() == 0 || isDummy(tuple)) {
                if (!isIncompatibleClient) {
                    return tuple;
                }
                // While rowCount < offset absorb the dummy and call next on the underlying scanner.
                // This is applicable to old client.
                continue;
            }
            rowCount++;
            lastScannedTuple = tuple;
            if (!isIncompatibleClient) {
                if (EnvironmentEdgeManager.currentTimeMillis() - startTime >= pageSizeMs) {
                    return getDummyTuple(lastScannedTuple);
                }
            }
        }
        Tuple result = super.next();
        if (result != null) {
            lastScannedTuple = result;
        }
        return result;
    }

    @Override
    public void explain(List<String> planSteps) {
        super.explain(planSteps);
        planSteps.add("CLIENT OFFSET " + offset);
    }

    @Override
    public void explain(List<String> planSteps,
            ExplainPlanAttributesBuilder explainPlanAttributesBuilder) {
        super.explain(planSteps, explainPlanAttributesBuilder);
        explainPlanAttributesBuilder.setClientOffset(offset);
        planSteps.add("CLIENT OFFSET " + offset);
    }

    @Override
    public String toString() {
        return "OffsetResultIterator [rowCount=" + rowCount + ", offset=" + offset + "]";
    }

    public Integer getRemainingOffset() {
        return Math.max(offset - rowCount, 0);
    }

    public Tuple getLastScannedTuple() {
        return lastScannedTuple;
    }

    public void setRowCountToOffset() {
        this.rowCount = this.offset;
    }
}
