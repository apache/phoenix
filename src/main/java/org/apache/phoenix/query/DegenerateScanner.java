/*
 * Copyright 2014 The Apache Software Foundation
 *
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
package org.apache.phoenix.query;

import java.sql.SQLException;
import java.util.Collections;

import org.apache.phoenix.compile.ExplainPlan;
import org.apache.phoenix.compile.RowProjector;
import org.apache.phoenix.iterate.ResultIterator;
import org.apache.phoenix.schema.TableRef;


public class DegenerateScanner implements Scanner {
    private final ExplainPlan explainPlan;
    private final RowProjector projector;
    
    public DegenerateScanner(TableRef table, RowProjector projector) {
        explainPlan = new ExplainPlan(Collections.singletonList("DEGENERATE SCAN OVER " + table.getTable().getName().getString()));
        this.projector = projector;
    }

    @Override
    public ResultIterator iterator() throws SQLException {
        return ResultIterator.EMPTY_ITERATOR;
    }

    @Override
    public int getEstimatedSize() {
        return 0;
    }

    @Override
    public RowProjector getProjection() {
        return projector;
    }

    @Override
    public ExplainPlan getExplainPlan() {
        return explainPlan;
    }

}
