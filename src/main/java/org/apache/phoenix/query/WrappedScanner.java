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

import java.util.List;

import com.google.common.collect.Lists;
import org.apache.phoenix.compile.ExplainPlan;
import org.apache.phoenix.compile.RowProjector;
import org.apache.phoenix.iterate.ResultIterator;


/**
 * Wrapper for ResultScanner to enable joins and aggregations to be composable.
 *
 * 
 * @since 0.1
 */
public class WrappedScanner implements Scanner {
    public static final int DEFAULT_ESTIMATED_SIZE = 10 * 1024; // 10 K

    private final ResultIterator scanner;
    private final RowProjector projector;
    // TODO: base on stats
    private static final int estimatedSize = DEFAULT_ESTIMATED_SIZE;

    public WrappedScanner(ResultIterator scanner, RowProjector projector) {
        this.scanner = scanner;
        this.projector = projector;
    }

    @Override
    public int getEstimatedSize() {
        return estimatedSize;
    }
    
    @Override
    public ResultIterator iterator() {
        return scanner;
    }

    @Override
    public RowProjector getProjection() {
        return projector;
    }
    
    @Override
    public ExplainPlan getExplainPlan() {
        List<String> planSteps = Lists.newArrayListWithExpectedSize(5);
        scanner.explain(planSteps);
        return new ExplainPlan(planSteps);
    }
}
