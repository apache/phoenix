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
package org.apache.phoenix.query;

import java.sql.SQLException;
import java.util.concurrent.ThreadPoolExecutor;

import org.apache.phoenix.memory.MemoryManager;
import org.apache.phoenix.optimize.QueryOptimizer;
import org.apache.phoenix.util.ReadOnlyProps;



/**
 * 
 * Class that delegates QueryService calls through to
 * a parent QueryService.
 *
 * 
 * @since 0.1
 */
public class DelegateQueryServices implements QueryServices {
    private final QueryServices parent;
    
    public DelegateQueryServices(QueryServices queryServices) {
        parent = queryServices;
    }
    
    protected QueryServices getDelegate() {
        return parent;
    }
    
    @Override
    public ThreadPoolExecutor getExecutor() {
        return parent.getExecutor();
    }

    @Override
    public MemoryManager getMemoryManager() {
        return parent.getMemoryManager();
    }

    @Override
    public void close() throws SQLException {
        parent.close();
    }

    @Override
    public ReadOnlyProps getProps() {
        return parent.getProps();
    }

    @Override
    public QueryOptimizer getOptimizer() {
        return parent.getOptimizer();
    }
}
