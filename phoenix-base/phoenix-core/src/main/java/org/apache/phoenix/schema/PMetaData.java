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
package org.apache.phoenix.schema;

import org.apache.phoenix.parse.PFunction;
import org.apache.phoenix.parse.PSchema;
import org.apache.phoenix.query.MetaDataMutated;

public interface PMetaData extends MetaDataMutated, Iterable<PTable>, Cloneable {

    public static interface Pruner {
        public boolean prune(PTable table);
        public boolean prune(PFunction function);
    }
    public int size();
    public PMetaData clone();
    public PTableRef getTableRef(PTableKey key) throws TableNotFoundException;
    public void pruneTables(Pruner pruner);
    public PFunction getFunction(PTableKey key) throws FunctionNotFoundException;
    public void pruneFunctions(Pruner pruner);
    public long getAge(PTableRef ref);
    public PSchema getSchema(PTableKey key) throws SchemaNotFoundException;
}
