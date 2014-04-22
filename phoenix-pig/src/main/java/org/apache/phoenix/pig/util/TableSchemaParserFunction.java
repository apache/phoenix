/*
 * Copyright 2010 The Apache Software Foundation
 *
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 *distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you maynot use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicablelaw or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.phoenix.pig.util;



import org.apache.hadoop.hbase.util.Pair;

import com.google.common.base.Function;
import com.google.common.base.Preconditions;
import com.google.common.base.Splitter;
import com.google.common.collect.Iterables;

/**
 * 
 * A function to parse the table schema passed to LOAD/STORE into a Pair of <table Name, columns>
 *
 */
public final class TableSchemaParserFunction implements Function<String,Pair<String,String>> {

    private static final char TABLE_COLUMN_DELIMITER    = '/';
    
    @Override
    public Pair<String, String> apply(final String tableSchema) {
        Preconditions.checkNotNull(tableSchema);
        Preconditions.checkArgument(!tableSchema.isEmpty(), "HBase Table name is empty!!");
        
        final String  tokens[] = Iterables.toArray(Splitter.on(TABLE_COLUMN_DELIMITER).
                                    trimResults().omitEmptyStrings().split(tableSchema) , String.class); 
        final String tableName = tokens[0];
        String columns = null;
        if(tokens.length > 1) {
            columns = tokens[1];    
        }
        return new Pair<String, String>(tableName, columns);
    }
}
