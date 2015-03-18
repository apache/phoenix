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
package org.apache.phoenix.mapreduce.index;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;

import org.apache.hadoop.mapreduce.lib.db.DBWritable;
import org.apache.phoenix.util.ColumnInfo;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;

/**
 * A {@link DBWritable} class that reads and write records.
 *
 * 
 */
public class PhoenixIndexDBWritable  implements DBWritable { 
    
    private List<ColumnInfo> columnMetadata;
    
    private List<Object> values;
    
    private int columnCount = -1;
    
    @Override
    public void write(PreparedStatement statement) throws SQLException {
       Preconditions.checkNotNull(values);
       Preconditions.checkNotNull(columnMetadata);
       for(int i = 0 ; i < values.size() ; i++) {
           Object value = values.get(i);
           ColumnInfo columnInfo = columnMetadata.get(i);
           if(value == null) {
               statement.setNull(i + 1, columnInfo.getSqlType());               
           } else {
               statement.setObject(i + 1, value , columnInfo.getSqlType());
           }
       }
       
    }

    @Override
    public void readFields(ResultSet resultSet) throws SQLException {
        // we do this once per mapper.
        if(columnCount == -1) {
            this.columnCount = resultSet.getMetaData().getColumnCount();
        }
  
        values = Lists.newArrayListWithCapacity(columnCount);
        for(int i = 0 ; i < columnCount ; i++) {
            Object value = resultSet.getObject(i + 1);
            values.add(value);
        }
        
    }

    public List<ColumnInfo> getColumnMetadata() {
        return columnMetadata;
    }

    public void setColumnMetadata(List<ColumnInfo> columnMetadata) {
        this.columnMetadata = columnMetadata;
    }

    public List<Object> getValues() {
        return values;
    }

    public void setValues(List<Object> values) {
        this.values = values;
    }

}