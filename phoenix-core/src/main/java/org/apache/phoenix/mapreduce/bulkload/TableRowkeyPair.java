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
package org.apache.phoenix.mapreduce.bulkload;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.io.WritableUtils;

import com.google.common.base.Preconditions;


/**
 * A WritableComparable to hold the table name and the rowkey.
 */
public class TableRowkeyPair implements WritableComparable<TableRowkeyPair> {

    /* The qualified table name */
    private String tableName;

    /* The rowkey for the record */
    private ImmutableBytesWritable rowkey;

    /**
     * Default constructor
     */
    public TableRowkeyPair() {
        super();
    }

    public TableRowkeyPair(String tableName, ImmutableBytesWritable rowkey) {
        super();
        Preconditions.checkNotNull(tableName);
        Preconditions.checkNotNull(rowkey);
        this.tableName = tableName;
        this.rowkey = rowkey;
    }

    public String getTableName() {
        return tableName;
    }

    public void setTableName(String tableName) {
        this.tableName = tableName;
    }

    public ImmutableBytesWritable getRowkey() {
        return rowkey;
    }

    public void setRowkey(ImmutableBytesWritable rowkey) {
        this.rowkey = rowkey;
    }

    @Override
    public void readFields(DataInput input) throws IOException {
        tableName = WritableUtils.readString(input);
        rowkey = new ImmutableBytesWritable();
        rowkey.readFields(input);
   }

    @Override
    public void write(DataOutput output) throws IOException {
        WritableUtils.writeString(output,tableName);
        rowkey.write(output);
    }
    
    @Override
    public int hashCode() {
        int result = this.tableName.hashCode();
        result = 31 * result + this.rowkey.hashCode();
        return result;
    }

    @Override
    public int compareTo(TableRowkeyPair other) {
        String otherTableName = other.getTableName();
        if(this.tableName.equals(otherTableName)) {
            return this.rowkey.compareTo(other.getRowkey());
        } else {
            return this.tableName.compareTo(otherTableName);
        }
    }
    
    static { 
        WritableComparator.define(TableRowkeyPair.class, new BytesWritable.Comparator());
    }

}
