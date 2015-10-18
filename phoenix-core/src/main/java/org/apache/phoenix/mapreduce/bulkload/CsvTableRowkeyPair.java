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
 *
 */
public class CsvTableRowkeyPair implements WritableComparable<CsvTableRowkeyPair> {

    /* The qualified table name */
    private String tableName;
    
    /* The rowkey for the record */
    private ImmutableBytesWritable rowkey;
    
    /**
     * Default constructor
     */
    public CsvTableRowkeyPair() {
        super();
    }
    
    /**
     * @param tableName
     * @param rowkey
     */
    public CsvTableRowkeyPair(String tableName, ImmutableBytesWritable rowkey) {
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
    public int compareTo(CsvTableRowkeyPair other) {
        String otherTableName = other.getTableName();
        if(this.tableName.equals(otherTableName)) {
            return this.rowkey.compareTo(other.getRowkey());
        } else {
            return this.tableName.compareTo(otherTableName);
        }
    }
    
    /** Comparator optimized for <code>CsvTableRowkeyPair</code>. */
    public static class Comparator extends WritableComparator {
        private BytesWritable.Comparator comparator = new BytesWritable.Comparator();
        
        public Comparator() {
            super(CsvTableRowkeyPair.class);
        }

        @Override
        public int compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2) {
            try {
                int vintL1 = WritableUtils.decodeVIntSize(b1[s1]);
                int vintL2 = WritableUtils.decodeVIntSize(b2[s2]);
                int strL1 = readVInt(b1, s1);
                int strL2 = readVInt(b2, s2);
                int cmp = compareBytes(b1, s1 + vintL1, strL1, b2, s2 + vintL2, strL2);
                if (cmp != 0) {
                  return cmp;
                }
                int vintL3 = WritableUtils.decodeVIntSize(b1[s1 + vintL1 + strL1]);
                int vintL4 = WritableUtils.decodeVIntSize(b2[s2 + vintL2 + strL2]);
                int strL3 = readVInt(b1, s1 + vintL1 + strL1);
                int strL4 = readVInt(b2, s2 + vintL2 + strL2);
                return comparator.compare(b1, s1 + vintL1 + strL1 + vintL3, strL3, b2, s2
                    + vintL2 + strL2 + vintL4, strL4);
                
            } catch(Exception ex) {
                throw new IllegalArgumentException(ex);
            }
        }
    }
 
    static { 
        WritableComparator.define(CsvTableRowkeyPair.class, new Comparator());
    }

}
