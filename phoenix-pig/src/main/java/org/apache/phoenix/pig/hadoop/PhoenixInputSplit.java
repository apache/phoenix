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
package org.apache.phoenix.pig.hadoop;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.phoenix.query.KeyRange;

import com.google.common.base.Preconditions;

/**
 * 
 * Input split class to hold the lower and upper bound range. {@link KeyRange}
 * 
 */
public class PhoenixInputSplit extends InputSplit implements Writable {

    private KeyRange keyRange;
   
    /**
     * No Arg constructor
     */
    public PhoenixInputSplit() {
    }
    
   /**
    * 
    * @param keyRange
    */
    public PhoenixInputSplit(final KeyRange keyRange) {
        Preconditions.checkNotNull(keyRange);
        this.keyRange = keyRange;
    }
    
    @Override
    public void readFields(DataInput input) throws IOException {
        this.keyRange = new KeyRange ();
        this.keyRange.readFields(input);
    }
    
    @Override
    public void write(DataOutput output) throws IOException {
        Preconditions.checkNotNull(keyRange);
        keyRange.write(output);
    }

    @Override
    public long getLength() throws IOException, InterruptedException {
         return 0;
    }

    @Override
    public String[] getLocations() throws IOException, InterruptedException {
        return new String[]{};
    }

    /**
     * @return Returns the keyRange.
     */
    public KeyRange getKeyRange() {
        return keyRange;
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + ((keyRange == null) ? 0 : keyRange.hashCode());
        return result;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) { return true; }
        if (obj == null) { return false; }
        if (!(obj instanceof PhoenixInputSplit)) { return false; }
        PhoenixInputSplit other = (PhoenixInputSplit)obj;
        if (keyRange == null) {
            if (other.keyRange != null) { return false; }
        } else if (!keyRange.equals(other.keyRange)) { return false; }
        return true;
    }
}
