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
package org.apache.phoenix.schema.tuple;

import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.phoenix.util.ResultUtil;


public class ResultTuple extends BaseTuple {
    private Result result;
    
    public ResultTuple(Result result) {
        this.result = result;
    }
    
    public ResultTuple() {
    }
    
    public Result getResult() {
        return this.result;
    }

    public void setResult(Result result) {
        this.result = result;
    }
    
    @Override
    public void getKey(ImmutableBytesWritable ptr) {
        ResultUtil.getKey(result, ptr);
    }

    @Override
    public boolean isImmutable() {
        return true;
    }

    @Override
    public KeyValue getValue(byte[] family, byte[] qualifier) {
        return result.getColumnLatest(family, qualifier);
    }

    @Override
    public String toString() {
      StringBuilder sb = new StringBuilder();
      sb.append("keyvalues=");
      if(this.result.isEmpty()) {
        sb.append("NONE");
        return sb.toString();
      }
      sb.append("{");
      boolean moreThanOne = false;
      for(KeyValue kv : this.result.list()) {
        if(moreThanOne) {
          sb.append(", \n");
        } else {
          moreThanOne = true;
        }
        sb.append(kv.toString()+"/value="+Bytes.toString(kv.getValue()));
      }
      sb.append("}\n");
      return sb.toString();
    }

    @Override
    public int size() {
        return result.size();
    }

    @Override
    public KeyValue getValue(int index) {
        return result.raw()[index];
    }

    @Override
    public boolean getValue(byte[] family, byte[] qualifier,
            ImmutableBytesWritable ptr) {
        KeyValue kv = getValue(family, qualifier);
        if (kv == null)
            return false;
        ptr.set(kv.getBuffer(), kv.getValueOffset(), kv.getValueLength());
        return true;
    }
}
