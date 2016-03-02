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

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.codehaus.jackson.map.ObjectMapper;

import com.google.common.base.Function;
import com.google.common.collect.Lists;

/**
  * Utility functions to get/put json.
  *
  */
public class TargetTableRefFunctions {

     public static Function<TargetTableRef,String> TO_JSON =  new Function<TargetTableRef,String>() {

         @Override
         public String apply(TargetTableRef input) {
             try {
                 ObjectMapper mapper = new ObjectMapper();
                 return mapper.writeValueAsString(input);
             } catch (IOException e) {
                 throw new RuntimeException(e);
             }

         }
     };

     public static Function<String,TargetTableRef> FROM_JSON =  new Function<String,TargetTableRef>() {

         @Override
         public TargetTableRef apply(String json) {
             try {
                 ObjectMapper mapper = new ObjectMapper();
                 return mapper.readValue(json, TargetTableRef.class);
             } catch (IOException e) {
                 throw new RuntimeException(e);
             }

         }
     };

     public static Function<List<TargetTableRef>,String> NAMES_TO_JSON =  new Function<List<TargetTableRef>,String>() {

         @Override
         public String apply(List<TargetTableRef> input) {
             try {
                 List<String> tableNames = Lists.newArrayListWithCapacity(input.size());
                 for(TargetTableRef table : input) {
                     tableNames.add(table.getPhysicalName());
                 }
                 ObjectMapper mapper = new ObjectMapper();
                 return mapper.writeValueAsString(tableNames);
             } catch (IOException e) {
                 throw new RuntimeException(e);
             }

         }
     };

    public static Function<List<TargetTableRef>,String> LOGICAL_NAMES_TO_JSON =  new Function<List<TargetTableRef>,String>() {

        @Override
        public String apply(List<TargetTableRef> input) {
            try {
                List<String> tableNames = Lists.newArrayListWithCapacity(input.size());
                for(TargetTableRef table : input) {
                    tableNames.add(table.getLogicalName());
                }
                ObjectMapper mapper = new ObjectMapper();
                return mapper.writeValueAsString(tableNames);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }

        }
    };

    public static Function<String,List<String>> NAMES_FROM_JSON =  new Function<String,List<String>>() {

         @SuppressWarnings("unchecked")
         @Override
         public List<String> apply(String json) {
             try {
                 ObjectMapper mapper = new ObjectMapper();
                 return mapper.readValue(json, ArrayList.class);
             } catch (IOException e) {
                 throw new RuntimeException(e);
             }

         }
     };
}
