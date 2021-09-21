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

import org.apache.phoenix.thirdparty.com.google.common.base.Function;
import org.apache.phoenix.thirdparty.com.google.common.collect.Lists;
import org.apache.phoenix.util.JacksonUtil;

/**
  * Utility functions to get/put json.
  *
  */
public class TargetTableRefFunctions {

     public static final Function<TargetTableRef,String> TO_JSON =  new Function<TargetTableRef,String>() {

         @Override
         public String apply(TargetTableRef input) {
             try {
                 return JacksonUtil.getObjectWriter().writeValueAsString(input);
             } catch (IOException e) {
                 throw new RuntimeException(e);
             }

         }
     };

     public static final Function<String,TargetTableRef> FROM_JSON =  new Function<String,TargetTableRef>() {

         @Override
         public TargetTableRef apply(String json) {
             try {
                 return JacksonUtil.getObjectReader(TargetTableRef.class).readValue(json);
             } catch (IOException e) {
                 throw new RuntimeException(e);
             }

         }
     };

     public static final Function<List<TargetTableRef>,String> NAMES_TO_JSON =  new Function<List<TargetTableRef>,String>() {

         @Override
         public String apply(List<TargetTableRef> input) {
             try {
                 List<String> tableNames = Lists.newArrayListWithCapacity(input.size());
                 for(TargetTableRef table : input) {
                     tableNames.add(table.getPhysicalName());
                 }
                 return JacksonUtil.getObjectWriter().writeValueAsString(tableNames);
             } catch (IOException e) {
                 throw new RuntimeException(e);
             }

         }
     };

    public static final Function<List<TargetTableRef>,String> LOGICAL_NAMES_TO_JSON =  new Function<List<TargetTableRef>,String>() {

        @Override
        public String apply(List<TargetTableRef> input) {
            try {
                List<String> tableNames = Lists.newArrayListWithCapacity(input.size());
                for(TargetTableRef table : input) {
                    tableNames.add(table.getLogicalName());
                }
                return JacksonUtil.getObjectWriter().writeValueAsString(tableNames);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }

        }
    };

    public static final Function<String,List<String>> NAMES_FROM_JSON =  new Function<String,List<String>>() {

         @SuppressWarnings("unchecked")
         @Override
         public List<String> apply(String json) {
             try {
                 return JacksonUtil.getObjectReader(ArrayList.class).readValue(json);
             } catch (IOException e) {
                 throw new RuntimeException(e);
             }

         }
     };
}
