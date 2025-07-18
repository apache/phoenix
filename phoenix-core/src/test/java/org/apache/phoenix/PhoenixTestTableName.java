/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to you under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.phoenix;

import org.apache.hadoop.hbase.TableName;
import org.junit.rules.TestWatcher;
import org.junit.runner.Description;

public class PhoenixTestTableName extends TestWatcher {

    private String tableName;

    @Override
    protected void starting(Description description) {
        tableName = TableName.valueOf(cleanUpTestName(description.getClassName())).getNameAsString();
    }

    /**
     * Helper to handle parameterized method names. Unlike regular test methods, parameterized method
     * names look like 'foo[x]'. This is problematic for tests that use this name for HBase tables.
     * This helper strips out the parameter suffixes.
     * @return current test method name with out parameterized suffixes.
     */
    public static String cleanUpTestName(String className) {
        int lastIndex = className.lastIndexOf('.');
        if (lastIndex == -1) {
            return className;
        }
        String testName = className.substring(lastIndex+1).toUpperCase();
        if(testName.contains("$")){
            return testName.replace("$", "_");
        }
        return className.substring(lastIndex+1).toUpperCase();
    }

    public String getTableName() {
        return tableName;
    }
    public void setTableName(String tableName) {
        this.tableName = tableName;
    }
}

