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
package org.apache.phoenix.util;

import javax.annotation.Nullable;

import org.apache.hadoop.hbase.util.Bytes;
import org.apache.phoenix.jdbc.PhoenixConnection;

public class LogUtil {

	private LogUtil() {
    }

    public static String addCustomAnnotations(@Nullable String logLine, @Nullable PhoenixConnection con) {
    	if (con == null || con.getCustomTracingAnnotations() == null || con.getCustomTracingAnnotations().isEmpty()) {
            return logLine;
    	} else {
    		return customAnnotationsToString(con) + ' ' + logLine;
    	}
    }
    
    public static String addCustomAnnotations(@Nullable String logLine, @Nullable byte[] annotations) {
    	if (annotations == null) {
            return logLine;
    	} else {
    		return Bytes.toString(annotations) + ' ' + logLine;
    	}
    }
    
    public static String customAnnotationsToString(@Nullable PhoenixConnection con) {
    	if (con == null || con.getCustomTracingAnnotations() == null || con.getCustomTracingAnnotations().isEmpty()) {
            return null;
        } else {
        	return con.getCustomTracingAnnotations().toString();
        }
    }
}
