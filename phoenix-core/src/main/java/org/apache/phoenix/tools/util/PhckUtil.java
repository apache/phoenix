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
package org.apache.phoenix.tools.util;

import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;
import org.slf4j.Logger;

import java.sql.Connection;
import java.sql.SQLException;

import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.*;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.SYSTEM_CATALOG_NAME;

public class PhckUtil {
    public static final String EMPTY_COLUMN_VALUE = " ";
    public static final String NULL_VALUE = "null";

    public enum PHCK_ROW_RESOURCE {
        CATALOG,
        CHILD_LINK,
    }

    public static Options getOptions() {
        final Options options = new Options();
        return options;
    }


    public static String[] purgeFirst(String[] args) {
        int size = args.length;
        if (size <= 1) {
            return null;
        }
        size--;
        String [] result = new String [size];
        System.arraycopy(args, 1, result, 0, size);
        return result;
    }
}
