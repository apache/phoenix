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
package org.apache.phoenix.util.queryid;

import org.apache.phoenix.query.QueryConstants;
import org.apache.phoenix.util.ReadOnlyProps;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.InvocationTargetException;

import static org.apache.phoenix.query.QueryConstants.GENERATE_QUERYID_DEFAUlT_IMPLEMENTATION;

/**
 * Utility class to handle QueryId
 */
public class QueryIdUtil {
    private static final Logger LOG = LoggerFactory.getLogger(QueryIdUtil.class);

    public static String GetQueryId(ReadOnlyProps props) {
        String queryId = null;
        try {
            QueryIdHandler
                    queryIdHandler =
                    getHandler(props.get(QueryConstants.GENERATE_QUERYID_IMPLEMENTATION,
                            GENERATE_QUERYID_DEFAUlT_IMPLEMENTATION));
            queryId = queryIdHandler.getQueryId(props);
        } catch (Exception exception) {
            LOG.error("An exception occurred while getting QueryId", exception);
        }
        return queryId;
    }

    private static QueryIdHandler getHandler(String classpath)
            throws ClassNotFoundException, NoSuchMethodException, InvocationTargetException,
            InstantiationException, IllegalAccessException {
        Class<?> handlerClass = Class.forName(classpath);
        handlerClass.getDeclaredConstructor().setAccessible(true);
        return (QueryIdHandler) handlerClass.getDeclaredConstructor().newInstance();
    }
}
