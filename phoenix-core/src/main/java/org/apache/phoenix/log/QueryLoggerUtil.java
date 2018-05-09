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
package org.apache.phoenix.log;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.List;

import org.apache.commons.lang.StringUtils;
import org.apache.phoenix.query.ConnectionQueryServices;
import org.apache.phoenix.schema.PName;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableMap.Builder;

public class QueryLoggerUtil {

    public static void logInitialDetails(QueryLogger queryLogger, PName tenantId,
            ConnectionQueryServices queryServices, String query, long startTime, List<Object> bindParameters) {
        queryLogger.log(QueryLogState.STARTED,
                getInitialDetails(tenantId, queryServices, query, startTime, bindParameters));

    }

    private static ImmutableMap<QueryLogInfo, Object> getInitialDetails(PName tenantId,
            ConnectionQueryServices queryServices, String query, long startTime, List<Object> bindParameters) {
        Builder<QueryLogInfo, Object> queryLogBuilder = ImmutableMap.builder();
        String clientIP;
        try {
            clientIP = InetAddress.getLocalHost().getHostAddress();
        } catch (UnknownHostException e) {
            clientIP = "UnknownHost";
        }
        queryLogBuilder.put(QueryLogInfo.CLIENT_IP_I, clientIP);
        queryLogBuilder.put(QueryLogInfo.QUERY_I, query);
        queryLogBuilder.put(QueryLogInfo.START_TIME_I, startTime);
        if (bindParameters != null) {
            queryLogBuilder.put(QueryLogInfo.BIND_PARAMETERS_I, StringUtils.join(bindParameters,","));
        }
        if (tenantId != null) {
            queryLogBuilder.put(QueryLogInfo.TENANT_ID_I, tenantId.getString());
        }
        queryLogBuilder.put(QueryLogInfo.USER_I, queryServices.getUserName() != null ? queryServices.getUserName()
                : queryServices.getUser().getShortName());
        return queryLogBuilder.build();
    }
}
