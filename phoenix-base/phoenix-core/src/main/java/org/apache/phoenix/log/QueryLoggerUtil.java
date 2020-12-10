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

import org.apache.commons.lang3.StringUtils;
import org.apache.phoenix.query.ConnectionQueryServices;
import org.apache.phoenix.schema.PName;

public class QueryLoggerUtil {


    public static void logInitialDetails(QueryLogger queryLogger, PName tenantId, ConnectionQueryServices queryServices,
            String query, List<Object> bindParameters) {
        try {
            String clientIP;
            try {
                clientIP = InetAddress.getLocalHost().getHostAddress();
            } catch (UnknownHostException e) {
                clientIP = "UnknownHost";
            }

            if (clientIP != null) {
                queryLogger.log(QueryLogInfo.CLIENT_IP_I, clientIP);
            }
            if (query != null) {
                queryLogger.log(QueryLogInfo.QUERY_I, query);
            }
            if (bindParameters != null) {
                queryLogger.log(QueryLogInfo.BIND_PARAMETERS_I, StringUtils.join(bindParameters, ","));
            }
            if (tenantId != null) {
                queryLogger.log(QueryLogInfo.TENANT_ID_I, tenantId.getString());
            }

            queryLogger.log(QueryLogInfo.USER_I, queryServices.getUserName() != null ? queryServices.getUserName()
                    : queryServices.getUser().getShortName());
        } catch (Exception e) {
            // Ignore
        }
    }
}
