/*
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license
 * agreements. See the NOTICE file distributed with this work for additional information regarding
 * copyright ownership. The ASF licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at http://www.apache.org/licenses/LICENSE-2.0 Unless required by applicable
 * law or agreed to in writing, software distributed under the License is distributed on an "AS IS"
 * BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License
 * for the specific language governing permissions and limitations under the License.
 */
package org.apache.phoenix.util;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.apache.phoenix.coprocessor.generated.MetaDataProtos.Property;
import org.apache.phoenix.query.QueryServices;
import org.apache.phoenix.query.QueryServicesOptions;

public class ExpressionContextFactory {

    public static ExpressionContext get(ReadOnlyProps props) {
        Boolean jdbcCompilantTZHandling =
                props.getBoolean(QueryServices.JDBC_COMPLIANT_TZ_HANDLING,
                    QueryServicesOptions.DEFAULT_JDBC_COMPLIANT_TZ_HANDLING);
        String dateFormatTimeZoneID =
                props.get(QueryServices.DATE_FORMAT_TIMEZONE_ATTRIB, DateUtil.DEFAULT_TIME_ZONE_ID);
        String datePattern =
                props.get(QueryServices.DATE_FORMAT_ATTRIB,
                    QueryServicesOptions.DEFAULT_DATE_FORMAT);
        String timePattern =
                props.get(QueryServices.TIME_FORMAT_ATTRIB,
                    QueryServicesOptions.DEFAULT_TIME_FORMAT);
        String timestampPattern =
                props.get(QueryServices.TIMESTAMP_FORMAT_ATTRIB,
                    QueryServicesOptions.DEFAULT_TIMESTAMP_FORMAT);
        String timezoneOverride = 
                props.get(QueryServices.TIMEZONE_OVERRIDE);
        if (jdbcCompilantTZHandling) {
            return new CompliantExpressionContext(datePattern, timePattern, timestampPattern,
                timezoneOverride);
        } else {
            return new GMTExpressionContext(datePattern, timePattern, timestampPattern,
                    dateFormatTimeZoneID);
        }
    }

    public static ExpressionContext get(Properties props) {
        return get(ReadOnlyProps.EMPTY_PROPS.addAll(props));
    }
    
    public static ExpressionContext getGMTServerSide() {
        return get(ReadOnlyProps.EMPTY_PROPS);
    }

    public static ExpressionContext getCompliantServerSide(String datePattern, String timePattern,
            String timestampPattern, String timezoneOverride) {
        return new CompliantExpressionContext(datePattern, timePattern, timestampPattern,
            timezoneOverride);
    }

    public static ExpressionContext fromProtobuf(List<Property> protoProps) {
        Map<String, String> propsMap = new HashMap<String, String>();
        for (Property protoProp : protoProps) {
            propsMap.put(protoProp.getKey(), protoProp.getValue());
        }
        return get(new ReadOnlyProps(propsMap));
    }
}
