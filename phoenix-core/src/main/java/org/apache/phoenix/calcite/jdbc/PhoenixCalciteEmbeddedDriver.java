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
package org.apache.phoenix.calcite.jdbc;

import static org.apache.phoenix.util.PhoenixRuntime.PHOENIX_TEST_DRIVER_URL_PARAM;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.Map;
import java.util.Properties;
import java.util.Map.Entry;

import org.apache.calcite.config.CalciteConnectionProperty;
import org.apache.calcite.jdbc.CalciteConnection;
import org.apache.calcite.jdbc.CalcitePrepare;
import org.apache.calcite.jdbc.Driver;
import org.apache.calcite.linq4j.function.Function0;
import org.apache.calcite.schema.Schema;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.phoenix.calcite.PhoenixPrepareImpl;
import org.apache.phoenix.calcite.PhoenixSchema;
import org.apache.phoenix.calcite.rules.PhoenixConverterRules;
import org.apache.phoenix.calcite.type.PhoenixRelDataTypeSystem;
import org.apache.phoenix.util.PhoenixRuntime;
import org.apache.phoenix.util.SQLCloseable;

import com.google.common.collect.Maps;

public abstract class PhoenixCalciteEmbeddedDriver extends Driver implements SQLCloseable {
    public static final String CONNECT_STRING_PREFIX = "jdbc:phoenixcalcite:";
    
    private static final String TEST_URL_AT_END =  "" + PhoenixRuntime.JDBC_PROTOCOL_TERMINATOR + PHOENIX_TEST_DRIVER_URL_PARAM;
    private static final String TEST_URL_IN_MIDDLE = TEST_URL_AT_END + PhoenixRuntime.JDBC_PROTOCOL_TERMINATOR;
    
    public static boolean isTestUrl(String url) {
        return url.endsWith(TEST_URL_AT_END) || url.contains(TEST_URL_IN_MIDDLE);
    }
   
    public PhoenixCalciteEmbeddedDriver() {
        super();
    }

    @Override protected Function0<CalcitePrepare> createPrepareFactory() {
        return new Function0<CalcitePrepare>() {
            @Override
            public CalcitePrepare apply() {
                return new PhoenixPrepareImpl(PhoenixConverterRules.RULES);
            }          
        };
    }

    @Override protected String getConnectStringPrefix() {
        return CONNECT_STRING_PREFIX;
    }
    
    @Override protected String getFactoryClassName(JdbcVersion jdbcVersion) {
        switch (jdbcVersion) {
        case JDBC_30:
        case JDBC_40:
            throw new IllegalArgumentException("JDBC version not supported: "
                    + jdbcVersion);
        case JDBC_41:
        default:
            return "org.apache.calcite.jdbc.PhoenixCalciteFactory";
        }
    }

    public Connection connect(String url, Properties info) throws SQLException {
        if (!acceptsURL(url)) {
            return null;
        }
        
        Properties info2 = new Properties(info);
        setPropertyIfNotSpecified(
                info2,
                CalciteConnectionProperty.TYPE_SYSTEM.camelName(),
                PhoenixRelDataTypeSystem.class.getName());
        setPropertyIfNotSpecified(
                info2,
                CalciteConnectionProperty.MATERIALIZATIONS_ENABLED.camelName(),
                Boolean.TRUE.toString());
        
        final String prefix = getConnectStringPrefix();
        assert url.startsWith(prefix);
        final String urlSuffix = url.substring(prefix.length());
        final int delimiter = urlSuffix.indexOf(';');
        final int eq = urlSuffix.indexOf('=');
        if ((delimiter < 0 && eq > 0) || eq < delimiter) {
            return super.connect(url, info2);
        }
        
        // URLs that start with a non-property-pair string will be treated as Phoenix
        // connection URL and will be used to create a Phoenix schema. A short form
        // of this URL can be the connection string prefix itself.
        final CalciteConnection connection = (CalciteConnection) super.connect(url, info2);
        Map<String, Object> operand = Maps.newHashMap();
        for (Entry<Object, Object> entry : info.entrySet()) {
            operand.put((String) entry.getKey(), entry.getValue());
        }
        final String phoenixUrl = url.replaceFirst(PhoenixRuntime.JDBC_PROTOCOL_CALCITE, PhoenixRuntime.JDBC_PROTOCOL);
        operand.put("url", phoenixUrl);
        SchemaPlus rootSchema = connection.getRootSchema();
        Schema schema = PhoenixSchema.FACTORY.create(rootSchema, "phoenix", operand);
        ((PhoenixSchema)schema).setTypeFactory(connection.getTypeFactory());
        rootSchema.add("phoenix",schema);
        connection.setSchema("phoenix");
        
        return connection;
    }
    
    private static void setPropertyIfNotSpecified(Properties props, String key, String value) {
        if (props.getProperty(key) == null) {
            props.setProperty(key, value);
        }
    }
}
