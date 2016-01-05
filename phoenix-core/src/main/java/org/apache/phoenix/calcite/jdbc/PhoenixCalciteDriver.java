package org.apache.phoenix.calcite.jdbc;

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
import org.apache.calcite.schema.SchemaPlus;
import org.apache.phoenix.calcite.PhoenixSchema;
import org.apache.phoenix.calcite.rules.PhoenixConverterRules;
import org.apache.phoenix.calcite.type.PhoenixRelDataTypeSystem;
import org.apache.phoenix.util.PhoenixRuntime;

import com.google.common.collect.Maps;

public class PhoenixCalciteDriver extends Driver {
    public static final String CONNECT_STRING_PREFIX = "jdbc:phoenixcalcite:";

    static {
        new PhoenixCalciteDriver().register();
    }
    
    public PhoenixCalciteDriver() {
        super();
    }

    @Override protected Function0<CalcitePrepare> createPrepareFactory() {
        return new Function0<CalcitePrepare>() {
            @Override
            public CalcitePrepare apply() {
                return new PhoenixPrepareImpl(PhoenixConverterRules.CONVERTIBLE_RULES);
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
        info2.setProperty(CalciteConnectionProperty.TYPE_SYSTEM.camelName(),
                PhoenixRelDataTypeSystem.class.getName());
        
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
        final String phoenixUrl = delimiter < 0 ? urlSuffix : urlSuffix.substring(0, delimiter);
        url = delimiter < 0 ? prefix : (prefix + urlSuffix.substring(delimiter + 1));
        final CalciteConnection connection = (CalciteConnection) super.connect(url, info2);
        Map<String, Object> operand = Maps.newHashMap();
        for (Entry<Object, Object> entry : info.entrySet()) {
            operand.put((String) entry.getKey(), entry.getValue());
        }
        operand.put("url", PhoenixRuntime.JDBC_PROTOCOL + PhoenixRuntime.JDBC_PROTOCOL_SEPARATOR + phoenixUrl);
        SchemaPlus rootSchema = connection.getRootSchema();
        rootSchema.add("phoenix",
                PhoenixSchema.FACTORY.create(rootSchema, "phoenix", operand));
        connection.setSchema("phoenix");
        
        return connection;
    }
}
