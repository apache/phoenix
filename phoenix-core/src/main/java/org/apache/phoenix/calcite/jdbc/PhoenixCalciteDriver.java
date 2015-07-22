package org.apache.phoenix.calcite.jdbc;

import org.apache.calcite.jdbc.CalcitePrepare;
import org.apache.calcite.jdbc.Driver;
import org.apache.calcite.linq4j.function.Function0;
import org.apache.phoenix.calcite.rules.PhoenixConverterRules;

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
}
