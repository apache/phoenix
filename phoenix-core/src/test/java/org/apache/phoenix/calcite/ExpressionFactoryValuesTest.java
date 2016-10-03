package org.apache.phoenix.calcite;

import java.sql.Connection;
import java.sql.DriverManager;

import org.apache.calcite.jdbc.CalcitePrepare;
import org.apache.calcite.jdbc.Driver;
import org.apache.calcite.linq4j.function.Function0;
import org.apache.phoenix.calcite.rules.PhoenixConverterRules;

public class ExpressionFactoryValuesTest extends SqlOperatorBaseTest {
    private static final String TEST_CONNECT_STRING_PREFIX = "jdbc:expressionfactorytest:";

    private static final ThreadLocal<Connection> LOCAL =
            new ThreadLocal<Connection>() {
        @Override protected Connection initialValue() {
            try {
                Class.forName(ExpressionFactoryTestDriver.class.getName());
                return DriverManager.getConnection(TEST_CONNECT_STRING_PREFIX);
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }
    };
    
    public static class ExpressionFactoryTestDriver extends Driver {
        static {
            new ExpressionFactoryTestDriver().register();
        }
        
        public ExpressionFactoryTestDriver() {
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
            return TEST_CONNECT_STRING_PREFIX;
        }
    }

    public ExpressionFactoryValuesTest() {
        super(false, tester(LOCAL.get()));
    }
}
