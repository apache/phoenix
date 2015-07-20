package org.apache.phoenix.calcite;

import java.sql.Connection;
import java.sql.DriverManager;

import org.apache.calcite.sql.test.SqlOperatorBaseTest;

public class ExpressionFactoryValuesTest extends SqlOperatorBaseTest {
    private static final ThreadLocal<Connection> LOCAL =
            new ThreadLocal<Connection>() {
        @Override protected Connection initialValue() {
            try {
                return DriverManager.getConnection("jdbc:phoenixcalcite:");
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }
    };

    public ExpressionFactoryValuesTest() {
        super(false, tester(LOCAL.get()));
    }
}
