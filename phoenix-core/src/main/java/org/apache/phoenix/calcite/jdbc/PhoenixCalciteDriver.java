package org.apache.phoenix.calcite.jdbc;

import java.sql.SQLException;

public class PhoenixCalciteDriver extends PhoenixCalciteEmbeddedDriver {
    
    static {
        new PhoenixCalciteDriver().register();
    }
    
    public PhoenixCalciteDriver() {
        super();
    }

    @Override
    public boolean acceptsURL(String url) throws SQLException {
        return super.acceptsURL(url) && !isTestUrl(url);
    }

    @Override
    public void close() throws SQLException {
    }

}
