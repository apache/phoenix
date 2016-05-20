package org.apache.phoenix.calcite;

import java.util.Properties;

import org.junit.Test;

public class CalciteDDLIT extends BaseCalciteIT {
    private static final Properties PROPS = new Properties();
    
    @Test public void testCreateView() throws Exception {
        start(PROPS).sql("create view v as select * from (values (1, 'a'), (2, 'b')) as t(x, y)").execute();
    }

    @Test public void testCreateTable() throws Exception {
        start(PROPS).sql("create table t(a varchar primary key, b integer not null) ").execute();
    }

    @Test public void testCreateTableWithPrimaryKeyConstraint() throws Exception {
        start(PROPS).sql("create table t(a varchar not null ROW_TIMESTAMP, b integer not null, c double, constraint pk primary key(a,b)) SPLIT ON('a','b')").execute();
    }
}
