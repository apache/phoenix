package org.apache.phoenix.calcite;

import java.util.Properties;

import org.junit.Test;

public class CalciteDDLIT extends BaseCalciteIT {
    private static final Properties PROPS = new Properties();
    
    @Test public void testCreateView() throws Exception {
        start(PROPS).sql("create view v as select * from (values (1, 'a'), (2, 'b')) as t(x, y)").execute();
    }

    @Test public void testCreateTable() throws Exception {
        start(PROPS).sql("create table t1(a varchar(20) not null primary key, b integer, c decimal(10, 2), d integer array, e varchar array[5])").execute();
        start(PROPS).sql("create table if not exists t1(a varchar not null primary key, b integer)").execute();
    }

    @Test public void testCreateTableWithPrimaryKeyConstraint() throws Exception {
        start(PROPS).sql("create table t2(a bigint not null ROW_TIMESTAMP, b integer not null, c double constraint pk primary key(a,b)) SPLIT ON('a','b')").execute();
    }

    @Test public void testCreateTableWithTableOptions() throws Exception {
        start(PROPS).sql("create table t3(a bigint not null ROW_TIMESTAMP, b integer not null, c double constraint pk primary key(a,b)) SALT_BUCKET=4").execute();
    }

    @Test public void testCreateTableWithTableOptionsAndSplits() throws Exception {
        start(PROPS).sql("create table t4(a bigint not null ROW_TIMESTAMP, b integer not null, c double constraint pk primary key(a,b)) SALT_BUCKET=4,VERSIONS=5 SPLIT ON('a','b')").execute();
    }
}
