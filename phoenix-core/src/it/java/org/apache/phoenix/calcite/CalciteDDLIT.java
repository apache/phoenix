package org.apache.phoenix.calcite;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.Properties;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.phoenix.query.QueryServices;
import org.junit.Ignore;
import org.junit.Test;

public class CalciteDDLIT extends BaseCalciteIT {
    private static final Properties PROPS = new Properties();
    
    @Test public void testCreateView() throws Exception {
        start(PROPS).sql("create table \"t0\"(\"a\" varchar(20) not null primary key, b integer)").execute().close();
        start(PROPS).sql("create view v1 as select * from \"t0\"").execute().close();
        start(PROPS).sql("create view v2 as select * from \"t0\" where \"a\" = 'x'").execute().close();
    }

    @Test public void testCreateTable() throws Exception {
        start(PROPS).sql("create table t1(a varchar(20) not null primary key, b integer, c decimal(10, 2), d integer array, e varchar array[5])").execute().close();
        start(PROPS).sql("create table if not exists t1(a varchar not null primary key, b integer)").execute().close();
    }

    @Test public void testCreateTableWithPrimaryKeyConstraint() throws Exception {
        start(PROPS).sql("create table t2(a bigint not null ROW_TIMESTAMP, b integer not null, c double constraint pk primary key(a,b)) SPLIT ON('a','b')").execute().close();
    }

    @Test public void testCreateTableWithTableOptions() throws Exception {
        start(PROPS).sql("create table t3(a bigint not null ROW_TIMESTAMP, b integer not null, c double constraint pk primary key(a,b)) SALT_BUCKET=4").execute().close();
    }

    @Test public void testCreateTableWithTableOptionsAndSplits() throws Exception {
        start(PROPS).sql("create table t4(a bigint not null ROW_TIMESTAMP, b integer not null, c double constraint pk primary key(a,b)) SALT_BUCKET=4,VERSIONS=5 SPLIT ON('a','b')").execute().close();
    }

    @Test public void testCreateAndDropIndex() throws Exception {
        start(PROPS).sql("create table itest(a varchar(20) not null primary key, b integer, c decimal(10, 2), d bigint, e varchar)").execute().close();
        start(PROPS).sql("create index idx1 on itest(b desc, c) include (d)").execute().close();
        start(PROPS).sql("create local index idx2 on itest(d, e desc)").execute().close();
        start(PROPS).sql("create index if not exists idx3 on itest(b + c) include (d, e)").execute().close();
        start(PROPS).sql("drop index idx1 on itest").execute().close();
        start(PROPS).sql("drop index if exists idx2 on itest").execute().close();
        start(PROPS).sql("drop index idx3 on itest").execute().close();
    }
    
    @Test public void testCreateAndDropSequence() throws Exception {
        start(PROPS).sql("create sequence if not exists s0 start with 2 increment 3 minvalue 2 maxvalue 90 cycle cache 3").execute().close();
        start(PROPS).sql("drop sequence if exists s0").execute().close();
    }
    
    @Test public void testDropTable() throws Exception {
        start(PROPS).sql("create table t5(a varchar not null primary key, b varchar)").execute().close();
        start(PROPS).sql("drop table t5").execute().close();
        start(PROPS).sql("create table t5(a bigint not null primary key, b varchar)").execute().close();
    }

    @Test public void testCreateAndDropFunction() throws Exception {
        start(PROPS).sql("create function myreverse(VARCHAR) returns VARCHAR as 'org.apache.phoenix.end2end.MyReverse' using jar 'hdfs://localhost:51573/hbase/tmpjars/myjar1.jar'").execute().close();
        start(PROPS).sql("create or replace function myfunction(INTEGER, INTEGER CONSTANT defaultValue=10 minvalue=1 maxvalue=15) returns INTEGER as 'org.apache.phoenix.end2end.MyReverse' using jar 'hdfs://localhost:51573/hbase/tmpjars/myjar1.jar'").execute().close();
        start(PROPS).sql("drop function if exists myfunction").execute().close();
        start(PROPS).sql("create or replace function myfunction(VARCHAR ARRAY, INTEGER CONSTANT defaultValue=10 minvalue=1 maxvalue=15) returns INTEGER as 'org.apache.phoenix.end2end.MyReverse' using jar 'hdfs://localhost:51573/hbase/tmpjars/myjar1.jar'").execute().close();
    }

    @Test public void testUploadAndDeleteJars() throws Exception {
        String jarName = "myjar.jar";
        String jarPath = "." + File.separator + jarName;
        File jarFile = new File(jarPath);
        jarFile.createNewFile();
        start(PROPS).sql("upload jars '"+jarFile.getAbsolutePath()+"'").execute().close();
        Start start = start(PROPS);
        Statement statement = start.getConnection().createStatement();
        final String sql = "select jar_location\n"
              + "from table(\"ListJars\"())";
        ResultSet rs = statement.executeQuery(sql);
        assertTrue(rs.next());
        Configuration conf = HBaseConfiguration.create();
        start(PROPS).sql("delete jar '"+ conf.get(QueryServices.DYNAMIC_JARS_DIR_KEY)+"/"+jarName+"'").execute().close();
        rs = statement.executeQuery(sql);
        assertFalse(rs.next());
    }
    
    @Ignore
    @Test public void testUpdateStatistics() throws Exception {
        start(PROPS).sql("create table stest(a varchar(20) not null primary key, b integer)").execute().close();
        start(PROPS).sql("update statistics stest columns set dummy=2").execute().close();
    }
}
