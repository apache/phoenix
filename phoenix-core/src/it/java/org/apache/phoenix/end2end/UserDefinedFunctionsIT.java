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
package org.apache.phoenix.end2end;

import static org.apache.phoenix.util.PhoenixRuntime.JDBC_PROTOCOL;
import static org.apache.phoenix.util.PhoenixRuntime.JDBC_PROTOCOL_SEPARATOR;
import static org.apache.phoenix.util.PhoenixRuntime.JDBC_PROTOCOL_TERMINATOR;
import static org.apache.phoenix.util.PhoenixRuntime.PHOENIX_TEST_DRIVER_URL_PARAM;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.SYSTEM_CATALOG_SCHEMA;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.SYSTEM_FUNCTION_TABLE;
import static org.apache.phoenix.query.QueryServices.DYNAMIC_JARS_DIR_KEY;
import static org.apache.phoenix.util.TestUtil.LOCALHOST;
import static org.junit.Assert.*;

import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.InputStream;
import java.io.OutputStream;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.Map;
import java.util.Properties;
import java.util.jar.Attributes;
import java.util.jar.JarEntry;
import java.util.jar.JarOutputStream;
import java.util.jar.Manifest;

import javax.tools.JavaCompiler;
import javax.tools.ToolProvider;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.io.IOUtils;
import org.apache.phoenix.expression.function.UDFExpression;
import org.apache.phoenix.jdbc.PhoenixTestDriver;
import org.apache.phoenix.query.BaseTest;
import org.apache.phoenix.query.QueryServices;
import org.apache.phoenix.schema.FunctionAlreadyExistsException;
import org.apache.phoenix.schema.FunctionNotFoundException;
import org.apache.phoenix.schema.ValueRangeExcpetion;
import org.apache.phoenix.util.PhoenixRuntime;
import org.apache.phoenix.util.QueryUtil;
import org.apache.phoenix.util.ReadOnlyProps;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import com.google.common.collect.Maps;

public class UserDefinedFunctionsIT extends BaseTest{
    
    protected static final String TENANT_ID = "ZZTop";
    private static String url;
    private static PhoenixTestDriver driver;
    private static HBaseTestingUtility util;

    private static String STRING_REVERSE_EVALUATE_METHOD =
            new StringBuffer()
                    .append("    public boolean evaluate(Tuple tuple, ImmutableBytesWritable ptr) {\n")
                    .append("        Expression arg = getChildren().get(0);\n")
                    .append("        if (!arg.evaluate(tuple, ptr)) {\n")
                    .append("           return false;\n")
                    .append("       }\n")
                    .append("       int targetOffset = ptr.getLength();\n")
                    .append("       if (targetOffset == 0) {\n")
                    .append("            return true;\n")
                    .append("        }\n")
                    .append("        byte[] source = ptr.get();\n")
                    .append("        byte[] target = new byte[targetOffset];\n")
                    .append("        int sourceOffset = ptr.getOffset(); \n")
                    .append("        int endOffset = sourceOffset + ptr.getLength();\n")
                    .append("        SortOrder sortOrder = arg.getSortOrder();\n")
                    .append("        while (sourceOffset < endOffset) {\n")
                    .append("            int nBytes = StringUtil.getBytesInChar(source[sourceOffset], sortOrder);\n")
                    .append("            targetOffset -= nBytes;\n")
                    .append("            System.arraycopy(source, sourceOffset, target, targetOffset, nBytes);\n")
                    .append("            sourceOffset += nBytes;\n")
                    .append("        }\n")
                    .append("        ptr.set(target);\n")
                    .append("        return true;\n")
                    .append("    }\n").toString();

    private static String SUM_COLUMN_VALUES_EVALUATE_METHOD =
            new StringBuffer()
                    .append("    public boolean evaluate(Tuple tuple, ImmutableBytesWritable ptr) {\n")
                    .append("        int[] array = new int[getChildren().size()];\n")
                    .append("        int i = 0;\n")
                    .append("        for(Expression child:getChildren()) {\n")
                    .append("            if (!child.evaluate(tuple, ptr)) {\n")
                    .append("                return false;\n")
                    .append("            }\n")
                    .append("            int targetOffset = ptr.getLength();\n")
                    .append("            if (targetOffset == 0) {\n")
                    .append("                return true;\n")
                    .append("            }\n")
                    .append("            array[i++] = (Integer) PInteger.INSTANCE.toObject(ptr);\n")
                    .append("        }\n")
                    .append("        int sum = 0;\n")
                    .append("        for(i=0;i<getChildren().size();i++) {\n")
                    .append("            sum+=array[i];\n")
                    .append("        }\n")
                    .append("        ptr.set(PInteger.INSTANCE.toBytes((Integer)sum));\n")
                    .append("        return true;\n")
                    .append("    }\n").toString();

    private static String MY_REVERSE_CLASS_NAME = "MyReverse";
    private static String MY_SUM_CLASS_NAME = "MySum";
    private static String MY_REVERSE_PROGRAM = getProgram(MY_REVERSE_CLASS_NAME, STRING_REVERSE_EVALUATE_METHOD, "PVarchar");
    private static String MY_SUM_PROGRAM = getProgram(MY_SUM_CLASS_NAME, SUM_COLUMN_VALUES_EVALUATE_METHOD, "PInteger");
    private static Properties EMPTY_PROPS = new Properties();
    

    private static String getProgram(String className, String evaluateMethod, String returnType) {
        return new StringBuffer()
                .append("package org.apache.phoenix.end2end;\n")
                .append("import java.sql.SQLException;\n")
                .append("import java.sql.SQLException;\n")
                .append("import java.util.List;\n")
                .append("import org.apache.hadoop.hbase.io.ImmutableBytesWritable;\n")
                .append("import org.apache.phoenix.expression.Expression;\n")
                .append("import org.apache.phoenix.expression.function.ScalarFunction;\n")
                .append("import org.apache.phoenix.schema.SortOrder;\n")
                .append("import org.apache.phoenix.schema.tuple.Tuple;\n")
                .append("import org.apache.phoenix.schema.types.PDataType;\n")
                .append("import org.apache.phoenix.schema.types.PInteger;\n")
                .append("import org.apache.phoenix.schema.types.PVarchar;\n")
                .append("import org.apache.phoenix.util.StringUtil;\n")
                .append("public class "+className+" extends ScalarFunction{\n")
                .append("    public static final String NAME = \"MY_REVERSE\";\n")
                .append("    public "+className+"() {\n")
                .append("    }\n")
                .append("    public "+className+"(List<Expression> children) throws SQLException {\n")
                .append("        super(children);\n")
                .append("    }\n")
                .append("    @Override\n")
                .append(evaluateMethod)
                .append("    @Override\n")
                .append("    public SortOrder getSortOrder() {\n")
                .append("        return getChildren().get(0).getSortOrder();\n")
                .append("    }\n")
                .append("  @Override\n")
                .append("   public PDataType getDataType() {\n")
                .append("       return "+returnType+".INSTANCE;\n")
                .append("    }\n")
                .append("    @Override\n")
                .append("    public String getName() {\n")
                .append("        return NAME;\n")
                .append("    }\n")
                .append("}\n").toString();
    }

    @BeforeClass
    public static void doSetup() throws Exception {
        Configuration conf = HBaseConfiguration.create();
        setUpConfigForMiniCluster(conf);
        util = new HBaseTestingUtility(conf);
        util.startMiniDFSCluster(1);
        util.startMiniZKCluster(1);
        String string = util.getConfiguration().get("fs.defaultFS");
        conf.set(DYNAMIC_JARS_DIR_KEY, string+"/hbase/tmpjars");
        util.startMiniHBaseCluster(1, 1);
        UDFExpression.setConfig(conf);
        compileTestClass(MY_REVERSE_CLASS_NAME, MY_REVERSE_PROGRAM, 1);
        compileTestClass(MY_SUM_CLASS_NAME, MY_SUM_PROGRAM, 2);
        String clientPort = util.getConfiguration().get(QueryServices.ZOOKEEPER_PORT_ATTRIB);
        url =
                JDBC_PROTOCOL + JDBC_PROTOCOL_SEPARATOR + LOCALHOST + JDBC_PROTOCOL_SEPARATOR
                        + clientPort + JDBC_PROTOCOL_TERMINATOR + PHOENIX_TEST_DRIVER_URL_PARAM;
        Map<String, String> props = Maps.newHashMapWithExpectedSize(1);
        props.put(QueryServices.ALLOW_USER_DEFINED_FUNCTIONS_ATTRIB, "true");
        driver = initAndRegisterDriver(url, new ReadOnlyProps(props.entrySet().iterator()));
    }
    
    @Test
    public void testCreateFunction() throws Exception {
        Connection conn = driver.connect(url, EMPTY_PROPS);
        Statement stmt = conn.createStatement();
        conn.createStatement().execute("create table t(k integer primary key, firstname varchar, lastname varchar)");
        stmt.execute("upsert into t values(1,'foo','jock')");
        conn.commit();
        stmt.execute("create function myreverse(VARCHAR) returns VARCHAR as 'org.apache.phoenix.end2end."+MY_REVERSE_CLASS_NAME+"' using jar "
                + "'"+util.getConfiguration().get(DYNAMIC_JARS_DIR_KEY) + "/myjar1.jar"+"'");
        ResultSet rs = stmt.executeQuery("select myreverse(firstname) from t");
        assertTrue(rs.next());
        assertEquals("oof", rs.getString(1));
        assertFalse(rs.next());
        rs = stmt.executeQuery("select * from t where myreverse(firstname)='oof'");
        assertTrue(rs.next());
        assertEquals(1, rs.getInt(1));
        assertEquals("foo", rs.getString(2));        
        assertEquals("jock", rs.getString(3));
        assertFalse(rs.next());
        
        try {
            stmt.execute("create function myreverse(VARCHAR) returns VARCHAR as 'org.apache.phoenix.end2end."+MY_REVERSE_CLASS_NAME+"' using jar "
                    + "'"+util.getConfiguration().get(DYNAMIC_JARS_DIR_KEY) + "/myjar1.jar"+"'");
            fail("Duplicate function should not be created.");
        } catch(FunctionAlreadyExistsException e) {
        }
        // without specifying the jar should pick the class from path of hbase.dynamic.jars.dir configuration. 
        stmt.execute("create function myreverse2(VARCHAR) returns VARCHAR as 'org.apache.phoenix.end2end."+MY_REVERSE_CLASS_NAME+"'");
        rs = stmt.executeQuery("select myreverse2(firstname) from t");
        assertTrue(rs.next());
        assertEquals("oof", rs.getString(1));        
        assertFalse(rs.next());
        rs = stmt.executeQuery("select * from t where myreverse2(firstname)='oof'");
        assertTrue(rs.next());
        assertEquals(1, rs.getInt(1));
        assertEquals("foo", rs.getString(2));        
        assertEquals("jock", rs.getString(3));
        assertFalse(rs.next());
        conn.createStatement().execute("create table t3(tenant_id varchar not null, k integer not null, firstname varchar, lastname varchar constraint pk primary key(tenant_id,k)) MULTI_TENANT=true");
        // Function created with global id should be accessible.
        Connection conn2 = driver.connect(url+";"+PhoenixRuntime.TENANT_ID_ATTRIB+"="+TENANT_ID, EMPTY_PROPS);
        try {
            conn2.createStatement().execute("upsert into t3 values(1,'foo','jock')");
            conn2.commit();
            conn2.createStatement().execute("create function myreverse(VARCHAR) returns VARCHAR as 'org.apache.phoenix.end2end."+MY_REVERSE_CLASS_NAME+"' using jar "
                    + "'"+util.getConfiguration().get(DYNAMIC_JARS_DIR_KEY) + "/myjar1.jar"+"'");
            rs = conn2.createStatement().executeQuery("select myreverse(firstname) from t3");
            assertTrue(rs.next());
            assertEquals("oof", rs.getString(1)); 
        } catch(FunctionAlreadyExistsException e) {
            fail("FunctionAlreadyExistsException should not be thrown");
        }
        // calling global udf on tenant specific specific connection.
        rs = conn2.createStatement().executeQuery("select myreverse2(firstname) from t3");
        assertTrue(rs.next());
        assertEquals("oof", rs.getString(1));
        try {
            conn2.createStatement().execute("drop function myreverse2");
            fail("FunctionNotFoundException should be thrown");
        } catch(FunctionNotFoundException e){
            
        }
        conn.createStatement().execute("drop function myreverse2");
        try {
            rs = conn2.createStatement().executeQuery("select myreverse2(firstname) from t3");
            fail("FunctionNotFoundException should be thrown.");
        } catch(FunctionNotFoundException e){
            
        }
        try{
            rs = conn2.createStatement().executeQuery("select unknownFunction(firstname) from t3");
            fail("FunctionNotFoundException should be thrown.");
        } catch(FunctionNotFoundException e) {
            
        }
    }

    @Test
    public void testSameUDFWithDifferentImplementationsInDifferentTenantConnections() throws Exception {
        Connection nonTenantConn = driver.connect(url, EMPTY_PROPS);
        nonTenantConn.createStatement().execute("create function myfunction(VARCHAR) returns VARCHAR as 'org.apache.phoenix.end2end."+MY_REVERSE_CLASS_NAME+"' using jar "
                + "'"+util.getConfiguration().get(DYNAMIC_JARS_DIR_KEY) + "/myjar1.jar"+"'");
        try {
            nonTenantConn.createStatement().execute("create function myfunction(VARCHAR) returns VARCHAR as 'org.apache.phoenix.end2end.UnknownClass' using jar "
                    + "'"+util.getConfiguration().get(DYNAMIC_JARS_DIR_KEY) + "/myjar1.jar"+"'");
            fail("FunctionAlreadyExistsException should be thrown.");
        } catch(FunctionAlreadyExistsException e) {
            
        }
        String tenantId1="tenId1";
        String tenantId2="tenId2";
        nonTenantConn.createStatement().execute("create table t7(tenant_id varchar not null, k integer not null, k1 integer, name varchar constraint pk primary key(tenant_id, k)) multi_tenant=true");
        Connection tenant1Conn = driver.connect(url+";"+PhoenixRuntime.TENANT_ID_ATTRIB+"="+tenantId1, EMPTY_PROPS);
        Connection tenant2Conn = driver.connect(url+";"+PhoenixRuntime.TENANT_ID_ATTRIB+"="+tenantId2, EMPTY_PROPS);
        tenant1Conn.createStatement().execute("upsert into t7 values(1,1,'jock')");
        tenant1Conn.commit();
        tenant2Conn.createStatement().execute("upsert into t7 values(1,2,'jock')");
        tenant2Conn.commit();
        tenant1Conn.createStatement().execute("create function myfunction(VARCHAR) returns VARCHAR as 'org.apache.phoenix.end2end."+MY_REVERSE_CLASS_NAME+"' using jar "
                + "'"+util.getConfiguration().get(DYNAMIC_JARS_DIR_KEY) + "/myjar1.jar"+"'");
        try {
            tenant1Conn.createStatement().execute("create function myfunction(VARCHAR) returns VARCHAR as 'org.apache.phoenix.end2end.UnknownClass' using jar "
                    + "'"+util.getConfiguration().get(DYNAMIC_JARS_DIR_KEY) + "/myjar1.jar"+"'");
            fail("FunctionAlreadyExistsException should be thrown.");
        } catch(FunctionAlreadyExistsException e) {
            
        }

        tenant2Conn.createStatement().execute("create function myfunction(INTEGER, INTEGER CONSTANT defaultValue='10' minvalue='1' maxvalue='15' ) returns INTEGER as 'org.apache.phoenix.end2end."+MY_SUM_CLASS_NAME+"' using jar "
                + "'"+util.getConfiguration().get(DYNAMIC_JARS_DIR_KEY) + "/myjar2.jar"+"'");
        try {
            tenant2Conn.createStatement().execute("create function myfunction(VARCHAR) returns VARCHAR as 'org.apache.phoenix.end2end.UnknownClass' using jar "
                    + "'"+util.getConfiguration().get(DYNAMIC_JARS_DIR_KEY) + "/unknown.jar"+"'");
            fail("FunctionAlreadyExistsException should be thrown.");
        } catch(FunctionAlreadyExistsException e) {
            
        }

        ResultSet rs = tenant1Conn.createStatement().executeQuery("select MYFUNCTION(name) from t7");
        assertTrue(rs.next());
        assertEquals("kcoj", rs.getString(1));
        assertFalse(rs.next());
        rs = tenant1Conn.createStatement().executeQuery("select * from t7 where MYFUNCTION(name)='kcoj'");
        assertTrue(rs.next());
        assertEquals(1, rs.getInt(1));
        assertEquals(1, rs.getInt(2));        
        assertEquals("jock", rs.getString(3));
        assertFalse(rs.next());

        rs = tenant2Conn.createStatement().executeQuery("select MYFUNCTION(k) from t7");
        assertTrue(rs.next());
        assertEquals(11, rs.getInt(1));
        assertFalse(rs.next());
        rs = tenant2Conn.createStatement().executeQuery("select * from t7 where MYFUNCTION(k1)=12");
        assertTrue(rs.next());
        assertEquals(1, rs.getInt(1));
        assertEquals(2, rs.getInt(2));        
        assertEquals("jock", rs.getString(3));
        assertFalse(rs.next());
    }

    @Test
    public void testUDFsWithMultipleConnections() throws Exception {
        Connection conn1 = driver.connect(url, EMPTY_PROPS);
        conn1.createStatement().execute("create function myfunction(VARCHAR) returns VARCHAR as 'org.apache.phoenix.end2end."+MY_REVERSE_CLASS_NAME+"' using jar "
                + "'"+util.getConfiguration().get(DYNAMIC_JARS_DIR_KEY) + "/myjar1.jar"+"'");
        Connection conn2 = driver.connect(url, EMPTY_PROPS);
        try{
            conn2.createStatement().execute("create function myfunction(VARCHAR) returns VARCHAR as 'org.apache.phoenix.end2end."+MY_REVERSE_CLASS_NAME+"' using jar "
                    + "'"+util.getConfiguration().get(DYNAMIC_JARS_DIR_KEY) + "/myjar1.jar"+"'");
            fail("FunctionAlreadyExistsException should be thrown.");
        } catch(FunctionAlreadyExistsException e) {
            
        }
        conn2.createStatement().execute("create table t8(k integer not null primary key, k1 integer, name varchar)");
        conn2.createStatement().execute("upsert into t8 values(1,1,'jock')");
        conn2.commit();
        ResultSet rs = conn2.createStatement().executeQuery("select MYFUNCTION(name) from t8");
        assertTrue(rs.next());
        assertEquals("kcoj", rs.getString(1));
        assertFalse(rs.next());
        rs = conn2.createStatement().executeQuery("select * from t8 where MYFUNCTION(name)='kcoj'");
        assertTrue(rs.next());
        assertEquals(1, rs.getInt(1));
        assertEquals(1, rs.getInt(2));
        assertEquals("jock", rs.getString(3));
        assertFalse(rs.next());
        conn2.createStatement().execute("drop function MYFUNCTION");
        try {
            rs = conn1.createStatement().executeQuery("select MYFUNCTION(name) from t8");
            fail("FunctionNotFoundException should be thrown");
        } catch(FunctionNotFoundException e) {
            
        }
    }
    @Test
    public void testUsingUDFFunctionInDifferentQueries() throws Exception {
        Connection conn = driver.connect(url, EMPTY_PROPS);
        Statement stmt = conn.createStatement();
        conn.createStatement().execute("create table t1(k integer primary key, firstname varchar, lastname varchar)");
        stmt.execute("upsert into t1 values(1,'foo','jock')");
        conn.commit();
        conn.createStatement().execute("create table t2(k integer primary key, k1 integer, lastname_reverse varchar)");
        conn.commit();
        stmt.execute("create function mysum3(INTEGER, INTEGER CONSTANT defaultValue='10' minvalue='1' maxvalue='15' ) returns INTEGER as 'org.apache.phoenix.end2end."+MY_SUM_CLASS_NAME+"' using jar "
                + "'"+util.getConfiguration().get(DYNAMIC_JARS_DIR_KEY) + "/myjar2.jar"+"'");
        stmt.execute("create function myreverse3(VARCHAR) returns VARCHAR as 'org.apache.phoenix.end2end."+MY_REVERSE_CLASS_NAME+"' using jar "
                + "'"+util.getConfiguration().get(DYNAMIC_JARS_DIR_KEY) + "/myjar1.jar"+"'");
        stmt.execute("upsert into t2(k,k1,lastname_reverse) select mysum3(k),mysum3(k,11),myreverse3(lastname) from t1");
        conn.commit();
        ResultSet rs = stmt.executeQuery("select * from t2");
        assertTrue(rs.next());
        assertEquals(11, rs.getInt(1));
        assertEquals(12, rs.getInt(2));
        assertEquals("kcoj", rs.getString(3));
        assertFalse(rs.next());
        stmt.execute("delete from t2 where myreverse3(lastname_reverse)='jock' and mysum3(k)=21");
        conn.commit();
        rs = stmt.executeQuery("select * from t2");
        assertFalse(rs.next());
        stmt.execute("create function myreverse4(VARCHAR CONSTANT defaultValue='null') returns VARCHAR as 'org.apache.phoenix.end2end."+MY_REVERSE_CLASS_NAME+"'");
        stmt.execute("upsert into t2 values(11,12,myreverse4('jock'))");
        conn.commit();
        rs = stmt.executeQuery("select * from t2");
        assertTrue(rs.next());
        assertEquals(11, rs.getInt(1));
        assertEquals(12, rs.getInt(2));
        assertEquals("kcoj", rs.getString(3));
        assertFalse(rs.next());
    }

    @Test
    public void testVerifyCreateFunctionArguments() throws Exception {
        Connection conn = driver.connect(url, EMPTY_PROPS);
        Statement stmt = conn.createStatement();
        conn.createStatement().execute("create table t4(k integer primary key, k1 integer, lastname varchar)");
        stmt.execute("upsert into t4 values(1,1,'jock')");
        conn.commit();
        stmt.execute("create function mysum(INTEGER, INTEGER CONSTANT defaultValue='10' minvalue='1' maxvalue='15' ) returns INTEGER as 'org.apache.phoenix.end2end."+MY_SUM_CLASS_NAME+"' using jar "
                + "'"+util.getConfiguration().get(DYNAMIC_JARS_DIR_KEY) + "/myjar2.jar"+"'");
        ResultSet rs = stmt.executeQuery("select mysum(k,12) from t4");
        assertTrue(rs.next());
        assertEquals(13, rs.getInt(1));
        rs = stmt.executeQuery("select mysum(k) from t4");
        assertTrue(rs.next());
        assertEquals(11, rs.getInt(1));
        try {
            stmt.executeQuery("select mysum(k,20) from t4");
            fail("Value Range Exception should be thrown.");
        } catch(ValueRangeExcpetion e) {
            
        }
    }

    @Test
    public void testTemporaryFunctions() throws Exception {
        Connection conn = driver.connect(url, EMPTY_PROPS);
        Statement stmt = conn.createStatement();
        conn.createStatement().execute("create table t9(k integer primary key, k1 integer, lastname varchar)");
        stmt.execute("upsert into t9 values(1,1,'jock')");
        conn.commit();
        stmt.execute("create temporary function mysum9(INTEGER, INTEGER CONSTANT defaultValue='10' minvalue='1' maxvalue='15' ) returns INTEGER as 'org.apache.phoenix.end2end."+MY_SUM_CLASS_NAME+"' using jar "
                + "'"+util.getConfiguration().get(DYNAMIC_JARS_DIR_KEY) + "/myjar2.jar"+"'");
        ResultSet rs = stmt.executeQuery("select mysum9(k,12) from t9");
        assertTrue(rs.next());
        assertEquals(13, rs.getInt(1));
        rs = stmt.executeQuery("select mysum9(k) from t9");
        assertTrue(rs.next());
        assertEquals(11, rs.getInt(1));
        rs = stmt.executeQuery("select k from t9 where mysum9(k)=11");
        assertTrue(rs.next());
        assertEquals(1, rs.getInt(1));
        stmt.execute("drop function mysum9");
        try {
            rs = stmt.executeQuery("select k from t9 where mysum9(k)=11");
            fail("FunctionNotFoundException should be thrown");
        } catch(FunctionNotFoundException e){
            
        }
    }

    @Test
    public void testDropFunction() throws Exception {
        Connection conn = driver.connect(url, EMPTY_PROPS);
        Statement stmt = conn.createStatement();
        String query = "select count(*) from "+ SYSTEM_CATALOG_SCHEMA + ".\"" + SYSTEM_FUNCTION_TABLE + "\"";
        ResultSet rs = stmt.executeQuery(query);
        rs.next();
        int numRowsBefore = rs.getInt(1);
        stmt.execute("create function mysum6(INTEGER, INTEGER CONSTANT defaultValue='10' minvalue='1' maxvalue='15' ) returns INTEGER as 'org.apache.phoenix.end2end."+MY_SUM_CLASS_NAME+"' using jar "
                + "'"+util.getConfiguration().get(DYNAMIC_JARS_DIR_KEY) + "/myjar2.jar"+"'");
        rs = stmt.executeQuery(query);
        rs.next();
        int numRowsAfter= rs.getInt(1);
        assertEquals(3, numRowsAfter - numRowsBefore);
        stmt.execute("drop function mysum6");
        rs = stmt.executeQuery(query);
        rs.next();
        assertEquals(numRowsBefore, rs.getInt(1));
        conn.createStatement().execute("create table t6(k integer primary key, k1 integer, lastname varchar)");
        try {
            rs = stmt.executeQuery("select mysum6(k1) from t6");
            fail("FunctionNotFoundException should be thrown");
        } catch(FunctionNotFoundException e) {
            
        }
        try {
            stmt.execute("drop function mysum6");
            fail("FunctionNotFoundException should be thrown");
        } catch(FunctionNotFoundException e) {
            
        }
        try {
            stmt.execute("drop function if exists mysum6");
        } catch(FunctionNotFoundException e) {
            fail("FunctionNotFoundException should not be thrown");
        }
        stmt.execute("create function mysum6(INTEGER, INTEGER CONSTANT defaultValue='10' minvalue='1' maxvalue='15' ) returns INTEGER as 'org.apache.phoenix.end2end."+MY_SUM_CLASS_NAME+"' using jar "
                + "'"+util.getConfiguration().get(DYNAMIC_JARS_DIR_KEY) + "/myjar2.jar"+"'");
        try {
            rs = stmt.executeQuery("select mysum6(k1) from t6");
        } catch(FunctionNotFoundException e) {
            fail("FunctionNotFoundException should not be thrown");
        }
    }

    @Test
    public void testFunctionalIndexesWithUDFFunction() throws Exception {
        Connection conn = driver.connect(url, EMPTY_PROPS);
        Statement stmt = conn.createStatement();
        stmt.execute("create table t5(k integer primary key, k1 integer, lastname_reverse varchar)");
        stmt.execute("create function myreverse5(VARCHAR) returns VARCHAR as 'org.apache.phoenix.end2end."+MY_REVERSE_CLASS_NAME+"'");
        stmt.execute("upsert into t5 values(1,1,'jock')");
        conn.commit();
        stmt.execute("create index idx on t5(myreverse5(lastname_reverse))");
        String query = "select myreverse5(lastname_reverse) from t5";
        ResultSet rs = stmt.executeQuery("explain " + query);
        assertEquals("CLIENT PARALLEL 1-WAY FULL SCAN OVER IDX\n"
                + "    SERVER FILTER BY FIRST KEY ONLY", QueryUtil.getExplainPlan(rs));
        rs = stmt.executeQuery(query);
        assertTrue(rs.next());
        assertEquals("kcoj", rs.getString(1));
        assertFalse(rs.next());
        stmt.execute("create local index idx2 on t5(myreverse5(lastname_reverse))");
        query = "select k,k1,myreverse5(lastname_reverse) from t5 where myreverse5(lastname_reverse)='kcoj'";
        rs = stmt.executeQuery("explain " + query);
        assertEquals("CLIENT PARALLEL 1-WAY RANGE SCAN OVER _LOCAL_IDX_T5 [-32768,'kcoj']\n"
                + "    SERVER FILTER BY FIRST KEY ONLY\n"
                +"CLIENT MERGE SORT", QueryUtil.getExplainPlan(rs));
        rs = stmt.executeQuery(query);
        assertTrue(rs.next());
        assertEquals(1, rs.getInt(1));
        assertEquals(1, rs.getInt(2));
        assertEquals("kcoj", rs.getString(3));
        assertFalse(rs.next());
    }

    @AfterClass
    public static void tearDown() throws Exception {
        try {
            destroyDriver(driver);
        } finally {
            util.shutdownMiniCluster();
        }
    }

    /**
     * Compiles the test class with bogus code into a .class file.
     */
    private static void compileTestClass(String className, String program, int counter) throws Exception {
        String javaFileName = className+".java";
        File javaFile = new File(javaFileName);
        String classFileName = className+".class";
        File classFile = new File(classFileName);
        String jarName = "myjar"+counter+".jar";
        String jarPath = "." + File.separator + jarName;
        File jarFile = new File(jarPath);
        try {
            String packageName = "org.apache.phoenix.end2end";
            FileOutputStream fos = new FileOutputStream(javaFileName);
            fos.write(program.getBytes());
            fos.close();
            
            JavaCompiler jc = ToolProvider.getSystemJavaCompiler();
            int result = jc.run(null, null, null, javaFileName);
            assertEquals(0, result);
            
            Manifest manifest = new Manifest();
            manifest.getMainAttributes().put(Attributes.Name.MANIFEST_VERSION, "1.0");
            FileOutputStream jarFos = new FileOutputStream(jarPath);
            JarOutputStream jarOutputStream = new JarOutputStream(jarFos, manifest);
            String pathToAdd =packageName.replace('.', File.separatorChar)
                    + File.separator;
            jarOutputStream.putNextEntry(new JarEntry(pathToAdd));
            jarOutputStream.closeEntry();
            jarOutputStream.putNextEntry(new JarEntry(pathToAdd + classFile.getName()));
            byte[] allBytes = new byte[(int) classFile.length()];
            FileInputStream fis = new FileInputStream(classFile);
            fis.read(allBytes);
            fis.close();
            jarOutputStream.write(allBytes);
            jarOutputStream.closeEntry();
            jarOutputStream.close();
            jarFos.close();
            
            assertTrue(jarFile.exists());
            
            InputStream inputStream = new BufferedInputStream(new FileInputStream(jarPath));
            FileSystem fs = util.getDefaultRootDirPath().getFileSystem(util.getConfiguration());
            Path jarsLocation = new Path(util.getConfiguration().get(DYNAMIC_JARS_DIR_KEY));
            Path myJarPath;
            if (jarsLocation.toString().endsWith("/")) {
                myJarPath = new Path(jarsLocation.toString() + jarName);
            } else {
                myJarPath = new Path(jarsLocation.toString() + "/" + jarName);
            }
            OutputStream outputStream = fs.create(myJarPath);
            try {
                IOUtils.copyBytes(inputStream, outputStream, 4096, false);
            } finally {
                IOUtils.closeStream(inputStream);
                IOUtils.closeStream(outputStream);
            }
        } finally {
            if (javaFile != null) javaFile.delete();
            if (classFile != null) classFile.delete();
            if (jarFile != null) jarFile.delete();
        }
    }

}
