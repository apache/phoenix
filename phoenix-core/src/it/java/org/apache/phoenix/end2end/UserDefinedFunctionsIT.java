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

import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.SYSTEM_CATALOG_SCHEMA;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.SYSTEM_FUNCTION_TABLE;
import static org.apache.phoenix.query.QueryServices.DYNAMIC_JARS_DIR_KEY;
import static org.apache.phoenix.util.PhoenixRuntime.JDBC_PROTOCOL;
import static org.apache.phoenix.util.PhoenixRuntime.JDBC_PROTOCOL_SEPARATOR;
import static org.apache.phoenix.util.PhoenixRuntime.JDBC_PROTOCOL_TERMINATOR;
import static org.apache.phoenix.util.PhoenixRuntime.PHOENIX_TEST_DRIVER_URL_PARAM;
import static org.apache.phoenix.util.TestUtil.JOIN_ITEM_TABLE_FULL_NAME;
import static org.apache.phoenix.util.TestUtil.JOIN_SUPPLIER_TABLE_FULL_NAME;
import static org.apache.phoenix.util.TestUtil.LOCALHOST;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.HashSet;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.jar.Attributes;
import java.util.jar.JarEntry;
import java.util.jar.JarOutputStream;
import java.util.jar.Manifest;

import javax.tools.JavaCompiler;
import javax.tools.ToolProvider;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.phoenix.expression.function.UDFExpression;
import org.apache.phoenix.jdbc.PhoenixTestDriver;
import org.apache.phoenix.query.QueryServices;
import org.apache.phoenix.schema.FunctionAlreadyExistsException;
import org.apache.phoenix.schema.FunctionNotFoundException;
import org.apache.phoenix.schema.ValueRangeExcpetion;
import org.apache.phoenix.util.PhoenixRuntime;
import org.apache.phoenix.util.QueryUtil;
import org.apache.phoenix.util.ReadOnlyProps;
import org.junit.After;
import org.junit.BeforeClass;
import org.junit.Test;

import com.google.common.collect.Maps;

public class UserDefinedFunctionsIT extends BaseOwnClusterIT {
    
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
    private static String ARRAY_INDEX_EVALUATE_METHOD =
            new StringBuffer()
                    .append("    public boolean evaluate(Tuple tuple, ImmutableBytesWritable ptr) {\n")
                    .append("        Expression indexExpr = children.get(1);\n")
                    .append("        if (!indexExpr.evaluate(tuple, ptr)) {\n")
                    .append("           return false;\n")
                    .append("        } else if (ptr.getLength() == 0) {\n")
                    .append("           return true;\n")
                    .append("        }\n")
                    .append("        // Use Codec to prevent Integer object allocation\n")
                    .append("        int index = PInteger.INSTANCE.getCodec().decodeInt(ptr, indexExpr.getSortOrder());\n")
                    .append("        if(index < 0) {\n")
                    .append("           throw new ParseException(\"Index cannot be negative :\" + index);\n")
                    .append("        }\n")
                    .append("        Expression arrayExpr = children.get(0);\n")
                    .append("        return PArrayDataTypeDecoder.positionAtArrayElement(tuple, ptr, index, arrayExpr, getDataType(),getMaxLength());\n")
                    .append("    }\n").toString();

    private static String GETY_EVALUATE_METHOD =
            new StringBuffer()
                    .append("    public boolean evaluate(Tuple tuple, ImmutableBytesWritable ptr) {\n")
                    .append("        Expression arg = getChildren().get(0);\n")
                    .append("        if (!arg.evaluate(tuple, ptr)) {\n")
                    .append("           return false;\n")
                    .append("        }\n")
                    .append("        int targetOffset = ptr.getLength();\n")
                    .append("        if (targetOffset == 0) {\n")
                    .append("           return true;\n")
                    .append("        }\n")
                    .append("        byte[] s = ptr.get();\n")
                    .append("        int retVal = (int)Bytes.toShort(s);\n")
                    .append("        ptr.set(PInteger.INSTANCE.toBytes(retVal));\n")
                    .append("        return true;\n")
                    .append("    }\n").toString();
    private static String GETX_EVALUATE_METHOD =
            new StringBuffer()
                    .append("    public boolean evaluate(Tuple tuple, ImmutableBytesWritable ptr) {\n")
                    .append("        Expression arg = getChildren().get(0);\n")
                    .append("        if (!arg.evaluate(tuple, ptr)) {\n")
                    .append("           return false;\n")
                    .append("        }\n")
                    .append("        int targetOffset = ptr.getLength();\n")
                    .append("        if (targetOffset == 0) {\n")
                    .append("           return true;\n")
                    .append("        }\n")
                    .append("        byte[] s = ptr.get();\n")
                    .append("        Long retVal = Long.reverseBytes(Bytes.toLong(s));\n")
                    .append("        ptr.set(PLong.INSTANCE.toBytes(retVal));\n")
                    .append("        return true;\n")
                    .append("    }\n").toString();

    
    private static String MY_REVERSE_CLASS_NAME = "MyReverse";
    private static String MY_SUM_CLASS_NAME = "MySum";
    private static String MY_ARRAY_INDEX_CLASS_NAME = "MyArrayIndex";
    private static String GETX_CLASSNAME = "GetX";
    private static String GETY_CLASSNAME = "GetY";
    private static String MY_REVERSE_PROGRAM = getProgram(MY_REVERSE_CLASS_NAME, STRING_REVERSE_EVALUATE_METHOD, "return PVarchar.INSTANCE;");
    private static String MY_SUM_PROGRAM = getProgram(MY_SUM_CLASS_NAME, SUM_COLUMN_VALUES_EVALUATE_METHOD, "return PInteger.INSTANCE;");
    private static String MY_ARRAY_INDEX_PROGRAM = getProgram(MY_ARRAY_INDEX_CLASS_NAME, ARRAY_INDEX_EVALUATE_METHOD, "return PDataType.fromTypeId(children.get(0).getDataType().getSqlType()- PDataType.ARRAY_TYPE_BASE);");
    private static String GETX_CLASSNAME_PROGRAM = getProgram(GETX_CLASSNAME, GETX_EVALUATE_METHOD, "return PLong.INSTANCE;");
    private static String GETY_CLASSNAME_PROGRAM = getProgram(GETY_CLASSNAME, GETY_EVALUATE_METHOD, "return PInteger.INSTANCE;");
    private static Properties EMPTY_PROPS = new Properties();
    

    @Override
    @After
    public void cleanUpAfterTest() throws Exception {}

    private static String getProgram(String className, String evaluateMethod, String returnType) {
        return new StringBuffer()
                .append("package org.apache.phoenix.end2end;\n")
                .append("import java.sql.SQLException;\n")
                .append("import java.util.List;\n")
                .append("import java.lang.Long;\n")
                .append("import java.lang.Integer;\n")
                .append("import org.apache.hadoop.hbase.io.ImmutableBytesWritable;\n")
                .append("import org.apache.hadoop.hbase.util.Bytes;\n")
                .append("import org.apache.phoenix.schema.types.PLong;")
                .append("import org.apache.phoenix.schema.types.PInteger;"
                		+ ""
                		+ "")
                .append("import org.apache.phoenix.expression.Expression;\n")
                .append("import org.apache.phoenix.expression.function.ScalarFunction;\n")
                .append("import org.apache.phoenix.schema.SortOrder;\n")
                .append("import org.apache.phoenix.schema.tuple.Tuple;\n")
                .append("import org.apache.phoenix.schema.types.PDataType;\n")
                .append("import org.apache.phoenix.schema.types.PInteger;\n")
                .append("import org.apache.phoenix.schema.types.PVarchar;\n")
                .append("import org.apache.phoenix.util.StringUtil;\n")
                .append("import org.apache.phoenix.schema.types.PArrayDataType;\n")
                .append("import org.apache.phoenix.schema.types.PArrayDataTypeDecoder;\n")
                .append("import org.apache.phoenix.parse.ParseException;\n")
                .append("public class "+className+" extends ScalarFunction{\n")
                .append("    public static final String NAME = \""+className+"\";\n")
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
                .append(returnType+"\n")
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

        String clientPort = util.getConfiguration().get(QueryServices.ZOOKEEPER_PORT_ATTRIB);
        url =
                JDBC_PROTOCOL + JDBC_PROTOCOL_SEPARATOR + LOCALHOST + JDBC_PROTOCOL_SEPARATOR
                        + clientPort + JDBC_PROTOCOL_TERMINATOR + PHOENIX_TEST_DRIVER_URL_PARAM;
        Map<String, String> props = Maps.newHashMapWithExpectedSize(1);
        props.put(QueryServices.ALLOW_USER_DEFINED_FUNCTIONS_ATTRIB, "true");
        props.put(QueryServices.DYNAMIC_JARS_DIR_KEY,string+"/hbase/tmpjars/");
        driver = initAndRegisterTestDriver(url, new ReadOnlyProps(props.entrySet().iterator()));
        compileTestClass(MY_REVERSE_CLASS_NAME, MY_REVERSE_PROGRAM, 1);
        compileTestClass(MY_SUM_CLASS_NAME, MY_SUM_PROGRAM, 2);
        compileTestClass(MY_ARRAY_INDEX_CLASS_NAME, MY_ARRAY_INDEX_PROGRAM, 3);
        compileTestClass(MY_ARRAY_INDEX_CLASS_NAME, MY_ARRAY_INDEX_PROGRAM, 4);
        compileTestClass(GETX_CLASSNAME, GETX_CLASSNAME_PROGRAM, 5);
        compileTestClass(GETY_CLASSNAME, GETY_CLASSNAME_PROGRAM, 6);
    }
    
    @Test
    public void testListJars() throws Exception {
        Connection conn = driver.connect(url, EMPTY_PROPS);
        Statement stmt = conn.createStatement();
        ResultSet rs = stmt.executeQuery("list jars");
        assertTrue(rs.next());
        assertEquals(util.getConfiguration().get(QueryServices.DYNAMIC_JARS_DIR_KEY)+"/"+"myjar1.jar", rs.getString("jar_location"));
        assertTrue(rs.next());
        assertEquals(util.getConfiguration().get(QueryServices.DYNAMIC_JARS_DIR_KEY)+"/"+"myjar2.jar", rs.getString("jar_location"));
        assertTrue(rs.next());
        assertEquals(util.getConfiguration().get(QueryServices.DYNAMIC_JARS_DIR_KEY)+"/"+"myjar3.jar", rs.getString("jar_location"));
        assertTrue(rs.next());
        assertEquals(util.getConfiguration().get(QueryServices.DYNAMIC_JARS_DIR_KEY)+"/"+"myjar5.jar", rs.getString("jar_location"));
        assertTrue(rs.next());
        assertEquals(util.getConfiguration().get(QueryServices.DYNAMIC_JARS_DIR_KEY)+"/"+"myjar6.jar", rs.getString("jar_location"));
        assertFalse(rs.next());
    }

    @Test
    public void testDeleteJar() throws Exception {
        Connection conn = driver.connect(url, EMPTY_PROPS);
        Statement stmt = conn.createStatement();
        ResultSet rs = stmt.executeQuery("list jars");
        assertTrue(rs.next());
        assertEquals(util.getConfiguration().get(QueryServices.DYNAMIC_JARS_DIR_KEY)+"/"+"myjar1.jar", rs.getString("jar_location"));
        assertTrue(rs.next());
        assertEquals(util.getConfiguration().get(QueryServices.DYNAMIC_JARS_DIR_KEY)+"/"+"myjar2.jar", rs.getString("jar_location"));
        assertTrue(rs.next());
        assertEquals(util.getConfiguration().get(QueryServices.DYNAMIC_JARS_DIR_KEY)+"/"+"myjar3.jar", rs.getString("jar_location"));
        assertTrue(rs.next());
        assertEquals(util.getConfiguration().get(QueryServices.DYNAMIC_JARS_DIR_KEY)+"/"+"myjar4.jar", rs.getString("jar_location"));
        assertTrue(rs.next());
        assertEquals(util.getConfiguration().get(QueryServices.DYNAMIC_JARS_DIR_KEY)+"/"+"myjar5.jar", rs.getString("jar_location"));
        assertTrue(rs.next());
        assertEquals(util.getConfiguration().get(QueryServices.DYNAMIC_JARS_DIR_KEY)+"/"+"myjar6.jar", rs.getString("jar_location"));
        assertFalse(rs.next());
        stmt.execute("delete jar '"+ util.getConfiguration().get(QueryServices.DYNAMIC_JARS_DIR_KEY)+"/"+"myjar4.jar'");
        rs = stmt.executeQuery("list jars");
        assertTrue(rs.next());
        assertEquals(util.getConfiguration().get(QueryServices.DYNAMIC_JARS_DIR_KEY)+"/"+"myjar1.jar", rs.getString("jar_location"));
        assertTrue(rs.next());
        assertEquals(util.getConfiguration().get(QueryServices.DYNAMIC_JARS_DIR_KEY)+"/"+"myjar2.jar", rs.getString("jar_location"));
        assertTrue(rs.next());
        assertEquals(util.getConfiguration().get(QueryServices.DYNAMIC_JARS_DIR_KEY)+"/"+"myjar3.jar", rs.getString("jar_location"));
        assertTrue(rs.next());
        assertEquals(util.getConfiguration().get(QueryServices.DYNAMIC_JARS_DIR_KEY)+"/"+"myjar5.jar", rs.getString("jar_location"));
        assertTrue(rs.next());
        assertEquals(util.getConfiguration().get(QueryServices.DYNAMIC_JARS_DIR_KEY)+"/"+"myjar6.jar", rs.getString("jar_location"));
        assertFalse(rs.next());
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
        conn.createStatement().execute("CREATE TABLE TESTTABLE10(ID VARCHAR NOT NULL, NAME VARCHAR ARRAY, CITY VARCHAR ARRAY CONSTRAINT pk PRIMARY KEY (ID) )");
        conn.createStatement().execute("create function UDF_ARRAY_ELEM(VARCHAR ARRAY, INTEGER) returns VARCHAR as 'org.apache.phoenix.end2end."+MY_ARRAY_INDEX_CLASS_NAME+"' using jar "
                + "'"+util.getConfiguration().get(DYNAMIC_JARS_DIR_KEY) + "/myjar3.jar"+"'");
        conn.createStatement().execute("UPSERT INTO TESTTABLE10(ID,NAME,CITY) VALUES('111', ARRAY['JOHN','MIKE','BOB'], ARRAY['NYC','LA','SF'])");
        conn.createStatement().execute("UPSERT INTO TESTTABLE10(ID,NAME,CITY) VALUES('112', ARRAY['CHEN','CARL','ALICE'], ARRAY['BOSTON','WASHINGTON','PALO ALTO'])");
        conn.commit();
        rs = conn.createStatement().executeQuery("SELECT ID, UDF_ARRAY_ELEM(NAME, 2) FROM TESTTABLE10");
        assertTrue(rs.next());
        assertEquals("111", rs.getString(1));
        assertEquals("MIKE", rs.getString(2));
        assertTrue(rs.next());
        assertEquals("112", rs.getString(1));
        assertEquals("CARL", rs.getString(2));
        assertFalse(rs.next());
        rs = conn2.createStatement().executeQuery("SELECT ID, UDF_ARRAY_ELEM(NAME, 2) FROM TESTTABLE10");
        assertTrue(rs.next());
        assertEquals("111", rs.getString(1));
        assertEquals("MIKE", rs.getString(2));
        assertTrue(rs.next());
        assertEquals("112", rs.getString(1));
        assertEquals("CARL", rs.getString(2));
        assertFalse(rs.next());
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

        tenant2Conn.createStatement().execute("create function myfunction(INTEGER, INTEGER CONSTANT defaultValue=10 minvalue=1 maxvalue=15 ) returns INTEGER as 'org.apache.phoenix.end2end."+MY_SUM_CLASS_NAME+"' using jar "
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
        stmt.execute("create function mysum3(INTEGER, INTEGER CONSTANT defaultValue=10 minvalue=1 maxvalue=15 ) returns INTEGER as 'org.apache.phoenix.end2end."+MY_SUM_CLASS_NAME+"' using jar "
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
        stmt.execute("create function mysum(INTEGER, INTEGER CONSTANT defaultValue=10 minvalue=1 maxvalue=15 ) returns INTEGER as 'org.apache.phoenix.end2end."+MY_SUM_CLASS_NAME+"' using jar "
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
        stmt.execute("create temporary function mysum9(INTEGER, INTEGER CONSTANT defaultValue=10 minvalue=1 maxvalue=15 ) returns INTEGER as 'org.apache.phoenix.end2end."+MY_SUM_CLASS_NAME+"' using jar "
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
        try {
            rs = stmt.executeQuery("select k from t9 where mysum9(k,10,'x')=11");
            fail("FunctionNotFoundException should be thrown");
        } catch(FunctionNotFoundException e) {
        } catch(Exception e) {
            fail("FunctionNotFoundException should be thrown");
        }
        try {
            rs = stmt.executeQuery("select mysum9() from t9");
            fail("FunctionNotFoundException should be thrown");
        } catch(FunctionNotFoundException e) {
        } catch(Exception e) {
            fail("FunctionNotFoundException should be thrown");
        }
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
        stmt.execute("create function mysum6(INTEGER, INTEGER CONSTANT defaultValue=10 minvalue=1 maxvalue=15 ) returns INTEGER as 'org.apache.phoenix.end2end."+MY_SUM_CLASS_NAME+"' using jar "
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
        stmt.execute("create function mysum6(INTEGER, INTEGER CONSTANT defaultValue=10 minvalue=1 maxvalue=15 ) returns INTEGER as 'org.apache.phoenix.end2end."+MY_SUM_CLASS_NAME+"' using jar "
                + "'"+util.getConfiguration().get(DYNAMIC_JARS_DIR_KEY) + "/myjar2.jar"+"'");
        try {
            rs = stmt.executeQuery("select mysum6(k1) from t6");
        } catch(FunctionNotFoundException e) {
            fail("FunctionNotFoundException should not be thrown");
        }
    }

    @Test
    public void testUDFsWhenTimestampManagedAtClient() throws Exception {
        long ts = 100;
        Properties props = new Properties();
        props.setProperty(PhoenixRuntime.CURRENT_SCN_ATTRIB, Long.toString(ts));
        Connection conn = DriverManager.getConnection(url, props);
        Statement stmt = conn.createStatement();
        String query = "select count(*) from "+ SYSTEM_CATALOG_SCHEMA + ".\"" + SYSTEM_FUNCTION_TABLE + "\"";
        ResultSet rs = stmt.executeQuery(query);
        rs.next();
        int numRowsBefore = rs.getInt(1);
        stmt.execute("create function mysum61(INTEGER, INTEGER CONSTANT defaultValue=10 minvalue=1 maxvalue=15 ) returns INTEGER as 'org.apache.phoenix.end2end."+MY_SUM_CLASS_NAME+"' using jar "
                + "'"+util.getConfiguration().get(DYNAMIC_JARS_DIR_KEY) + "/myjar2.jar"+"'");
        props.setProperty(PhoenixRuntime.CURRENT_SCN_ATTRIB, Long.toString(ts + 10));
        conn = DriverManager.getConnection(url, props);
        stmt = conn.createStatement();
        rs = stmt.executeQuery(query);
        rs.next();
        int numRowsAfter= rs.getInt(1);
        assertEquals(3, numRowsAfter - numRowsBefore);
        stmt.execute("drop function mysum61");
        props.setProperty(PhoenixRuntime.CURRENT_SCN_ATTRIB, Long.toString(ts + 20));
        conn = DriverManager.getConnection(url, props);
        stmt = conn.createStatement();
        rs = stmt.executeQuery(query);
        rs.next();
        assertEquals(numRowsBefore, rs.getInt(1));
        conn.createStatement().execute("create table t62(k integer primary key, k1 integer, lastname varchar)");
        try {
            rs = stmt.executeQuery("select mysum61(k1) from t62");
            fail("FunctionNotFoundException should be thrown");
        } catch(FunctionNotFoundException e) {
            
        }
        try {
            stmt.execute("drop function mysum61");
            fail("FunctionNotFoundException should be thrown");
        } catch(FunctionNotFoundException e) {
            
        }
        try {
            stmt.execute("drop function if exists mysum61");
        } catch(FunctionNotFoundException e) {
            fail("FunctionNotFoundException should not be thrown");
        }
        stmt.execute("create function mysum61(INTEGER, INTEGER CONSTANT defaultValue=10 minvalue=1 maxvalue=15 ) returns INTEGER as 'org.apache.phoenix.end2end."+MY_SUM_CLASS_NAME+"' using jar "
                + "'"+util.getConfiguration().get(DYNAMIC_JARS_DIR_KEY) + "/myjar2.jar"+"'");
        props.setProperty(PhoenixRuntime.CURRENT_SCN_ATTRIB, Long.toString(ts + 30));
        conn = DriverManager.getConnection(url, props);
        stmt = conn.createStatement();
        try {
            rs = stmt.executeQuery("select mysum61(k1) from t62");
        } catch(FunctionNotFoundException e) {
            fail("FunctionNotFoundException should not be thrown");
        }
        conn.createStatement().execute("create table t61(k integer primary key, k1 integer, lastname varchar)");
        props.setProperty(PhoenixRuntime.CURRENT_SCN_ATTRIB, Long.toString(ts + 40));
        conn = DriverManager.getConnection(url, props);
        stmt = conn.createStatement();
        stmt.execute("upsert into t61 values(1,1,'jock')");
        conn.commit();
        stmt.execute("create function myfunction6(VARCHAR) returns VARCHAR as 'org.apache.phoenix.end2end."+MY_REVERSE_CLASS_NAME+"' using jar "
                + "'"+util.getConfiguration().get(DYNAMIC_JARS_DIR_KEY) + "/myjar1.jar"+"'");
        stmt.execute("create or replace function myfunction6(INTEGER, INTEGER CONSTANT defaultValue=10 minvalue=1 maxvalue=15 ) returns INTEGER as 'org.apache.phoenix.end2end."+MY_SUM_CLASS_NAME+"' using jar "
                + "'"+util.getConfiguration().get(DYNAMIC_JARS_DIR_KEY) + "/myjar2.jar"+"'");
        props.setProperty(PhoenixRuntime.CURRENT_SCN_ATTRIB, Long.toString(ts + 50));
        conn = DriverManager.getConnection(url, props);
        stmt = conn.createStatement();
        rs = stmt.executeQuery("select myfunction6(k,12) from t61");
        assertTrue(rs.next());
        assertEquals(13, rs.getInt(1));
        rs = stmt.executeQuery("select myfunction6(k) from t61");
        assertTrue(rs.next());
        assertEquals(11, rs.getInt(1));
        rs = stmt.executeQuery("select k from t61 where myfunction6(k)=11");
        assertTrue(rs.next());
        assertEquals(1, rs.getInt(1));
        stmt.execute("create or replace function myfunction6(VARCHAR) returns VARCHAR as 'org.apache.phoenix.end2end."+MY_REVERSE_CLASS_NAME+"' using jar "
                + "'"+util.getConfiguration().get(DYNAMIC_JARS_DIR_KEY) + "/myjar1.jar"+"'");
        props.setProperty(PhoenixRuntime.CURRENT_SCN_ATTRIB, Long.toString(ts + 60));
        conn = DriverManager.getConnection(url, props);
        stmt = conn.createStatement();
        rs = stmt.executeQuery("select k from t61 where myfunction6(lastname)='kcoj'");
        assertTrue(rs.next());
        assertEquals(1, rs.getInt(1));
        props.setProperty(PhoenixRuntime.CURRENT_SCN_ATTRIB, Long.toString(ts + 60));
        props.setProperty(QueryServices.ALLOW_USER_DEFINED_FUNCTIONS_ATTRIB, "false");
        conn = DriverManager.getConnection(url, props);
        stmt = conn.createStatement();
        try {
            rs = stmt.executeQuery("select k from t61 where reverse(lastname,11)='kcoj'");
            fail("FunctionNotFoundException should be thrown.");
        } catch(FunctionNotFoundException e) {
            
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
        assertEquals("CLIENT PARALLEL 1-WAY RANGE SCAN OVER T5 [1,'kcoj']\n"
                + "    SERVER FILTER BY FIRST KEY ONLY\n"
                +"CLIENT MERGE SORT", QueryUtil.getExplainPlan(rs));
        rs = stmt.executeQuery(query);
        assertTrue(rs.next());
        assertEquals(1, rs.getInt(1));
        assertEquals(1, rs.getInt(2));
        assertEquals("kcoj", rs.getString(3));
        assertFalse(rs.next());
    }

    private static void initJoinTableValues(Connection conn) throws Exception {
        conn.createStatement().execute("create table " + JOIN_ITEM_TABLE_FULL_NAME +
                "   (\"item_id\" varchar(10) not null primary key, " +
                "    name varchar, " +
                "    price integer, " +
                "    discount1 integer, " +
                "    discount2 integer, " +
                "    \"supplier_id\" varchar(10), " +
                "    description varchar)");
        conn.createStatement().execute("create table " + JOIN_SUPPLIER_TABLE_FULL_NAME +
                "   (\"supplier_id\" varchar(10) not null primary key, " +
                "    name varchar, " +
                "    phone varchar(12), " +
                "    address varchar, " +
                "    loc_id varchar(5))");
        PreparedStatement stmt;
        conn.createStatement().execute("CREATE SEQUENCE my.seq");
        
        // Insert into item table
        stmt = conn.prepareStatement(
                "upsert into " + JOIN_ITEM_TABLE_FULL_NAME +
                "   (\"item_id\", " +
                "    NAME, " +
                "    PRICE, " +
                "    DISCOUNT1, " +
                "    DISCOUNT2, " +
                "    \"supplier_id\", " +
                "    DESCRIPTION) " +
                "values (?, ?, ?, ?, ?, ?, ?)");
        stmt.setString(1, "0000000001");
        stmt.setString(2, "T1");
        stmt.setInt(3, 100);
        stmt.setInt(4, 5);
        stmt.setInt(5, 10);
        stmt.setString(6, "0000000001");
        stmt.setString(7, "Item T1");
        stmt.execute();

        stmt.setString(1, "0000000002");
        stmt.setString(2, "T2");
        stmt.setInt(3, 200);
        stmt.setInt(4, 5);
        stmt.setInt(5, 8);
        stmt.setString(6, "0000000001");
        stmt.setString(7, "Item T2");
        stmt.execute();

        stmt.setString(1, "0000000003");
        stmt.setString(2, "T3");
        stmt.setInt(3, 300);
        stmt.setInt(4, 8);
        stmt.setInt(5, 12);
        stmt.setString(6, "0000000002");
        stmt.setString(7, "Item T3");
        stmt.execute();

        stmt.setString(1, "0000000004");
        stmt.setString(2, "T4");
        stmt.setInt(3, 400);
        stmt.setInt(4, 6);
        stmt.setInt(5, 10);
        stmt.setString(6, "0000000002");
        stmt.setString(7, "Item T4");
        stmt.execute();

        stmt.setString(1, "0000000005");
        stmt.setString(2, "T5");
        stmt.setInt(3, 500);
        stmt.setInt(4, 8);
        stmt.setInt(5, 15);
        stmt.setString(6, "0000000005");
        stmt.setString(7, "Item T5");
        stmt.execute();

        stmt.setString(1, "0000000006");
        stmt.setString(2, "T6");
        stmt.setInt(3, 600);
        stmt.setInt(4, 8);
        stmt.setInt(5, 15);
        stmt.setString(6, "0000000006");
        stmt.setString(7, "Item T6");
        stmt.execute();
        
        stmt.setString(1, "invalid001");
        stmt.setString(2, "INVALID-1");
        stmt.setInt(3, 0);
        stmt.setInt(4, 0);
        stmt.setInt(5, 0);
        stmt.setString(6, "0000000000");
        stmt.setString(7, "Invalid item for join test");
        stmt.execute();

        // Insert into supplier table
        stmt = conn.prepareStatement(
                "upsert into " + JOIN_SUPPLIER_TABLE_FULL_NAME +
                "   (\"supplier_id\", " +
                "    NAME, " +
                "    PHONE, " +
                "    ADDRESS, " +
                "    LOC_ID) " +
                "values (?, ?, ?, ?, ?)");
        stmt.setString(1, "0000000001");
        stmt.setString(2, "S1");
        stmt.setString(3, "888-888-1111");
        stmt.setString(4, "101 YYY Street");
        stmt.setString(5, "10001");
        stmt.execute();
            
        stmt.setString(1, "0000000002");
        stmt.setString(2, "S2");
        stmt.setString(3, "888-888-2222");
        stmt.setString(4, "202 YYY Street");
        stmt.setString(5, "10002");
        stmt.execute();

        stmt.setString(1, "0000000003");
        stmt.setString(2, "S3");
        stmt.setString(3, "888-888-3333");
        stmt.setString(4, "303 YYY Street");
        stmt.setString(5, null);
        stmt.execute();

        stmt.setString(1, "0000000004");
        stmt.setString(2, "S4");
        stmt.setString(3, "888-888-4444");
        stmt.setString(4, "404 YYY Street");
        stmt.setString(5, null);
        stmt.execute();

        stmt.setString(1, "0000000005");
        stmt.setString(2, "S5");
        stmt.setString(3, "888-888-5555");
        stmt.setString(4, "505 YYY Street");
        stmt.setString(5, "10005");
        stmt.execute();

        stmt.setString(1, "0000000006");
        stmt.setString(2, "S6");
        stmt.setString(3, "888-888-6666");
        stmt.setString(4, "606 YYY Street");
        stmt.setString(5, "10006");
        stmt.execute();

        conn.commit();
    }
    
    @Test
    public void testUdfWithJoin() throws Exception {
        String query = "SELECT /*+ USE_SORT_MERGE_JOIN*/ item.\"item_id\", item.name, supp.\"supplier_id\", myreverse8(supp.name) FROM "
                + JOIN_SUPPLIER_TABLE_FULL_NAME + " supp RIGHT JOIN " + JOIN_ITEM_TABLE_FULL_NAME
                + " item ON myreverse8(item.\"supplier_id\") = myreverse8(supp.\"supplier_id\") ORDER BY \"item_id\"";
        Connection conn = driver.connect(url, EMPTY_PROPS);
        initJoinTableValues(conn);
        conn.createStatement().execute(
                "create function myreverse8(VARCHAR) returns VARCHAR as 'org.apache.phoenix.end2end.MyReverse' using jar "
                        + "'" + util.getConfiguration().get(DYNAMIC_JARS_DIR_KEY) + "/myjar1.jar" + "'");
        try {
            PreparedStatement statement = conn.prepareStatement(query);
            ResultSet rs = statement.executeQuery();
            assertTrue(rs.next());
            assertEquals(rs.getString(1), "0000000001");
            assertEquals(rs.getString(2), "T1");
            assertEquals(rs.getString(3), "0000000001");
            assertEquals(rs.getString(4), "1S");
            assertTrue(rs.next());
            assertEquals(rs.getString(1), "0000000002");
            assertEquals(rs.getString(2), "T2");
            assertEquals(rs.getString(3), "0000000001");
            assertEquals(rs.getString(4), "1S");
            assertTrue(rs.next());
            assertEquals(rs.getString(1), "0000000003");
            assertEquals(rs.getString(2), "T3");
            assertEquals(rs.getString(3), "0000000002");
            assertEquals(rs.getString(4), "2S");
            assertTrue(rs.next());
            assertEquals(rs.getString(1), "0000000004");
            assertEquals(rs.getString(2), "T4");
            assertEquals(rs.getString(3), "0000000002");
            assertEquals(rs.getString(4), "2S");
            assertTrue(rs.next());
            assertEquals(rs.getString(1), "0000000005");
            assertEquals(rs.getString(2), "T5");
            assertEquals(rs.getString(3), "0000000005");
            assertEquals(rs.getString(4), "5S");
            assertTrue(rs.next());
            assertEquals(rs.getString(1), "0000000006");
            assertEquals(rs.getString(2), "T6");
            assertEquals(rs.getString(3), "0000000006");
            assertEquals(rs.getString(4), "6S");
            assertTrue(rs.next());
            assertEquals(rs.getString(1), "invalid001");
            assertEquals(rs.getString(2), "INVALID-1");
            assertNull(rs.getString(3));
            assertNull(rs.getString(4));

            assertFalse(rs.next());
        } finally {
            conn.close();
        }
    }

    @Test
    public void testReplaceFunction() throws Exception {
        Connection conn = driver.connect(url, EMPTY_PROPS);
        Statement stmt = conn.createStatement();
        conn.createStatement().execute("create table t10(k integer primary key, k1 integer, lastname varchar)");
        stmt.execute("upsert into t10 values(1,1,'jock')");
        conn.commit();
        stmt.execute("create function myfunction63(VARCHAR) returns VARCHAR as 'org.apache.phoenix.end2end."+MY_REVERSE_CLASS_NAME+"' using jar "
                + "'"+util.getConfiguration().get(DYNAMIC_JARS_DIR_KEY) + "/myjar1.jar"+"'");
        stmt.execute("create or replace function myfunction63(INTEGER, INTEGER CONSTANT defaultValue=10 minvalue=1 maxvalue=15 ) returns INTEGER as 'org.apache.phoenix.end2end."+MY_SUM_CLASS_NAME+"' using jar "
                + "'"+util.getConfiguration().get(DYNAMIC_JARS_DIR_KEY) + "/myjar2.jar"+"'");
        ResultSet rs = stmt.executeQuery("select myfunction63(k,12) from t10");
        assertTrue(rs.next());
        assertEquals(13, rs.getInt(1));
        rs = stmt.executeQuery("select myfunction63(k) from t10");
        assertTrue(rs.next());
        assertEquals(11, rs.getInt(1));
        rs = stmt.executeQuery("select k from t10 where myfunction63(k)=11");
        assertTrue(rs.next());
        assertEquals(1, rs.getInt(1));
        Connection conn2 = driver.connect(url, EMPTY_PROPS);
        stmt = conn2.createStatement();
        rs = stmt.executeQuery("select myfunction63(k,12) from t10");
        assertTrue(rs.next());
        assertEquals(13, rs.getInt(1));
        rs = stmt.executeQuery("select myfunction63(k) from t10");
        assertTrue(rs.next());
        assertEquals(11, rs.getInt(1));
        rs = stmt.executeQuery("select k from t10 where myfunction63(k)=11");
        assertTrue(rs.next());
        assertEquals(1, rs.getInt(1));
    }

    @Test
    public void testUDFsWithSameChildrenInAQuery() throws Exception {
        Connection conn = driver.connect(url, EMPTY_PROPS);
        Statement stmt = conn.createStatement();
        conn.createStatement().execute("create table t11(k varbinary primary key, k1 integer, lastname varchar)");
        String query = "UPSERT INTO t11"
                + "(k, k1, lastname) "
                + "VALUES(?,?,?)";
        PreparedStatement pStmt = conn.prepareStatement(query);
        pStmt.setBytes(1, new byte[] {0,0,0,0,0,0,0,1});
        pStmt.setInt(2, 1);
        pStmt.setString(3, "jock");
        pStmt.execute();
        conn.commit();
        stmt.execute("create function udf1(VARBINARY) returns UNSIGNED_LONG as 'org.apache.phoenix.end2end."+GETX_CLASSNAME+"' using jar "
                + "'"+util.getConfiguration().get(DYNAMIC_JARS_DIR_KEY) + "/myjar5.jar"+"'");
        stmt.execute("create function udf2(VARBINARY) returns INTEGER as 'org.apache.phoenix.end2end."+GETY_CLASSNAME+"' using jar "
                + "'"+util.getConfiguration().get(DYNAMIC_JARS_DIR_KEY) + "/myjar6.jar"+"'");
        ResultSet rs = stmt.executeQuery("select udf1(k), udf2(k) from t11");
        assertTrue(rs.next());
        assertEquals(72057594037927936l, rs.getLong(1));
        assertEquals(0, rs.getInt(2));
        rs = stmt.executeQuery("select udf2(k), udf1(k) from t11");
        assertTrue(rs.next());
        assertEquals(0, rs.getInt(1));
        assertEquals(72057594037927936l, rs.getLong(2));
        rs = stmt.executeQuery("select udf1(k), udf1(k) from t11");
        assertTrue(rs.next());
        assertEquals(72057594037927936l, rs.getLong(1));
        assertEquals(72057594037927936l, rs.getLong(2));
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
            String pathToAdd = packageName.replace('.', '/') + '/';
            String jarPathStr = new String(pathToAdd);
            Set<String> pathsInJar = new HashSet<String>();

            while (pathsInJar.add(jarPathStr)) {
                int ix = jarPathStr.lastIndexOf('/', jarPathStr.length() - 2);
                if (ix < 0) {
                    break;
                }
                jarPathStr = jarPathStr.substring(0, ix);
            }
            for (String pathInJar : pathsInJar) {
                jarOutputStream.putNextEntry(new JarEntry(pathInJar));
                jarOutputStream.closeEntry();
            }

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
            Connection conn = driver.connect(url, EMPTY_PROPS);
            Statement stmt = conn.createStatement();
            stmt.execute("add jars '"+jarFile.getAbsolutePath()+"'");
        } finally {
            if (javaFile != null) javaFile.delete();
            if (classFile != null) classFile.delete();
            if (jarFile != null) jarFile.delete();
        }
    }

}
