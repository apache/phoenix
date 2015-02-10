/*
 * Copyright 2010 The Apache Software Foundation
 *
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 *distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you maynot use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicablelaw or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.phoenix.pig;

import static org.apache.phoenix.query.BaseTest.setUpConfigForMiniCluster;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Properties;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.phoenix.end2end.NeedsOwnMiniClusterTest;
import org.apache.phoenix.jdbc.PhoenixDriver;
import org.apache.phoenix.query.QueryServices;
import org.apache.phoenix.util.PhoenixRuntime;
import org.apache.phoenix.util.PropertiesUtil;
import org.apache.phoenix.util.SchemaUtil;
import org.apache.phoenix.util.TestUtil;
import org.apache.pig.ExecType;
import org.apache.pig.PigServer;
import org.apache.pig.backend.hadoop.datastorage.ConfigurationUtil;
import org.apache.pig.builtin.mock.Storage;
import org.apache.pig.builtin.mock.Storage.Data;
import org.apache.pig.data.DataType;
import org.apache.pig.data.Tuple;
import org.apache.pig.impl.logicalLayer.schema.Schema;
import org.apache.pig.impl.logicalLayer.schema.Schema.FieldSchema;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import com.google.common.base.Preconditions;

/**
 * 
 * Test class to run all the integration tests against a virtual map reduce cluster.
 */
@Category(NeedsOwnMiniClusterTest.class)
public class PhoenixHBaseLoaderIT {
    
    private static final Log LOG = LogFactory.getLog(PhoenixHBaseLoaderIT.class);
    private static final String SCHEMA_NAME = "T";
    private static final String TABLE_NAME = "A";
    private static final String INDEX_NAME = "I";
    private static final String TABLE_FULL_NAME = SchemaUtil.getTableName(SCHEMA_NAME, TABLE_NAME);
    private static HBaseTestingUtility hbaseTestUtil;
    private static String zkQuorum;
    private static Connection conn;
    private static PigServer pigServer;
    private static Configuration conf;

    @BeforeClass
    public static void setUpBeforeClass() throws Exception {
        hbaseTestUtil = new HBaseTestingUtility();
        conf = hbaseTestUtil.getConfiguration();
        setUpConfigForMiniCluster(conf);
        conf.set(QueryServices.DROP_METADATA_ATTRIB, Boolean.toString(true));
        hbaseTestUtil.startMiniCluster();

        Class.forName(PhoenixDriver.class.getName());
        zkQuorum = "localhost:" + hbaseTestUtil.getZkCluster().getClientPort();
        Properties props = PropertiesUtil.deepCopy(TestUtil.TEST_PROPERTIES);
        props.put(QueryServices.DROP_METADATA_ATTRIB, Boolean.toString(true));
        conn = DriverManager.getConnection(PhoenixRuntime.JDBC_PROTOCOL +
                 PhoenixRuntime.JDBC_PROTOCOL_SEPARATOR + zkQuorum,props);
     }
    
    @Before
    public void setUp() throws Exception {
        pigServer = new PigServer(ExecType.LOCAL,
                ConfigurationUtil.toProperties(conf));
    }

    /**
     * Validates the schema returned for a table with Pig data types.
     * @throws Exception
     */
    @Test
    public void testSchemaForTable() throws Exception {
        final String ddl = String.format("CREATE TABLE %s "
                + "  (a_string varchar not null, a_binary varbinary not null, a_integer integer, cf1.a_float float"
                + "  CONSTRAINT pk PRIMARY KEY (a_string, a_binary))\n", TABLE_FULL_NAME);
        conn.createStatement().execute(ddl);

        pigServer.registerQuery(String.format(
                "A = load 'hbase://table/%s' using " + PhoenixHBaseLoader.class.getName() + "('%s');", TABLE_FULL_NAME,
                zkQuorum));
        
        final Schema schema = pigServer.dumpSchema("A");
        List<FieldSchema> fields = schema.getFields();
        assertEquals(4, fields.size());
        assertTrue(fields.get(0).alias.equalsIgnoreCase("a_string"));
        assertTrue(fields.get(0).type == DataType.CHARARRAY);
        assertTrue(fields.get(1).alias.equalsIgnoreCase("a_binary"));
        assertTrue(fields.get(1).type == DataType.BYTEARRAY);
        assertTrue(fields.get(2).alias.equalsIgnoreCase("a_integer"));
        assertTrue(fields.get(2).type == DataType.INTEGER);
        assertTrue(fields.get(3).alias.equalsIgnoreCase("a_float"));
        assertTrue(fields.get(3).type == DataType.FLOAT);
    }

    /**
     * Validates the schema returned when specific columns of a table are given as part of LOAD .
     * @throws Exception
     */
    @Test
    public void testSchemaForTableWithSpecificColumns() throws Exception {
        
        //create the table
        final String ddl = "CREATE TABLE " + TABLE_FULL_NAME 
                + "  (ID INTEGER NOT NULL PRIMARY KEY,NAME VARCHAR, AGE INTEGER) ";
        conn.createStatement().execute(ddl);
        
        
        final String selectColumns = "ID,NAME";
        pigServer.registerQuery(String.format(
                "A = load 'hbase://table/%s/%s' using " + PhoenixHBaseLoader.class.getName() + "('%s');",
                TABLE_FULL_NAME, selectColumns, zkQuorum));
        
        Schema schema = pigServer.dumpSchema("A");
        List<FieldSchema> fields = schema.getFields();
        assertEquals(2, fields.size());
        assertTrue(fields.get(0).alias.equalsIgnoreCase("ID"));
        assertTrue(fields.get(0).type == DataType.INTEGER);
        assertTrue(fields.get(1).alias.equalsIgnoreCase("NAME"));
        assertTrue(fields.get(1).type == DataType.CHARARRAY);
        
    }
    
    /**
     * Validates the schema returned when a SQL SELECT query is given as part of LOAD .
     * @throws Exception
     */
    @Test
    public void testSchemaForQuery() throws Exception {
        
       //create the table.
        String ddl = String.format("CREATE TABLE " + TABLE_FULL_NAME +
                 "  (A_STRING VARCHAR NOT NULL, A_DECIMAL DECIMAL NOT NULL, CF1.A_INTEGER INTEGER, CF2.A_DOUBLE DOUBLE"
                + "  CONSTRAINT pk PRIMARY KEY (A_STRING, A_DECIMAL))\n", TABLE_FULL_NAME);
        conn.createStatement().execute(ddl);
        
        
        
        //sql query for LOAD
        final String sqlQuery = "SELECT A_STRING,CF1.A_INTEGER,CF2.A_DOUBLE FROM " + TABLE_FULL_NAME;
        pigServer.registerQuery(String.format(
                "A = load 'hbase://query/%s' using " + PhoenixHBaseLoader.class.getName() + "('%s');",
                sqlQuery, zkQuorum));
        
        //assert the schema.
        Schema schema = pigServer.dumpSchema("A");
        List<FieldSchema> fields = schema.getFields();
        assertEquals(3, fields.size());
        assertTrue(fields.get(0).alias.equalsIgnoreCase("a_string"));
        assertTrue(fields.get(0).type == DataType.CHARARRAY);
        assertTrue(fields.get(1).alias.equalsIgnoreCase("a_integer"));
        assertTrue(fields.get(1).type == DataType.INTEGER);
        assertTrue(fields.get(2).alias.equalsIgnoreCase("a_double"));
        assertTrue(fields.get(2).type == DataType.DOUBLE);
    }
    
    /**
     * Validates the schema when it is given as part of LOAD..AS
     * @throws Exception
     */
    @Test
    public void testSchemaForTableWithAlias() throws Exception {
        
        //create the table.
        String ddl = "CREATE TABLE  " + TABLE_FULL_NAME 
                + "  (A_STRING VARCHAR NOT NULL, A_DECIMAL DECIMAL NOT NULL, CF1.A_INTEGER INTEGER, CF2.A_DOUBLE DOUBLE"
                + "  CONSTRAINT pk PRIMARY KEY (A_STRING, A_DECIMAL)) \n";
        conn.createStatement().execute(ddl);
        
        //select query given as part of LOAD.
        final String sqlQuery = "SELECT A_STRING,A_DECIMAL,CF1.A_INTEGER,CF2.A_DOUBLE FROM " + TABLE_FULL_NAME;
        
        LOG.info(String.format("Generated SQL Query [%s]",sqlQuery));
        
        pigServer.registerQuery(String.format(
                "raw = load 'hbase://query/%s' using " + PhoenixHBaseLoader.class.getName() + "('%s') AS (a:chararray,b:bigdecimal,c:int,d:double);",
                sqlQuery, zkQuorum));
        
        //test the schema.
        Schema schema = pigServer.dumpSchema("raw");
        List<FieldSchema> fields = schema.getFields();
        assertEquals(4, fields.size());
        assertTrue(fields.get(0).alias.equalsIgnoreCase("a"));
        assertTrue(fields.get(0).type == DataType.CHARARRAY);
        assertTrue(fields.get(1).alias.equalsIgnoreCase("b"));
        assertTrue(fields.get(1).type == DataType.BIGDECIMAL);
        assertTrue(fields.get(2).alias.equalsIgnoreCase("c"));
        assertTrue(fields.get(2).type == DataType.INTEGER);
        assertTrue(fields.get(3).alias.equalsIgnoreCase("d"));
        assertTrue(fields.get(3).type == DataType.DOUBLE);
    }
    
    /**
     * @throws Exception
     */
    @Test
    public void testDataForTable() throws Exception {
        
         //create the table
         String ddl = "CREATE TABLE  " + TABLE_FULL_NAME 
                + "  (ID  INTEGER NOT NULL PRIMARY KEY, NAME VARCHAR, AGE INTEGER) ";
                
        conn.createStatement().execute(ddl);
        
        //prepare data with 10 rows having age 25 and the other 30.
        final String dml = "UPSERT INTO " + TABLE_FULL_NAME + " VALUES(?,?,?)";
        PreparedStatement stmt = conn.prepareStatement(dml);
        int rows = 20;
        for(int i = 0 ; i < rows; i++) {
            stmt.setInt(1, i);
            stmt.setString(2, "a"+i);
            stmt.setInt(3, (i % 2 == 0) ? 25 : 30);
            stmt.execute();    
        }
        conn.commit();
         
        //load data and filter rows whose age is > 25
        pigServer.registerQuery(String.format(
                "A = load 'hbase://table/%s' using "  + PhoenixHBaseLoader.class.getName() + "('%s');", TABLE_FULL_NAME,
                zkQuorum));
        pigServer.registerQuery("B = FILTER A BY AGE > 25;");
        
        final Iterator<Tuple> iterator = pigServer.openIterator("B");
        int recordsRead = 0;
        while (iterator.hasNext()) {
            final Tuple each = iterator.next();
            assertEquals(3, each.size());
            recordsRead++;
        }
        assertEquals(rows/2, recordsRead);
    }
    
    /**
     * @throws Exception
     */
    @Test
    public void testDataForSQLQuery() throws Exception {
        
         //create the table
         String ddl = "CREATE TABLE  " + TABLE_FULL_NAME 
                + "  (ID  INTEGER NOT NULL PRIMARY KEY, NAME VARCHAR, AGE INTEGER) ";
                
        conn.createStatement().execute(ddl);
        
        //prepare data with 10 rows having age 25 and the other 30.
        final String dml = "UPSERT INTO " + TABLE_FULL_NAME + " VALUES(?,?,?)";
        PreparedStatement stmt = conn.prepareStatement(dml);
        int rows = 20;
        for(int i = 0 ; i < rows; i++) {
            stmt.setInt(1, i);
            stmt.setString(2, "a"+i);
            stmt.setInt(3, (i % 2 == 0) ? 25 : 30);
            stmt.execute();    
        }
        conn.commit();
        
        //sql query
        final String sqlQuery = " SELECT ID,NAME,AGE FROM " + TABLE_FULL_NAME + " WHERE AGE > 25";
        //load data and filter rows whose age is > 25
        pigServer.registerQuery(String.format(
                "A = load 'hbase://query/%s' using org.apache.phoenix.pig.PhoenixHBaseLoader('%s');", sqlQuery,
                zkQuorum));
        
        final Iterator<Tuple> iterator = pigServer.openIterator("A");
        int recordsRead = 0;
        while (iterator.hasNext()) {
            iterator.next();
            recordsRead++;
        }
        assertEquals(rows/2, recordsRead);
    }
    
    /**
     * 
     * @throws Exception
     */
    @Test
    public void testForNonPKSQLQuery() throws Exception {
        
         //create the table
         String ddl = "CREATE TABLE  " + TABLE_FULL_NAME 
                + " ( ID VARCHAR PRIMARY KEY, FOO VARCHAR, BAR INTEGER, BAZ UNSIGNED_INT)";
                
        conn.createStatement().execute(ddl);
        
        //upsert data.
        final String dml = "UPSERT INTO " + TABLE_FULL_NAME + " VALUES(?,?,?,?) ";
        PreparedStatement stmt = conn.prepareStatement(dml);
        stmt.setString(1, "a");
        stmt.setString(2, "a");
        stmt.setInt(3,-1);
        stmt.setInt(4,1);
        stmt.execute();
        stmt.setString(1, "b");
        stmt.setString(2, "b");
        stmt.setInt(3,-2);
        stmt.setInt(4,2);
        stmt.execute();
        
        conn.commit();
        
        //sql query
        final String sqlQuery = String.format(" SELECT FOO, BAZ FROM %s WHERE BAR = -1 " , TABLE_FULL_NAME);
      
        pigServer.registerQuery(String.format(
                "A = load 'hbase://query/%s' using " + PhoenixHBaseLoader.class.getName() + "('%s');", sqlQuery,
                zkQuorum));
        
        final Iterator<Tuple> iterator = pigServer.openIterator("A");
        int recordsRead = 0;
        while (iterator.hasNext()) {
            final Tuple tuple = iterator.next();
            assertEquals("a", tuple.get(0));
            assertEquals(1, tuple.get(1));
            recordsRead++;
        }
        assertEquals(1, recordsRead);
        
        //test the schema. Test for PHOENIX-1123
        Schema schema = pigServer.dumpSchema("A");
        List<FieldSchema> fields = schema.getFields();
        assertEquals(2, fields.size());
        assertTrue(fields.get(0).alias.equalsIgnoreCase("FOO"));
        assertTrue(fields.get(0).type == DataType.CHARARRAY);
        assertTrue(fields.get(1).alias.equalsIgnoreCase("BAZ"));
        assertTrue(fields.get(1).type == DataType.INTEGER);
    }
    
    /**
     * @throws Exception
     */
    @Test
    public void testGroupingOfDataForTable() throws Exception {
        
         //create the table
         String ddl = "CREATE TABLE  " + TABLE_FULL_NAME 
                + "  (ID  INTEGER NOT NULL PRIMARY KEY, NAME VARCHAR, AGE INTEGER, SAL INTEGER) ";
                
        conn.createStatement().execute(ddl);
        
        //prepare data with 10 rows having age 25 and the other 30.
        final String dml = "UPSERT INTO " + TABLE_FULL_NAME + " VALUES(?,?,?,?)";
        PreparedStatement stmt = conn.prepareStatement(dml);
        int rows = 20;
        int j = 0, k = 0;
        for(int i = 0 ; i < rows; i++) {
            stmt.setInt(1, i);
            stmt.setString(2, "a"+i);
            if(i % 2 == 0) {
                stmt.setInt(3, 25);
                stmt.setInt(4, 10 * 2 * j++);    
            } else {
                stmt.setInt(3, 30);
                stmt.setInt(4, 10 * 3 * k++);
            }
            
            stmt.execute();    
        }
        conn.commit();
        
        //prepare the mock storage with expected output
        final Data data = Storage.resetData(pigServer);
        List<Tuple> expectedList = new ArrayList<Tuple>();
        expectedList.add(Storage.tuple(0,180));
        expectedList.add(Storage.tuple(0,270));
        
         //load data and filter rows whose age is > 25
        pigServer.setBatchOn();
        pigServer.registerQuery(String.format(
                "A = load 'hbase://table/%s' using " + PhoenixHBaseLoader.class.getName() + "('%s');", TABLE_FULL_NAME,
                zkQuorum));
        
        pigServer.registerQuery("B = GROUP A BY AGE;");
        pigServer.registerQuery("C = FOREACH B GENERATE MIN(A.SAL),MAX(A.SAL);");
        pigServer.registerQuery("STORE C INTO 'out' using mock.Storage();");
        pigServer.executeBatch();
        
        List<Tuple> actualList = data.get("out");
        assertEquals(expectedList, actualList);
    }
    
    /**
     * Tests both  {@link PhoenixHBaseLoader} and {@link PhoenixHBaseStorage} 
     * @throws Exception
     */
    @Test
    public void testLoadAndStore() throws Exception {
        
         //create the tables
         final String sourceTableddl = "CREATE TABLE  " + TABLE_FULL_NAME 
                + "  (ID  INTEGER NOT NULL PRIMARY KEY, NAME VARCHAR, AGE INTEGER, SAL INTEGER) ";
         
         final String targetTable = "AGGREGATE";
         final String targetTableddl = "CREATE TABLE " + targetTable 
                 +  "(AGE INTEGER NOT NULL PRIMARY KEY , MIN_SAL INTEGER , MAX_SAL INTEGER) ";
                 
        conn.createStatement().execute(sourceTableddl);
        conn.createStatement().execute(targetTableddl);
        
        //prepare data with 10 rows having age 25 and the other 30.
        final String dml = "UPSERT INTO " + TABLE_FULL_NAME + " VALUES(?,?,?,?)";
        PreparedStatement stmt = conn.prepareStatement(dml);
        int rows = 20;
        int j = 0, k = 0;
        for(int i = 0 ; i < rows; i++) {
            stmt.setInt(1, i);
            stmt.setString(2, "a"+i);
            if(i % 2 == 0) {
                stmt.setInt(3, 25);
                stmt.setInt(4, 10 * 2 * j++);    
            } else {
                stmt.setInt(3, 30);
                stmt.setInt(4, 10 * 3 * k++);
            }
            
            stmt.execute();    
        }
        conn.commit();
        
            
         //load data and filter rows whose age is > 25
        pigServer.setBatchOn();
        pigServer.registerQuery(String.format(
                "A = load 'hbase://table/%s' using " + PhoenixHBaseLoader.class.getName() + "('%s');", TABLE_FULL_NAME,
                zkQuorum));
        
        pigServer.registerQuery("B = GROUP A BY AGE;");
        pigServer.registerQuery("C = FOREACH B GENERATE group as AGE,MIN(A.SAL),MAX(A.SAL);");
        pigServer.registerQuery("STORE C INTO 'hbase://" + targetTable 
                + "' using " + PhoenixHBaseStorage.class.getName() + "('"
                 + zkQuorum + "', '-batchSize 1000');");
        pigServer.executeBatch();
        
        //validate the data with what is stored.
        final String selectQuery = "SELECT AGE , MIN_SAL ,MAX_SAL FROM " + targetTable + " ORDER BY AGE";
        final ResultSet rs = conn.createStatement().executeQuery(selectQuery);
        assertTrue(rs.next());
        assertEquals(25, rs.getInt("AGE"));
        assertEquals(0, rs.getInt("MIN_SAL"));
        assertEquals(180, rs.getInt("MAX_SAL"));
        assertTrue(rs.next());
        assertEquals(30, rs.getInt("AGE"));
        assertEquals(0, rs.getInt("MIN_SAL"));
        assertEquals(270, rs.getInt("MAX_SAL"));
    }
	
	/**
     * Test for Sequence
     * @throws Exception
     */
    @Test
    public void testDataForSQLQueryWithSequences() throws Exception {
        
         //create the table
         String ddl = "CREATE TABLE " + TABLE_FULL_NAME
                + " (ID INTEGER NOT NULL PRIMARY KEY, NAME VARCHAR, AGE INTEGER) ";
                
        conn.createStatement().execute(ddl);
        
        String sequenceDdl = "CREATE SEQUENCE my_sequence";
                
        conn.createStatement().execute(sequenceDdl);
           
        //prepare data with 10 rows having age 25 and the other 30.
        final String dml = "UPSERT INTO " + TABLE_FULL_NAME + " VALUES(?,?,?)";
        PreparedStatement stmt = conn.prepareStatement(dml);
        int rows = 20;
        for(int i = 0 ; i < rows; i++) {
            stmt.setInt(1, i);
            stmt.setString(2, "a"+i);
            stmt.setInt(3, (i % 2 == 0) ? 25 : 30);
            stmt.execute();
        }
        conn.commit();
        
        //sql query load data and filter rows whose age is > 25
        final String sqlQuery = " SELECT NEXT VALUE FOR my_sequence AS my_seq,ID,NAME,AGE FROM " + TABLE_FULL_NAME + " WHERE AGE > 25";
        pigServer.registerQuery(String.format(
                "A = load 'hbase://query/%s' using " + PhoenixHBaseLoader.class.getName() + "('%s');", sqlQuery,
                zkQuorum));
        
        
        Iterator<Tuple> iterator = pigServer.openIterator("A");
        int recordsRead = 0;
        while (iterator.hasNext()) {
            iterator.next();
            recordsRead++;
        }
        assertEquals(rows/2, recordsRead);
    }
	
	 @Test
    public void testDataForSQLQueryWithFunctions() throws Exception {
        
         //create the table
         String ddl = "CREATE TABLE " + TABLE_FULL_NAME
                + " (ID INTEGER NOT NULL PRIMARY KEY, NAME VARCHAR) ";
                
        conn.createStatement().execute(ddl);
        
        final String dml = "UPSERT INTO " + TABLE_FULL_NAME + " VALUES(?,?)";
        PreparedStatement stmt = conn.prepareStatement(dml);
        int rows = 20;
        for(int i = 0 ; i < rows; i++) {
            stmt.setInt(1, i);
            stmt.setString(2, "a"+i);
            stmt.execute();
        }
        conn.commit();
        
        //sql query
        final String sqlQuery = " SELECT UPPER(NAME) AS n FROM " + TABLE_FULL_NAME + " ORDER BY ID" ;

        pigServer.registerQuery(String.format(
                "A = load 'hbase://query/%s' using "  + PhoenixHBaseLoader.class.getName() + "('%s');", sqlQuery,
                zkQuorum));
        
        
        Iterator<Tuple> iterator = pigServer.openIterator("A");
        int i = 0;
        while (iterator.hasNext()) {
            Tuple tuple = iterator.next();
            String name = (String)tuple.get(0);
            assertEquals("A" + i, name);
            i++;
        }
    
    }
	 @Test
	 public void testDataFromIndexTable() throws Exception {
        try {
            //create the table
            String ddl = "CREATE TABLE " + TABLE_NAME
                    + " (ID INTEGER NOT NULL, NAME VARCHAR NOT NULL, EMPLID INTEGER CONSTRAINT pk PRIMARY KEY (ID, NAME)) IMMUTABLE_ROWS=true";
                   
            conn.createStatement().execute(ddl);
           
            //create a index table
            String indexDdl = " CREATE INDEX " + INDEX_NAME + " ON " + TABLE_NAME + " (EMPLID) INCLUDE (NAME) ";
            conn.createStatement().execute(indexDdl);
           
            //upsert the data.
            final String dml = "UPSERT INTO " + TABLE_NAME + " VALUES(?,?,?)";
            PreparedStatement stmt = conn.prepareStatement(dml);
            int rows = 20;
            for(int i = 0 ; i < rows; i++) {
                stmt.setInt(1, i);
                stmt.setString(2, "a"+i);
                stmt.setInt(3, i * 5);
                stmt.execute();
            }
            conn.commit();
            pigServer.registerQuery("A = load 'hbase://query/SELECT NAME , EMPLID FROM A WHERE EMPLID = 25 ' using " + PhoenixHBaseLoader.class.getName() + "('"+zkQuorum + "')  ;");
            Iterator<Tuple> iterator = pigServer.openIterator("A");
            while (iterator.hasNext()) {
                Tuple tuple = iterator.next();
                assertEquals("a5", tuple.get(0));
                assertEquals(25, tuple.get(1));
            }
        } finally {
          dropTable(TABLE_NAME);
          dropTable(INDEX_NAME);
        }
    }
    
    @After
    public void tearDown() throws Exception {
        dropTable(TABLE_FULL_NAME);
        pigServer.shutdown();
    }


    private void dropTable(String tableFullName) throws SQLException {
      Preconditions.checkNotNull(conn);
      conn.createStatement().execute(String.format("DROP TABLE IF EXISTS %s",tableFullName));
    }

    @AfterClass
    public static void tearDownAfterClass() throws Exception {
        try {
            conn.close();
        } finally {
            hbaseTestUtil.shutdownMiniCluster();
        }
    }
}