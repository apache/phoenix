/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.sql.Array;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.phoenix.util.SchemaUtil;
import org.apache.pig.builtin.mock.Storage;
import org.apache.pig.builtin.mock.Storage.Data;
import org.apache.pig.data.DataType;
import org.apache.pig.data.Tuple;
import org.apache.pig.impl.logicalLayer.schema.Schema;
import org.apache.pig.impl.logicalLayer.schema.Schema.FieldSchema;
import org.junit.Test;

import com.google.common.collect.Lists;

/**
 * 
 * Test class to run all the integration tests against a virtual map reduce cluster.
 */
public class PhoenixHBaseLoaderIT extends BasePigIT {

    private static final Log LOG = LogFactory.getLog(PhoenixHBaseLoaderIT.class);
    private static final String SCHEMA_NAME = "T";
    private static final String TABLE_NAME = "A";
    private static final String INDEX_NAME = "I";
    private static final String TABLE_FULL_NAME = SchemaUtil.getTableName(SCHEMA_NAME, TABLE_NAME);
    private static final String CASE_SENSITIVE_TABLE_NAME = SchemaUtil.getEscapedArgument("a");
    private static final String CASE_SENSITIVE_TABLE_FULL_NAME = SchemaUtil.getTableName(SCHEMA_NAME,CASE_SENSITIVE_TABLE_NAME);

    /**
     * Validates the schema returned for a table with Pig data types.
     * @throws Exception
     */
    @Test
    public void testSchemaForTable() throws Exception {
        final String TABLE = "TABLE1";
        final String ddl = String.format("CREATE TABLE %s "
                + "  (a_string varchar not null, a_binary varbinary not null, a_integer integer, cf1.a_float float"
                + "  CONSTRAINT pk PRIMARY KEY (a_string, a_binary))\n", TABLE);
        conn.createStatement().execute(ddl);
        conn.commit();
        pigServer.registerQuery(String.format(
                "A = load 'hbase://table/%s' using " + PhoenixHBaseLoader.class.getName() + "('%s');", TABLE,
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
        final String TABLE = "TABLE2";
        final String ddl = "CREATE TABLE " + TABLE
                + "  (ID INTEGER NOT NULL PRIMARY KEY,NAME VARCHAR, AGE INTEGER) ";
        conn.createStatement().execute(ddl);

        final String selectColumns = "ID,NAME";
        pigServer.registerQuery(String.format(
                "A = load 'hbase://table/%s/%s' using " + PhoenixHBaseLoader.class.getName() + "('%s');",
                TABLE, selectColumns, zkQuorum));

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
        final String TABLE = "TABLE3";
        String ddl = String.format("CREATE TABLE " + TABLE +
                "  (A_STRING VARCHAR NOT NULL, A_DECIMAL DECIMAL NOT NULL, CF1.A_INTEGER INTEGER, CF2.A_DOUBLE DOUBLE"
                + "  CONSTRAINT pk PRIMARY KEY (A_STRING, A_DECIMAL))\n", TABLE);
        conn.createStatement().execute(ddl);



        //sql query for LOAD
        final String sqlQuery = "SELECT A_STRING,CF1.A_INTEGER,CF2.A_DOUBLE FROM " + TABLE;
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
        final String TABLE = "S.TABLE4";
        String ddl = "CREATE TABLE  " + TABLE
                + "  (A_STRING VARCHAR NOT NULL, A_DECIMAL DECIMAL NOT NULL, CF1.A_INTEGER INTEGER, CF2.A_DOUBLE DOUBLE"
                + "  CONSTRAINT pk PRIMARY KEY (A_STRING, A_DECIMAL)) \n";
        conn.createStatement().execute(ddl);

        //select query given as part of LOAD.
        final String sqlQuery = "SELECT A_STRING,A_DECIMAL,CF1.A_INTEGER,CF2.A_DOUBLE FROM " + TABLE;

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
        String ddl = "CREATE TABLE  " + CASE_SENSITIVE_TABLE_FULL_NAME 
                + "  (ID  INTEGER NOT NULL PRIMARY KEY, NAME VARCHAR, AGE INTEGER) ";

        conn.createStatement().execute(ddl);

        //prepare data with 10 rows having age 25 and the other 30.
        final String dml = "UPSERT INTO " + CASE_SENSITIVE_TABLE_FULL_NAME + " VALUES(?,?,?)";
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
                "A = load 'hbase://table/%s' using "  + PhoenixHBaseLoader.class.getName() + "('%s');", CASE_SENSITIVE_TABLE_FULL_NAME,
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
        final String TABLE = "TABLE5";
        String ddl = "CREATE TABLE  " + TABLE
                + " ( ID VARCHAR PRIMARY KEY, FOO VARCHAR, BAR INTEGER, BAZ UNSIGNED_INT)";

        conn.createStatement().execute(ddl);

        //upsert data.
        final String dml = "UPSERT INTO " + TABLE + " VALUES(?,?,?,?) ";
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
        final String sqlQuery = String.format(" SELECT FOO, BAZ FROM %s WHERE BAR = -1 " , TABLE);

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
        final String TABLE = "TABLE6";
        String ddl = "CREATE TABLE  " + TABLE
                + "  (ID  INTEGER NOT NULL PRIMARY KEY, NAME VARCHAR, AGE INTEGER, SAL INTEGER) ";

        conn.createStatement().execute(ddl);

        //prepare data with 10 rows having age 25 and the other 30.
        final String dml = "UPSERT INTO " + TABLE + " VALUES(?,?,?,?)";
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
                "A = load 'hbase://table/%s' using " + PhoenixHBaseLoader.class.getName() + "('%s');", TABLE,
                zkQuorum));

        pigServer.registerQuery("B = GROUP A BY AGE;");
        pigServer.registerQuery("C = FOREACH B GENERATE MIN(A.SAL),MAX(A.SAL);");
        pigServer.registerQuery("STORE C INTO 'out' using mock.Storage();");
        pigServer.executeBatch();

        List<Tuple> actualList = data.get("out");
        assertEquals(expectedList, actualList);
    }

    @Test
    public void testTimestampForSQLQuery() throws Exception {
        //create the table
        String ddl = "CREATE TABLE TIMESTAMP_T (MYKEY VARCHAR,DATE_STP TIMESTAMP CONSTRAINT PK PRIMARY KEY (MYKEY)) ";
        conn.createStatement().execute(ddl);

        final String dml = "UPSERT INTO TIMESTAMP_T VALUES('foo',TO_TIMESTAMP('2006-04-12 00:00:00'))";
        conn.createStatement().execute(dml);
        conn.commit();

        //sql query
        final String sqlQuery = " SELECT mykey, year(DATE_STP) FROM TIMESTAMP_T ";
        pigServer.registerQuery(String.format(
                "A = load 'hbase://query/%s' using org.apache.phoenix.pig.PhoenixHBaseLoader('%s');", sqlQuery,
                zkQuorum));

        final Iterator<Tuple> iterator = pigServer.openIterator("A");
        while (iterator.hasNext()) {
            Tuple tuple = iterator.next();
            assertEquals("foo", tuple.get(0));
            assertEquals(2006, tuple.get(1));
        }
    }

    @Test
    public void testDateForSQLQuery() throws Exception {
        //create the table
        String ddl = "CREATE TABLE DATE_T (MYKEY VARCHAR,DATE_STP Date CONSTRAINT PK PRIMARY KEY (MYKEY)) ";
        conn.createStatement().execute(ddl);

        final String dml = "UPSERT INTO DATE_T VALUES('foo',TO_DATE('2004-03-10 10:00:00'))";
        conn.createStatement().execute(dml);
        conn.commit();

        //sql query
        final String sqlQuery = " SELECT mykey, hour(DATE_STP) FROM DATE_T ";
        pigServer.registerQuery(String.format(
                "A = load 'hbase://query/%s' using org.apache.phoenix.pig.PhoenixHBaseLoader('%s');", sqlQuery,
                zkQuorum));

        final Iterator<Tuple> iterator = pigServer.openIterator("A");
        while (iterator.hasNext()) {
            Tuple tuple = iterator.next();
            assertEquals("foo", tuple.get(0));
            assertEquals(10, tuple.get(1));
        }
    }

    @Test
    public void testTimeForSQLQuery() throws Exception {
        //create the table
        String ddl = "CREATE TABLE TIME_T (MYKEY VARCHAR,DATE_STP TIME CONSTRAINT PK PRIMARY KEY (MYKEY)) ";
        conn.createStatement().execute(ddl);

        final String dml = "UPSERT INTO TIME_T VALUES('foo',TO_TIME('2008-05-16 00:30:00'))";
        conn.createStatement().execute(dml);
        conn.commit();

        //sql query
        final String sqlQuery = " SELECT mykey, minute(DATE_STP) FROM TIME_T ";
        pigServer.registerQuery(String.format(
                "A = load 'hbase://query/%s' using org.apache.phoenix.pig.PhoenixHBaseLoader('%s');", sqlQuery,
                zkQuorum));

        final Iterator<Tuple> iterator = pigServer.openIterator("A");
        while (iterator.hasNext()) {
            Tuple tuple = iterator.next();
            assertEquals("foo", tuple.get(0));
            assertEquals(30, tuple.get(1));
        }
    }

    /**
     * Tests both  {@link PhoenixHBaseLoader} and {@link PhoenixHBaseStorage} 
     * @throws Exception
     */
    @Test
    public void testLoadAndStore() throws Exception {

        //create the tables
        final String TABLE = "TABLE7";
        final String sourceTableddl = "CREATE TABLE  " + TABLE
                + "  (ID  INTEGER NOT NULL PRIMARY KEY, NAME VARCHAR, AGE INTEGER, SAL INTEGER) ";

        final String targetTable = "AGGREGATE";
        final String targetTableddl = "CREATE TABLE " + targetTable
                +  "(AGE INTEGER NOT NULL PRIMARY KEY , MIN_SAL INTEGER , MAX_SAL INTEGER) ";

        conn.createStatement().execute(sourceTableddl);
        conn.createStatement().execute(targetTableddl);

        //prepare data with 10 rows having age 25 and the other 30.
        final String dml = "UPSERT INTO " + TABLE + " VALUES(?,?,?,?)";
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
                "A = load 'hbase://table/%s' using " + PhoenixHBaseLoader.class.getName() + "('%s');", TABLE,
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
        final String TABLE = "TABLE8";
        String ddl = "CREATE TABLE " + TABLE
                + " (ID INTEGER NOT NULL PRIMARY KEY, NAME VARCHAR, AGE INTEGER) ";

        conn.createStatement().execute(ddl);

        String sequenceDdl = "CREATE SEQUENCE my_sequence";

        conn.createStatement().execute(sequenceDdl);

        //prepare data with 10 rows having age 25 and the other 30.
        final String dml = "UPSERT INTO " + TABLE + " VALUES(?,?,?)";
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
        final String sqlQuery = " SELECT NEXT VALUE FOR my_sequence AS my_seq,ID,NAME,AGE FROM " + TABLE + " WHERE AGE > 25";
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
        final String TABLE = "TABLE9";
        String ddl = "CREATE TABLE " + TABLE
                + " (ID INTEGER NOT NULL PRIMARY KEY, NAME VARCHAR) ";

        conn.createStatement().execute(ddl);

        final String dml = "UPSERT INTO " + TABLE + " VALUES(?,?)";
        PreparedStatement stmt = conn.prepareStatement(dml);
        int rows = 20;
        for(int i = 0 ; i < rows; i++) {
            stmt.setInt(1, i);
            stmt.setString(2, "a"+i);
            stmt.execute();
        }
        conn.commit();

        //sql query
        final String sqlQuery = " SELECT UPPER(NAME) AS n FROM " + TABLE + " ORDER BY ID" ;

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
    }

    @Test 
    public void testLoadOfSaltTable() throws Exception {
        final String TABLE = "TABLE11";
        final String sourceTableddl = "CREATE TABLE  " + TABLE
                + "  (ID  INTEGER NOT NULL PRIMARY KEY, NAME VARCHAR, AGE INTEGER, SAL INTEGER) SALT_BUCKETS=2  ";

        conn.createStatement().execute(sourceTableddl);

        //prepare data with 10 rows having age 25 and the other 30.
        final String dml = "UPSERT INTO " + TABLE + " VALUES(?,?,?,?)";
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

        final Data data = Storage.resetData(pigServer);
        List<Tuple> expectedList = new ArrayList<Tuple>();
        expectedList.add(Storage.tuple(25,10));
        expectedList.add(Storage.tuple(30,10));

        pigServer.setBatchOn();
        pigServer.registerQuery(String.format(
                "A = load 'hbase://table/%s' using " + PhoenixHBaseLoader.class.getName() + "('%s');", TABLE,
                zkQuorum));

        pigServer.registerQuery("B = GROUP A BY AGE;");
        pigServer.registerQuery("C = FOREACH B GENERATE group,COUNT(A);");
        pigServer.registerQuery("STORE C INTO 'out' using mock.Storage();");
        pigServer.executeBatch();

        List<Tuple> actualList = data.get("out");
        assertEquals(expectedList.size(), actualList.size());
    }
    
   /**
    * 
    * @throws Exception
    */
    @Test
    public void testLoadForArrayWithQuery() throws Exception {
         //create the table
        final String TABLE = "TABLE14";
        String ddl = "CREATE TABLE  " + TABLE
                + " ( ID INTEGER PRIMARY KEY, a_double_array double array[] , a_varchar_array varchar array, a_concat_str varchar, sep varchar)";
                
        conn.createStatement().execute(ddl);
        
        Double[] doubleArr =  new Double[3];
        doubleArr[0] = 2.2;
        doubleArr[1] = 4.4;
        doubleArr[2] = 6.6;
        Array doubleArray = conn.createArrayOf("DOUBLE", doubleArr);
        Tuple doubleArrTuple = Storage.tuple(2.2d, 4.4d, 6.6d);
        
        Double[] doubleArr2 =  new Double[2];
        doubleArr2[0] = 12.2;
        doubleArr2[1] = 22.2;
        Array doubleArray2 = conn.createArrayOf("DOUBLE", doubleArr2);
        Tuple doubleArrTuple2 = Storage.tuple(12.2d, 22.2d);
        
        String[] strArr =  new String[4];
        strArr[0] = "ABC";
        strArr[1] = "DEF";
        strArr[2] = "GHI";
        strArr[3] = "JKL";
        Array strArray  = conn.createArrayOf("VARCHAR", strArr);
        Tuple strArrTuple = Storage.tuple("ABC", "DEF", "GHI", "JKL");
        
        String[] strArr2 =  new String[2];
        strArr2[0] = "ABC";
        strArr2[1] = "XYZ";
        Array strArray2  = conn.createArrayOf("VARCHAR", strArr2);
        Tuple strArrTuple2 = Storage.tuple("ABC", "XYZ");
        
        //upsert data.
        final String dml = "UPSERT INTO " + TABLE + " VALUES(?, ?, ?, ?, ?) ";
        PreparedStatement stmt = conn.prepareStatement(dml);
        stmt.setInt(1, 1);
        stmt.setArray(2, doubleArray);
        stmt.setArray(3, strArray);
        stmt.setString(4, "ONE,TWO,THREE");
        stmt.setString(5, ",");
        stmt.execute();
        
        stmt.setInt(1, 2);
        stmt.setArray(2, doubleArray2);
        stmt.setArray(3, strArray2);
        stmt.setString(4, "FOUR:five:six");
        stmt.setString(5, ":");
        stmt.execute();
       
        conn.commit();
        
        Tuple dynArrTuple = Storage.tuple("ONE", "TWO", "THREE");
        Tuple dynArrTuple2 = Storage.tuple("FOUR", "five", "six");
        
        //sql query
        final String sqlQuery = String.format(" SELECT ID, A_DOUBLE_ARRAY, A_VARCHAR_ARRAY, REGEXP_SPLIT(a_concat_str, sep) AS flattend_str FROM %s ", TABLE); 
      
        final Data data = Storage.resetData(pigServer);
        List<Tuple> expectedList = new ArrayList<Tuple>();
        expectedList.add(Storage.tuple(1, 3L, 4L, dynArrTuple));
        expectedList.add(Storage.tuple(2, 2L, 2L, dynArrTuple2));
        final String load = String.format("A = load 'hbase://query/%s' using " + PhoenixHBaseLoader.class.getName() + "('%s');",sqlQuery,zkQuorum);
        pigServer.setBatchOn();
        pigServer.registerQuery(load);
        pigServer.registerQuery("B = FOREACH A GENERATE ID, SIZE(A_DOUBLE_ARRAY), SIZE(A_VARCHAR_ARRAY), FLATTEND_STR;");
        pigServer.registerQuery("STORE B INTO 'out' using mock.Storage();");
        pigServer.executeBatch();
        
        List<Tuple> actualList = data.get("out");
        assertEquals(expectedList.size(), actualList.size());
        assertEquals(expectedList, actualList);
        
        Schema schema = pigServer.dumpSchema("A");
        List<FieldSchema> fields = schema.getFields();
        assertEquals(4, fields.size());
        assertTrue(fields.get(0).alias.equalsIgnoreCase("ID"));
        assertTrue(fields.get(0).type == DataType.INTEGER);
        assertTrue(fields.get(1).alias.equalsIgnoreCase("A_DOUBLE_ARRAY"));
        assertTrue(fields.get(1).type == DataType.TUPLE);
        assertTrue(fields.get(2).alias.equalsIgnoreCase("A_VARCHAR_ARRAY"));
        assertTrue(fields.get(2).type == DataType.TUPLE);
        assertTrue(fields.get(3).alias.equalsIgnoreCase("FLATTEND_STR"));
        assertTrue(fields.get(3).type == DataType.TUPLE);
        
        Iterator<Tuple> iterator = pigServer.openIterator("A");
        Tuple firstTuple = Storage.tuple(1, doubleArrTuple, strArrTuple, dynArrTuple);
        Tuple secondTuple = Storage.tuple(2, doubleArrTuple2, strArrTuple2, dynArrTuple2);
        List<Tuple> expectedRows = Lists.newArrayList(firstTuple, secondTuple);
        List<Tuple> actualRows = Lists.newArrayList();
        while (iterator.hasNext()) {
            Tuple tuple = iterator.next();
            actualRows.add(tuple);
        }
        assertEquals(expectedRows, actualRows);
    }
    
    
    /**
     * 
     * @throws Exception
     */
     @Test
     public void testLoadForArrayWithTable() throws Exception {
          //create the table
         final String TABLE = "TABLE15";
         String ddl = "CREATE TABLE  " + TABLE
                 + " ( ID INTEGER PRIMARY KEY, a_double_array double array[])";
                 
         conn.createStatement().execute(ddl);
         
         Double[] doubleArr =  new Double[3];
         doubleArr[0] = 2.2;
         doubleArr[1] = 4.4;
         doubleArr[2] = 6.6;
         Array doubleArray = conn.createArrayOf("DOUBLE", doubleArr);
         Tuple doubleArrTuple = Storage.tuple(2.2d, 4.4d, 6.6d);
         
         Double[] doubleArr2 =  new Double[2];
         doubleArr2[0] = 12.2;
         doubleArr2[1] = 22.2;
         Array doubleArray2 = conn.createArrayOf("DOUBLE", doubleArr2);
         Tuple doubleArrTuple2 = Storage.tuple(12.2d, 22.2d);
         
         //upsert data.
         final String dml = "UPSERT INTO " + TABLE + " VALUES(?, ?) ";
         PreparedStatement stmt = conn.prepareStatement(dml);
         stmt.setInt(1, 1);
         stmt.setArray(2, doubleArray);
         stmt.execute();
         
         stmt.setInt(1, 2);
         stmt.setArray(2, doubleArray2);
         stmt.execute();
        
         conn.commit();
         
         final Data data = Storage.resetData(pigServer);
         List<Tuple> expectedList = new ArrayList<Tuple>();
         expectedList.add(Storage.tuple(1, doubleArrTuple));
         expectedList.add(Storage.tuple(2, doubleArrTuple2));
         
         pigServer.setBatchOn();
         pigServer.registerQuery(String.format(
             "A = load 'hbase://table/%s' using " + PhoenixHBaseLoader.class.getName() + "('%s');", TABLE,
             zkQuorum));

         pigServer.registerQuery("STORE A INTO 'out' using mock.Storage();");
         pigServer.executeBatch();
         
         List<Tuple> actualList = data.get("out");
         assertEquals(expectedList.size(), actualList.size());
         assertEquals(expectedList, actualList);
     }
}
