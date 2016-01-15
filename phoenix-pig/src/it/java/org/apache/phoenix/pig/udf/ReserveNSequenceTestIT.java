/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.phoenix.pig.udf;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Properties;

import org.apache.phoenix.pig.BasePigIT;
import org.apache.phoenix.util.PhoenixRuntime;
import org.apache.pig.data.Tuple;
import org.apache.pig.impl.util.UDFContext;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

/**
 * Test class to run all the Pig Sequence UDF integration tests against a virtual map reduce cluster.
 */
public class ReserveNSequenceTestIT extends BasePigIT {

    private static final String CREATE_SEQUENCE_SYNTAX = "CREATE SEQUENCE %s START WITH %s INCREMENT BY %s MINVALUE %s MAXVALUE %s CACHE %s";
    private static final String SEQUENCE_NAME = "my_schema.my_sequence";
    private static final long MAX_VALUE = 10;

    private static UDFContext udfContext;

    @Rule
    public ExpectedException thrown = ExpectedException.none();

    @Override
    @Before
    public void setUp() throws Exception {
        super.setUp();
        createSequence(conn);
        createUdfContext();
    }

    @Override
    @After
    public void tearDown() throws Exception {
        udfContext.reset();
        dropSequence(conn);
        super.tearDown();
    }

    @Test
    public void testReserve() throws Exception {
        doTest(new UDFTestProperties(1));
    }

    @Test
    public void testReserveN() throws Exception {
        doTest(new UDFTestProperties(5));
    }

    @Test
    public void testReserveNwithPreviousAllocations() throws Exception {
        UDFTestProperties props = new UDFTestProperties(5);
        props.setCurrentValue(4);
        doTest(props);
    }

    @Test
    public void testReserveWithZero() throws Exception {
        UDFTestProperties props = new UDFTestProperties(0);
        props.setExceptionExpected(true);
        props.setExceptionClass(IllegalArgumentException.class);
        props.setErrorMessage(ReserveNSequence.INVALID_NUMBER_MESSAGE);
        doTest(props);
    }

    @Test
    public void testReserveWithNegativeNumber() throws Exception {
        UDFTestProperties props = new UDFTestProperties(-1);
        props.setExceptionExpected(true);
        props.setExceptionClass(IllegalArgumentException.class);
        props.setErrorMessage(ReserveNSequence.INVALID_NUMBER_MESSAGE);
        doTest(props);
    }

    @Test
    public void testReserveMaxLimit() throws Exception {
        UDFTestProperties props = new UDFTestProperties(MAX_VALUE);
        props.setExceptionExpected(true);
        props.setExceptionClass(IOException.class);
        props.setErrorMessage("Reached MAXVALUE of sequence");
        doTest(props);
    }

    @Test
    public void testNoSequenceName() throws Exception {
        UDFTestProperties props = new UDFTestProperties(1);
        props.setExceptionExpected(true);
        props.setSequenceName(null);
        props.setExceptionClass(NullPointerException.class);
        props.setErrorMessage(ReserveNSequence.EMPTY_SEQUENCE_NAME_MESSAGE);
        doTest(props);
    }

    @Test
    public void testSequenceNotExisting() throws Exception {
        UDFTestProperties props = new UDFTestProperties(1);
        props.setExceptionExpected(true);
        props.setSequenceName("foo.bar");
        props.setExceptionClass(IOException.class);
        props.setErrorMessage("Sequence undefined");
        doTest(props);
    }
    
    /**
     * Test reserving sequence with tenant Id passed to udf.
     * @throws Exception
     */
    @Test
    public void testTenantSequence() throws Exception {
        Properties tentantProps = new Properties();
        String tenantId = "TENANT";
        tentantProps.put(PhoenixRuntime.TENANT_ID_ATTRIB, tenantId);
        Connection tenantConn = DriverManager.getConnection(getUrl(), tentantProps);
        createSequence(tenantConn);

        try {
            UDFTestProperties props = new UDFTestProperties(3);

            // validates UDF reservation is for that tentant
            doTest(tenantConn, props);

            // validate global sequence value is still set to 1
            assertEquals(1L, getNextSequenceValue(conn));
        } finally {
            dropSequence(tenantConn);
        }
    }
    
    /**
     * Test Use the udf to reserve multiple tuples
     * 
     * @throws Exception
     */
    @Test
    public void testMultipleTuples() throws Exception {
        Tuple tuple = tupleFactory.newTuple(2);
        tuple.set(0, 2L);
        tuple.set(1, SEQUENCE_NAME);

        final String tentantId = conn.getClientInfo(PhoenixRuntime.TENANT_ID_ATTRIB);
        ReserveNSequence udf = new ReserveNSequence(zkQuorum, tentantId);

        for (int i = 0; i < 2; i++) {
            udf.exec(tuple);
        }
        long nextValue = getNextSequenceValue(conn);
        assertEquals(5L, nextValue);
    }
    
    private void doTest(UDFTestProperties props) throws Exception {
        doTest(conn, props);
    }

    private void doTest(Connection conn, UDFTestProperties props) throws Exception {
        setCurrentValue(conn, props.getCurrentValue());
        Tuple tuple = tupleFactory.newTuple(3);
        tuple.set(0, props.getNumToReserve());
        tuple.set(1, props.getSequenceName());
        tuple.set(2, zkQuorum);
        Long result = null;
        try {
            final String tenantId = conn.getClientInfo(PhoenixRuntime.TENANT_ID_ATTRIB);
            ReserveNSequence udf = new ReserveNSequence(zkQuorum, tenantId);
            result = udf.exec(tuple);
            validateReservedSequence(conn, props.getCurrentValue(), props.getNumToReserve(), result);
            // Calling this to cleanup for the udf. To close the connection
            udf.finish();
        } catch (Exception e) {
            if (props.isExceptionExpected()) {
                assertEquals(props.getExceptionClass(), e.getClass());
                e.getMessage().contains(props.getErrorMessage());
            } else {
                throw e;
            }
        }
    }

    private void createUdfContext() {
        udfContext = UDFContext.getUDFContext();
        udfContext.addJobConf(conf);
    }

    private void validateReservedSequence(Connection conn, Long currentValue, long count, Long result) throws SQLException {
        Long startIndex = currentValue + 1;
        assertEquals("Start index is incorrect", startIndex, result);
        final long newNextSequenceValue = getNextSequenceValue(conn);
        assertEquals(startIndex + count, newNextSequenceValue);
    }

    private void createSequence(Connection conn) throws SQLException {
        conn.createStatement().execute(String.format(CREATE_SEQUENCE_SYNTAX, SEQUENCE_NAME, 1, 1, 1, MAX_VALUE, 1));
        conn.commit();
    }

    private void setCurrentValue(Connection conn, long currentValue) throws SQLException {
        for (int i = 1; i <= currentValue; i++) {
            getNextSequenceValue(conn);
        }
    }

    private long getNextSequenceValue(Connection conn) throws SQLException {
        String ddl = new StringBuilder().append("SELECT NEXT VALUE FOR ").append(SEQUENCE_NAME).toString();
        ResultSet rs = conn.createStatement().executeQuery(ddl);
        assertTrue(rs.next());
        conn.commit();
        return rs.getLong(1);
    }

    private void dropSequence(Connection conn) throws Exception {
        String ddl = new StringBuilder().append("DROP SEQUENCE ").append(SEQUENCE_NAME).toString();
        conn.createStatement().execute(ddl);
        conn.commit();
    }

    /**
     * Static class to define properties for the test
     */
    private static class UDFTestProperties {
        private final Long numToReserve;
        private Long currentValue = 1L;
        private String sequenceName = SEQUENCE_NAME;
        private boolean exceptionExpected = false;
        private Class exceptionClass = null;
        private String errorMessage = null;

        public UDFTestProperties(long numToReserve) {
            this.numToReserve = numToReserve;
        }

        public Long getCurrentValue() {
            return currentValue;
        }

        public void setCurrentValue(long currentValue) {
            this.currentValue = currentValue;
        }

        public String getSequenceName() {
            return sequenceName;
        }

        public void setSequenceName(String sequenceName) {
            this.sequenceName = sequenceName;
        }

        public boolean isExceptionExpected() {
            return exceptionExpected;
        }

        public void setExceptionExpected(boolean shouldThrowException) {
            this.exceptionExpected = shouldThrowException;
        }

        public String getErrorMessage() {
            return errorMessage;
        }

        public void setErrorMessage(String errorMessage) {
            this.errorMessage = errorMessage;
        }

        public Long getNumToReserve() {
            return numToReserve;
        }

        public Class getExceptionClass() {
            return exceptionClass;
        }

        public void setExceptionClass(Class exceptionClass) {
            this.exceptionClass = exceptionClass;
        }

    }

}
