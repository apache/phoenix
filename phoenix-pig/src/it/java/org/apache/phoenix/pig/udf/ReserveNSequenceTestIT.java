/**
 * 
 */
package org.apache.phoenix.pig.udf;

import static org.apache.phoenix.util.PhoenixRuntime.JDBC_PROTOCOL_SEPARATOR;
import static org.apache.phoenix.util.TestUtil.LOCALHOST;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HConstants;
import org.apache.phoenix.end2end.BaseHBaseManagedTimeIT;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;
import org.apache.pig.impl.util.UDFContext;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

/**
 * Test class to run all the Pig Sequence UDF integration tests against a virtual map reduce cluster.
 */
public class ReserveNSequenceTestIT extends BaseHBaseManagedTimeIT {

    private static final String CREATE_SEQUENCE_SYNTAX = "CREATE SEQUENCE %s START WITH %s INCREMENT BY %s MINVALUE %s MAXVALUE %s CACHE %s";
    private static final String SEQUENCE_NAME = "my_schema.my_sequence";
    private static final long MAX_VALUE = 10;

    private static TupleFactory TF;
    private static Connection conn;
    private static String zkQuorum;
    private static Configuration conf;
    private static UDFContext udfContext;

    @Rule
    public ExpectedException thrown = ExpectedException.none();

    @BeforeClass
    public static void setUpBeforeClass() throws Exception {
        conf = getTestClusterConfig();
        zkQuorum = LOCALHOST + JDBC_PROTOCOL_SEPARATOR + getZKClientPort(getTestClusterConfig());
        conf.set(HConstants.ZOOKEEPER_QUORUM, zkQuorum);
        // Properties props = PropertiesUtil.deepCopy(TestUtil.TEST_PROPERTIES);
        conn = DriverManager.getConnection(getUrl());
        // Pig variables
        TF = TupleFactory.getInstance();
    }

    @Before
    public void setUp() throws SQLException {
        createSequence();
        createUdfContext();
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

    private void doTest(UDFTestProperties props) throws Exception {
        setCurrentValue(props.getCurrentValue());
        Tuple tuple = TF.newTuple(3);
        tuple.set(0, props.getNumToReserve());
        tuple.set(1, props.getSequenceName());
        tuple.set(2, zkQuorum);
        Long result = null;
        try {
            ReserveNSequence udf = new ReserveNSequence();
            result = udf.exec(tuple);
            validateReservedSequence(props.getCurrentValue(), props.getNumToReserve(), result);
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
        conf.set(ReserveNSequence.SEQUENCE_NAME_CONF_KEY, SEQUENCE_NAME);
        udfContext = UDFContext.getUDFContext();
        udfContext.addJobConf(conf);
    }

    private void validateReservedSequence(Long currentValue, long count, Long result) throws SQLException {
        Long startIndex = currentValue + 1;
        assertEquals("Start index is incorrect", startIndex, result);
        final long newNextSequenceValue = getNextSequenceValue();
        assertEquals(startIndex + count, newNextSequenceValue);
    }

    private void createSequence() throws SQLException {
        conn.createStatement().execute(String.format(CREATE_SEQUENCE_SYNTAX, SEQUENCE_NAME, 1, 1, 1, MAX_VALUE, 1));
        conn.commit();
    }

    private void setCurrentValue(long currentValue) throws SQLException {
        for (int i = 1; i <= currentValue; i++) {
            getNextSequenceValue();
        }
    }

    private long getNextSequenceValue() throws SQLException {
        String ddl = new StringBuilder().append("SELECT NEXT VALUE FOR ").append(SEQUENCE_NAME).toString();
        ResultSet rs = conn.createStatement().executeQuery(ddl);
        assertTrue(rs.next());
        conn.commit();
        return rs.getLong(1);
    }

    private void dropSequence() throws Exception {
        String ddl = new StringBuilder().append("DROP SEQUENCE ").append(SEQUENCE_NAME).toString();
        conn.createStatement().execute(ddl);
        conn.commit();
    }

    @After
    public void tearDown() throws Exception {
        udfContext.reset();
        dropSequence();
    }

    @AfterClass
    public static void tearDownAfterClass() throws Exception {
        conn.close();
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
