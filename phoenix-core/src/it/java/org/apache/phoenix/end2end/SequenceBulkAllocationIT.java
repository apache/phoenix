/*
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license
 * agreements. See the NOTICE file distributed with this work for additional information regarding
 * copyright ownership. The ASF licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at http://www.apache.org/licenses/LICENSE-2.0 Unless required by applicable
 * law or agreed to in writing, software distributed under the License is distributed on an "AS IS"
 * BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License
 * for the specific language governing permissions and limitations under the License.
 */

package org.apache.phoenix.end2end;

import static org.apache.phoenix.util.PhoenixRuntime.TENANT_ID_ATTRIB;
import static org.apache.phoenix.util.TestUtil.TEST_PROPERTIES;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import org.apache.phoenix.compile.ExplainPlan;
import org.apache.phoenix.compile.ExplainPlanAttributes;
import org.apache.phoenix.exception.SQLExceptionCode;
import org.apache.phoenix.jdbc.PhoenixPreparedStatement;
import org.apache.phoenix.util.PropertiesUtil;
import org.apache.phoenix.util.SchemaUtil;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

import org.apache.phoenix.thirdparty.com.google.common.collect.Lists;

/**
 * Suite of integration tests that validate that Bulk Allocation of Sequence values
 * using the NEXT <n> VALUES FOR <seq> syntax works as expected and interacts
 * correctly with NEXT VALUE FOR <seq> and CURRENT VALUE FOR <seq>.
 * 
 * All tests are run with both a generic connection and a multi-tenant connection.
 * 
 */
@Category(ParallelStatsDisabledTest.class)
@RunWith(Parameterized.class)
public class SequenceBulkAllocationIT extends ParallelStatsDisabledIT {

    private static final String SELECT_NEXT_VALUE_SQL =
            "SELECT NEXT VALUE FOR %s";
    private static final String SELECT_CURRENT_VALUE_SQL =
            "SELECT CURRENT VALUE FOR %s";
    private static final String CREATE_SEQUENCE_NO_MIN_MAX_TEMPLATE =
            "CREATE SEQUENCE %s START WITH %s INCREMENT BY %s CACHE %s";
    private static final String CREATE_SEQUENCE_WITH_MIN_MAX_TEMPLATE =
            "CREATE SEQUENCE %s START WITH %s INCREMENT BY %s MINVALUE %s MAXVALUE %s CACHE %s";
    private static final String CREATE_SEQUENCE_WITH_MIN_MAX_AND_CYCLE_TEMPLATE =
            "CREATE SEQUENCE %s START WITH %s INCREMENT BY %s MINVALUE %s MAXVALUE %s CYCLE CACHE %s";
    private static final String SCHEMA_NAME = "S";
    
    private Connection conn;
    private String tenantId;
    
    public SequenceBulkAllocationIT(String tenantId) {
        this.tenantId = tenantId;
    }
    
    @Parameters(name="SequenceBulkAllocationIT_tenantId={0}") // name is used by failsafe as file name in reports
    public static synchronized Object[] data() {
        return new Object[] {null, "tenant1"};
    }

    private static String generateTableNameWithSchema() {
        return SchemaUtil.getTableName(SCHEMA_NAME, generateUniqueName());
    }
    
    private static String generateSequenceNameWithSchema() {
        return SchemaUtil.getTableName(SCHEMA_NAME, generateUniqueSequenceName());
    }

    @Before
    public void init() throws Exception {
    	createConnection();
    }    
    
    @After
    public void tearDown() throws Exception {
        // close any open connection between tests, so that connections are not leaked
        if (conn != null) {
            conn.close();
        }
    }
    
    @Test
    public void testSequenceParseNextValuesWithNull() throws Exception {
    	String sequenceName = generateSequenceNameWithSchema();
        try {
            conn.createStatement().executeQuery(
                "SELECT NEXT NULL VALUES FOR  " + sequenceName);
            fail("null is not allowed to be used for <n> in NEXT <n> VALUES FOR <seq>");
        } catch (SQLException e) {
            assertEquals(SQLExceptionCode.NUM_SEQ_TO_ALLOCATE_MUST_BE_CONSTANT.getErrorCode(),
                e.getErrorCode());
            assertTrue(e.getNextException() == null);
        }
    }
    
    @Test
    public void testSequenceParseNextValuesWithNonNumber() throws Exception {
    	String sequenceName = generateSequenceNameWithSchema();    
        try {
            conn.createStatement().executeQuery(
                "SELECT NEXT '89b' VALUES FOR  " + sequenceName);
            fail("Only integers and longs are allowed to be used for <n> in NEXT <n> VALUES FOR <seq>");
        } catch (SQLException e) {
            assertEquals(SQLExceptionCode.NUM_SEQ_TO_ALLOCATE_MUST_BE_CONSTANT.getErrorCode(),
                e.getErrorCode());
            assertTrue(e.getNextException() == null);
        }
    }
    
    
    @Test
    public void testSequenceParseNextValuesWithNegativeNumber() throws Exception {
    	String sequenceName = generateSequenceNameWithSchema();
        try {
            conn.createStatement().executeQuery(
                "SELECT NEXT '-1' VALUES FOR  " + sequenceName);
            fail("null is not allowed to be used for <n> in NEXT <n> VALUES FOR <seq>");
        } catch (SQLException e) {
            assertEquals(SQLExceptionCode.NUM_SEQ_TO_ALLOCATE_MUST_BE_CONSTANT.getErrorCode(),
                e.getErrorCode());
            assertTrue(e.getNextException() == null);
        }
    }

    @Test
    public void testParseNextValuesSequenceWithZeroAllocated() throws Exception {
    	String sequenceName = generateSequenceNameWithSchema();    
        try {
            conn.createStatement().executeQuery(
                "SELECT NEXT 0 VALUES FOR  " + sequenceName);
            fail("Only integers and longs are allowed to be used for <n> in NEXT <n> VALUES FOR <seq>");
        } catch (SQLException e) {
            assertEquals(SQLExceptionCode.NUM_SEQ_TO_ALLOCATE_MUST_BE_CONSTANT.getErrorCode(),
                e.getErrorCode());
            assertTrue(e.getNextException() == null);
        }
    }
    

    @Test
    public void testNextValuesForSequenceWithNoAllocatedValues() throws Exception {
    	String sequenceName = generateSequenceNameWithSchema();
        final SequenceProperties props =
                new SequenceProperties.Builder().name(sequenceName).incrementBy(1).startsWith(1).cacheSize(1)
                        .numAllocated(100).build();

        createSequenceWithNoMinMax(props);

        // Bulk Allocate Sequence Slots
        final int currentValueAfterAllocation = 100;
        reserveSlotsInBulkAndAssertValue(sequenceName, 1, props.numAllocated);
        assertExpectedStateInSystemSequence(props, 101);
        assertExpectedNumberOfValuesAllocated(1, currentValueAfterAllocation, props.incrementBy, props.numAllocated);
        
        // Assert standard Sequence Operations return expected values
        assertExpectedCurrentValueForSequence(sequenceName, currentValueAfterAllocation);
        assertExpectedNextValueForSequence(sequenceName, 101);
    }
    
    @Test
    /**
     * Validates we can invoke NEXT <n> VALUES FOR using bind vars.
     */
    public void testNextValuesForSequenceUsingBinds() throws Exception {
        // Create Sequence
    	String sequenceName = generateSequenceNameWithSchema();
        final SequenceProperties props =
                new SequenceProperties.Builder().name(sequenceName).incrementBy(1).startsWith(1).cacheSize(1)
                        .numAllocated(100).build();

        createSequenceWithNoMinMax(props);
        
        // Allocate 100 slots using SQL with Bind Params and a PreparedStatement
        final int currentValueAfterAllocation = 100;
        reserveSlotsInBulkUsingBindsAndAssertValue(sequenceName, 1,props.numAllocated);
        assertExpectedStateInSystemSequence(props, 101);
        assertExpectedNumberOfValuesAllocated(1, currentValueAfterAllocation, props.incrementBy, props.numAllocated);

        // Assert standard Sequence Operations return expected values
        assertExpectedCurrentValueForSequence(sequenceName, currentValueAfterAllocation);
        assertExpectedNextValueForSequence(sequenceName, 101);
    }
    

    @Test
    public void testNextValuesForSequenceWithPreviouslyAllocatedValues() throws Exception {
    	String sequenceName = generateSequenceNameWithSchema();
        final SequenceProperties props =
                new SequenceProperties.Builder().name(sequenceName).incrementBy(1).startsWith(1).cacheSize(100)
                        .numAllocated(1000).build();

        createSequenceWithNoMinMax(props);
        assertExpectedNextValueForSequence(sequenceName, 1);
        assertExpectedCurrentValueForSequence(sequenceName, 1);
        assertExpectedNextValueForSequence(sequenceName, 2);

        // Bulk Allocate Sequence Slots
        int currentValueAfterAllocation = 1100;
        int nextValueAfterAllocation = currentValueAfterAllocation + props.incrementBy;
        int startValueAfterAllocation = 101;
        reserveSlotsInBulkAndAssertValue(sequenceName, startValueAfterAllocation, props.numAllocated);
        assertBulkAllocationSucceeded(props, currentValueAfterAllocation, startValueAfterAllocation);

        // Assert standard Sequence Operations return expected values
        assertExpectedCurrentValueForSequence(sequenceName, currentValueAfterAllocation);
        assertExpectedNextValueForSequence(sequenceName, nextValueAfterAllocation);
    }
   
    
    @Test
    /**
     * Validates that if we close a connection after performing 
     * NEXT <n> VALUES FOR <seq> the values are correctly returned from
     * the latest batch.
     */
    public void testConnectionCloseReturnsSequenceValuesCorrectly() throws Exception {
    	String sequenceName = generateSequenceNameWithSchema();
        final SequenceProperties props =
                new SequenceProperties.Builder().name(sequenceName).incrementBy(2).startsWith(1).cacheSize(100)
                        .numAllocated(100).build();
        createSequenceWithNoMinMax(props);
        
        assertExpectedNextValueForSequence(sequenceName, 1);
        assertExpectedCurrentValueForSequence(sequenceName, 1);
        assertExpectedNextValueForSequence(sequenceName, 3);
        
        // Bulk Allocate Sequence Slots
        int currentValueAfterAllocation = 399;
        int nextValueAfterAllocation = currentValueAfterAllocation + props.incrementBy;
        int startValueAfterAllocation = 201;
        reserveSlotsInBulkAndAssertValue(sequenceName, startValueAfterAllocation, props.numAllocated);
        assertExpectedCurrentValueForSequence(sequenceName, currentValueAfterAllocation);
        
        // Close the Connection
        conn.close();
        
        // Test that sequence, doesn't have gaps after closing the connection
        createConnection();
        assertExpectedNextValueForSequence(sequenceName, nextValueAfterAllocation);
        assertExpectedCurrentValueForSequence(sequenceName, nextValueAfterAllocation);

    }
    
    @Test
    /**
     * Validates that calling NEXT <n> VALUES FOR <seq> works correctly with UPSERT.
     */
    public void testNextValuesForSequenceWithUpsert() throws Exception {
    	String sequenceName = generateSequenceNameWithSchema();
    	String tableName = generateTableNameWithSchema();
    	
        // Create Sequence
        final SequenceProperties props =
                new SequenceProperties.Builder().name(sequenceName).incrementBy(1).startsWith(1).cacheSize(100)
                        .numAllocated(1000).build();
        createSequenceWithNoMinMax(props);

        // Create TABLE
        Connection genericConn = createGenericConnection();
        genericConn.createStatement().execute("CREATE TABLE " + tableName + " ( id INTEGER NOT NULL PRIMARY KEY)");
        genericConn.close();
        
        // Grab batch from Sequence
        assertExpectedNextValueForSequence(sequenceName, 1);
        assertExpectedCurrentValueForSequence(sequenceName, 1);
        assertExpectedNextValueForSequence(sequenceName, 2);
        assertExpectedStateInSystemSequence(props, 101);
        
        // Perform UPSERT and validate Sequence was incremented as expected
        conn.createStatement().execute("UPSERT INTO " + tableName + " (id) VALUES (NEXT " + props.numAllocated +  " VALUES FOR  " + sequenceName + " )");
        conn.commit();
        assertExpectedStateInSystemSequence(props, 1101);
        
        // SELECT values out and verify
        String query = "SELECT id, NEXT VALUE FOR  " + sequenceName + "  FROM " + tableName;
        ResultSet rs = conn.prepareStatement(query).executeQuery();
        assertTrue(rs.next());
        assertEquals(101, rs.getInt(1)); // Threw out cache of 100, incremented by 1000
        assertEquals(1101, rs.getInt(2));
        assertFalse(rs.next());
    }




    @Test
    public void testNextValuesForSequenceWithIncrementBy() throws Exception {
    	String sequenceName = generateSequenceNameWithSchema();
        final SequenceProperties props =
                new SequenceProperties.Builder().name(sequenceName).incrementBy(3).startsWith(1).cacheSize(100)
                        .numAllocated(1000).build();
        createSequenceWithNoMinMax(props);

        assertExpectedNextValueForSequence(sequenceName, 1);
        assertExpectedCurrentValueForSequence(sequenceName, 1);
        assertExpectedNextValueForSequence(sequenceName, 4);

        // Bulk Allocate Sequence Slots
        int currentValueAfterAllocation = 3298;
        int startValueAfterAllocation = 301;
        reserveSlotsInBulkAndAssertValue(sequenceName, startValueAfterAllocation, props.numAllocated);
        assertBulkAllocationSucceeded(props, currentValueAfterAllocation, startValueAfterAllocation);

        // Assert standard Sequence Operations return expected values
        assertExpectedCurrentValueForSequence(sequenceName, 3298);
        assertExpectedNextValueForSequence(sequenceName, 3301);
    }
    
    @Test
    public void testNextValuesForSequenceWithNegativeIncrementBy() throws Exception {
    	String sequenceName = generateSequenceNameWithSchema();
        final SequenceProperties props =
                new SequenceProperties.Builder().name(sequenceName).incrementBy(-1).startsWith(2000).cacheSize(100)
                        .numAllocated(1000).build();
        createSequenceWithNoMinMax(props);

        assertExpectedNextValueForSequence(sequenceName, 2000);
        assertExpectedCurrentValueForSequence(sequenceName, 2000);
        assertExpectedNextValueForSequence(sequenceName, 1999);

        // Bulk Allocate Sequence Slots
        int currentValueAfterAllocation = 901;
        int startValueAfterAllocation = 1900;
        reserveSlotsInBulkAndAssertValue(sequenceName, startValueAfterAllocation, props.numAllocated);
        assertBulkAllocationSucceeded(props, currentValueAfterAllocation, startValueAfterAllocation);


        // Assert standard Sequence Operations return expected values
        assertExpectedCurrentValueForSequence(sequenceName, 901);
        assertExpectedNextValueForSequence(sequenceName, 900);
    }

    @Test
    public void testNextValuesForSequenceWithNegativeIncrementByGreaterThanOne() throws Exception {
    	String sequenceName = generateSequenceNameWithSchema();
        final SequenceProperties props =
                new SequenceProperties.Builder().name(sequenceName).incrementBy(-5).startsWith(2000).cacheSize(100)
                        .numAllocated(100).build();
        createSequenceWithNoMinMax(props);
        
        // Pull first batch from Sequence
        assertExpectedNextValueForSequence(sequenceName, 2000);
        assertExpectedCurrentValueForSequence(sequenceName, 2000);
        assertExpectedNextValueForSequence(sequenceName, 1995);

        // Bulk Allocate Sequence Slots
        int currentValueAfterAllocation = 1005;
        int startValueAfterAllocation = 1500;
        reserveSlotsInBulkAndAssertValue(sequenceName, startValueAfterAllocation, props.numAllocated);
        assertBulkAllocationSucceeded(props, currentValueAfterAllocation, startValueAfterAllocation);
        

        // Assert standard Sequence Operations return expected values
        assertExpectedCurrentValueForSequence(sequenceName, 1005);
        assertExpectedNextValueForSequence(sequenceName, 1000);
    }
    
    
    @Test
    /**
     * Validates that for NEXT <n> VALUES FOR if you try an allocate more slots such that that
     * we exceed the max value of the sequence we throw an exception. Allocating sequence values in bulk
     * should be an all or nothing operation - if the operation succeeds clients are guaranteed that they
     * have access to all slots requested.
     */
    public void testNextValuesForSequenceExceedsMaxValue() throws Exception {
    	String sequenceName = generateSequenceNameWithSchema();
        final SequenceProperties sequenceProps =
                new SequenceProperties.Builder().name(sequenceName).incrementBy(1).startsWith(100).cacheSize(100)
                        .numAllocated(1000).minValue(100).maxValue(900).build();

        createSequenceWithMinMax(sequenceProps);

        // Pull first batch from the sequence
        assertExpectedNextValueForSequence(sequenceName, 100);
        assertExpectedCurrentValueForSequence(sequenceName, 100);
        assertExpectedNextValueForSequence(sequenceName, 101);

        // Attempt to bulk Allocate more slots than available
        try {
            conn.createStatement().executeQuery(
                        "SELECT NEXT " + sequenceProps.numAllocated
                                + " VALUES FOR  " + sequenceName + "  LIMIT 1");
            fail("Invoking SELECT NEXT VALUES should have thrown Reached Max Value Exception");
        } catch (SQLException e) {
            assertEquals(SQLExceptionCode.SEQUENCE_VAL_REACHED_MAX_VALUE.getErrorCode(),
                e.getErrorCode());
            assertTrue(e.getNextException() == null);
        }

        // Assert sequence didn't advance
        assertExpectedCurrentValueForSequence(sequenceName, 101);
        assertExpectedNextValueForSequence(sequenceName, 102);
    }

    @Test
    /**
     * Validates that for NEXT <n> VALUES FOR if you try an allocate more slots such that that
     * we exceed the min value of the sequence we throw an exception. Allocating sequence values in bulk
     * should be an all or nothing operation - if the operation succeeds clients are guaranteed that they
     * have access to all slots requested.
     */
    public void testNextValuesForSequenceExceedsMinValue() throws Exception {
    	String sequenceName = generateSequenceNameWithSchema();
        final SequenceProperties sequenceProps =
                new SequenceProperties.Builder().name(sequenceName).incrementBy(-5).startsWith(900).cacheSize(100)
                        .numAllocated(160).minValue(100).maxValue(900).build();

        createSequenceWithMinMax(sequenceProps);

        // Pull first batch from the sequence
        assertExpectedNextValueForSequence(sequenceName, 900);
        assertExpectedCurrentValueForSequence(sequenceName, 900);
        assertExpectedNextValueForSequence(sequenceName, 895);

        // Attempt to bulk Allocate more slots than available
        try {
            conn.createStatement().executeQuery(
                        "SELECT NEXT " + sequenceProps.numAllocated
                                + " VALUES FOR  " + sequenceName + "  LIMIT 1");
            fail("Invoking SELECT NEXT VALUES should have thrown Reached Max Value Exception");
        } catch (SQLException e) {
            assertEquals(SQLExceptionCode.SEQUENCE_VAL_REACHED_MIN_VALUE.getErrorCode(),
                e.getErrorCode());
            assertTrue(e.getNextException() == null);
        }

        // Assert sequence didn't advance (we still throw out the cached values)
        assertExpectedCurrentValueForSequence(sequenceName, 895);
        assertExpectedNextValueForSequence(sequenceName, 890);
    }

    
    @Test
    /**
     * Validates that if we don't exceed the limit bulk allocation works with sequences with a 
     * min and max defined.
     */
    public void testNextValuesForSequenceWithMinMaxDefined() throws Exception {
    	String sequenceName = generateSequenceNameWithSchema();
        final SequenceProperties props =
                new SequenceProperties.Builder().name(sequenceName).incrementBy(5).startsWith(100).cacheSize(100)
                        .numAllocated(1000).minValue(100).maxValue(6000).build();

        createSequenceWithMinMax(props);
        assertExpectedNextValueForSequence(sequenceName, 100);
        assertExpectedCurrentValueForSequence(sequenceName, 100);
        assertExpectedNextValueForSequence(sequenceName, 105);

        // Bulk Allocate Sequence Slots
        int currentValueAfterAllocation = 5595;
        int startValueAfterAllocation = 600;
        reserveSlotsInBulkAndAssertValue(sequenceName, startValueAfterAllocation, props.numAllocated);
        assertBulkAllocationSucceeded(props, currentValueAfterAllocation, startValueAfterAllocation);

        // Assert standard Sequence Operations return expected values
        assertExpectedCurrentValueForSequence(sequenceName, 5595);
        assertExpectedNextValueForSequence(sequenceName, 5600);
    }
    
    @Test
    public void testNextValuesForSequenceWithDefaultMax() throws Exception {
    	String sequenceName = generateSequenceNameWithSchema();
        final SequenceProperties props =
                new SequenceProperties.Builder().name(sequenceName).incrementBy(1).startsWith(100).cacheSize(100)
                        .numAllocated(Long.MAX_VALUE - 100).build();
        
        // Create Sequence
        createSequenceWithMinMax(props);

        // Bulk Allocate Sequence Slots
        long currentValueAfterAllocation = 100;
        long startValueAfterAllocation = Long.MAX_VALUE;
        reserveSlotsInBulkAndAssertValue(sequenceName, currentValueAfterAllocation, props.numAllocated);
        assertExpectedStateInSystemSequence(props, startValueAfterAllocation);
        
        // Try and get next value
        try {
            conn.createStatement().executeQuery(String.format(SELECT_NEXT_VALUE_SQL, sequenceName));
        } catch (SQLException e) {
            assertEquals(SQLExceptionCode.SEQUENCE_VAL_REACHED_MAX_VALUE.getErrorCode(),
                e.getErrorCode());
            assertTrue(e.getNextException() == null);
        }
    }
    
    @Test
    /**
     * Validates that if our current or start value is > 0 and we ask for Long.MAX
     * and overflow to the next value, the correct Exception is thrown when 
     * the expression is evaluated.
     */
    public void testNextValuesForSequenceOverflowAllocation() throws Exception {
    	String sequenceName = generateSequenceNameWithSchema();
        final SequenceProperties props =
                new SequenceProperties.Builder().name(sequenceName).incrementBy(1).startsWith(100).cacheSize(100)
                        .numAllocated(Long.MAX_VALUE).build();
        
        // Create Sequence
        createSequenceWithMinMax(props);

        // Bulk Allocate Sequence Slots
        try {
            conn.createStatement().executeQuery(
                "SELECT NEXT " + Long.MAX_VALUE
                        + " VALUES FOR  " + sequenceName + " ");
        } catch (SQLException e) {
            assertEquals(SQLExceptionCode.SEQUENCE_VAL_REACHED_MAX_VALUE.getErrorCode(),
                e.getErrorCode());
            assertTrue(e.getNextException() == null);
        }
    }
    
    
    @Test
    /**
     * Validates that specifying an bulk allocation less than the size of the cache defined on the sequence works
     * as expected.
     */
    public void testNextValuesForSequenceAllocationLessThanCacheSize() throws Exception {
    	String sequenceName = generateSequenceNameWithSchema();
        final SequenceProperties props =
                new SequenceProperties.Builder().name(sequenceName).incrementBy(5).startsWith(100).cacheSize(100)
                        .numAllocated(50).minValue(100).maxValue(6000).build();

        createSequenceWithMinMax(props);

        assertExpectedNextValueForSequence(sequenceName, 100);
        assertExpectedCurrentValueForSequence(sequenceName, 100);
        assertExpectedNextValueForSequence(sequenceName, 105);

        // Bulk Allocate Sequence Slots
        int currentValueAfterAllocation = 355;
        int startValueAfterAllocation = 110;
        reserveSlotsInBulkAndAssertValue(sequenceName, startValueAfterAllocation, props.numAllocated);
        assertExpectedStateInSystemSequence(props, 600);
        assertExpectedNumberOfValuesAllocated(startValueAfterAllocation, currentValueAfterAllocation, props.incrementBy, props.numAllocated);

        // Assert standard Sequence Operations return expected values
        // 105 + (50 * 5) = 355 
        assertExpectedCurrentValueForSequence(sequenceName, 355);
        assertExpectedNextValueForSequence(sequenceName, 360);
        assertExpectedNextValueForSequence(sequenceName, 365);
        assertExpectedNextValueForSequence(sequenceName, 370);
    }
    
    @Test
    /**
     * Validates that specifying an bulk allocation less than the size of the cache defined on the sequence works
     * as expected if we don't have enough values in the cache to support the allocation.
     */
    public void testNextValuesForInsufficentCacheValuesAllocationLessThanCacheSize() throws Exception {
    	String sequenceName = generateSequenceNameWithSchema();
        final SequenceProperties props =
                new SequenceProperties.Builder().name(sequenceName).incrementBy(5).startsWith(100).cacheSize(100)
                        .numAllocated(50).minValue(100).maxValue(6000).build();

        createSequenceWithMinMax(props);
        
        // Allocate 51 slots, only 49 will be left
        int currentValueAfter51Allocations  = 355; // 100 + 51 * 5
        for (int i = 100; i <= currentValueAfter51Allocations; i = i + 5) {
            assertExpectedNextValueForSequence(sequenceName, i);
        }
        assertExpectedCurrentValueForSequence(sequenceName, currentValueAfter51Allocations);

        // Bulk Allocate 50 Sequence Slots which greater than amount left in cache
        // This should throw away rest of the cache, and allocate the request slot
        // from the next start value
        int currentValueAfterAllocation = 845;
        int startValueAfterAllocation = 600;
        reserveSlotsInBulkAndAssertValue(sequenceName, startValueAfterAllocation, props.numAllocated);
        assertBulkAllocationSucceeded(props, currentValueAfterAllocation, startValueAfterAllocation);

        // Assert standard Sequence Operations return expected values
        assertExpectedCurrentValueForSequence(sequenceName, 845);
        assertExpectedNextValueForSequence(sequenceName, 850);
        assertExpectedNextValueForSequence(sequenceName, 855);
        assertExpectedNextValueForSequence(sequenceName, 860);
    }
    
    @Test
    /**
     * Validates that for NEXT <n> VALUES FOR is not supported on Sequences that have the
     * CYCLE flag set to true.
     */
    public void testNextValuesForSequenceWithCycles() throws Exception {
    	String sequenceName = generateSequenceNameWithSchema();
        final SequenceProperties sequenceProps =
                new SequenceProperties.Builder().name(sequenceName).incrementBy(5).startsWith(100).cacheSize(100)
                        .numAllocated(1000).minValue(100).maxValue(900).build();

        createSequenceWithMinMaxAndCycle(sequenceProps);

        // Full first batch from the sequence
        assertExpectedNextValueForSequence(sequenceName, 100);
        assertExpectedCurrentValueForSequence(sequenceName, 100);
        assertExpectedNextValueForSequence(sequenceName, 105);

        // Attempt to bulk Allocate more slots than available
        try {
             conn.createStatement().executeQuery(
                        "SELECT NEXT " + sequenceProps.numAllocated
                                + " VALUES FOR  " + sequenceName + "  LIMIT 1");
            fail("Invoking SELECT NEXT VALUES should have failed as operation is not supported for sequences with Cycles.");
        } catch (SQLException e) {
            assertEquals(SQLExceptionCode.NUM_SEQ_TO_ALLOCATE_NOT_SUPPORTED.getErrorCode(),
                e.getErrorCode());
            assertTrue(e.getNextException() == null);
        }

        // Assert sequence didn't advance
        assertExpectedCurrentValueForSequence(sequenceName, 105);
        assertExpectedNextValueForSequence(sequenceName, 110);
        assertExpectedNextValueForSequence(sequenceName, 115);
    }

    @Test
    /**
     * Validates that if we have multiple NEXT <n> VALUES FOR <seq> expression and the 
     * CURRENT VALUE FOR expression work correctly when used in the same statement.
     */
    public void testCurrentValueForAndNextValuesForExpressionsForSameSequence() throws Exception {
    	String sequenceName = generateSequenceNameWithSchema();
        final SequenceProperties props =
                new SequenceProperties.Builder().name(sequenceName).incrementBy(1).startsWith(1).cacheSize(100)
                        .numAllocated(1000).build();

        createSequenceWithNoMinMax(props);

        assertExpectedNextValueForSequence(sequenceName, 1);
        assertExpectedCurrentValueForSequence(sequenceName, 1);
        assertExpectedNextValueForSequence(sequenceName, 2);

        // Bulk Allocate Sequence Slots
        int currentValueAfterAllocation = 1100;
        int nextValueAfterAllocation = currentValueAfterAllocation + props.incrementBy;
        int startValueAfterAllocation = 101;
        ResultSet rs =
                conn.createStatement().executeQuery(
                    "SELECT CURRENT VALUE FOR  " + sequenceName + " , NEXT " + props.numAllocated + " VALUES FOR  " + sequenceName + " ");
        assertTrue(rs.next());
        assertBulkAllocationSucceeded(props, currentValueAfterAllocation, startValueAfterAllocation);
        int currentValueFor = rs.getInt(1);
        int nextValuesFor = rs.getInt(2);
        assertEquals("Expected the next value to be first value reserved", startValueAfterAllocation, nextValuesFor);
        assertEquals("Expected current value to be the same as next value", startValueAfterAllocation, currentValueFor);
        
        // Assert standard Sequence Operations return expected values
        assertExpectedCurrentValueForSequence(sequenceName, currentValueAfterAllocation);
        assertExpectedNextValueForSequence(sequenceName, nextValueAfterAllocation);
    }
    
    @Test
    /**
     * Validates that if we have multiple NEXT <n> VALUES FOR <seq> expressions for the *same* sequence
     * in a statement we only process the one which has the highest value of <n> and return the start
     * value for that for all expressions.
     */
    public void testMultipleNextValuesForExpressionsForSameSequence() throws Exception {
    	String sequenceName = generateSequenceNameWithSchema();
        final SequenceProperties props =
                new SequenceProperties.Builder().name(sequenceName).incrementBy(1).startsWith(1).cacheSize(100)
                        .numAllocated(1000).build();

        createSequenceWithNoMinMax(props);

        assertExpectedNextValueForSequence(sequenceName, 1);
        assertExpectedCurrentValueForSequence(sequenceName, 1);
        assertExpectedNextValueForSequence(sequenceName, 2);

        // Bulk Allocate Sequence Slots - One for 5 and one for 1000, 1000 should have precedence
        int currentValueAfterAllocation = 1100;
        int nextValueAfterAllocation = currentValueAfterAllocation + props.incrementBy;
        int startValueAfterAllocation = 101;
        ResultSet rs =
                conn.createStatement().executeQuery(
                    "SELECT NEXT 5 VALUES FOR  " + sequenceName + " , NEXT " + props.numAllocated + " VALUES FOR  " + sequenceName + " FROM \"SYSTEM\".\"SEQUENCE\"");
        assertTrue(rs.next());
        int firstValue = rs.getInt(1);
        int secondValue = rs.getInt(2);
        assertEquals("Expected both expressions to return the same value", firstValue, secondValue);
        assertEquals("Expected the value returned to be the highest allocation", startValueAfterAllocation, firstValue);
        assertBulkAllocationSucceeded(props, currentValueAfterAllocation, startValueAfterAllocation);
        
        // Assert standard Sequence Operations return expected values
        assertExpectedCurrentValueForSequence(sequenceName, currentValueAfterAllocation);
        assertExpectedNextValueForSequence(sequenceName, nextValueAfterAllocation);
    }
    
    @Test
    /**
     * Validates that if we have NEXT VALUE FOR <seq> and NEXT <n> VALUES FOR <seq> expressions for the *same* sequence
     * in a statement we only process way and honor the value of the highest value of <n>, where for 
     * NEXT VALUE FOR <seq> is assumed to be 1.
     */
    public void testMultipleDifferentExpressionsForSameSequence() throws Exception {
    	String sequenceName = generateSequenceNameWithSchema();
        final SequenceProperties props =
                new SequenceProperties.Builder().name(sequenceName).incrementBy(1).startsWith(1).cacheSize(100)
                        .numAllocated(1000).build();

        createSequenceWithNoMinMax(props);
        
        // Pull First Batch from Sequence
        assertExpectedNextValueForSequence(sequenceName, 1);

        // Bulk Allocate Sequence Slots and Get Next Value in Same Statement
        int currentValueAfterAllocation = 1100;
        int nextValueAfterAllocation = currentValueAfterAllocation + props.incrementBy;
        int startValueAfterAllocation = 101;
        ResultSet rs =
                conn.createStatement().executeQuery(
                    "SELECT NEXT VALUE FOR  " + sequenceName + " , "
                    + "NEXT " + props.numAllocated + " VALUES FOR  " + sequenceName + " , "
                    + "CURRENT VALUE FOR  " + sequenceName + " , "
                    + "NEXT 999 VALUES FOR  " + sequenceName + "  "
                    + "FROM \"SYSTEM\".\"SEQUENCE\"");
        assertTrue(rs.next());
        assertBulkAllocationSucceeded(props, currentValueAfterAllocation, startValueAfterAllocation);
        
        // Assert all values returned are the same
        // Expect them to be the highest value from NEXT VALUE or NEXT <n> VALUES FOR
        int previousVal = 0;
        for (int i = 1; i <= 4; i++) {
            int currentVal = rs.getInt(i);
            if (i != 1) {
                assertEquals(
                    "Expected all NEXT VALUE FOR and NEXT <n> VALUES FOR expressions to return the same value",
                    previousVal, currentVal);                
            }
            previousVal = currentVal;
        }
        
        // Assert standard Sequence Operations return expected values
        assertExpectedCurrentValueForSequence(sequenceName, currentValueAfterAllocation);
        assertExpectedNextValueForSequence(sequenceName, nextValueAfterAllocation);
    }
    
    
    @Test
    /**
     * Validates that using NEXT <n> VALUES FOR on different sequences in the 
     * same statement with *different* values of <n> works as expected. This
     * test validates that we keep our numAllocated array and sequence keys in 
     * sync during the sequence management process.
     */
    public void testMultipleNextValuesForExpressionsForDifferentSequences() throws Exception {
    	String sequenceName = generateSequenceNameWithSchema();
    	String secondSequenceName = generateSequenceNameWithSchema();

        conn.createStatement().execute("CREATE SEQUENCE  " + sequenceName + "  START WITH 30 INCREMENT BY 3 CACHE 100");
        conn.createStatement().execute("CREATE SEQUENCE " + secondSequenceName + " START WITH 100 INCREMENT BY 5 CACHE 50");

        // Bulk Allocate Sequence Slots for Two Sequences
        ResultSet rs =
                conn.createStatement().executeQuery(
                    "SELECT NEXT 100 VALUES FOR  " + sequenceName + " , NEXT 1000 VALUES FOR " + secondSequenceName + "");
        assertTrue(rs.next());
        assertEquals(30, rs.getInt(1));
        assertEquals(100, rs.getInt(2));
        
        // Assert standard Sequence Operations return expected values
        for (int i = 330; i < 330 + (2 * 100); i += 3) {
            assertExpectedCurrentValueForSequence(sequenceName, i - 3);
            assertExpectedNextValueForSequence(sequenceName, i);            
        }
        
        for (int i = 5100; i < 5100 + (2 * 1000); i += 5) {
            assertExpectedCurrentValueForSequence(secondSequenceName, i - 5);
            assertExpectedNextValueForSequence(secondSequenceName, i);            
        }
    }
    
    @Test
    /**
     * Validates that calling NEXT <n> VALUES FOR with EXPLAIN PLAN doesn't use 
     * allocate any slots. 
     */
    public void testExplainPlanValidatesSequences() throws Exception {
    	String sequenceName = generateSequenceNameWithSchema();
    	String tableName = generateTableNameWithSchema();
        final SequenceProperties props =
                new SequenceProperties.Builder().name(sequenceName).incrementBy(3).startsWith(30).cacheSize(100)
                        .numAllocated(1000).build();

        createSequenceWithNoMinMax(props);
        Connection genericConn = createGenericConnection();
        genericConn.createStatement().execute("CREATE TABLE " + tableName + " (k BIGINT NOT NULL PRIMARY KEY)");
        genericConn.close();
        
        // Bulk Allocate Sequence Slots
        int startValueAfterAllocation = 30;
        reserveSlotsInBulkAndAssertValue(sequenceName, startValueAfterAllocation, props.numAllocated);
        
        // Execute EXPLAIN PLAN multiple times, which should not change Sequence values
        for (int i = 0; i < 3; i++) {
            conn.createStatement().executeQuery("EXPLAIN SELECT NEXT 1000 VALUES FOR  " + sequenceName + "  FROM " + tableName);
        }
        
        // Validate the current value was not advanced and was the starting value
        assertExpectedStateInSystemSequence(props, 3030);
        
        // Assert standard Sequence Operations return expected values
        int startValue = 3030;
        for (int i = startValue; i < startValue + (2 * props.cacheSize); i += props.incrementBy) {
            assertExpectedCurrentValueForSequence(sequenceName, i - props.incrementBy);
            assertExpectedNextValueForSequence(sequenceName, i);            
        }
    }
    
    @Test
    public void testExplainPlanForNextValuesFor() throws Exception {
    	String sequenceName = generateSequenceNameWithSchema();
    	String tableName = generateTableNameWithSchema();
        final SequenceProperties props =
                new SequenceProperties.Builder().name(sequenceName).incrementBy(3).startsWith(30).cacheSize(100)
                        .numAllocated(1000).build();

        createSequenceWithNoMinMax(props);
        Connection genericConn = createGenericConnection();
        genericConn.createStatement().execute("CREATE TABLE "+ tableName + " (k BIGINT NOT NULL PRIMARY KEY)");
        genericConn.close();
        
        // Execute EXPLAIN PLAN which should not change Sequence values
        String query = "SELECT NEXT 1000 VALUES FOR  " + sequenceName
            + "  FROM " + tableName;

        // Assert output for Explain Plain result is as expected
        ExplainPlan plan = conn.prepareStatement(query)
            .unwrap(PhoenixPreparedStatement.class).optimizeQuery()
            .getExplainPlan();
        ExplainPlanAttributes explainPlanAttributes =
            plan.getPlanStepsAsAttributes();
        assertEquals("PARALLEL 1-WAY",
            explainPlanAttributes.getIteratorTypeAndScanSize());
        assertEquals("FULL SCAN ",
            explainPlanAttributes.getExplainScanType());
        assertEquals(tableName, explainPlanAttributes.getTableName());
        assertEquals("SERVER FILTER BY FIRST KEY ONLY",
            explainPlanAttributes.getServerWhereFilter());
        assertEquals(1,
            explainPlanAttributes.getClientSequenceCount().intValue());
    }
    
    
    /**
     * Performs a multithreaded test whereby we interleave reads from the result set of
     * NEXT VALUE FOR and NEXT <n> VALUES FOR to make sure we get expected values with the
     * following order of execution:
     * 
     * 1) Execute expression NEXT <n> VALUES FOR <seq>
     * 2) Execute expression NEXT VALUE FOR <seq>
     * 3) Read back value from expression NEXT VALUE FOR <seq> via rs.next()
     * 4) Read back value from expression NEXT <n> VALUES FOR <seq> via rs.next()
     */
    public void testNextValuesForMixedWithNextValueForMultiThreaded() throws Exception {
    	final String sequenceName = generateSequenceNameWithSchema();
        final SequenceProperties props =
                new SequenceProperties.Builder().name(sequenceName).incrementBy(1).startsWith(1).cacheSize(100)
                        .numAllocated(1000).build();

        createSequenceWithNoMinMax(props);
        assertExpectedNextValueForSequence(sequenceName, 1);
        assertExpectedCurrentValueForSequence(sequenceName, 1);
        assertExpectedNextValueForSequence(sequenceName, 2);

        // Bulk Allocate Sequence Slots
        final long startValueAfterAllocation1 = 101;
        final long startValueAfterAllocation2 = 1101;
        final long numSlotToAllocate = props.numAllocated;
        
        // Setup and run tasks in independent Threads
        ExecutorService executorService = Executors.newCachedThreadPool();
        try {
            final CountDownLatch latch1 = new CountDownLatch(1);
            final CountDownLatch latch2 = new CountDownLatch(1);
            
            Callable<Long> task1 = new Callable<Long>() {
    
                @Override
                public Long call() throws Exception {
                    ResultSet rs =
                            conn.createStatement().executeQuery(
                                "SELECT NEXT " + numSlotToAllocate + " VALUES FOR  " + sequenceName + " ");
                    latch1.countDown(); // Allows NEXT VALUE FOR thread to proceed
                    latch2.await(); // Waits until NEXT VALUE FOR thread reads and increments currentValue
                    rs.next();
                    return rs.getLong(1);    
                }
                
            };
    
            Callable<Long> task2 = new Callable<Long>() {
    
                @Override
                public Long call() throws Exception {
                    latch1.await(); // Wait for execution of NEXT <n> VALUES FOR expression
                    ResultSet rs =
                            conn.createStatement().executeQuery(
                                "SELECT NEXT VALUE FOR  " + sequenceName);
                    rs.next();
                    long retVal = rs.getLong(1);
                    latch2.countDown(); // Allow NEXT <n> VALUES for thread to completed
                    return retVal;
                }
                
            };
            
            @SuppressWarnings("unchecked")
            List<Future<Long>> futures = executorService.invokeAll(Lists.newArrayList(task1, task2), 20, TimeUnit.SECONDS);
            assertEquals(startValueAfterAllocation1, futures.get(0).get(10, TimeUnit.SECONDS).longValue());
            assertEquals(startValueAfterAllocation2, futures.get(1).get(10, TimeUnit.SECONDS).longValue());
        } finally {
            executorService.shutdown();
        }
    }

    @Test
    public void testMultipleNextValuesWithDiffAllocsForMultiThreaded() throws Exception {
    	final String sequenceName = generateSequenceNameWithSchema();
        final SequenceProperties props =
                new SequenceProperties.Builder().name(sequenceName).incrementBy(1).startsWith(1).cacheSize(100)
                        .numAllocated(1000).build();

        createSequenceWithNoMinMax(props);
        assertExpectedNextValueForSequence(sequenceName, 1);
        assertExpectedCurrentValueForSequence(sequenceName, 1);
        assertExpectedNextValueForSequence(sequenceName, 2);

        // Bulk Allocate Sequence Slots
        final long startValueAfterAllocation1 = 101;
        final long startValueAfterAllocation2 = 1101;
        final long numSlotToAllocate1 = 1000;
        final long numSlotToAllocate2 = 100;
        
        // Setup and run tasks in independent Threads
        ExecutorService executorService = Executors.newCachedThreadPool();
        try {
            final CountDownLatch latch1 = new CountDownLatch(1);
            final CountDownLatch latch2 = new CountDownLatch(1);
            
            Callable<Long> task1 = new Callable<Long>() {
    
                @Override
                public Long call() throws Exception {
                    ResultSet rs =
                            conn.createStatement().executeQuery(
                                "SELECT NEXT " + numSlotToAllocate1 + " VALUES FOR  " + sequenceName + " ");
                    rs.next();
                    latch1.countDown(); // Allows other thread to proceed
                    latch2.await(); 
                    return rs.getLong(1);    
                }
                
            };
    
            Callable<Long> task2 = new Callable<Long>() {
    
                @Override
                public Long call() throws Exception {
                    latch1.await(); // Wait for other thread to execut of NEXT <n> VALUES FOR expression
                    ResultSet rs =
                            conn.createStatement().executeQuery(
                                "SELECT NEXT " + numSlotToAllocate2 + " VALUES FOR  " + sequenceName + " ");
                    rs.next();
                    long retVal = rs.getLong(1);
                    latch2.countDown(); // Allow thread to completed
                    return retVal;
                }
                
            };
            
            @SuppressWarnings("unchecked")
            List<Future<Long>> futures = executorService.invokeAll(Lists.newArrayList(task1, task2), 5, TimeUnit.SECONDS);
            
            // Retrieve value from Thread running NEXT <n> VALUES FOR
            Long retValue1 = futures.get(0).get(5, TimeUnit.SECONDS);
            assertEquals(startValueAfterAllocation1, retValue1.longValue());
    
            // Retrieve value from Thread running NEXT VALUE FOR
            Long retValue2 = futures.get(1).get(5, TimeUnit.SECONDS);
            assertEquals(startValueAfterAllocation2, retValue2.longValue());

        } finally {
            executorService.shutdown();
        }
    }
    
    @Test
    public void testMultipleNextValuesWithSameAllocsForMultiThreaded() throws Exception {
    	final String sequenceName = generateSequenceNameWithSchema();
        final SequenceProperties props =
                new SequenceProperties.Builder().name(sequenceName).incrementBy(1).startsWith(1).cacheSize(100)
                        .numAllocated(1000).build();

        createSequenceWithNoMinMax(props);

        // Bulk Allocate Sequence Slots
        final long startValueAfterAllocation1 = 1;
        final long startValueAfterAllocation2 = 1001;
        final long numSlotToAllocate1 = 1000;
        final long numSlotToAllocate2 = 1000;
        
        // Setup and run tasks in independent Threads
        ExecutorService executorService = Executors.newCachedThreadPool();
        try {
            final CountDownLatch latch1 = new CountDownLatch(1);
            final CountDownLatch latch2 = new CountDownLatch(1);
            
            Callable<Long> task1 = new Callable<Long>() {
    
                @Override
                public Long call() throws Exception {
                    ResultSet rs =
                            conn.createStatement().executeQuery(
                                "SELECT NEXT " + numSlotToAllocate1 + " VALUES FOR  " + sequenceName + " ");
                    latch1.countDown(); // Allows other thread to proceed
                    latch2.await(); 
                    rs.next();
                    return rs.getLong(1);    
                }
                
            };
    
            Callable<Long> task2 = new Callable<Long>() {
    
                @Override
                public Long call() throws Exception {
                    latch1.await(); // Wait for other thread to execut of NEXT <n> VALUES FOR expression
                    ResultSet rs =
                            conn.createStatement().executeQuery(
                                "SELECT NEXT " + numSlotToAllocate2 + " VALUES FOR  " + sequenceName + " ");
                    rs.next();
                    long retVal = rs.getLong(1);
                    latch2.countDown(); // Allow thread to completed
                    return retVal;
                }
                
            };
            
            // Because of the way the threads are interleaved the ranges used by each thread will the reserve
            // of the order to statement execution
            @SuppressWarnings("unchecked")
            List<Future<Long>> futures = executorService.invokeAll(Lists.newArrayList(task1, task2), 5, TimeUnit.SECONDS);
            assertEquals(startValueAfterAllocation2, futures.get(0).get(5, TimeUnit.SECONDS).longValue());
            assertEquals(startValueAfterAllocation1, futures.get(1).get(5, TimeUnit.SECONDS).longValue());

        } finally {
            executorService.shutdown();
        }
    }

    // -----------------------------------------------------------------
    // Private Helper Methods
    // -----------------------------------------------------------------
    private void assertBulkAllocationSucceeded(SequenceProperties props,
            int currentValueAfterAllocation, int startValueAfterAllocation) throws SQLException {
        int nextValueAfterAllocation = currentValueAfterAllocation + props.incrementBy;
        assertExpectedStateInSystemSequence(props, nextValueAfterAllocation);
        assertExpectedNumberOfValuesAllocated(startValueAfterAllocation, currentValueAfterAllocation, props.incrementBy, props.numAllocated);
    }
    
    
    private void createSequenceWithNoMinMax(final SequenceProperties props) throws SQLException {
        conn.createStatement().execute(
            String.format(CREATE_SEQUENCE_NO_MIN_MAX_TEMPLATE, props.name, props.startsWith,
                props.incrementBy, props.cacheSize));
    }

    private void createSequenceWithMinMax(final SequenceProperties props) throws SQLException {
        conn.createStatement().execute(
            String.format(CREATE_SEQUENCE_WITH_MIN_MAX_TEMPLATE, props.name, props.startsWith,
                props.incrementBy, props.minValue, props.maxValue, props.cacheSize));
    }

    private void createSequenceWithMinMaxAndCycle(final SequenceProperties props) throws SQLException {
        conn.createStatement().execute(
            String.format(CREATE_SEQUENCE_WITH_MIN_MAX_AND_CYCLE_TEMPLATE, props.name, props.startsWith,
                props.incrementBy, props.minValue, props.maxValue, props.cacheSize));
    }
    
    private void reserveSlotsInBulkAndAssertValue(String sequenceName, long expectedValue, long numSlotToAllocate)
            throws SQLException {
        ResultSet rs =
                conn.createStatement().executeQuery(
                    "SELECT NEXT " + numSlotToAllocate + " VALUES FOR  " + sequenceName + " ");
        assertTrue(rs.next());
        assertEquals(expectedValue, rs.getInt(1));
    }

    private void reserveSlotsInBulkUsingBindsAndAssertValue(String sequenceName, int expectedValue, long numSlotToAllocate)
            throws SQLException {
    	
        PreparedStatement ps = conn.prepareStatement("SELECT NEXT ? VALUES FOR  " + sequenceName + " ");
        ps.getMetaData(); // check for PHOENIX-6665
        ps.setLong(1, numSlotToAllocate);
        ResultSet rs = ps.executeQuery();
        assertTrue(rs.next());
        int retValue = rs.getInt(1);
        assertEquals(expectedValue, retValue);
    }

    private void assertExpectedCurrentValueForSequence(String sequenceName, int expectedValue) throws SQLException {
        ResultSet rs;
        rs = conn.createStatement().executeQuery(String.format(SELECT_CURRENT_VALUE_SQL, sequenceName));
        assertTrue(rs.next());
        assertEquals(expectedValue, rs.getInt(1));
    }
    
    private void assertExpectedNextValueForSequence(String sequenceName, int expectedValue) throws SQLException {
        ResultSet rs;
        rs = conn.createStatement().executeQuery(String.format(SELECT_NEXT_VALUE_SQL, sequenceName));
        assertTrue(rs.next());
        assertEquals(expectedValue, rs.getInt(1));
    }

    private Connection createGenericConnection() throws Exception {
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        return DriverManager.getConnection(getUrl(), props);
    }
    
    private void createConnection() throws Exception {
        if (conn != null) conn.close();
        if (tenantId != null) {
            Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
            this.conn =  DriverManager.getConnection(getUrl() + ';' + TENANT_ID_ATTRIB + '=' + "tenant1", props);

        } else {
            Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
            conn = DriverManager.getConnection(getUrl(), props);
        }
    }

    private void assertExpectedStateInSystemSequence(SequenceProperties props, long currentValue)
            throws SQLException {
        // Validate state in System.Sequence
        ResultSet rs =
                conn.createStatement()
                        .executeQuery(
                            "SELECT start_with, current_value, increment_by, cache_size, min_value, max_value, cycle_flag, sequence_schema, sequence_name FROM \"SYSTEM\".\"SEQUENCE\" where sequence_name='" + props.getNameWithoutSchema() + "'");
        assertTrue(rs.next());
        assertEquals("start_with", props.startsWith, rs.getLong("start_with"));
        assertEquals("increment_by", props.incrementBy, rs.getLong("increment_by"));
        assertEquals("cache_size", props.cacheSize, rs.getLong("cache_size"));
        assertEquals("cycle_flag", false, rs.getBoolean("cycle_flag"));
        assertEquals("sequence_schema", props.getSchemaName(), rs.getString("sequence_schema"));
        assertEquals("sequence_name", props.getNameWithoutSchema(), rs.getString("sequence_name"));
        assertEquals("current_value", currentValue, rs.getLong("current_value"));
        assertEquals("min_value", props.minValue, rs.getLong("min_value"));
        assertEquals("max_value", props.maxValue, rs.getLong("max_value"));
        assertFalse(rs.next());
    }

    private void assertExpectedNumberOfValuesAllocated(long firstValue, long lastValue,
            int incrementBy, long numAllocated) {
        int cnt = 0;
        for (long i = firstValue; (incrementBy > 0 ? i <= lastValue : i >= lastValue); i += incrementBy) {
            cnt++;
        }
        assertEquals("Incorrect number of values allocated: " + cnt, numAllocated, cnt);
    }

    private static class SequenceProperties {

       private final long numAllocated;
       private final int incrementBy;
       private final int startsWith;
       private final int cacheSize;
       private final long minValue;
       private final long maxValue;
       private final String name;

        public SequenceProperties(Builder builder) {
            this.numAllocated = builder.numAllocated;
            this.incrementBy = builder.incrementBy;
            this.startsWith = builder.startsWith;
            this.cacheSize = builder.cacheSize;
            this.minValue = builder.minValue;
            this.maxValue = builder.maxValue;
            this.name = builder.name;
        }

        private static class Builder {

            long maxValue = Long.MAX_VALUE;
            long minValue = Long.MIN_VALUE;
            long numAllocated = 100;
            int incrementBy = 1;
            int startsWith = 1;
            int cacheSize = 100;
            String name = null;

            public Builder numAllocated(long numAllocated) {
                this.numAllocated = numAllocated;
                return this;
            }

            public Builder startsWith(int startsWith) {
                this.startsWith = startsWith;
                return this;
            }

            public Builder cacheSize(int cacheSize) {
                this.cacheSize = cacheSize;
                return this;
            }

            public Builder incrementBy(int incrementBy) {
                this.incrementBy = incrementBy;
                return this;
            }

            public Builder minValue(long minValue) {
                this.minValue = minValue;
                return this;
            }
            
            public Builder maxValue(long maxValue) {
                this.maxValue = maxValue;
                return this;
            }
            
            public Builder name(String name) {
                this.name = name;
                return this;
            }
            
            public SequenceProperties build() {
                return new SequenceProperties(this);
            }
        }
        
        private String getSchemaName() {
        	return name.substring(0, name.indexOf("."));
        }
         
        private String getNameWithoutSchema() {
        	return name.substring(name.indexOf(".") + 1, name.length());
        }  

    }
    
}