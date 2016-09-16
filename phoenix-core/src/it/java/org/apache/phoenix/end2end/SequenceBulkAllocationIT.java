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
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import org.apache.phoenix.exception.SQLExceptionCode;
import org.apache.phoenix.query.QueryServices;
import org.apache.phoenix.util.PhoenixRuntime;
import org.apache.phoenix.util.PropertiesUtil;
import org.apache.phoenix.util.QueryUtil;
import org.apache.phoenix.util.ReadOnlyProps;
import org.junit.After;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

import com.google.common.collect.Lists;

/**
 * Suite of integration tests that validate that Bulk Allocation of Sequence values
 * using the NEXT <n> VALUES FOR <seq> syntax works as expected and interacts
 * correctly with NEXT VALUE FOR <seq> and CURRENT VALUE FOR <seq>.
 * 
 * All tests are run with both a generic connection and a multi-tenant connection.
 * 
 */
@RunWith(Parameterized.class)
public class SequenceBulkAllocationIT extends BaseClientManagedTimeIT {

    private static final long BATCH_SIZE = 3;
    private static final String SELECT_NEXT_VALUE_SQL =
            "SELECT NEXT VALUE FOR %s FROM SYSTEM.\"SEQUENCE\" LIMIT 1";
    private static final String SELECT_CURRENT_VALUE_SQL =
            "SELECT CURRENT VALUE FOR %s FROM SYSTEM.\"SEQUENCE\" LIMIT 1";
    private static final String CREATE_SEQUENCE_NO_MIN_MAX_TEMPLATE =
            "CREATE SEQUENCE bulkalloc.alpha START WITH %s INCREMENT BY %s CACHE %s";
    private static final String CREATE_SEQUENCE_WITH_MIN_MAX_TEMPLATE =
            "CREATE SEQUENCE bulkalloc.alpha START WITH %s INCREMENT BY %s MINVALUE %s MAXVALUE %s CACHE %s";
    private static final String CREATE_SEQUENCE_WITH_MIN_MAX_AND_CYCLE_TEMPLATE =
            "CREATE SEQUENCE bulkalloc.alpha START WITH %s INCREMENT BY %s MINVALUE %s MAXVALUE %s CYCLE CACHE %s";

    
    private Connection conn;
    private String tenantId;
    
    public SequenceBulkAllocationIT(String tenantId) {
        this.tenantId = tenantId;
    }

    @BeforeClass
    @Shadower(classBeingShadowed = BaseClientManagedTimeIT.class)
    public static void doSetup() throws Exception {
        Map<String, String> props = getDefaultProps();
        // Must update config before starting server
        props.put(QueryServices.SEQUENCE_CACHE_SIZE_ATTRIB, Long.toString(BATCH_SIZE));
        setUpTestDriver(new ReadOnlyProps(props.entrySet().iterator()));
    }

    @After
    public void tearDown() throws Exception {
        // close any open connection between tests, so that connections are not leaked
        if (conn != null) {
            conn.close();
        }
    }
    
    @Parameters(name="SequenceBulkAllocationIT_tenantId={0}") // name is used by failsafe as file name in reports
    public static Object[] data() {
        return new Object[] {null, "tenant1"};
    }
    
    
    @Test
    public void testSequenceParseNextValuesWithNull() throws Exception {
        nextConnection();
        try {
            conn.createStatement().executeQuery(
                "SELECT NEXT NULL VALUES FOR bulkalloc.alpha FROM SYSTEM.\"SEQUENCE\" LIMIT 1");
            fail("null is not allowed to be used for <n> in NEXT <n> VALUES FOR <seq>");
        } catch (SQLException e) {
            assertEquals(SQLExceptionCode.NUM_SEQ_TO_ALLOCATE_MUST_BE_CONSTANT.getErrorCode(),
                e.getErrorCode());
            assertTrue(e.getNextException() == null);
        }
    }
    
    @Test
    public void testSequenceParseNextValuesWithNonNumber() throws Exception {
        nextConnection();    
        try {
            conn.createStatement().executeQuery(
                "SELECT NEXT '89b' VALUES FOR bulkalloc.alpha FROM SYSTEM.\"SEQUENCE\" LIMIT 1");
            fail("Only integers and longs are allowed to be used for <n> in NEXT <n> VALUES FOR <seq>");
        } catch (SQLException e) {
            assertEquals(SQLExceptionCode.NUM_SEQ_TO_ALLOCATE_MUST_BE_CONSTANT.getErrorCode(),
                e.getErrorCode());
            assertTrue(e.getNextException() == null);
        }
    }
    
    
    @Test
    public void testSequenceParseNextValuesWithNegativeNumber() throws Exception {
        nextConnection();
        try {
            conn.createStatement().executeQuery(
                "SELECT NEXT '-1' VALUES FOR bulkalloc.alpha FROM SYSTEM.\"SEQUENCE\" LIMIT 1");
            fail("null is not allowed to be used for <n> in NEXT <n> VALUES FOR <seq>");
        } catch (SQLException e) {
            assertEquals(SQLExceptionCode.NUM_SEQ_TO_ALLOCATE_MUST_BE_CONSTANT.getErrorCode(),
                e.getErrorCode());
            assertTrue(e.getNextException() == null);
        }
    }

    @Test
    public void testParseNextValuesSequenceWithZeroAllocated() throws Exception {
        nextConnection();    
        try {
            conn.createStatement().executeQuery(
                "SELECT NEXT 0 VALUES FOR bulkalloc.alpha FROM SYSTEM.\"SEQUENCE\" LIMIT 1");
            fail("Only integers and longs are allowed to be used for <n> in NEXT <n> VALUES FOR <seq>");
        } catch (SQLException e) {
            assertEquals(SQLExceptionCode.NUM_SEQ_TO_ALLOCATE_MUST_BE_CONSTANT.getErrorCode(),
                e.getErrorCode());
            assertTrue(e.getNextException() == null);
        }
    }
    

    @Test
    public void testNextValuesForSequenceWithNoAllocatedValues() throws Exception {
        // Create Sequence
        final SequenceProperties props =
                new SequenceProperties.Builder().incrementBy(1).startsWith(1).cacheSize(1)
                        .numAllocated(100).build();

        nextConnection();
        createSequenceWithNoMinMax(props);
        nextConnection();

        // Bulk Allocate Sequence Slots
        final int currentValueAfterAllocation = 100;
        reserveSlotsInBulkAndAssertValue(1, props.numAllocated);
        assertExpectedStateInSystemSequence(props, 101);
        assertExpectedNumberOfValuesAllocated(1, currentValueAfterAllocation, props.incrementBy, props.numAllocated);
        
        // Assert standard Sequence Operations return expected values
        assertExpectedCurrentValueForSequence(currentValueAfterAllocation);
        assertExpectedNextValueForSequence(101);
    }
    
    @Test
    /**
     * Validates we can invoke NEXT <n> VALUES FOR using bind vars.
     */
    public void testNextValuesForSequenceUsingBinds() throws Exception {
        
        // Create Sequence
        final SequenceProperties props =
                new SequenceProperties.Builder().incrementBy(1).startsWith(1).cacheSize(1)
                        .numAllocated(100).build();

        nextConnection();
        createSequenceWithNoMinMax(props);
        nextConnection();
        
        // Allocate 100 slots using SQL with Bind Params and a PreparedStatement
        final int currentValueAfterAllocation = 100;
        reserveSlotsInBulkUsingBindsAndAssertValue(1,props.numAllocated);
        assertExpectedStateInSystemSequence(props, 101);
        assertExpectedNumberOfValuesAllocated(1, currentValueAfterAllocation, props.incrementBy, props.numAllocated);

        // Assert standard Sequence Operations return expected values
        assertExpectedCurrentValueForSequence(currentValueAfterAllocation);
        assertExpectedNextValueForSequence(101);
    }
    

    @Test
    public void testNextValuesForSequenceWithPreviouslyAllocatedValues() throws Exception {
        final SequenceProperties props =
                new SequenceProperties.Builder().incrementBy(1).startsWith(1).cacheSize(100)
                        .numAllocated(1000).build();

        nextConnection();
        createSequenceWithNoMinMax(props);
        nextConnection();

        assertExpectedNextValueForSequence(1);
        assertExpectedCurrentValueForSequence(1);
        assertExpectedNextValueForSequence(2);

        // Bulk Allocate Sequence Slots
        int currentValueAfterAllocation = 1100;
        int nextValueAfterAllocation = currentValueAfterAllocation + props.incrementBy;
        int startValueAfterAllocation = 101;
        reserveSlotsInBulkAndAssertValue(startValueAfterAllocation, props.numAllocated);
        assertBulkAllocationSucceeded(props, currentValueAfterAllocation, startValueAfterAllocation);

        // Assert standard Sequence Operations return expected values
        assertExpectedCurrentValueForSequence(currentValueAfterAllocation);
        assertExpectedNextValueForSequence(nextValueAfterAllocation);
    }
   
    
    @Test
    /**
     * Validates that if we close a connection after performing 
     * NEXT <n> VALUES FOR <seq> the values are correctly returned from
     * the latest batch.
     */
    public void testConnectionCloseReturnsSequenceValuesCorrectly() throws Exception {
        final SequenceProperties props =
                new SequenceProperties.Builder().incrementBy(2).startsWith(1).cacheSize(100)
                        .numAllocated(100).build();
        nextConnection();
        createSequenceWithNoMinMax(props);
        nextConnection();
        
        assertExpectedNextValueForSequence(1);
        assertExpectedCurrentValueForSequence(1);
        assertExpectedNextValueForSequence(3);
        
        // Bulk Allocate Sequence Slots
        int currentValueAfterAllocation = 399;
        int nextValueAfterAllocation = currentValueAfterAllocation + props.incrementBy;
        int startValueAfterAllocation = 201;
        reserveSlotsInBulkAndAssertValue(startValueAfterAllocation, props.numAllocated);
        assertExpectedCurrentValueForSequence(currentValueAfterAllocation);
        
        // Close the Connection
        conn.close();
        
        // Test that sequence, doesn't have gaps after closing the connection
        nextConnection();
        assertExpectedNextValueForSequence(nextValueAfterAllocation);
        assertExpectedCurrentValueForSequence(nextValueAfterAllocation);

    }
    
    @Test
    /**
     * Validates that calling NEXT <n> VALUES FOR <seq> works correctly with UPSERT.
     */
    public void testNextValuesForSequenceWithUpsert() throws Exception {
        
        // Create Sequence
        final SequenceProperties props =
                new SequenceProperties.Builder().incrementBy(1).startsWith(1).cacheSize(100)
                        .numAllocated(1000).build();
        nextConnection();
        createSequenceWithNoMinMax(props);

        // Create TABLE
        nextGenericConnection();
        conn.createStatement().execute("CREATE TABLE bulkalloc.test ( id INTEGER NOT NULL PRIMARY KEY)");
        nextConnection();
        
        // Grab batch from Sequence
        assertExpectedNextValueForSequence(1);
        assertExpectedCurrentValueForSequence(1);
        assertExpectedNextValueForSequence(2);
        assertExpectedStateInSystemSequence(props, 101);
        
        
        // Perform UPSERT and validate Sequence was incremented as expected
        conn.createStatement().execute("UPSERT INTO bulkalloc.test (id) VALUES (NEXT " + props.numAllocated +  " VALUES FOR bulkalloc.alpha)");
        conn.commit();
        assertExpectedStateInSystemSequence(props, 1101);
        
        // SELECT values out and verify
        nextConnection();
        String query = "SELECT id, NEXT VALUE FOR bulkalloc.alpha FROM bulkalloc.test";
        ResultSet rs = conn.prepareStatement(query).executeQuery();
        assertTrue(rs.next());
        assertEquals(101, rs.getInt(1)); // Threw out cache of 100, incremented by 1000
        assertEquals(1101, rs.getInt(2));
        assertFalse(rs.next());
    }




    @Test
    public void testNextValuesForSequenceWithIncrementBy() throws Exception {
        final SequenceProperties props =
                new SequenceProperties.Builder().incrementBy(3).startsWith(1).cacheSize(100)
                        .numAllocated(1000).build();
        nextConnection();
        createSequenceWithNoMinMax(props);
        nextConnection();

        assertExpectedNextValueForSequence(1);
        assertExpectedCurrentValueForSequence(1);
        assertExpectedNextValueForSequence(4);

        // Bulk Allocate Sequence Slots
        int currentValueAfterAllocation = 3298;
        int startValueAfterAllocation = 301;
        reserveSlotsInBulkAndAssertValue(startValueAfterAllocation, props.numAllocated);
        assertBulkAllocationSucceeded(props, currentValueAfterAllocation, startValueAfterAllocation);

        // Assert standard Sequence Operations return expected values
        assertExpectedCurrentValueForSequence(3298);
        assertExpectedNextValueForSequence(3301);
    }
    
    @Test
    public void testNextValuesForSequenceWithNegativeIncrementBy() throws Exception {
        final SequenceProperties props =
                new SequenceProperties.Builder().incrementBy(-1).startsWith(2000).cacheSize(100)
                        .numAllocated(1000).build();
        nextConnection();
        createSequenceWithNoMinMax(props);
        nextConnection();

        assertExpectedNextValueForSequence(2000);
        assertExpectedCurrentValueForSequence(2000);
        assertExpectedNextValueForSequence(1999);

        // Bulk Allocate Sequence Slots
        int currentValueAfterAllocation = 901;
        int startValueAfterAllocation = 1900;
        reserveSlotsInBulkAndAssertValue(startValueAfterAllocation, props.numAllocated);
        assertBulkAllocationSucceeded(props, currentValueAfterAllocation, startValueAfterAllocation);


        // Assert standard Sequence Operations return expected values
        assertExpectedCurrentValueForSequence(901);
        assertExpectedNextValueForSequence(900);
    }

    @Test
    public void testNextValuesForSequenceWithNegativeIncrementByGreaterThanOne() throws Exception {
        final SequenceProperties props =
                new SequenceProperties.Builder().incrementBy(-5).startsWith(2000).cacheSize(100)
                        .numAllocated(100).build();
        nextConnection();
        createSequenceWithNoMinMax(props);
        nextConnection();
        
        // Pull first batch from Sequence
        assertExpectedNextValueForSequence(2000);
        assertExpectedCurrentValueForSequence(2000);
        assertExpectedNextValueForSequence(1995);

        // Bulk Allocate Sequence Slots
        int currentValueAfterAllocation = 1005;
        int startValueAfterAllocation = 1500;
        reserveSlotsInBulkAndAssertValue(startValueAfterAllocation, props.numAllocated);
        assertBulkAllocationSucceeded(props, currentValueAfterAllocation, startValueAfterAllocation);
        

        // Assert standard Sequence Operations return expected values
        assertExpectedCurrentValueForSequence(1005);
        assertExpectedNextValueForSequence(1000);
    }
    
    
    @Test
    /**
     * Validates that for NEXT <n> VALUES FOR if you try an allocate more slots such that that
     * we exceed the max value of the sequence we throw an exception. Allocating sequence values in bulk
     * should be an all or nothing operation - if the operation succeeds clients are guaranteed that they
     * have access to all slots requested.
     */
    public void testNextValuesForSequenceExceedsMaxValue() throws Exception {
        final SequenceProperties sequenceProps =
                new SequenceProperties.Builder().incrementBy(1).startsWith(100).cacheSize(100)
                        .numAllocated(1000).minValue(100).maxValue(900).build();

        nextConnection();
        createSequenceWithMinMax(sequenceProps);
        nextConnection();

        // Pull first batch from the sequence
        assertExpectedNextValueForSequence(100);
        assertExpectedCurrentValueForSequence(100);
        assertExpectedNextValueForSequence(101);

        // Attempt to bulk Allocate more slots than available
        try {
            conn.createStatement().executeQuery(
                        "SELECT NEXT " + sequenceProps.numAllocated
                                + " VALUES FOR bulkalloc.alpha FROM SYSTEM.\"SEQUENCE\" LIMIT 1");
            fail("Invoking SELECT NEXT VALUES should have thrown Reached Max Value Exception");
        } catch (SQLException e) {
            assertEquals(SQLExceptionCode.SEQUENCE_VAL_REACHED_MAX_VALUE.getErrorCode(),
                e.getErrorCode());
            assertTrue(e.getNextException() == null);
        }

        // Assert sequence didn't advance
        assertExpectedCurrentValueForSequence(101);
        assertExpectedNextValueForSequence(102);
    }

    @Test
    /**
     * Validates that for NEXT <n> VALUES FOR if you try an allocate more slots such that that
     * we exceed the min value of the sequence we throw an exception. Allocating sequence values in bulk
     * should be an all or nothing operation - if the operation succeeds clients are guaranteed that they
     * have access to all slots requested.
     */
    public void testNextValuesForSequenceExceedsMinValue() throws Exception {
        final SequenceProperties sequenceProps =
                new SequenceProperties.Builder().incrementBy(-5).startsWith(900).cacheSize(100)
                        .numAllocated(160).minValue(100).maxValue(900).build();

        nextConnection();
        createSequenceWithMinMax(sequenceProps);
        nextConnection();

        // Pull first batch from the sequence
        assertExpectedNextValueForSequence(900);
        assertExpectedCurrentValueForSequence(900);
        assertExpectedNextValueForSequence(895);

        // Attempt to bulk Allocate more slots than available
        try {
            conn.createStatement().executeQuery(
                        "SELECT NEXT " + sequenceProps.numAllocated
                                + " VALUES FOR bulkalloc.alpha FROM SYSTEM.\"SEQUENCE\" LIMIT 1");
            fail("Invoking SELECT NEXT VALUES should have thrown Reached Max Value Exception");
        } catch (SQLException e) {
            assertEquals(SQLExceptionCode.SEQUENCE_VAL_REACHED_MIN_VALUE.getErrorCode(),
                e.getErrorCode());
            assertTrue(e.getNextException() == null);
        }

        // Assert sequence didn't advance (we still throw out the cached values)
        assertExpectedCurrentValueForSequence(895);
        assertExpectedNextValueForSequence(890);
    }

    
    @Test
    /**
     * Validates that if we don't exceed the limit bulk allocation works with sequences with a 
     * min and max defined.
     */
    public void testNextValuesForSequenceWithMinMaxDefined() throws Exception {
        final SequenceProperties props =
                new SequenceProperties.Builder().incrementBy(5).startsWith(100).cacheSize(100)
                        .numAllocated(1000).minValue(100).maxValue(6000).build();

        nextConnection();
        createSequenceWithMinMax(props);
        nextConnection();

        assertExpectedNextValueForSequence(100);
        assertExpectedCurrentValueForSequence(100);
        assertExpectedNextValueForSequence(105);

        // Bulk Allocate Sequence Slots
        int currentValueAfterAllocation = 5595;
        int startValueAfterAllocation = 600;
        reserveSlotsInBulkAndAssertValue(startValueAfterAllocation, props.numAllocated);
        assertBulkAllocationSucceeded(props, currentValueAfterAllocation, startValueAfterAllocation);

        // Assert standard Sequence Operations return expected values
        assertExpectedCurrentValueForSequence(5595);
        assertExpectedNextValueForSequence(5600);
    }
    
    @Test
    public void testNextValuesForSequenceWithDefaultMax() throws Exception {
        final SequenceProperties props =
                new SequenceProperties.Builder().incrementBy(1).startsWith(100).cacheSize(100)
                        .numAllocated(Long.MAX_VALUE - 100).build();
        
        // Create Sequence
        nextConnection();
        createSequenceWithMinMax(props);
        nextConnection();

        // Bulk Allocate Sequence Slots
        long currentValueAfterAllocation = 100;
        long startValueAfterAllocation = Long.MAX_VALUE;
        reserveSlotsInBulkAndAssertValue(currentValueAfterAllocation, props.numAllocated);
        assertExpectedStateInSystemSequence(props, startValueAfterAllocation);
        
        // Try and get next value
        try {
            conn.createStatement().executeQuery(String.format(SELECT_NEXT_VALUE_SQL, "bulkalloc.alpha"));
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
        final SequenceProperties props =
                new SequenceProperties.Builder().incrementBy(1).startsWith(100).cacheSize(100)
                        .numAllocated(Long.MAX_VALUE).build();
        
        // Create Sequence
        nextConnection();
        createSequenceWithMinMax(props);
        nextConnection();

        // Bulk Allocate Sequence Slots
        try {
            conn.createStatement().executeQuery(
                "SELECT NEXT " + Long.MAX_VALUE
                        + " VALUES FOR bulkalloc.alpha FROM SYSTEM.\"SEQUENCE\"");
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
        final SequenceProperties props =
                new SequenceProperties.Builder().incrementBy(5).startsWith(100).cacheSize(100)
                        .numAllocated(50).minValue(100).maxValue(6000).build();

        nextConnection();
        createSequenceWithMinMax(props);
        nextConnection();

        assertExpectedNextValueForSequence(100);
        assertExpectedCurrentValueForSequence(100);
        assertExpectedNextValueForSequence(105);

        // Bulk Allocate Sequence Slots
        int currentValueAfterAllocation = 355;
        int startValueAfterAllocation = 110;
        reserveSlotsInBulkAndAssertValue(startValueAfterAllocation, props.numAllocated);
        assertExpectedStateInSystemSequence(props, 600);
        assertExpectedNumberOfValuesAllocated(startValueAfterAllocation, currentValueAfterAllocation, props.incrementBy, props.numAllocated);

        // Assert standard Sequence Operations return expected values
        // 105 + (50 * 5) = 355 
        assertExpectedCurrentValueForSequence(355);
        assertExpectedNextValueForSequence(360);
        assertExpectedNextValueForSequence(365);
        assertExpectedNextValueForSequence(370);
    }
    
    @Test
    /**
     * Validates that specifying an bulk allocation less than the size of the cache defined on the sequence works
     * as expected if we don't have enough values in the cache to support the allocation.
     */
    public void testNextValuesForInsufficentCacheValuesAllocationLessThanCacheSize() throws Exception {
        final SequenceProperties props =
                new SequenceProperties.Builder().incrementBy(5).startsWith(100).cacheSize(100)
                        .numAllocated(50).minValue(100).maxValue(6000).build();

        nextConnection();
        createSequenceWithMinMax(props);
        nextConnection();
        
        // Allocate 51 slots, only 49 will be left
        int currentValueAfter51Allocations  = 355; // 100 + 51 * 5
        for (int i = 100; i <= currentValueAfter51Allocations; i = i + 5) {
            assertExpectedNextValueForSequence(i);
        }
        assertExpectedCurrentValueForSequence(currentValueAfter51Allocations);

        // Bulk Allocate 50 Sequence Slots which greater than amount left in cache
        // This should throw away rest of the cache, and allocate the request slot
        // from the next start value
        int currentValueAfterAllocation = 845;
        int startValueAfterAllocation = 600;
        reserveSlotsInBulkAndAssertValue(startValueAfterAllocation, props.numAllocated);
        assertBulkAllocationSucceeded(props, currentValueAfterAllocation, startValueAfterAllocation);

        // Assert standard Sequence Operations return expected values
        assertExpectedCurrentValueForSequence(845);
        assertExpectedNextValueForSequence(850);
        assertExpectedNextValueForSequence(855);
        assertExpectedNextValueForSequence(860);
    }
    
    @Test
    /**
     * Validates that for NEXT <n> VALUES FOR is not supported on Sequences that have the
     * CYCLE flag set to true.
     */
    public void testNextValuesForSequenceWithCycles() throws Exception {
        final SequenceProperties sequenceProps =
                new SequenceProperties.Builder().incrementBy(5).startsWith(100).cacheSize(100)
                        .numAllocated(1000).minValue(100).maxValue(900).build();

        nextConnection();
        createSequenceWithMinMaxAndCycle(sequenceProps);
        nextConnection();

        // Full first batch from the sequence
        assertExpectedNextValueForSequence(100);
        assertExpectedCurrentValueForSequence(100);
        assertExpectedNextValueForSequence(105);

        // Attempt to bulk Allocate more slots than available
        try {
             conn.createStatement().executeQuery(
                        "SELECT NEXT " + sequenceProps.numAllocated
                                + " VALUES FOR bulkalloc.alpha FROM SYSTEM.\"SEQUENCE\" LIMIT 1");
            fail("Invoking SELECT NEXT VALUES should have failed as operation is not supported for sequences with Cycles.");
        } catch (SQLException e) {
            assertEquals(SQLExceptionCode.NUM_SEQ_TO_ALLOCATE_NOT_SUPPORTED.getErrorCode(),
                e.getErrorCode());
            assertTrue(e.getNextException() == null);
        }

        // Assert sequence didn't advance
        assertExpectedCurrentValueForSequence(105);
        assertExpectedNextValueForSequence(110);
        assertExpectedNextValueForSequence(115);
    }

    @Test
    /**
     * Validates that if we have multiple NEXT <n> VALUES FOR <seq> expression and the 
     * CURRENT VALUE FOR expression work correctly when used in the same statement.
     */
    public void testCurrentValueForAndNextValuesForExpressionsForSameSequence() throws Exception {
        final SequenceProperties props =
                new SequenceProperties.Builder().incrementBy(1).startsWith(1).cacheSize(100)
                        .numAllocated(1000).build();

        nextConnection();
        createSequenceWithNoMinMax(props);
        nextConnection();

        assertExpectedNextValueForSequence(1);
        assertExpectedCurrentValueForSequence(1);
        assertExpectedNextValueForSequence(2);

        // Bulk Allocate Sequence Slots
        int currentValueAfterAllocation = 1100;
        int nextValueAfterAllocation = currentValueAfterAllocation + props.incrementBy;
        int startValueAfterAllocation = 101;
        ResultSet rs =
                conn.createStatement().executeQuery(
                    "SELECT CURRENT VALUE FOR bulkalloc.alpha, NEXT " + props.numAllocated + " VALUES FOR bulkalloc.alpha FROM SYSTEM.\"SEQUENCE\"");
        assertTrue(rs.next());
        assertBulkAllocationSucceeded(props, currentValueAfterAllocation, startValueAfterAllocation);
        int currentValueFor = rs.getInt(1);
        int nextValuesFor = rs.getInt(2);
        assertEquals("Expected the next value to be first value reserved", startValueAfterAllocation, nextValuesFor);
        assertEquals("Expected current value to be the same as next value", startValueAfterAllocation, currentValueFor);
        
        // Assert standard Sequence Operations return expected values
        assertExpectedCurrentValueForSequence(currentValueAfterAllocation);
        assertExpectedNextValueForSequence(nextValueAfterAllocation);
    }
    
    @Test
    /**
     * Validates that if we have multiple NEXT <n> VALUES FOR <seq> expressions for the *same* sequence
     * in a statement we only process the one which has the highest value of <n> and return the start
     * value for that for all expressions.
     */
    public void testMultipleNextValuesForExpressionsForSameSequence() throws Exception {
        final SequenceProperties props =
                new SequenceProperties.Builder().incrementBy(1).startsWith(1).cacheSize(100)
                        .numAllocated(1000).build();

        nextConnection();
        createSequenceWithNoMinMax(props);
        nextConnection();

        assertExpectedNextValueForSequence(1);
        assertExpectedCurrentValueForSequence(1);
        assertExpectedNextValueForSequence(2);

        // Bulk Allocate Sequence Slots - One for 5 and one for 1000, 1000 should have precedence
        int currentValueAfterAllocation = 1100;
        int nextValueAfterAllocation = currentValueAfterAllocation + props.incrementBy;
        int startValueAfterAllocation = 101;
        ResultSet rs =
                conn.createStatement().executeQuery(
                    "SELECT NEXT 5 VALUES FOR bulkalloc.alpha, NEXT " + props.numAllocated + " VALUES FOR bulkalloc.alpha FROM SYSTEM.\"SEQUENCE\"");
        assertTrue(rs.next());
        int firstValue = rs.getInt(1);
        int secondValue = rs.getInt(2);
        assertEquals("Expected both expressions to return the same value", firstValue, secondValue);
        assertEquals("Expected the value returned to be the highest allocation", startValueAfterAllocation, firstValue);
        assertBulkAllocationSucceeded(props, currentValueAfterAllocation, startValueAfterAllocation);
        
        // Assert standard Sequence Operations return expected values
        assertExpectedCurrentValueForSequence(currentValueAfterAllocation);
        assertExpectedNextValueForSequence(nextValueAfterAllocation);
    }
    
    @Test
    /**
     * Validates that if we have NEXT VALUE FOR <seq> and NEXT <n> VALUES FOR <seq> expressions for the *same* sequence
     * in a statement we only process way and honor the value of the highest value of <n>, where for 
     * NEXT VALUE FOR <seq> is assumed to be 1.
     */
    public void testMultipleDifferentExpressionsForSameSequence() throws Exception {
        final SequenceProperties props =
                new SequenceProperties.Builder().incrementBy(1).startsWith(1).cacheSize(100)
                        .numAllocated(1000).build();

        nextConnection();
        createSequenceWithNoMinMax(props);
        nextConnection();
        
        // Pull First Batch from Sequence
        assertExpectedNextValueForSequence(1);

        // Bulk Allocate Sequence Slots and Get Next Value in Same Statement
        int currentValueAfterAllocation = 1100;
        int nextValueAfterAllocation = currentValueAfterAllocation + props.incrementBy;
        int startValueAfterAllocation = 101;
        ResultSet rs =
                conn.createStatement().executeQuery(
                    "SELECT NEXT VALUE FOR bulkalloc.alpha, "
                    + "NEXT " + props.numAllocated + " VALUES FOR bulkalloc.alpha, "
                    + "CURRENT VALUE FOR bulkalloc.alpha, "
                    + "NEXT 999 VALUES FOR bulkalloc.alpha "
                    + "FROM SYSTEM.\"SEQUENCE\"");
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
        assertExpectedCurrentValueForSequence(currentValueAfterAllocation);
        assertExpectedNextValueForSequence(nextValueAfterAllocation);
    }
    
    
    @Test
    /**
     * Validates that using NEXT <n> VALUES FOR on different sequences in the 
     * same statement with *different* values of <n> works as expected. This
     * test validates that we keep our numAllocated array and sequence keys in 
     * sync during the sequence management process.
     */
    public void testMultipleNextValuesForExpressionsForDifferentSequences() throws Exception {

        nextConnection();
        conn.createStatement().execute("CREATE SEQUENCE bulkalloc.alpha START WITH 30 INCREMENT BY 3 CACHE 100");
        conn.createStatement().execute("CREATE SEQUENCE bulkalloc.beta START WITH 100 INCREMENT BY 5 CACHE 50");
        nextConnection();

        // Bulk Allocate Sequence Slots for Two Sequences
        ResultSet rs =
                conn.createStatement().executeQuery(
                    "SELECT NEXT 100 VALUES FOR bulkalloc.alpha, NEXT 1000 VALUES FOR bulkalloc.beta FROM SYSTEM.\"SEQUENCE\"");
        assertTrue(rs.next());
        assertEquals(30, rs.getInt(1));
        assertEquals(100, rs.getInt(2));
        
        // Assert standard Sequence Operations return expected values
        for (int i = 330; i < 330 + (2 * 100); i += 3) {
            assertExpectedCurrentValueForSequence(i - 3, "bulkalloc.alpha");
            assertExpectedNextValueForSequence(i, "bulkalloc.alpha");            
        }
        
        for (int i = 5100; i < 5100 + (2 * 1000); i += 5) {
            assertExpectedCurrentValueForSequence(i - 5, "bulkalloc.beta");
            assertExpectedNextValueForSequence(i, "bulkalloc.beta");            
        }
    }
    
    @Test
    /**
     * Validates that calling NEXT <n> VALUES FOR with EXPLAIN PLAN doesn't use 
     * allocate any slots. 
     */
    public void testExplainPlanValidatesSequences() throws Exception {
        
        final SequenceProperties props =
                new SequenceProperties.Builder().incrementBy(3).startsWith(30).cacheSize(100)
                        .numAllocated(1000).build();

        nextConnection();
        createSequenceWithNoMinMax(props);
        
        nextGenericConnection();
        conn.createStatement().execute("CREATE TABLE bulkalloc.simpletbl (k BIGINT NOT NULL PRIMARY KEY)");
        nextConnection();

        // Bulk Allocate Sequence Slots
        int startValueAfterAllocation = 30;
        reserveSlotsInBulkAndAssertValue(startValueAfterAllocation, props.numAllocated);
        
        // Execute EXPLAIN PLAN multiple times, which should not change Sequence values
        for (int i = 0; i < 3; i++) {
            conn.createStatement().executeQuery("EXPLAIN SELECT NEXT 1000 VALUES FOR bulkalloc.alpha FROM bulkalloc.simpletbl");
        }
        
        // Validate the current value was not advanced and was the starting value
        assertExpectedStateInSystemSequence(props, 3030);
        
        // Assert standard Sequence Operations return expected values
        int startValue = 3030;
        for (int i = startValue; i < startValue + (2 * props.cacheSize); i += props.incrementBy) {
            assertExpectedCurrentValueForSequence(i - props.incrementBy);
            assertExpectedNextValueForSequence(i);            
        }
    }
    
    @Test
    public void testExplainPlanForNextValuesFor() throws Exception {
        
        final SequenceProperties props =
                new SequenceProperties.Builder().incrementBy(3).startsWith(30).cacheSize(100)
                        .numAllocated(1000).build();

        nextConnection();
        createSequenceWithNoMinMax(props);
        nextGenericConnection();
        conn.createStatement().execute("CREATE TABLE bulkalloc.simpletbl (k BIGINT NOT NULL PRIMARY KEY)");
        nextConnection();
        
        // Execute EXPLAIN PLAN which should not change Sequence values
        ResultSet rs = conn.createStatement().executeQuery("EXPLAIN SELECT NEXT 1000 VALUES FOR bulkalloc.alpha FROM bulkalloc.simpletbl");

        // Assert output for Explain Plain result is as expected
        assertEquals("CLIENT PARALLEL 1-WAY FULL SCAN OVER BULKALLOC.SIMPLETBL\n" + 
                "    SERVER FILTER BY FIRST KEY ONLY\n" +
                "CLIENT RESERVE VALUES FROM 1 SEQUENCE", QueryUtil.getExplainPlan(rs));
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
        final SequenceProperties props =
                new SequenceProperties.Builder().incrementBy(1).startsWith(1).cacheSize(100)
                        .numAllocated(1000).build();

        nextConnection();
        createSequenceWithNoMinMax(props);
        nextConnection();

        assertExpectedNextValueForSequence(1);
        assertExpectedCurrentValueForSequence(1);
        assertExpectedNextValueForSequence(2);

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
                                "SELECT NEXT " + numSlotToAllocate + " VALUES FOR bulkalloc.alpha FROM SYSTEM.\"SEQUENCE\"");
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
                                "SELECT NEXT VALUE FOR bulkalloc.alpha FROM SYSTEM.\"SEQUENCE\"");
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
        final SequenceProperties props =
                new SequenceProperties.Builder().incrementBy(1).startsWith(1).cacheSize(100)
                        .numAllocated(1000).build();

        nextConnection();
        createSequenceWithNoMinMax(props);
        nextConnection();

        assertExpectedNextValueForSequence(1);
        assertExpectedCurrentValueForSequence(1);
        assertExpectedNextValueForSequence(2);

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
                                "SELECT NEXT " + numSlotToAllocate1 + " VALUES FOR bulkalloc.alpha FROM SYSTEM.\"SEQUENCE\"");
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
                                "SELECT NEXT " + numSlotToAllocate2 + " VALUES FOR bulkalloc.alpha FROM SYSTEM.\"SEQUENCE\"");
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
        final SequenceProperties props =
                new SequenceProperties.Builder().incrementBy(1).startsWith(1).cacheSize(100)
                        .numAllocated(1000).build();

        nextConnection();
        createSequenceWithNoMinMax(props);
        nextConnection();

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
                                "SELECT NEXT " + numSlotToAllocate1 + " VALUES FOR bulkalloc.alpha FROM SYSTEM.\"SEQUENCE\"");
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
                                "SELECT NEXT " + numSlotToAllocate2 + " VALUES FOR bulkalloc.alpha FROM SYSTEM.\"SEQUENCE\"");
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
            String.format(CREATE_SEQUENCE_NO_MIN_MAX_TEMPLATE, props.startsWith,
                props.incrementBy, props.cacheSize));
    }

    private void createSequenceWithMinMax(final SequenceProperties props) throws SQLException {
        conn.createStatement().execute(
            String.format(CREATE_SEQUENCE_WITH_MIN_MAX_TEMPLATE, props.startsWith,
                props.incrementBy, props.minValue, props.maxValue, props.cacheSize));
    }

    private void createSequenceWithMinMaxAndCycle(final SequenceProperties props) throws SQLException {
        conn.createStatement().execute(
            String.format(CREATE_SEQUENCE_WITH_MIN_MAX_AND_CYCLE_TEMPLATE, props.startsWith,
                props.incrementBy, props.minValue, props.maxValue, props.cacheSize));
    }
    
    private void reserveSlotsInBulkAndAssertValue(long expectedValue, long numSlotToAllocate)
            throws SQLException {
        ResultSet rs =
                conn.createStatement().executeQuery(
                    "SELECT NEXT " + numSlotToAllocate + " VALUES FOR bulkalloc.alpha FROM SYSTEM.\"SEQUENCE\"");
        assertTrue(rs.next());
        assertEquals(expectedValue, rs.getInt(1));
    }

    private void reserveSlotsInBulkUsingBindsAndAssertValue(int expectedValue, long numSlotToAllocate)
            throws SQLException {
        PreparedStatement ps = conn.prepareStatement("SELECT NEXT ? VALUES FOR bulkalloc.alpha FROM SYSTEM.\"SEQUENCE\"");
        ps.setLong(1, numSlotToAllocate);
        ResultSet rs = ps.executeQuery();
        assertTrue(rs.next());
        int retValue = rs.getInt(1);
        assertEquals(expectedValue, retValue);
    }

    private void assertExpectedCurrentValueForSequence(int expectedValue) throws SQLException {
        assertExpectedCurrentValueForSequence(expectedValue, "bulkalloc.alpha");
    }
    
    private void assertExpectedCurrentValueForSequence(int expectedValue, String sequenceName) throws SQLException {
        ResultSet rs;
        rs = conn.createStatement().executeQuery(String.format(SELECT_CURRENT_VALUE_SQL, sequenceName));
        assertTrue(rs.next());
        assertEquals(expectedValue, rs.getInt(1));
    }

    private void assertExpectedNextValueForSequence(int expectedValue) throws SQLException {
        assertExpectedNextValueForSequence(expectedValue, "bulkalloc.alpha");
    }
    
    private void assertExpectedNextValueForSequence(int expectedValue, String sequenceName) throws SQLException {
        ResultSet rs;
        rs = conn.createStatement().executeQuery(String.format(SELECT_NEXT_VALUE_SQL, sequenceName));
        assertTrue(rs.next());
        assertEquals(expectedValue, rs.getInt(1));
    }

    
    /**
     * Returns a non-tenant specific connection.
     */
    private void nextGenericConnection() throws Exception {
        if (conn != null) conn.close();
        long ts = nextTimestamp();
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        props.setProperty(PhoenixRuntime.CURRENT_SCN_ATTRIB, Long.toString(ts));
        conn = DriverManager.getConnection(getUrl(), props);    
    }
    
    private void nextConnection() throws Exception {
        if (conn != null) conn.close();
        long ts = nextTimestamp();
        if (tenantId != null) {
            // Create tenant specific connection
            Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
            props.setProperty(PhoenixRuntime.CURRENT_SCN_ATTRIB, Long.toString(nextTimestamp()));
            this.conn =  DriverManager.getConnection(getUrl() + ';' + TENANT_ID_ATTRIB + '=' + "tenant1", props);

        } else {
            Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
            props.setProperty(PhoenixRuntime.CURRENT_SCN_ATTRIB, Long.toString(ts));
            conn = DriverManager.getConnection(getUrl(), props);
        }
    }

    private void assertExpectedStateInSystemSequence(SequenceProperties props, long currentValue)
            throws SQLException {
        // Validate state in System.Sequence
        ResultSet rs =
                conn.createStatement()
                        .executeQuery(
                            "SELECT start_with, current_value, increment_by, cache_size, min_value, max_value, cycle_flag, sequence_schema, sequence_name FROM SYSTEM.\"SEQUENCE\"");
        assertTrue(rs.next());
        assertEquals(props.startsWith, rs.getLong("start_with"));
        assertEquals(props.incrementBy, rs.getLong("increment_by"));
        assertEquals(props.cacheSize, rs.getLong("cache_size"));
        assertEquals(false, rs.getBoolean("cycle_flag"));
        assertEquals("BULKALLOC", rs.getString("sequence_schema"));
        assertEquals("ALPHA", rs.getString("sequence_name"));
        assertEquals(currentValue, rs.getLong("current_value"));
        assertEquals(props.minValue, rs.getLong("min_value"));
        assertEquals(props.maxValue, rs.getLong("max_value"));
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

        public SequenceProperties(Builder builder) {
            this.numAllocated = builder.numAllocated;
            this.incrementBy = builder.incrementBy;
            this.startsWith = builder.startsWith;
            this.cacheSize = builder.cacheSize;
            this.minValue = builder.minValue;
            this.maxValue = builder.maxValue;
        }

        private static class Builder {

            long maxValue = Long.MAX_VALUE;
            long minValue = Long.MIN_VALUE;
            long numAllocated = 100;
            int incrementBy = 1;
            int startsWith = 1;
            int cacheSize = 100;

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
            
            public SequenceProperties build() {
                return new SequenceProperties(this);
            }

        }

    }
    
}