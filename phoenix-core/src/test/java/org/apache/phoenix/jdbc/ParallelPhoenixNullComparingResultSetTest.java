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
package org.apache.phoenix.jdbc;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.when;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Properties;
import java.util.concurrent.CompletableFuture;

import org.apache.phoenix.exception.SQLExceptionCode;
import org.apache.phoenix.jdbc.HighAvailabilityGroup.HAGroupInfo;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;
import org.mockito.stubbing.Answer;

public class ParallelPhoenixNullComparingResultSetTest {

    ParallelPhoenixContext context;
    ResultSet rs1;
    ResultSet rs2;
    CompletableFuture<ResultSet> completableRs1;
    CompletableFuture<ResultSet> completableRs2;

    ParallelPhoenixResultSet resultSet;

    @Before
    public void init() {
        HAGroupInfo haGroupInfo = new HAGroupInfo("test", "test1", "test2");
        context = new ParallelPhoenixContext(new Properties(),
                new HighAvailabilityGroup(haGroupInfo,
                        Mockito.mock(Properties.class),
                        Mockito.mock(ClusterRoleRecord.class),
                        HighAvailabilityGroup.State.READY),
                HighAvailabilityTestingUtility.getListOfSingleThreadExecutorServices(), null);
        rs1 = Mockito.mock(ResultSet.class);
        rs2 = Mockito.mock(ResultSet.class);
        completableRs1 = CompletableFuture.completedFuture(rs1);
        completableRs2 = CompletableFuture.completedFuture(rs2);
        resultSet = new ParallelPhoenixResultSet(context, completableRs1, completableRs2);
    }

    @Test
    public void testRs1Null() throws SQLException {
        when(rs1.next()).thenReturn(false);
        when(rs2.next()).thenReturn(true);
        ParallelPhoenixNullComparingResultSet ncrs =
                new ParallelPhoenixNullComparingResultSet(context, completableRs1, completableRs2);
        assertNull(ncrs.getResultSet());
        assertTrue(ncrs.next());
        assertEquals(rs2, ncrs.getResultSet());
        Mockito.verify(rs2).next();
    }

    @Test
    public void testRs2Null() throws SQLException {
        when(rs1.next()).thenReturn(true);
        when(rs2.next()).thenReturn(false);
        ParallelPhoenixNullComparingResultSet ncrs =
                new ParallelPhoenixNullComparingResultSet(context, completableRs1, completableRs2);
        assertNull(ncrs.getResultSet());
        assertTrue(ncrs.next());
        assertEquals(rs1, ncrs.getResultSet());
        Mockito.verify(rs1).next();
    }

    @Test
    public void testRs1Rs2Null() throws SQLException {
        when(rs1.next()).thenReturn(false);
        when(rs2.next()).thenReturn(false);
        ParallelPhoenixNullComparingResultSet ncrs =
                new ParallelPhoenixNullComparingResultSet(context, completableRs1, completableRs2);
        assertNull(ncrs.getResultSet());
        assertFalse(ncrs.next());
        assertTrue(rs1 == ncrs.getResultSet() || rs2 == ncrs.getResultSet());
        Mockito.verify(rs1).next();
        Mockito.verify(rs2).next();
    }

    @Test
    public void testRs1ExceptionRs2Null() throws SQLException {
        when(rs1.next()).thenThrow(new RuntimeException());
        when(rs2.next()).thenReturn(false);
        ParallelPhoenixNullComparingResultSet ncrs =
                new ParallelPhoenixNullComparingResultSet(context, completableRs1, completableRs2);
        assertNull(ncrs.getResultSet());
        assertFalse(ncrs.next());
        assertEquals(rs2, ncrs.getResultSet());
        Mockito.verify(rs1).next();
        Mockito.verify(rs2).next();
    }

    @Test
    public void testRs2Exception() throws SQLException {
        when(rs1.next()).thenReturn(true);
        when(rs2.next()).thenThrow(new RuntimeException());
        ParallelPhoenixNullComparingResultSet ncrs =
                new ParallelPhoenixNullComparingResultSet(context, completableRs1, completableRs2);
        assertNull(ncrs.getResultSet());
        assertTrue(ncrs.next());
        assertEquals(rs1, ncrs.getResultSet());
        Mockito.verify(rs1).next();
    }

    @Test
    public void testRs1Rs2Exception() throws SQLException {
        when(rs1.next()).thenThrow(new SQLException());
        when(rs2.next()).thenThrow(new SQLException());
        ParallelPhoenixNullComparingResultSet ncrs =
                new ParallelPhoenixNullComparingResultSet(context, completableRs1, completableRs2);
        assertNull(ncrs.getResultSet());
        try {
            ncrs.next();
            fail("RS should've thrown exception");
        } catch (SQLException e) {
        }
        Mockito.verify(rs1).next();
        Mockito.verify(rs2).next();
    }

    @Test
    public void testErrorOnSingleNullRs1Null() throws SQLException {
        when(rs1.next()).thenReturn(false);
        when(rs2.next()).thenThrow(new RuntimeException());
        context.getProperties().setProperty(
                ParallelPhoenixNullComparingResultSet.ERROR_ON_SINGLE_NULL_ATTRIB, "true");
        ParallelPhoenixNullComparingResultSet ncrs =
                new ParallelPhoenixNullComparingResultSet(context, completableRs1, completableRs2);
        assertNull(ncrs.getResultSet());
        try {
            ncrs.next();
            fail("RS should've thrown exception");
        } catch (SQLException e) {
            assertEquals(SQLExceptionCode.HA_READ_FROM_CLUSTER_FAILED_ON_NULL.getErrorCode(),
                    e.getErrorCode());
        }
        Mockito.verify(rs1).next();
        Mockito.verify(rs2).next();
    }

    @Test
    public void testErrorOnSingleNullRs2Null() throws SQLException {
        when(rs1.next()).thenThrow(new RuntimeException());
        when(rs2.next()).thenReturn(false);
        context.getProperties().setProperty(
                ParallelPhoenixNullComparingResultSet.ERROR_ON_SINGLE_NULL_ATTRIB, "true");
        ParallelPhoenixNullComparingResultSet ncrs =
                new ParallelPhoenixNullComparingResultSet(context, completableRs1, completableRs2);
        assertNull(ncrs.getResultSet());
        try {
            ncrs.next();
            fail("RS should've thrown exception");
        } catch (SQLException e) {
            assertEquals(SQLExceptionCode.HA_READ_FROM_CLUSTER_FAILED_ON_NULL.getErrorCode(),
                    e.getErrorCode());
        }
        Mockito.verify(rs1).next();
        Mockito.verify(rs2).next();
    }

    @Test
    public void testReadValueAfterWaitRs2Null() throws SQLException {
        Answer<Boolean> answer = (i -> {
            Thread.sleep(2000);
            return true;
        });
        doAnswer(answer).when(rs1).next();
        when(rs1.getString(0)).thenReturn("test");
        when(rs2.next()).thenReturn(false);
        ParallelPhoenixNullComparingResultSet ncrs =
                new ParallelPhoenixNullComparingResultSet(context, completableRs1, completableRs2);
        assertTrue(ncrs.next());
        assertEquals(rs1, ncrs.getResultSet());
        assertEquals("test", ncrs.getString(0));
    }
}