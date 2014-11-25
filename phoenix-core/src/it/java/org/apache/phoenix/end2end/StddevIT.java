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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;

import org.junit.Test;

public class StddevIT extends BaseHBaseManagedTimeIT {

    @Test
    public void testSTDDEV_POP() throws Exception {
        String tenantId = getOrganizationId();
        initATableValues(tenantId, getDefaultSplits(tenantId), getUrl());

        String query = "SELECT STDDEV_POP(A_INTEGER) FROM aTable";

        Connection conn = DriverManager.getConnection(getUrl());
        try {
            PreparedStatement statement = conn.prepareStatement(query);
            ResultSet rs = statement.executeQuery();
            assertTrue(rs.next());
            BigDecimal stddev = rs.getBigDecimal(1);
            stddev = stddev.setScale(1, RoundingMode.HALF_UP);
            assertEquals(2.6, stddev.doubleValue(),0.0);
            assertFalse(rs.next());
        } finally {
            conn.close();
        }
    }
    
    @Test
    public void testSTDDEV_SAMP() throws Exception {
        String tenantId = getOrganizationId();
        initATableValues(tenantId, getDefaultSplits(tenantId), getUrl());

        String query = "SELECT STDDEV_SAMP(x_decimal) FROM aTable";

        Connection conn = DriverManager.getConnection(getUrl());
        try {
            PreparedStatement statement = conn.prepareStatement(query);
            ResultSet rs = statement.executeQuery();
            assertTrue(rs.next());
            BigDecimal stddev = rs.getBigDecimal(1);
            stddev = stddev.setScale(1, RoundingMode.HALF_UP);
            assertEquals(2.0, stddev.doubleValue(),0.0);
            assertFalse(rs.next());
        } finally {
            conn.close();
        }
    }

    @Test
    public void testSTDDEV_POPOnDecimalColType() throws Exception {
        String tenantId = getOrganizationId();
        initATableValues(tenantId, getDefaultSplits(tenantId), getUrl());

        String query = "SELECT STDDEV_POP(x_decimal) FROM aTable";

        Connection conn = DriverManager.getConnection(getUrl());
        try {
            PreparedStatement statement = conn.prepareStatement(query);
            ResultSet rs = statement.executeQuery();
            assertTrue(rs.next());
            BigDecimal stddev = rs.getBigDecimal(1);
            stddev = stddev.setScale(10, RoundingMode.HALF_UP);
            assertEquals(1.6679994671, stddev.doubleValue(), 0.0);
            assertFalse(rs.next());
        } finally {
            conn.close();
        }
    }

    @Test
    public void testSTDDEV_SAMPOnDecimalColType() throws Exception {
        String tenantId = getOrganizationId();
        initATableValues(tenantId, getDefaultSplits(tenantId), getUrl());

        String query = "SELECT STDDEV_SAMP(x_decimal) FROM aTable";

        Connection conn = DriverManager.getConnection(getUrl());
        try {
            PreparedStatement statement = conn.prepareStatement(query);
            ResultSet rs = statement.executeQuery();
            assertTrue(rs.next());
            BigDecimal stddev = rs.getBigDecimal(1);
            stddev = stddev.setScale(10, RoundingMode.HALF_UP);
            assertEquals(2.0428737928, stddev.doubleValue(), 0.0);
            assertFalse(rs.next());
        } finally {
            conn.close();
        }
    }

}
