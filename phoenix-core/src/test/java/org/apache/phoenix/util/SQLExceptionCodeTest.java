/*
 *  Licensed to the Apache Software Foundation (ASF) under one or more
 *  contributor license agreements.  See the NOTICE file distributed with
 *  this work for additional information regarding copyright ownership.
 *  The ASF licenses this file to You under the Apache License, Version 2.0
 *  (the "License"); you may not use this file except in compliance with
 *  the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package org.apache.phoenix.util;

import org.apache.phoenix.exception.SQLExceptionCode;
import org.apache.phoenix.exception.SQLExceptionInfo;
import org.junit.Assert;
import org.junit.Test;

import java.sql.SQLException;

public class SQLExceptionCodeTest {

    @Test
    public void testOperationTimedOutTest() {
        SQLException sqlException = new SQLExceptionInfo
            .Builder(SQLExceptionCode.OPERATION_TIMED_OUT)
            .setMessage("Test Operation Timedout")
            .setRootCause(new IllegalArgumentException("TestOpsTimeout1"))
            .build().buildException();
        Assert.assertEquals("Test Operation Timedout",
            sqlException.getMessage());
        Assert.assertEquals("TestOpsTimeout1",
            sqlException.getCause().getMessage());
        Assert.assertTrue(sqlException.getCause() instanceof
            IllegalArgumentException);
        Assert.assertEquals(sqlException.getErrorCode(),
            SQLExceptionCode.OPERATION_TIMED_OUT.getErrorCode());
        Assert.assertEquals(sqlException.getSQLState(),
            SQLExceptionCode.OPERATION_TIMED_OUT.getSQLState());
        sqlException = new SQLExceptionInfo
            .Builder(SQLExceptionCode.OPERATION_TIMED_OUT)
            .build().buildException();
        Assert.assertEquals(SQLExceptionCode.OPERATION_TIMED_OUT.getMessage(),
            sqlException.getMessage());
        Assert.assertNull(sqlException.getCause());
        Assert.assertEquals(sqlException.getErrorCode(),
            SQLExceptionCode.OPERATION_TIMED_OUT.getErrorCode());
        Assert.assertEquals(sqlException.getSQLState(),
            SQLExceptionCode.OPERATION_TIMED_OUT.getSQLState());
    }

}
