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
package org.apache.phoenix.schema.types;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.TimeZone;

import org.apache.phoenix.util.DateUtil;

public class PDateArrayToStringTest extends BasePhoenixArrayToStringTest {
    private static final String DATE1 = "2001-01-01 12:15:15.123";
    private static final String DATE2 = "2002-02-02 14:30:30.456";
    private static final String DATE3 = "2003-03-03 16:45:45.789";

    @Override
    protected PDataType getBaseType() {
        return PDate.INSTANCE;
    }

    @Override
    protected boolean isPrimitive(PhoenixArray arr) {
        // dates have codec like primitive times but date is not primitive/scalar && there's no
        // primitive date array
        return false;
    }

    @Override
    protected Object getElement1() {
        return parseDate(DATE1);
    }

    @Override
    protected String getString1() {
        return "'" + DATE1 + "'";
    }

    @Override
    protected Object getElement2() {
        return parseDate(DATE2);
    }

    @Override
    protected String getString2() {
        return "'" + DATE2 + "'";
    }

    @Override
    protected Object getElement3() {
        return parseDate(DATE3);
    }

    @Override
    protected String getString3() {
        return "'" + DATE3 + "'";
    }

    private Object parseDate(String dateString) {
        try {
            java.util.Date date =
                    new SimpleDateFormat(DateUtil.DEFAULT_DATE_FORMAT).parse(dateString);
            Calendar cal = Calendar.getInstance();
            cal.setTimeInMillis(date.getTime());
            cal.add(Calendar.MILLISECOND, TimeZone.getDefault().getOffset(date.getTime()));
            return cal.getTime();
        } catch (ParseException e) {
            throw new RuntimeException(e);
        }
    }

}
