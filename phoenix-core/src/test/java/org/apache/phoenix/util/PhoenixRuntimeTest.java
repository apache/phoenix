package org.apache.phoenix.util;

import static org.junit.Assert.assertEquals;

import java.sql.Connection;
import java.sql.Date;
import java.sql.DriverManager;
import java.util.Arrays;

import org.apache.phoenix.query.BaseConnectionlessQueryTest;
import org.junit.Test;

public class PhoenixRuntimeTest extends BaseConnectionlessQueryTest {
    @Test
    public void testEncodeDecode() throws Exception {
        Connection conn = DriverManager.getConnection(getUrl());
        conn.createStatement().execute(
                "CREATE TABLE t(org_id CHAR(3) not null, p_id CHAR(3) not null, date DATE not null, e_id CHAR(3) not null, old_value VARCHAR, new_value VARCHAR " +
                "CONSTRAINT pk PRIMARY KEY (org_id, p_id, date, e_id))");
        Date date = new Date(System.currentTimeMillis());
        Object[] expectedValues = new Object[] {"abc", "def", date, "123"};
        byte[] value = PhoenixRuntime.encodePK(conn, "T", expectedValues);
        Object[] actualValues = PhoenixRuntime.decodePK(conn, "T", value);
        assertEquals(Arrays.asList(expectedValues), Arrays.asList(actualValues));
    }
}
