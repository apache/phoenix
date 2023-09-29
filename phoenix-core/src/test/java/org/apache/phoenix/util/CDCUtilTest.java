package org.apache.phoenix.util;

import org.apache.phoenix.schema.PTable;
import org.junit.Test;

import java.util.Arrays;
import java.util.HashSet;

import static org.apache.phoenix.schema.PTable.CDCChangeScope.*;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class CDCUtilTest {
    @Test
    public void testScopeSetConstruction() throws Exception {
        assertEquals(new HashSet<>(Arrays.asList(PRE)), CDCUtil.makeChangeScopeEnumsFromString(
                "PRE"));
        assertEquals(new HashSet<>(Arrays.asList(PRE)),
                CDCUtil.makeChangeScopeEnumsFromString("PRE,"));
        assertEquals(new HashSet<>(Arrays.asList(PRE)),
                CDCUtil.makeChangeScopeEnumsFromString("PRE, PRE"));
        assertEquals(new HashSet<>(Arrays.asList(PRE)),
                CDCUtil.makeChangeScopeEnumsFromString("PRE,DUMMY"));
        assertEquals(new HashSet<>(Arrays.asList(CHANGE, PRE, POST, LATEST)),
                CDCUtil.makeChangeScopeEnumsFromString("POST,PRE,CHANGE,LATEST"));
    }

    @Test
    public void testScopeStringConstruction() throws Exception {
        assertEquals(null, CDCUtil.makeChangeScopeStringFromEnums(null));
        assertEquals("", CDCUtil.makeChangeScopeStringFromEnums(
                new HashSet<PTable.CDCChangeScope>()));
        assertEquals("CHANGE,PRE,POST,LATEST", CDCUtil.makeChangeScopeStringFromEnums(
                new HashSet<>(Arrays.asList(CHANGE, PRE, POST, LATEST))));
        assertEquals("CHANGE,PRE,POST,LATEST", CDCUtil.makeChangeScopeStringFromEnums(
                new HashSet<>(Arrays.asList(PRE, LATEST, POST, CHANGE))));
    }
}
