package org.apache.phoenix.query;

import static org.apache.phoenix.query.KeyRange.EMPTY_RANGE;
import static org.apache.phoenix.query.KeyRange.EVERYTHING_RANGE;
import static org.apache.hadoop.hbase.util.Bytes.toBytes;

import java.util.Arrays;
import java.util.Collection;

import junit.framework.TestCase;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

import org.apache.phoenix.schema.PDataType;

@RunWith(Parameterized.class)
public class KeyRangeIntersectTests extends TestCase {
    private final KeyRange a, b, intersection;

    public KeyRangeIntersectTests(KeyRange a, KeyRange b, KeyRange intersection) {
        this.a = a;
        this.b = b;
        this.intersection = intersection;
    }

    @Parameters(name="intersection of {0} and {1} is {2}")
    public static Collection<?> data() {
        return Arrays.asList(new Object[][] {
                {
                    PDataType.CHAR.getKeyRange(toBytes("C"), true, toBytes("E"), true),
                    PDataType.CHAR.getKeyRange(toBytes("D"), true, toBytes("F"), true),
                    PDataType.CHAR.getKeyRange(toBytes("D"), true, toBytes("E"), true)
                },
                {
                    PDataType.CHAR.getKeyRange(toBytes("C"), true, toBytes("E"), true),
                    PDataType.CHAR.getKeyRange(toBytes("D"), false, toBytes("F"), true),
                    PDataType.CHAR.getKeyRange(toBytes("D"), false, toBytes("E"), true)
                },
                {
                    PDataType.CHAR.getKeyRange(toBytes("C"), true, toBytes("E"), false),
                    PDataType.CHAR.getKeyRange(toBytes("D"), false, toBytes("F"), true),
                    PDataType.CHAR.getKeyRange(toBytes("D"), false, toBytes("E"), false)
                },
                {
                    PDataType.CHAR.getKeyRange(toBytes("C"), true, toBytes("E"), false),
                    PDataType.CHAR.getKeyRange(toBytes("E"), false, toBytes("F"), true),
                    EMPTY_RANGE
                },
                {
                    EVERYTHING_RANGE,
                    PDataType.CHAR.getKeyRange(toBytes("E"), false, toBytes("F"), true),
                    PDataType.CHAR.getKeyRange(toBytes("E"), false, toBytes("F"), true),
                },
                {
                    EVERYTHING_RANGE,
                    EVERYTHING_RANGE,
                    EVERYTHING_RANGE,
                },
                {
                    EMPTY_RANGE,
                    EVERYTHING_RANGE,
                    EMPTY_RANGE
                },
                {
                    EMPTY_RANGE,
                    PDataType.CHAR.getKeyRange(toBytes("E"), false, toBytes("F"), true),
                    EMPTY_RANGE
                },
        });
    }
    @Test
    public void intersect() {
        assertEquals(intersection, a.intersect(b));
        assertEquals(intersection, b.intersect(a));
    }
}
