package org.apache.phoenix.schema.types;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.io.IOException;

import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.phoenix.exception.SQLExceptionCode;
import org.apache.phoenix.schema.ConstraintViolationException;
import org.apache.phoenix.schema.SortOrder;
import org.apache.phoenix.schema.TypeMismatchException;
import org.apache.phoenix.schema.json.PhoenixJson;
import org.apache.phoenix.schema.json.PhoenixJsonTest;
import org.junit.Test;

/**
 * Unit test for {@link PJsonDataType}.
 *
 */
public class PJsonDataTypeTest {

	final byte[] json = PhoenixJsonTest.TEST_JSON_STR.getBytes();

	@Test
	public void testToBytesWithOffset() throws Exception {
		PhoenixJson phoenixJson = PhoenixJson.getPhoenixJson(json, 0,
				json.length);

		byte[] bytes = new byte[json.length];

		assertEquals(json.length,
				PJsonDataType.INSTANCE.toBytes(phoenixJson, bytes, 0));

		assertArrayEquals(json, bytes);
	}

	@Test
	public void testToBytes() throws Exception {
		PhoenixJson phoenixJson = PhoenixJson.getPhoenixJson(json, 0,
				json.length);

		byte[] bytes = PJsonDataType.INSTANCE.toBytes(phoenixJson);
		assertArrayEquals(json, bytes);
	}

	@Test
	public void testToObjectWithSortOrder() {
		Object object = PJsonDataType.INSTANCE.toObject(json, 0, json.length,
				PVarchar.INSTANCE, SortOrder.ASC, Integer.MAX_VALUE,
				Integer.MAX_VALUE);
		PhoenixJson phoenixJson = (PhoenixJson) object;
		assertEquals(PhoenixJsonTest.TEST_JSON_STR, phoenixJson.toString());
	}

	@Test
	public void testToObject() {
		Object object = PJsonDataType.INSTANCE.toObject(json, 0, json.length,
				PVarchar.INSTANCE, SortOrder.ASC, Integer.MAX_VALUE,
				Integer.MAX_VALUE);
		PhoenixJson phoenixJson = (PhoenixJson) object;
		assertEquals(PhoenixJsonTest.TEST_JSON_STR, phoenixJson.toString());

		Object object2 = PJsonDataType.INSTANCE.toObject(phoenixJson,
				PJsonDataType.INSTANCE);
		assertEquals(phoenixJson, object2);

		Object object3 = PJsonDataType.INSTANCE.toObject(
				PhoenixJsonTest.TEST_JSON_STR, PVarchar.INSTANCE);
		assertEquals(phoenixJson, object2);

		try {
			Object object4 = PJsonDataType.INSTANCE.toObject(
					PhoenixJsonTest.TEST_JSON_STR, PChar.INSTANCE);
		} catch (ConstraintViolationException sqe) {
			TypeMismatchException e = (TypeMismatchException) sqe.getCause();
			assertEquals(SQLExceptionCode.TYPE_MISMATCH.getErrorCode(),
					e.getErrorCode());
		}

		PhoenixJson object4 = (PhoenixJson) PJsonDataType.INSTANCE
				.toObject(PhoenixJsonTest.TEST_JSON_STR);
		assertEquals(phoenixJson, object4);

	}

	@Test
	public void isFixedWidth() {
		assertFalse(PJsonDataType.INSTANCE.isFixedWidth());
	}

	public void getByteSize() {
		assertNull(PJsonDataType.INSTANCE.getByteSize());
	}

	public void estimateByteSize() throws Exception {
		PhoenixJson phoenixJson = PhoenixJson.getPhoenixJson(json, 0,
				json.length);
		assertEquals(PhoenixJsonTest.TEST_JSON_STR.length(),
				PJsonDataType.INSTANCE.estimateByteSize(phoenixJson));
	}

	@Test
	public void compareTo() throws Exception {
		PhoenixJson phoenixJson1 = PhoenixJson.getPhoenixJson(json, 0,
				json.length);

		PhoenixJson phoenixJson2 = PhoenixJson.getPhoenixJson(json, 0,
				json.length);

		assertEquals(0, PJsonDataType.INSTANCE.compareTo(phoenixJson1,
				phoenixJson2, PJsonDataType.INSTANCE));

		try {
			PJsonDataType.INSTANCE.compareTo(phoenixJson1,
					PhoenixJsonTest.TEST_JSON_STR, PVarchar.INSTANCE);
		} catch (ConstraintViolationException cve) {
			TypeMismatchException e = (TypeMismatchException) cve.getCause();
			assertEquals(SQLExceptionCode.TYPE_MISMATCH.getErrorCode(),
					e.getErrorCode());
		}
	}

	@Test
	public void isBytesComparableWith() {
		assertTrue(PJsonDataType.INSTANCE
				.isBytesComparableWith(PJsonDataType.INSTANCE));
		assertTrue(PJsonDataType.INSTANCE
				.isBytesComparableWith(PVarchar.INSTANCE));
		assertFalse(PJsonDataType.INSTANCE
				.isBytesComparableWith(PChar.INSTANCE));
	}

	@Test
	public void toStringLiteral() throws Exception {
		PhoenixJson phoenixJson = PhoenixJson.getPhoenixJson(json, 0,
				json.length);
		String stringLiteral = PJsonDataType.INSTANCE.toStringLiteral(
				phoenixJson, null);
		assertEquals(PVarchar.INSTANCE.toStringLiteral(
				PhoenixJsonTest.TEST_JSON_STR, null), stringLiteral);
	}

	@Test
	public void coerceBytes() throws IOException {
		PhoenixJson phoenixJson = PhoenixJson.getPhoenixJson(json, 0,
				json.length);
		ImmutableBytesWritable ptr = new ImmutableBytesWritable();
		ptr.set(json, 0, 0);
		PJsonDataType.INSTANCE.coerceBytes(ptr, phoenixJson,
				PJsonDataType.INSTANCE, json.length, new Integer(10),
				SortOrder.ASC, json.length, new Integer(10), SortOrder.ASC);
		assertEquals(0, ptr.getLength());

		ptr.set(json);
		PJsonDataType.INSTANCE.coerceBytes(ptr, phoenixJson, PVarchar.INSTANCE,
				json.length, new Integer(10), SortOrder.ASC, json.length,
				new Integer(10), SortOrder.ASC);
		assertArrayEquals(json, ptr.get());
	}

}
