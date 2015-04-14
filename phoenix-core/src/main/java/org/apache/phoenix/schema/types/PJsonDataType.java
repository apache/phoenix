package org.apache.phoenix.schema.types;

import java.io.IOException;
import java.sql.Types;
import java.text.Format;

import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.phoenix.exception.SQLExceptionCode;
import org.apache.phoenix.exception.SQLExceptionInfo;
import org.apache.phoenix.schema.IllegalDataException;
import org.apache.phoenix.schema.SortOrder;
import org.apache.phoenix.schema.json.PhoenixJson;
import org.apache.phoenix.util.ByteUtil;
import org.apache.phoenix.util.StringUtil;
import org.codehaus.jackson.JsonParseException;

import com.google.common.base.Preconditions;

/**
 * <p>
 * A Phoenix data type to represent JSON. The json data type stores an exact
 * copy of the input text, which processing functions must reparse on each
 * execution. Because the json type stores an exact copy of the input text, it
 * will preserve semantically-insignificant white space between tokens, as well
 * as the order of keys within JSON objects. Also, if a JSON object within the
 * value contains the same key more than once, all the key/value pairs are kept.
 * It stores the data as string in single column of HBase and it has same data
 * size limit as Phoenix's Varchar.
 * <p>
 * JSON data types are for storing JSON (JavaScript Object Notation) data, as
 * specified in RFC 7159. Such data can also be stored as text, but the JSON
 * data types have the advantage of enforcing that each stored value is valid
 * according to the JSON rules.
 */
public class PJsonDataType extends PDataType<String> {

	public static final PJsonDataType INSTANCE = new PJsonDataType();

	PJsonDataType() {
		super("JSON", Types.OTHER, PhoenixJson.class, null, 48);
	}

	@Override
	public int toBytes(Object object, byte[] bytes, int offset) {

		if (object == null) {
			return 0;
		}
		byte[] b = toBytes(object);
		System.arraycopy(b, 0, bytes, offset, b.length);
		return b.length;

	}

	@Override
	public byte[] toBytes(Object object) {
		if (object == null) {
			return ByteUtil.EMPTY_BYTE_ARRAY;
		}
		PhoenixJson phoenixJson = (PhoenixJson) object;
		return PVarchar.INSTANCE.toBytes(phoenixJson.toString());
	}

	@Override
	public Object toObject(byte[] bytes, int offset, int length,
			@SuppressWarnings("rawtypes") PDataType actualType,
			SortOrder sortOrder, Integer maxLength, Integer scale) {

		if (!actualType.isCoercibleTo(this)) {
			throwConstraintViolationException(actualType, this);
		}
		if (length == 0) {
			return null;
		}
		return getPhoenixJson(bytes, offset, length);

	}

	@Override
	public Object toObject(Object object,
			@SuppressWarnings("rawtypes") PDataType actualType) {
		if (object == null) {
			return null;
		}
		if (equalsAny(actualType, PJsonDataType.INSTANCE)) {
			return object;
		}
		if (equalsAny(actualType, PVarchar.INSTANCE)) {
			return getJsonFromVarchar(object, actualType);
		}
		return throwConstraintViolationException(actualType, this);
	}

	@Override
	public boolean isCoercibleTo(
			@SuppressWarnings("rawtypes") PDataType targetType) {
		return equalsAny(targetType, this, PVarchar.INSTANCE);

	}

	@Override
	public boolean isCoercibleTo(
			@SuppressWarnings("rawtypes") PDataType targetType, Object value) {
		return isCoercibleTo(targetType);
	}

	@Override
	public boolean isSizeCompatible(ImmutableBytesWritable ptr, Object value,
			@SuppressWarnings("rawtypes") PDataType srcType, Integer maxLength,
			Integer scale, Integer desiredMaxLength, Integer desiredScale) {
		if (ptr.getLength() != 0 && maxLength != null
				&& desiredMaxLength != null) {
			return maxLength <= desiredMaxLength;
		}
		return true;
	}

	@Override
	public boolean isFixedWidth() {
		return false;
	}

	@Override
	public int estimateByteSize(Object o) {
		PhoenixJson phoenixJson = (PhoenixJson) o;
		return phoenixJson.toString().length();
	}

	@Override
	public Integer getByteSize() {
		return null;
	}

	@Override
	public int compareTo(Object lhs, Object rhs,
			@SuppressWarnings("rawtypes") PDataType rhsType) {
		if (PJsonDataType.INSTANCE != rhsType) {
			throwConstraintViolationException(rhsType, this);
		}
		PhoenixJson phoenixJsonLHS = (PhoenixJson) lhs;
		PhoenixJson phoenixJsonRHS = (PhoenixJson) rhs;
		return PVarchar.INSTANCE.compareTo(phoenixJsonLHS.toString(),
				phoenixJsonRHS.toString(), rhsType);
	}

	@Override
	public Object toObject(String value) {
		byte[] jsonData = Bytes.toBytes(value);
		return getPhoenixJson(jsonData, 0, jsonData.length);
	}

	@Override
	public boolean isBytesComparableWith(
			@SuppressWarnings("rawtypes") PDataType otherType) {
		return otherType == PJsonDataType.INSTANCE
				|| otherType == PVarchar.INSTANCE;
	}

	@Override
	public String toStringLiteral(Object o, Format formatter) {
		if (o == null) {
			return StringUtil.EMPTY_STRING;
		}
		PhoenixJson phoenixJson = (PhoenixJson) o;
		return PVarchar.INSTANCE.toStringLiteral(phoenixJson.toString(),
				formatter);
	}

	@Override
	public void coerceBytes(ImmutableBytesWritable ptr, Object o,
			@SuppressWarnings("rawtypes") PDataType actualType,
			Integer actualMaxLength, Integer actualScale,
			SortOrder actualModifier, Integer desiredMaxLength,
			Integer desiredScale, SortOrder expectedModifier) {
		Preconditions.checkNotNull(actualModifier);
		Preconditions.checkNotNull(expectedModifier);
		if (ptr.getLength() == 0) {
			return;
		}
		if (this.isBytesComparableWith(actualType)) { // No coerce necessary
			return;
		}

		// Optimization for cases in which we already have the object around
		if (o == null) {
			o = actualType.toObject(ptr, actualType, actualModifier);
		}

		o = toObject(o, actualType);
		byte[] b = toBytes(o, expectedModifier);
		ptr.set(b);
	}

	@Override
	public Object getSampleValue(Integer maxLength, Integer arrayLength) {
		Preconditions.checkArgument(maxLength == null || maxLength >= 0);

		char[] key = new char[4];
		char[] value = new char[4];
		int length = maxLength != null ? maxLength : 1;
		if (length > (key.length + value.length)) {
			key = new char[length + 2];
			value = new char[length - key.length];
		}
		int j = 1;
		key[0] = '"';
		key[j++] = 'k';
		for (int i = 2; i < key.length - 1; i++) {
			key[j++] = (char) ('0' + RANDOM.get().nextInt(Byte.MAX_VALUE) % 10);
		}
		key[j] = '"';

		int k = 1;
		value[0] = '"';
		value[k++] = 'v';
		for (int i = 2; i < value.length - 1; i++) {
			value[k++] = (char) ('0' + RANDOM.get().nextInt(Byte.MAX_VALUE) % 10);
		}
		value[k] = '"';
		StringBuilder sbr = new StringBuilder();
		sbr.append("{").append(key).append(":").append(value).append("}");

		byte[] bytes = Bytes.toBytes(sbr.toString());
		return getPhoenixJson(bytes, 0, bytes.length);
	}

	private Object getJsonFromVarchar(Object object,
			@SuppressWarnings("rawtypes") PDataType actualType) {
		String s = (String) object;
		if (s.length() > 0) {
			byte[] jsonData = Bytes.toBytes(s);
			return getPhoenixJson(jsonData, 0, jsonData.length);
		} else {
			return null;
		}

	}

	private Object getPhoenixJson(byte[] bytes, int offset, int length) {
		try {
			return PhoenixJson.getPhoenixJson(bytes, offset, length);
		} catch (JsonParseException jpe) {
			throw new IllegalDataException(new SQLExceptionInfo.Builder(
					SQLExceptionCode.ILLEGAL_DATA)
					.setMessage(
							"json value cannot be parsed: " + jpe.getMessage())
					.build().buildException());
		} catch (IOException e) {
			throw new IllegalDataException(new SQLExceptionInfo.Builder(
					SQLExceptionCode.IO_EXCEPTION)
					.setMessage(
							"exception while parsing json : " + e.getMessage())
					.build().buildException());
		}
	}

}
