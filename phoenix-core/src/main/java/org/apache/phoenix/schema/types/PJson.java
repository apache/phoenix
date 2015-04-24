package org.apache.phoenix.schema.types;

import java.text.Format;

import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.phoenix.schema.SortOrder;
import org.apache.phoenix.util.ByteUtil;
import org.apache.phoenix.util.StringUtil;


public class PJson extends PDataType<String> {
	public static final PJson INSTANCE = new PJson();

	  private PJson() {
	    super("JSON", 21, String.class, null, 48);
	  }

	  @Override
	  public byte[] toBytes(Object object) {
	    // TODO: consider using avro UTF8 object instead of String
	    // so that we get get the size easily
	    if (object == null) {
	      return ByteUtil.EMPTY_BYTE_ARRAY;
	    }
	    return Bytes.toBytes((String) object);
	  }

	  @Override
	  public int toBytes(Object object, byte[] bytes, int offset) {
	    if (object == null) {
	      return 0;
	    }
	    byte[] b = toBytes(object); // TODO: no byte[] allocation: use CharsetEncoder
	    System.arraycopy(b, 0, bytes, offset, b.length);
	    return b.length;
	  }

	  @Override
	  public Object toObject(byte[] bytes, int offset, int length, PDataType actualType,
	      SortOrder sortOrder, Integer maxLength, Integer scale) {
	    if (!actualType.isCoercibleTo(this)) {
	      throwConstraintViolationException(actualType, this);
	    }
	    if (length == 0) {
	      return null;
	    }
	    if (sortOrder == SortOrder.DESC) {
	      bytes = SortOrder.invert(bytes, offset, length);
	      offset = 0;
	    }
	    return Bytes.toString(bytes, offset, length);
	  }

	  @Override
	  public Object toObject(Object object, PDataType actualType) {
	    if (equalsAny(actualType, this, PVarchar.INSTANCE,PChar.INSTANCE)) {
	      String s = (String) object;
	      return s == null || s.length() > 0 ? s : null;
	    }
	    return throwConstraintViolationException(actualType, this);
	  }

	  @Override
	  public boolean isCoercibleTo(PDataType targetType) {
	    return equalsAny(targetType, this,PVarchar.INSTANCE ,PChar.INSTANCE, PVarbinary.INSTANCE, PBinary.INSTANCE);
	  }

	  @Override
	  public boolean isSizeCompatible(ImmutableBytesWritable ptr, Object value, PDataType srcType,
	      Integer maxLength, Integer scale, Integer desiredMaxLength,
	      Integer desiredScale) {
	    if (ptr.getLength() != 0 && maxLength != null && desiredMaxLength != null) {
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
	    String value = (String) o;
	    return value == null ? 1 : value.length();
	  }

	  @Override
	  public Integer getByteSize() {
	    return null;
	  }

	  @Override
	  public int compareTo(Object lhs, Object rhs, PDataType rhsType) {
	    return ((String) lhs).compareTo((String) rhs);
	  }

	  @Override
	  public Object toObject(String value) {
	    return value;
	  }

	  @Override
	  public boolean isBytesComparableWith(PDataType otherType) {
	    return super.isBytesComparableWith(otherType) || otherType == PVarchar.INSTANCE|| otherType == PChar.INSTANCE;
	  }

	  @Override
	  public String toStringLiteral(Object o, Format formatter) {
	    if (formatter != null) {
	      return "'" + formatter.format(o) + "'";
	    }
	    return "'" + StringUtil.escapeStringConstant(o.toString()) + "'";
	  }
	  
	  @Override
	  public Object getSampleValue(Integer maxLength, Integer arrayLength) {
	    return new String("{\""+String.valueOf((char)RANDOM.get().nextInt(26)+97)+"\":"+RANDOM.get().nextInt()+"}");
	  }
}
