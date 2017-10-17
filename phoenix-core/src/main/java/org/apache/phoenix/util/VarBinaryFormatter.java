package org.apache.phoenix.util;

import java.text.FieldPosition;
import java.text.Format;
import java.text.ParsePosition;
import java.util.FormatFlagsConversionMismatchException;

import org.apache.commons.codec.binary.Hex;
import org.apache.phoenix.schema.types.PVarbinary;

public class VarBinaryFormatter extends Format {

	/**
	 * 
	 */
	private static final long serialVersionUID = -7940880118392024750L;

    public static final VarBinaryFormatter INSTANCE = new VarBinaryFormatter();
	
	@Override
	public StringBuffer format(Object obj, StringBuffer toAppendTo, FieldPosition pos) {
		if (!(obj instanceof byte[])) {
			throw new IllegalArgumentException("VarBinaryFormatter can only format byte arrays");
		}
		String hexString = Hex.encodeHexString((byte[])obj);
		toAppendTo.append(hexString);
		return toAppendTo;
	}

	@Override
	public Object parseObject(String source, ParsePosition pos) {
		return new UnsupportedOperationException();
	}

}
