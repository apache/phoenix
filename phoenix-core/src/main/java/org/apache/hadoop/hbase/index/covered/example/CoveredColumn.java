package org.apache.hadoop.hbase.index.covered.example;

import org.apache.hadoop.hbase.util.Bytes;

import org.apache.hadoop.hbase.index.covered.update.ColumnReference;

/**
 * A single Column (either a Column Family or a full Family:Qualifier pair) in a {@link ColumnGroup}
 * . If no column qualifier is specified (null), matches all known qualifiers of the family.
 */
public class CoveredColumn extends ColumnReference {

  public static final String SEPARATOR = ":";
  String familyString;
  private final int hashCode;

  public CoveredColumn(byte[] family, byte[] qualifier){
    this(Bytes.toString(family), qualifier);
  }

  public CoveredColumn(String family, byte[] qualifier) {
    super(Bytes.toBytes(family), qualifier == null ? ColumnReference.ALL_QUALIFIERS : qualifier);
    this.familyString = family;
    this.hashCode = calcHashCode(family, qualifier);
  }

  public static CoveredColumn parse(String spec) {
    int sep = spec.indexOf(SEPARATOR);
    if (sep < 0) {
      throw new IllegalArgumentException(spec + " is not a valid specifier!");
    }
    String family = spec.substring(0, sep);
    String qual = spec.substring(sep + 1);
    byte[] column = qual.length() == 0 ? null : Bytes.toBytes(qual);
    return new CoveredColumn(family, column);
  }

  public String serialize() {
    return CoveredColumn.serialize(familyString, qualifier);
  }

  public static String serialize(String first, byte[] second) {
    String nextValue = first + CoveredColumn.SEPARATOR;
    if (second != null) {
      nextValue += Bytes.toString(second);
    }
    return nextValue;
  }

  /**
   * @param family2 to check
   * @return <tt>true</tt> if the passed family matches the family this column covers
   */
  public boolean matchesFamily(String family2) {
    return this.familyString.equals(family2);
  }

  @Override
  public int hashCode() {
    return hashCode;
  }

  private static int calcHashCode(String familyString, byte[] qualifier) {
    final int prime = 31;
    int result = 1;
    result = prime * result + familyString.hashCode();
    if (qualifier != null) {
      result = prime * result + Bytes.hashCode(qualifier);
    }
    return result;
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj) return true;
    if (!super.equals(obj)) return false;
    if (getClass() != obj.getClass()) return false;
    CoveredColumn other = (CoveredColumn) obj;
    if (hashCode != other.hashCode) return false;
    if (!familyString.equals(other.familyString)) return false;
    return Bytes.equals(qualifier, other.qualifier);
  }

  @Override
  public String toString() {
    String qualString = qualifier == null ? "null" : Bytes.toString(qualifier);
    return "CoveredColumn:[" + familyString + ":" + qualString + "]";
  }
}