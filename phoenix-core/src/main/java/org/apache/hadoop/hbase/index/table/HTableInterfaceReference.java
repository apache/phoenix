package org.apache.hadoop.hbase.index.table;

import org.apache.hadoop.hbase.util.Bytes;

import org.apache.hadoop.hbase.index.util.ImmutableBytesPtr;

/**
 * Reference to an HTableInterface. Currently, its pretty simple in that it is just a wrapper around
 * the table name.
 */
public class HTableInterfaceReference {

  private ImmutableBytesPtr tableName;


  public HTableInterfaceReference(ImmutableBytesPtr tableName) {
    this.tableName = tableName;
  }

  public ImmutableBytesPtr get() {
    return this.tableName;
  }

  public String getTableName() {
    return Bytes.toString(this.tableName.get(),this.tableName.getOffset(), this.tableName.getLength());
  }

  @Override
  public int hashCode() {
      return tableName.hashCode();
  }

  @Override
  public boolean equals(Object obj) {
      if (this == obj) return true;
      if (obj == null) return false;
      if (getClass() != obj.getClass()) return false;
      HTableInterfaceReference other = (HTableInterfaceReference)obj;
      return tableName.equals(other.tableName);
  }

  @Override
  public String toString() {
    return Bytes.toString(this.tableName.get());
  }
}