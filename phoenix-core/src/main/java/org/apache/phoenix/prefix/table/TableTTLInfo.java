package org.apache.phoenix.prefix.table;

import org.apache.hadoop.hbase.util.Bytes;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;

/**
 * Simple POJO class to hold TTL info
 */
public class TableTTLInfo implements Comparable {
	private final byte[] physicalTableName;
	private final byte[] tenantId;
	private final byte[] tableName;
	private final byte[] prefix;
	private final int ttl;

	public TableTTLInfo(String physicalTableName, String tenantId, String tableName, String prefix, int ttl) {
		super();
		this.physicalTableName = physicalTableName.getBytes(StandardCharsets.UTF_8);
		this.tenantId = tenantId.getBytes(StandardCharsets.UTF_8);
		this.tableName = tableName.getBytes(StandardCharsets.UTF_8);
		this.prefix = prefix.getBytes(StandardCharsets.UTF_8);
		this.ttl = ttl;
	}

	public TableTTLInfo(byte[] physicalTableName, byte[] tenantId, byte[] tableName, byte[] prefix, int ttl) {
		super();
		this.physicalTableName = physicalTableName;
		this.tenantId = tenantId;
		this.prefix = prefix;
		this.tableName = tableName;
		this.ttl = ttl;
	}

	public int getTTL() {
		return ttl;
	}
	public byte[] getTenantId() {
		return tenantId;
	}

	public byte[] getTableName() {
		return tableName;
	}

	public byte[] getPrefix() {
		return prefix;
	}
	public byte[] getPhysicalTableName() {
		return physicalTableName;
	}


	@Override
	public boolean equals(Object o) {
		if (this == o) return true;
		if (o == null || getClass() != o.getClass()) return false;
		TableTTLInfo that = (TableTTLInfo) o;
		return Arrays.equals(physicalTableName, that.physicalTableName) &&
				Arrays.equals(tenantId, that.tenantId) &&
				Arrays.equals(tableName, that.tableName);
	}

	@Override
	public int hashCode() {
		return Arrays.hashCode(tenantId) +  Arrays.hashCode(tableName);
	}
	@Override
	public int compareTo(Object obj) {
		if (this == obj)
			return 0;
		if (obj == null)
			throw new NullPointerException();
		TableTTLInfo other = (TableTTLInfo) obj;
		int result = Bytes.BYTES_COMPARATOR.compare(this.physicalTableName,other.physicalTableName);
		if (result == 0) {
			result = Bytes.BYTES_COMPARATOR.compare(this.tableName,other.tableName);
		}
		if (result == 0)  {
			result = Bytes.BYTES_COMPARATOR.compare(this.tenantId, other.tenantId);
		}
		return result;
	}

	@Override
	public String toString() {
		return "TableTTLInfo {" +
				"physicalTableName=" + Bytes.toString(physicalTableName) +
				", tenantId=" + Bytes.toString(tenantId) +
				", tableName=" + Bytes.toString(tableName) +
				", prefix=" + Bytes.toStringBinary(prefix) +
				", ttl=" + ttl +
				'}';
	}

}
