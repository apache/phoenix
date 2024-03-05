package org.apache.phoenix.cache;

import java.sql.SQLException;

public interface ServerMetadataCache {
    long getLastDDLTimestampForTable(byte[] tenantID, byte[] schemaName, byte[] tableName) throws SQLException;
    void invalidate(byte[] tenantID, byte[] schemaName, byte[] tableName);
}
