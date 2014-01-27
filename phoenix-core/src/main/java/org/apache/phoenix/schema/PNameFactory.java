package org.apache.phoenix.schema;

import org.apache.hadoop.hbase.util.Bytes;

import org.apache.phoenix.query.QueryConstants;

public class PNameFactory {

    private PNameFactory() {
    }

    public static PName newName(String name) {
        return name == null || name.isEmpty() ? PName.EMPTY_NAME : 
            name.equals(QueryConstants.EMPTY_COLUMN_NAME ) ?  PName.EMPTY_COLUMN_NAME : 
                new PNameImpl(name);
    }
    
    public static PName newName(byte[] bytes) {
        return bytes == null || bytes.length == 0 ? PName.EMPTY_NAME : 
            Bytes.compareTo(bytes, QueryConstants.EMPTY_COLUMN_BYTES) == 0 ? PName.EMPTY_COLUMN_NAME :
                new PNameImpl(bytes);
    }
}
