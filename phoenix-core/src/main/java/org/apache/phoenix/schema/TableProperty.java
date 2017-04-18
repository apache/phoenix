/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.phoenix.schema;

import static org.apache.phoenix.exception.SQLExceptionCode.CANNOT_ALTER_PROPERTY;
import static org.apache.phoenix.exception.SQLExceptionCode.COLUMN_FAMILY_NOT_ALLOWED_FOR_TTL;
import static org.apache.phoenix.exception.SQLExceptionCode.COLUMN_FAMILY_NOT_ALLOWED_TABLE_PROPERTY;
import static org.apache.phoenix.exception.SQLExceptionCode.DEFAULT_COLUMN_FAMILY_ONLY_ON_CREATE_TABLE;
import static org.apache.phoenix.exception.SQLExceptionCode.SALT_ONLY_ON_CREATE_TABLE;
import static org.apache.phoenix.exception.SQLExceptionCode.VIEW_WITH_PROPERTIES;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.DEFAULT_COLUMN_FAMILY_NAME;

import java.sql.SQLException;
import java.util.Map;

import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.phoenix.exception.SQLExceptionCode;
import org.apache.phoenix.exception.SQLExceptionInfo;
import org.apache.phoenix.jdbc.PhoenixDatabaseMetaData;
import org.apache.phoenix.schema.PTable.ImmutableStorageScheme;
import org.apache.phoenix.util.SchemaUtil;

public enum TableProperty {
    
    @Deprecated // use the IMMUTABLE keyword while creating the table
	IMMUTABLE_ROWS(PhoenixDatabaseMetaData.IMMUTABLE_ROWS, true, true, false) {
        @Override
        public Object getPTableValue(PTable table) {
            return table.isImmutableRows();
        }
    },

	MULTI_TENANT(PhoenixDatabaseMetaData.MULTI_TENANT, true, false, false) {
        @Override
        public Object getPTableValue(PTable table) {
            return table.isMultiTenant();
        }
    },

	DISABLE_WAL(PhoenixDatabaseMetaData.DISABLE_WAL, true, false, false) {
        @Override
        public Object getPTableValue(PTable table) {
            return table.isWALDisabled();
        }
    },

	SALT_BUCKETS(PhoenixDatabaseMetaData.SALT_BUCKETS, COLUMN_FAMILY_NOT_ALLOWED_TABLE_PROPERTY, false, SALT_ONLY_ON_CREATE_TABLE, false, false) {
        @Override
        public Object getPTableValue(PTable table) {
            return table.getBucketNum();
        }
    },

	DEFAULT_COLUMN_FAMILY(DEFAULT_COLUMN_FAMILY_NAME, COLUMN_FAMILY_NOT_ALLOWED_TABLE_PROPERTY, false, DEFAULT_COLUMN_FAMILY_ONLY_ON_CREATE_TABLE, false, false) {
        @Override
        public Object getPTableValue(PTable table) {
            return table.getDefaultFamilyName();
        }
    },

	TTL(HColumnDescriptor.TTL, COLUMN_FAMILY_NOT_ALLOWED_FOR_TTL, true, CANNOT_ALTER_PROPERTY, false, false) {
        @Override
        public Object getPTableValue(PTable table) {
            return null;
        }
    },

    STORE_NULLS(PhoenixDatabaseMetaData.STORE_NULLS, COLUMN_FAMILY_NOT_ALLOWED_TABLE_PROPERTY, true, false, false) {
        @Override
        public Object getPTableValue(PTable table) {
            return table.getStoreNulls();
        }
    },
    
    TRANSACTIONAL(PhoenixDatabaseMetaData.TRANSACTIONAL, COLUMN_FAMILY_NOT_ALLOWED_TABLE_PROPERTY, true, false, false) {
        @Override
        public Object getPTableValue(PTable table) {
            return table.isTransactional();
        }
    },
    
    UPDATE_CACHE_FREQUENCY(PhoenixDatabaseMetaData.UPDATE_CACHE_FREQUENCY, true, true, true) {
	    @Override
        public Object getValue(Object value) {
	        if (value instanceof String) {
	            String strValue = (String) value;
	            if ("ALWAYS".equalsIgnoreCase(strValue)) {
	                return 0L;
	            } else if ("NEVER".equalsIgnoreCase(strValue)) {
	                return Long.MAX_VALUE;
	            }
	        } else {
	            return value == null ? null : ((Number) value).longValue();
	        }
	        return value;
	    }

        @Override
        public Object getPTableValue(PTable table) {
            return table.getUpdateCacheFrequency();
        }	    
	},
	
	AUTO_PARTITION_SEQ(PhoenixDatabaseMetaData.AUTO_PARTITION_SEQ, COLUMN_FAMILY_NOT_ALLOWED_TABLE_PROPERTY, false, false, false) {
        @Override
        public Object getValue(Object value) {
            return value == null ? null : SchemaUtil.normalizeIdentifier(value.toString());
        }

        @Override
        public Object getPTableValue(PTable table) {
            return table.getAutoPartitionSeqName();
        }  
	},
	
	APPEND_ONLY_SCHEMA(PhoenixDatabaseMetaData.APPEND_ONLY_SCHEMA, COLUMN_FAMILY_NOT_ALLOWED_TABLE_PROPERTY, true, true, false) {
        @Override
        public Object getPTableValue(PTable table) {
            return table.isAppendOnlySchema();
        }
    },
    GUIDE_POSTS_WIDTH(PhoenixDatabaseMetaData.GUIDE_POSTS_WIDTH, true, false, false) {
        @Override
        public Object getValue(Object value) {
            return value == null ? null : ((Number) value).longValue();
        }

        @Override
        public Object getPTableValue(PTable table) {
            return null;
        }       
	    
	},
	
	COLUMN_ENCODED_BYTES(PhoenixDatabaseMetaData.ENCODING_SCHEME, COLUMN_FAMILY_NOT_ALLOWED_TABLE_PROPERTY, false, false, false) {
	    @Override
        public Object getValue(Object value) {
	        if (value instanceof String) {
	            String strValue = (String) value;
	            if ("NONE".equalsIgnoreCase(strValue)) {
	                return (byte)0;
	            } 
	        } else {
	            return value == null ? null : ((Number) value).byteValue();
	        }
	        return value;
	    }

		@Override
		public Object getPTableValue(PTable table) {
			return table.getEncodingScheme();
		}	
	    
	},
    
    IMMUTABLE_STORAGE_SCHEME(PhoenixDatabaseMetaData.IMMUTABLE_STORAGE_SCHEME, COLUMN_FAMILY_NOT_ALLOWED_TABLE_PROPERTY, true, false, false) {
        @Override
        public ImmutableStorageScheme getValue(Object value) {
            if (value == null) {
                return null;
            } else if (value instanceof String) {
                String strValue = (String) value;
                return ImmutableStorageScheme.valueOf(strValue.toUpperCase());
            } else {
                throw new IllegalArgumentException("Immutable storage scheme table property must be a string");
            }
        }

        @Override
        public Object getPTableValue(PTable table) {
            return table.getImmutableStorageScheme();
        }   
        
    }
    ;
	
	private final String propertyName;
	private final SQLExceptionCode colFamSpecifiedException;
	private final boolean isMutable; // whether or not a property can be changed through statements like ALTER TABLE.
	private final SQLExceptionCode mutatingImmutablePropException;
	private final boolean isValidOnView;
	private final boolean isMutableOnView;

	private TableProperty(String propertyName, boolean isMutable, boolean isValidOnView, boolean isMutableOnView) {
		this(propertyName, COLUMN_FAMILY_NOT_ALLOWED_TABLE_PROPERTY, isMutable, CANNOT_ALTER_PROPERTY, isValidOnView, isMutableOnView);
	}

	private TableProperty(String propertyName, SQLExceptionCode colFamilySpecifiedException, boolean isMutable, boolean isValidOnView, boolean isMutableOnView) {
		this(propertyName, colFamilySpecifiedException, isMutable, CANNOT_ALTER_PROPERTY, isValidOnView, isMutableOnView);
	}

	private TableProperty(String propertyName, boolean isMutable, boolean isValidOnView, boolean isMutableOnView, SQLExceptionCode isMutatingException) {
		this(propertyName, COLUMN_FAMILY_NOT_ALLOWED_TABLE_PROPERTY, isMutable, isMutatingException, isValidOnView, isMutableOnView);
	}

	private TableProperty(String propertyName, SQLExceptionCode colFamSpecifiedException, boolean isMutable, SQLExceptionCode mutatingException, boolean isValidOnView, boolean isMutableOnView) {
		this.propertyName = propertyName;
		this.colFamSpecifiedException = colFamSpecifiedException;
		this.isMutable = isMutable;
		this.mutatingImmutablePropException = mutatingException;
		this.isValidOnView = isValidOnView;
		this.isMutableOnView = isMutableOnView;
	}

	public static boolean isPhoenixTableProperty(String property) {
		try {
			TableProperty.valueOf(property);
		} catch (IllegalArgumentException e) {
			return false;
		}
		return true;
	}

	public Object getValue(Object value) {
	    return value;
	}
	
    public Object getValue(Map<String, Object> props) {
        return getValue(props.get(this.toString()));
    }
    
	// isQualified is true if column family name is specified in property name
	public void validate(boolean isMutating, boolean isQualified, PTableType tableType) throws SQLException {
		checkForColumnFamily(isQualified);
		checkIfApplicableForView(tableType);
		checkForMutability(isMutating,tableType);
	}

	private void checkForColumnFamily(boolean isQualified) throws SQLException {
		if (isQualified) {
			throw new SQLExceptionInfo.Builder(colFamSpecifiedException).setMessage(". Property: " + propertyName).build().buildException();
		}
	}

	private void checkForMutability(boolean isMutating, PTableType tableType) throws SQLException {
		if (isMutating && !isMutable) {
			throw new SQLExceptionInfo.Builder(mutatingImmutablePropException).setMessage(". Property: " + propertyName).build().buildException();
		}
		if (isMutating && tableType == PTableType.VIEW && !isMutableOnView) {
			throw new SQLExceptionInfo.Builder(SQLExceptionCode.CANNOT_ALTER_TABLE_PROPERTY_ON_VIEW).setMessage(". Property: " + propertyName).build().buildException();
		}
	}

	private void checkIfApplicableForView(PTableType tableType)
			throws SQLException {
		if (tableType == PTableType.VIEW && !isValidOnView) {
			throw new SQLExceptionInfo.Builder(
					VIEW_WITH_PROPERTIES).setMessage("Property: " + propertyName).build().buildException();
		}
	}

    public String getPropertyName() {
        return propertyName;
    }

    public boolean isValidOnView() {
        return isValidOnView;
    }

    public boolean isMutable() {
        return isMutable;
    }

    public boolean isMutableOnView() {
        return isMutableOnView;
    }
    
    abstract public Object getPTableValue(PTable table);
    
}
