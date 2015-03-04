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

import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.phoenix.exception.SQLExceptionCode;
import org.apache.phoenix.exception.SQLExceptionInfo;
import org.apache.phoenix.jdbc.PhoenixDatabaseMetaData;

public enum TableProperty {

	IMMUTABLE_ROWS(PhoenixDatabaseMetaData.IMMUTABLE_ROWS, true, true),

	MULTI_TENANT(PhoenixDatabaseMetaData.MULTI_TENANT, true, false),

	DISABLE_WAL(PhoenixDatabaseMetaData.DISABLE_WAL, true, false),

	SALT_BUCKETS(PhoenixDatabaseMetaData.SALT_BUCKETS, COLUMN_FAMILY_NOT_ALLOWED_TABLE_PROPERTY, false, SALT_ONLY_ON_CREATE_TABLE, false),

	DEFAULT_COLUMN_FAMILY(DEFAULT_COLUMN_FAMILY_NAME, COLUMN_FAMILY_NOT_ALLOWED_TABLE_PROPERTY, false, DEFAULT_COLUMN_FAMILY_ONLY_ON_CREATE_TABLE, false),

	TTL(HColumnDescriptor.TTL, COLUMN_FAMILY_NOT_ALLOWED_FOR_TTL, true, CANNOT_ALTER_PROPERTY, false),

    STORE_NULLS(PhoenixDatabaseMetaData.STORE_NULLS, COLUMN_FAMILY_NOT_ALLOWED_TABLE_PROPERTY, true, false);


	private final String propertyName;
	private final SQLExceptionCode colFamSpecifiedException;
	private final boolean isMutable; // whether or not a property can be changed through statements like ALTER TABLE.
	private final SQLExceptionCode mutatingImmutablePropException;
	private final boolean isValidOnView;

	private TableProperty(String propertyName, boolean isMutable, boolean isValidOnView) {
		this(propertyName, COLUMN_FAMILY_NOT_ALLOWED_TABLE_PROPERTY, isMutable, CANNOT_ALTER_PROPERTY, isValidOnView);
	}

	private TableProperty(String propertyName, SQLExceptionCode colFamilySpecifiedException, boolean isMutable, boolean isValidOnView) {
		this(propertyName, colFamilySpecifiedException, isMutable, CANNOT_ALTER_PROPERTY, isValidOnView);
	}

	private TableProperty(String propertyName, boolean isMutable, boolean isValidOnView, SQLExceptionCode isMutatingException) {
		this(propertyName, COLUMN_FAMILY_NOT_ALLOWED_TABLE_PROPERTY, isMutable, isMutatingException, isValidOnView);
	}

	private TableProperty(String propertyName, SQLExceptionCode colFamSpecifiedException, boolean isMutable, SQLExceptionCode mutatingException, boolean isValidOnView) {
		this.propertyName = propertyName;
		this.colFamSpecifiedException = colFamSpecifiedException;
		this.isMutable = isMutable;
		this.mutatingImmutablePropException = mutatingException;
		this.isValidOnView = isValidOnView;
	}

	public static boolean isPhoenixTableProperty(String property) {
		try {
			TableProperty.valueOf(property);
		} catch (IllegalArgumentException e) {
			return false;
		}
		return true;
	}

	// isQualified is true if column family name is specified in property name
	public void validate(boolean isMutating, boolean isQualified, PTableType tableType) throws SQLException {
		checkForColumnFamily(isQualified);
		checkForMutability(isMutating);
		checkIfApplicableForView(tableType);
	}

	private void checkForColumnFamily(boolean isQualified) throws SQLException {
		if (isQualified) {
			throw new SQLExceptionInfo.Builder(colFamSpecifiedException).setMessage(". Property: " + propertyName).build().buildException();
		}
	}

	private void checkForMutability(boolean isMutating) throws SQLException {
		if (isMutating && !isMutable) {
			throw new SQLExceptionInfo.Builder(mutatingImmutablePropException).setMessage(". Property: " + propertyName).build().buildException();
		}
	}

	private void checkIfApplicableForView(PTableType tableType)
			throws SQLException {
		if (tableType == PTableType.VIEW && !isValidOnView) {
			throw new SQLExceptionInfo.Builder(
					VIEW_WITH_PROPERTIES).setMessage("Property: " + propertyName).build().buildException();
		}
	}

}
