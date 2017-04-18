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
package org.apache.phoenix.exception;

import java.sql.SQLException;
import java.sql.SQLTimeoutException;
import java.util.Map;

import org.apache.phoenix.hbase.index.util.IndexManagementUtil;
import org.apache.phoenix.jdbc.PhoenixDatabaseMetaData;
import org.apache.phoenix.query.QueryConstants;
import org.apache.phoenix.query.QueryServices;
import org.apache.phoenix.schema.AmbiguousColumnException;
import org.apache.phoenix.schema.AmbiguousTableException;
import org.apache.phoenix.schema.ColumnAlreadyExistsException;
import org.apache.phoenix.schema.ColumnFamilyNotFoundException;
import org.apache.phoenix.schema.ColumnNotFoundException;
import org.apache.phoenix.schema.ConcurrentTableMutationException;
import org.apache.phoenix.schema.FunctionAlreadyExistsException;
import org.apache.phoenix.schema.FunctionNotFoundException;
import org.apache.phoenix.schema.ReadOnlyTableException;
import org.apache.phoenix.schema.SchemaAlreadyExistsException;
import org.apache.phoenix.schema.SchemaNotFoundException;
import org.apache.phoenix.schema.SequenceAlreadyExistsException;
import org.apache.phoenix.schema.SequenceNotFoundException;
import org.apache.phoenix.schema.StaleRegionBoundaryCacheException;
import org.apache.phoenix.schema.TableAlreadyExistsException;
import org.apache.phoenix.schema.TableNotFoundException;
import org.apache.phoenix.schema.TypeMismatchException;
import org.apache.phoenix.schema.types.PDataType;
import org.apache.phoenix.util.MetaDataUtil;

import com.google.common.collect.Maps;


/**
 * Various SQLException Information. Including a vendor-specific errorcode and a standard SQLState.
 *
 *
 * @since 1.0
 */
public enum SQLExceptionCode {

    /**
     * Connection Exception (errorcode 01, sqlstate 08)
     */
    IO_EXCEPTION(101, "08000", "Unexpected IO exception."),
    MALFORMED_CONNECTION_URL(102, "08001", "Malformed connection url."),
    CANNOT_ESTABLISH_CONNECTION(103, "08004", "Unable to establish connection."),

    /**
     * Data Exception (errorcode 02, sqlstate 22)
     */
    ILLEGAL_DATA(201, "22000", "Illegal data."),
    DIVIDE_BY_ZERO(202, "22012", "Divide by zero."),
    TYPE_MISMATCH(203, "22005", "Type mismatch.", new Factory() {
        @Override
        public SQLException newException(SQLExceptionInfo info) {
            return new TypeMismatchException(info.getMessage());
        }
    }),
    VALUE_IN_UPSERT_NOT_CONSTANT(204, "22008", "Values in UPSERT must evaluate to a constant."),
    MALFORMED_URL(205, "22009", "Malformed URL."),
    DATA_EXCEEDS_MAX_CAPACITY(206, "22003", "The data exceeds the max capacity for the data type."),
    MISSING_MAX_LENGTH(207, "22004", "Max length must be specified for type."),
    NONPOSITIVE_MAX_LENGTH(208, "22006", "Max length must have a positive length for type."),
    DECIMAL_PRECISION_OUT_OF_RANGE(209, "22003", "Decimal precision outside of range. Should be within 1 and " + PDataType.MAX_PRECISION + "."),
    SERVER_ARITHMETIC_ERROR(212, "22012", "Arithmetic error on server."),
    VALUE_OUTSIDE_RANGE(213,"22003","Value outside range."),
    VALUE_IN_LIST_NOT_CONSTANT(214, "22008", "Values in IN must evaluate to a constant."),
    SINGLE_ROW_SUBQUERY_RETURNS_MULTIPLE_ROWS(215, "22015", "Single-row sub-query returns more than one row."),
    SUBQUERY_RETURNS_DIFFERENT_NUMBER_OF_FIELDS(216, "22016", "Sub-query must return the same number of fields as the left-hand-side expression of 'IN'."),
    AMBIGUOUS_JOIN_CONDITION(217, "22017", "Ambiguous or non-equi join condition specified. Consider using table list with where clause."),
    CONSTRAINT_VIOLATION(218, "23018", "Constraint violation."),

    CONCURRENT_TABLE_MUTATION(301, "23000", "Concurrent modification to table.", new Factory() {
        @Override
        public SQLException newException(SQLExceptionInfo info) {
            return new ConcurrentTableMutationException(info.getSchemaName(), info.getTableName());
        }
    }),
    CANNOT_INDEX_COLUMN_ON_TYPE(302, "23100", "The column cannot be index due to its type."),

    /**
     * Invalid Cursor State (errorcode 04, sqlstate 24)
     */
    CURSOR_BEFORE_FIRST_ROW(401, "24015","Cursor before first row."),
    CURSOR_PAST_LAST_ROW(402, "24016", "Cursor past last row."),

    /**
     * Syntax Error or Access Rule Violation (errorcode 05, sqlstate 42)
     */
    AMBIGUOUS_TABLE(501, "42000", "Table name exists in more than one table schema and is used without being qualified.", new Factory() {
        @Override
        public SQLException newException(SQLExceptionInfo info) {
            return new AmbiguousTableException(info.getTableName(), info.getRootCause());
        }
    }),
    AMBIGUOUS_COLUMN(502, "42702", "Column reference ambiguous or duplicate names.", new Factory() {
        @Override
        public SQLException newException(SQLExceptionInfo info) {
            return new AmbiguousColumnException(info.getColumnName(), info.getRootCause());
        }
    }),
    INDEX_MISSING_PK_COLUMNS(503, "42602", "Index table missing PK Columns."),
     COLUMN_NOT_FOUND(504, "42703", "Undefined column.", new Factory() {
        @Override
        public SQLException newException(SQLExceptionInfo info) {
            return new ColumnNotFoundException(info.getSchemaName(), info.getTableName(), info.getFamilyName(), info.getColumnName());
        }
    }),
    READ_ONLY_TABLE(505, "42000", "Table is read only.", new Factory() {
        @Override
        public SQLException newException(SQLExceptionInfo info) {
            return new ReadOnlyTableException(info.getMessage(), info.getSchemaName(), info.getTableName(), info.getFamilyName());
        }
    }),
    CANNOT_DROP_PK(506, "42817", "Primary key column may not be dropped."),
    PRIMARY_KEY_MISSING(509, "42888", "The table does not have a primary key."),
    PRIMARY_KEY_ALREADY_EXISTS(510, "42889", "The table already has a primary key."),
    ORDER_BY_NOT_IN_SELECT_DISTINCT(511, "42890", "All ORDER BY expressions must appear in SELECT DISTINCT:"),
    INVALID_PRIMARY_KEY_CONSTRAINT(512, "42891", "Invalid column reference in primary key constraint."),
    ARRAY_NOT_ALLOWED_IN_PRIMARY_KEY(513, "42892", "Array type not allowed as primary key constraint."),
    COLUMN_EXIST_IN_DEF(514, "42892", "A duplicate column name was detected in the object definition or ALTER TABLE statement.", new Factory() {
        @Override
        public SQLException newException(SQLExceptionInfo info) {
            return new ColumnAlreadyExistsException(info.getSchemaName(), info.getTableName(), info.getColumnName());
        }
    }),
    ORDER_BY_ARRAY_NOT_SUPPORTED(515, "42893", "ORDER BY of an array type is not allowed."),
    NON_EQUALITY_ARRAY_COMPARISON(516, "42894", "Array types may only be compared using = or !=."),
    INVALID_NOT_NULL_CONSTRAINT(517, "42895", "Invalid not null constraint on non primary key column."),

    /**
     *  Invalid Transaction State (errorcode 05, sqlstate 25)
     */
     READ_ONLY_CONNECTION(518,"25502","Mutations are not permitted for a read-only connection."),

     VARBINARY_ARRAY_NOT_SUPPORTED(519, "42896", "VARBINARY ARRAY is not supported."),

     /**
      *  Expression Index exceptions.
      */
     AGGREGATE_EXPRESSION_NOT_ALLOWED_IN_INDEX(520, "42897", "Aggregate expression not allowed in an index."),
     NON_DETERMINISTIC_EXPRESSION_NOT_ALLOWED_IN_INDEX(521, "42898", "Non-deterministic expression not allowed in an index."),
     STATELESS_EXPRESSION_NOT_ALLOWED_IN_INDEX(522, "42899", "Stateless expression not allowed in an index."),
     
     /**
      *  Transaction exceptions.
      */
     TRANSACTION_CONFLICT_EXCEPTION(523, "42900", "Transaction aborted due to conflict with other mutations."),
     TRANSACTION_EXCEPTION(524, "42901", "Transaction aborted due to error."),

     /**
      * Union All related errors
      */
     SELECT_COLUMN_NUM_IN_UNIONALL_DIFFS(525, "42902", "SELECT column number differs in a Union All query is not allowed."),
     SELECT_COLUMN_TYPE_IN_UNIONALL_DIFFS(526, "42903", "SELECT column types differ in a Union All query is not allowed."),

     /**
      * Row timestamp column related errors
      */
     ROWTIMESTAMP_ONE_PK_COL_ONLY(527, "42904", "Only one column that is part of the primary key can be declared as a ROW_TIMESTAMP."),
     ROWTIMESTAMP_PK_COL_ONLY(528, "42905", "Only columns part of the primary key can be declared as a ROW_TIMESTAMP."),
     ROWTIMESTAMP_CREATE_ONLY(529, "42906", "A column can be added as ROW_TIMESTAMP only in CREATE TABLE."),
     ROWTIMESTAMP_COL_INVALID_TYPE(530, "42907", "A column can be added as ROW_TIMESTAMP only if it is of type DATE, BIGINT, TIME OR TIMESTAMP."),
     ROWTIMESTAMP_NOT_ALLOWED_ON_VIEW(531, "42908", "Declaring a column as row_timestamp is not allowed for views."),
     INVALID_SCN(532, "42909", "Value of SCN cannot be less than zero."),
     /**
     * HBase and Phoenix specific implementation defined sub-classes.
     * Column family related exceptions.
     *
     * For the following exceptions, use errorcode 10.
     */
    SINGLE_PK_MAY_NOT_BE_NULL(1000, "42I00", "Single column primary key may not be NULL."),
    COLUMN_FAMILY_NOT_FOUND(1001, "42I01", "Undefined column family.", new Factory() {
        @Override
        public SQLException newException(SQLExceptionInfo info) {
            return new ColumnFamilyNotFoundException(info.getFamilyName());
        }
    }),
    PROPERTIES_FOR_FAMILY(1002, "42I02","Properties may not be defined for an unused family name."),
    // Primary/row key related exceptions.
    PRIMARY_KEY_WITH_FAMILY_NAME(1003, "42J01", "Primary key columns must not have a family name."),
    PRIMARY_KEY_OUT_OF_ORDER(1004, "42J02", "Order of columns in primary key constraint must match the order in which they're declared."),
    VARBINARY_IN_ROW_KEY(1005, "42J03", "The VARBINARY/ARRAY type can only be used as the last part of a multi-part row key."),
    NOT_NULLABLE_COLUMN_IN_ROW_KEY(1006, "42J04", "Only nullable columns may be added to a multi-part row key."),
    VARBINARY_LAST_PK(1015, "42J04", "Cannot add column to table when the last PK column is of type VARBINARY or ARRAY."),
    NULLABLE_FIXED_WIDTH_LAST_PK(1023, "42J04", "Cannot add column to table when the last PK column is nullable and fixed width."),
    CANNOT_MODIFY_VIEW_PK(1036, "42J04", "Cannot modify the primary key of a VIEW if last PK column of parent is variable length."),
    BASE_TABLE_COLUMN(1037, "42J04", "Cannot modify columns of base table used by tenant-specific tables."),
    UNALLOWED_COLUMN_FAMILY(1090, "42J04", "Column family names should not contain local index column prefix: "+QueryConstants.LOCAL_INDEX_COLUMN_FAMILY_PREFIX),
    // Key/value column related errors
    KEY_VALUE_NOT_NULL(1007, "42K01", "A key/value column may not be declared as not null."),
    // View related errors.
    VIEW_WITH_TABLE_CONFIG(1008, "42L01", "A view may not contain table configuration properties."),
    VIEW_WITH_PROPERTIES(1009, "42L02", "Properties may not be defined for a view."),
    // Table related errors that are not in standard code.
    CANNOT_MUTATE_TABLE(1010, "42M01", "Not allowed to mutate table."),
    UNEXPECTED_MUTATION_CODE(1011, "42M02", "Unexpected mutation code."),
    TABLE_UNDEFINED(1012, "42M03", "Table undefined.", new Factory() {
        @Override
        public SQLException newException(SQLExceptionInfo info) {
            return new TableNotFoundException(info.getSchemaName(), info.getTableName());
        }
    }),
    TABLE_ALREADY_EXIST(1013, "42M04", "Table already exists.", new Factory() {
        @Override
        public SQLException newException(SQLExceptionInfo info) {
            return new TableAlreadyExistsException(info.getSchemaName(), info.getTableName());
        }
    }),

    // Syntax error
    TYPE_NOT_SUPPORTED_FOR_OPERATOR(1014, "42Y01", "The operator does not support the operand type."),
    AGGREGATE_IN_GROUP_BY(1016, "42Y26", "Aggregate expressions may not be used in GROUP BY."),
    AGGREGATE_IN_WHERE(1017, "42Y26", "Aggregate may not be used in WHERE."),
    AGGREGATE_WITH_NOT_GROUP_BY_COLUMN(1018, "42Y27", "Aggregate may not contain columns not in GROUP BY."),
    ONLY_AGGREGATE_IN_HAVING_CLAUSE(1019, "42Y26", "Only aggregate maybe used in the HAVING clause."),
    UPSERT_COLUMN_NUMBERS_MISMATCH(1020, "42Y60", "Number of columns upserting must match number of values."),
    // Table properties exception.
    INVALID_BUCKET_NUM(1021, "42Y80", "Salt bucket numbers should be with 1 and 256."),
    NO_SPLITS_ON_SALTED_TABLE(1022, "42Y81", "Should not specify split points on salted table with default row key order."),
    SALT_ONLY_ON_CREATE_TABLE(1024, "42Y82", "Salt bucket number may only be specified when creating a table."),
    SET_UNSUPPORTED_PROP_ON_ALTER_TABLE(1025, "42Y83", "Unsupported property set in ALTER TABLE command."),
    CANNOT_ADD_NOT_NULLABLE_COLUMN(1038, "42Y84", "Only nullable columns may be added for a pre-existing table."),
    NO_MUTABLE_INDEXES(1026, "42Y85", "Mutable secondary indexes are only supported for HBase version " + MetaDataUtil.decodeHBaseVersionAsString(PhoenixDatabaseMetaData.MUTABLE_SI_VERSION_THRESHOLD) + " and above."),
    INVALID_FILTER_ON_IMMUTABLE_ROWS(1027, "42Y86", "All columns referenced in a WHERE clause must be available in every index for a table with immutable rows."),
    INVALID_INDEX_STATE_TRANSITION(1028, "42Y87", "Invalid index state transition."),
    INVALID_MUTABLE_INDEX_CONFIG(1029, "42Y88", "Mutable secondary indexes must have the "
            + IndexManagementUtil.WAL_EDIT_CODEC_CLASS_KEY + " property set to "
            +  IndexManagementUtil.INDEX_WAL_EDIT_CODEC_CLASS_NAME + " in the hbase-sites.xml of every region server."),
    CANNOT_CREATE_DEFAULT(1031, "42Y90", "Cannot create column with a stateful default value."),
    CANNOT_CREATE_DEFAULT_ROWTIMESTAMP(1032, "42Y90", "Cannot create ROW_TIMESTAMP column with a default value."),

    CANNOT_CREATE_TENANT_SPECIFIC_TABLE(1030, "42Y89", "Cannot create table for tenant-specific connection."),
    DEFAULT_COLUMN_FAMILY_ONLY_ON_CREATE_TABLE(1034, "42Y93", "Default column family may only be specified when creating a table."),
    INSUFFICIENT_MULTI_TENANT_COLUMNS(1040, "42Y96", "A MULTI_TENANT table must have two or more PK columns with the first column being NOT NULL."),
    TENANTID_IS_OF_WRONG_TYPE(1041, "42Y97", "The TenantId could not be converted to correct format for this table."),
    VIEW_WHERE_IS_CONSTANT(1045, "43A02", "WHERE clause in VIEW should not evaluate to a constant."),
    CANNOT_UPDATE_VIEW_COLUMN(1046, "43A03", "Column updated in VIEW may not differ from value specified in WHERE clause."),
    TOO_MANY_INDEXES(1047, "43A04", "Too many indexes have already been created on the physical table."),
    NO_LOCAL_INDEX_ON_TABLE_WITH_IMMUTABLE_ROWS(1048,"43A05","Local indexes aren't allowed on tables with immutable rows."),
    COLUMN_FAMILY_NOT_ALLOWED_TABLE_PROPERTY(1049, "43A06", "Column family not allowed for table properties."),
    COLUMN_FAMILY_NOT_ALLOWED_FOR_TTL(1050, "43A07", "Setting TTL for a column family not supported. You can only have TTL for the entire table."),
    CANNOT_ALTER_PROPERTY(1051, "43A08", "Property can be specified or changed only when creating a table."),
    CANNOT_SET_PROPERTY_FOR_COLUMN_NOT_ADDED(1052, "43A09", "Property cannot be specified for a column family that is not being added or modified."),
    CANNOT_SET_TABLE_PROPERTY_ADD_COLUMN(1053, "43A10", "Table level property cannot be set when adding a column."),

    NO_LOCAL_INDEXES(1054, "43A11", "Local secondary indexes are not supported for HBase versions " +
        MetaDataUtil.decodeHBaseVersionAsString(PhoenixDatabaseMetaData.MIN_LOCAL_SI_VERSION_DISALLOW) + " through " + MetaDataUtil.decodeHBaseVersionAsString(PhoenixDatabaseMetaData.MAX_LOCAL_SI_VERSION_DISALLOW) + " inclusive."),
    UNALLOWED_LOCAL_INDEXES(1055, "43A12", "Local secondary indexes are configured to not be allowed."),
    
    DESC_VARBINARY_NOT_SUPPORTED(1056, "43A13", "Descending VARBINARY columns not supported."),
    NO_TABLE_SPECIFIED_FOR_WILDCARD_SELECT(1057, "42Y10", "No table specified for wildcard select."),
    UNSUPPORTED_GROUP_BY_EXPRESSIONS(1058, "43A14", "Only a single VARBINARY, ARRAY, or nullable BINARY type may be referenced in a GROUP BY."),
    
    DEFAULT_COLUMN_FAMILY_ON_SHARED_TABLE(1069, "43A69", "Default column family not allowed on VIEW or shared INDEX."),
    ONLY_TABLE_MAY_BE_DECLARED_TRANSACTIONAL(1070, "44A01", "Only tables may be declared as transactional."),
    TX_MAY_NOT_SWITCH_TO_NON_TX(1071, "44A02", "A transactional table may not be switched to non transactional."),
	STORE_NULLS_MUST_BE_TRUE_FOR_TRANSACTIONAL(1072, "44A03", "Store nulls must be true when a table is transactional."),
    CANNOT_START_TRANSACTION_WITH_SCN_SET(1073, "44A04", "Cannot start a transaction on a connection with SCN set."),
    TX_MAX_VERSIONS_MUST_BE_GREATER_THAN_ONE(1074, "44A05", "A transactional table must define VERSION of greater than one."),
    CANNOT_SPECIFY_SCN_FOR_TXN_TABLE(1075, "44A06", "Cannot use a connection with SCN set for a transactional table."),
    NULL_TRANSACTION_CONTEXT(1076, "44A07", "No Transaction Context available."),
    TRANSACTION_FAILED(1077, "44A08", "Transaction Failure "),
    CANNOT_CREATE_TXN_TABLE_IF_TXNS_DISABLED(1078, "44A09", "Cannot create a transactional table if transactions are disabled."),
    CANNOT_ALTER_TO_BE_TXN_IF_TXNS_DISABLED(1079, "44A10", "Cannot alter table to be transactional table if transactions are disabled."),
    CANNOT_CREATE_TXN_TABLE_WITH_ROW_TIMESTAMP(1080, "44A11", "Cannot create a transactional table if transactions are disabled."),
    CANNOT_ALTER_TO_BE_TXN_WITH_ROW_TIMESTAMP(1081, "44A12", "Cannot alter table to be transactional table if transactions are disabled."),
    TX_MUST_BE_ENABLED_TO_SET_TX_CONTEXT(1082, "44A13", "Cannot set transaction context if transactions are disabled."),
    TX_MUST_BE_ENABLED_TO_SET_AUTO_FLUSH(1083, "44A14", "Cannot set auto flush if transactions are disabled."),
    TX_MUST_BE_ENABLED_TO_SET_ISOLATION_LEVEL(1084, "44A15", "Cannot set isolation level to TRANSACTION_REPEATABLE_READ if transactions are disabled."),
    TX_UNABLE_TO_GET_WRITE_FENCE(1085, "44A16", "Unable to obtain write fence for DDL operation."),
    
    SEQUENCE_NOT_CASTABLE_TO_AUTO_PARTITION_ID_COLUMN(1086, "44A17", "Sequence Value not castable to auto-partition id column"),
    CANNOT_COERCE_AUTO_PARTITION_ID(1087, "44A18", "Auto-partition id cannot be coerced"),

    /** Sequence related */
    SEQUENCE_ALREADY_EXIST(1200, "42Z00", "Sequence already exists.", new Factory() {
        @Override
        public SQLException newException(SQLExceptionInfo info) {
            return new SequenceAlreadyExistsException(info.getSchemaName(), info.getTableName());
        }
    }),
    SEQUENCE_UNDEFINED(1201, "42Z01", "Sequence undefined.", new Factory() {
        @Override
        public SQLException newException(SQLExceptionInfo info) {
            return new SequenceNotFoundException(info.getSchemaName(), info.getTableName());
        }
    }),
    START_WITH_MUST_BE_CONSTANT(1202, "42Z02", "Sequence START WITH value must be an integer or long constant."),
    INCREMENT_BY_MUST_BE_CONSTANT(1203, "42Z03", "Sequence INCREMENT BY value must be an integer or long constant."),
    CACHE_MUST_BE_NON_NEGATIVE_CONSTANT(1204, "42Z04", "Sequence CACHE value must be a non negative integer constant."),
    INVALID_USE_OF_NEXT_VALUE_FOR(1205, "42Z05", "NEXT VALUE FOR may only be used as in a SELECT or an UPSERT VALUES expression."),
    CANNOT_CALL_CURRENT_BEFORE_NEXT_VALUE(1206, "42Z06", "NEXT VALUE FOR must be called before CURRENT VALUE FOR is called."),
    EMPTY_SEQUENCE_CACHE(1207, "42Z07", "No more cached sequence values."),
    MINVALUE_MUST_BE_CONSTANT(1208, "42Z08", "Sequence MINVALUE must be an integer or long constant."),
    MAXVALUE_MUST_BE_CONSTANT(1209, "42Z09", "Sequence MAXVALUE must be an integer or long constant."),
    MINVALUE_MUST_BE_LESS_THAN_OR_EQUAL_TO_MAXVALUE(1210, "42Z10", "Sequence MINVALUE must be less than or equal to MAXVALUE."),
    STARTS_WITH_MUST_BE_BETWEEN_MIN_MAX_VALUE(1211, "42Z11",
            "STARTS WITH value must be greater than or equal to MINVALUE and less than or equal to MAXVALUE."),
    SEQUENCE_VAL_REACHED_MAX_VALUE(1212, "42Z12", "Reached MAXVALUE of sequence."),
    SEQUENCE_VAL_REACHED_MIN_VALUE(1213, "42Z13", "Reached MINVALUE of sequence."),
    INCREMENT_BY_MUST_NOT_BE_ZERO(1214, "42Z14", "Sequence INCREMENT BY value cannot be zero."),
    NUM_SEQ_TO_ALLOCATE_MUST_BE_CONSTANT(1215, "42Z15", "Sequence NEXT n VALUES FOR must be a positive integer or constant." ),
    NUM_SEQ_TO_ALLOCATE_NOT_SUPPORTED(1216, "42Z16", "Sequence NEXT n VALUES FOR is not supported for Sequences with the CYCLE flag." ),
    AUTO_PARTITION_SEQUENCE_UNDEFINED(1217, "42Z17", "Auto Partition Sequence undefined", new Factory() {
        @Override
        public SQLException newException(SQLExceptionInfo info) {
            return new SequenceNotFoundException(info.getSchemaName(), info.getTableName());
        }
    }),
    CANNOT_UPDATE_PK_ON_DUP_KEY(1218, "42Z18", "Primary key columns may not be udpated in ON DUPLICATE KEY UPDATE clause." ),
    CANNOT_USE_ON_DUP_KEY_FOR_IMMUTABLE(1219, "42Z19", "The ON DUPLICATE KEY UPDATE clause may not be used for immutable tables." ),
    CANNOT_USE_ON_DUP_KEY_FOR_TRANSACTIONAL(1220, "42Z20", "The ON DUPLICATE KEY UPDATE clause may not be used for transactional tables." ),
    DUPLICATE_COLUMN_IN_ON_DUP_KEY(1221, "42Z21", "Duplicate column in ON DUPLICATE KEY UPDATE." ),
    AGGREGATION_NOT_ALLOWED_IN_ON_DUP_KEY(1222, "42Z22", "Aggregation in ON DUPLICATE KEY UPDATE is not allowed." ),
    CANNOT_SET_SCN_IN_ON_DUP_KEY(1223, "42Z23", "The CURRENT_SCN may not be set for statement using ON DUPLICATE KEY." ),

    /** Parser error. (errorcode 06, sqlState 42P) */
    PARSER_ERROR(601, "42P00", "Syntax error.", Factory.SYNTAX_ERROR),
    MISSING_TOKEN(602, "42P00", "Syntax error.", Factory.SYNTAX_ERROR),
    UNWANTED_TOKEN(603, "42P00", "Syntax error.", Factory.SYNTAX_ERROR),
    MISMATCHED_TOKEN(604, "42P00", "Syntax error.", Factory.SYNTAX_ERROR),
    UNKNOWN_FUNCTION(605, "42P00", "Syntax error.", Factory.SYNTAX_ERROR),

    /**
     * Implementation defined class. Execution exceptions (errorcode 11, sqlstate XCL).
     */
    RESULTSET_CLOSED(1101, "XCL01", "ResultSet is closed."),
    GET_TABLE_REGIONS_FAIL(1102, "XCL02", "Cannot get all table regions."),
    EXECUTE_QUERY_NOT_APPLICABLE(1103, "XCL03", "executeQuery may not be used."),
    EXECUTE_UPDATE_NOT_APPLICABLE(1104, "XCL04", "executeUpdate may not be used."),
    SPLIT_POINT_NOT_CONSTANT(1105, "XCL05", "Split points must be constants."),
    BATCH_EXCEPTION(1106, "XCL06", "Exception while executing batch."),
    EXECUTE_UPDATE_WITH_NON_EMPTY_BATCH(1107, "XCL07", "An executeUpdate is prohibited when the batch is not empty. Use clearBatch to empty the batch first."),
    STALE_REGION_BOUNDARY_CACHE(1108, "XCL08", "Cache of region boundaries are out of date.", new Factory() {
        @Override
        public SQLException newException(SQLExceptionInfo info) {
            return new StaleRegionBoundaryCacheException(info.getSchemaName(), info.getTableName());
        }
    }),
    CANNOT_SPLIT_LOCAL_INDEX(1109,"XCL09", "Local index may not be pre-split."),
    CANNOT_SALT_LOCAL_INDEX(1110,"XCL10", "Local index may not be salted."),

    INDEX_FAILURE_BLOCK_WRITE(1120, "XCL20", "Writes to table blocked until index can be updated."),
    
    UPDATE_CACHE_FREQUENCY_INVALID(1130, "XCL30", "UPDATE_CACHE_FREQUENCY cannot be set to ALWAYS if APPEND_ONLY_SCHEMA is true."),
    CANNOT_DROP_COL_APPEND_ONLY_SCHEMA(1131, "XCL31", "Cannot drop column from table that with append only schema."),
    
    CANNOT_ALTER_IMMUTABLE_ROWS_PROPERTY(1133, "XCL33", "IMMUTABLE_ROWS property can be changed only if the table storage scheme is ONE_CELL_PER_KEYVALUE_COLUMN"),
    CANNOT_ALTER_TABLE_PROPERTY_ON_VIEW(1134, "XCL34", "Altering this table property on a view is not allowed"),
    
    IMMUTABLE_TABLE_PROPERTY_INVALID(1135, "XCL35", "IMMUTABLE table property cannot be used with CREATE IMMUTABLE TABLE statement "),
    
    MAX_COLUMNS_EXCEEDED(1136, "XCL36", "The number of columns exceed the maximum supported by the table's qualifier encoding scheme"),
    INVALID_IMMUTABLE_STORAGE_SCHEME_AND_COLUMN_QUALIFIER_BYTES(1137, "XCL37", "If IMMUTABLE_STORAGE_SCHEME property is not set to ONE_CELL_PER_COLUMN COLUMN_ENCODED_BYTES cannot be 0"),
    INVALID_IMMUTABLE_STORAGE_SCHEME_CHANGE(1138, "XCL38", "IMMUTABLE_STORAGE_SCHEME property cannot be changed from/to ONE_CELL_PER_COLUMN "),

    /**
     * Implementation defined class. Phoenix internal error. (errorcode 20, sqlstate INT).
     */
    CANNOT_CALL_METHOD_ON_TYPE(2001, "INT01", "Cannot call method on the argument type."),
    CLASS_NOT_UNWRAPPABLE(2002, "INT03", "Class not unwrappable."),
    PARAM_INDEX_OUT_OF_BOUND(2003, "INT04", "Parameter position is out of range."),
    PARAM_VALUE_UNBOUND(2004, "INT05", "Parameter value unbound."),
    INTERRUPTED_EXCEPTION(2005, "INT07", "Interrupted exception."),
    INCOMPATIBLE_CLIENT_SERVER_JAR(2006, "INT08", "Incompatible jars detected between client and server."),
    OUTDATED_JARS(2007, "INT09", "Outdated jars."),
    INDEX_METADATA_NOT_FOUND(2008, "INT10", "Unable to find cached index metadata. "),
    UNKNOWN_ERROR_CODE(2009, "INT11", "Unknown error code."),
    CONCURRENT_UPGRADE_IN_PROGRESS(2010, "INT12", ""),
    UPGRADE_REQUIRED(2011, "INT13", ""),
    UPGRADE_NOT_REQUIRED(2012, "INT14", ""),
    OPERATION_TIMED_OUT(6000, "TIM01", "Operation timed out.", new Factory() {
        @Override
        public SQLException newException(SQLExceptionInfo info) {
            return new SQLTimeoutException(OPERATION_TIMED_OUT.getMessage(),
                    OPERATION_TIMED_OUT.getSQLState(), OPERATION_TIMED_OUT.getErrorCode());
        }
    }),
    FUNCTION_UNDEFINED(6001, "42F01", "Function undefined.", new Factory() {
        @Override
        public SQLException newException(SQLExceptionInfo info) {
            return new FunctionNotFoundException(info.getFunctionName());
        }
    }),
    FUNCTION_ALREADY_EXIST(6002, "42F02", "Function already exists.", new Factory() {
        @Override
        public SQLException newException(SQLExceptionInfo info) {
            return new FunctionAlreadyExistsException(info.getSchemaName(), info.getTableName());
        }
    }),
    UNALLOWED_USER_DEFINED_FUNCTIONS(6003, "42F03",
            "User defined functions are configured to not be allowed. To allow configure "
                    + QueryServices.ALLOW_USER_DEFINED_FUNCTIONS_ATTRIB + " to true."), 

    SCHEMA_ALREADY_EXISTS(721, "42M04", "Schema with given name already exists", new Factory() {
        @Override
        public SQLException newException(SQLExceptionInfo info) {
            return new SchemaAlreadyExistsException(info.getSchemaName());
        }
    }), SCHEMA_NOT_FOUND(722, "43M05", "Schema does not exists", new Factory() {
        @Override
        public SQLException newException(SQLExceptionInfo info) {
            return new SchemaNotFoundException(info.getSchemaName());
        }
    }), CANNOT_MUTATE_SCHEMA(723, "43M06", "Cannot mutate schema as schema has existing tables"), SCHEMA_NOT_ALLOWED(
            724, "43M07", "Schema name not allowed!!"), CREATE_SCHEMA_NOT_ALLOWED(725, "43M08",
                    "Cannot create schema because config " + QueryServices.IS_NAMESPACE_MAPPING_ENABLED
                            + " for enabling name space mapping isn't enabled."), INCONSISTENET_NAMESPACE_MAPPING_PROPERTIES(
                                    726, "43M10", " Inconsistent namespace mapping properites.."), ASYNC_NOT_ALLOWED(
                                    727, "43M11", " ASYNC option is not allowed.. "),
    NEW_CONNECTION_THROTTLED(728, "410M1", "Could not create connection " +
        "because this client already has the maximum number" +
        " of connections to the target cluster.");

    private final int errorCode;
    private final String sqlState;
    private final String message;
    private final Factory factory;

    private SQLExceptionCode(int errorCode, String sqlState, String message) {
        this(errorCode, sqlState, message, Factory.DEFAULT);
    }

    private SQLExceptionCode(int errorCode, String sqlState, String message, Factory factory) {
        this.errorCode = errorCode;
        this.sqlState = sqlState;
        this.message = message;
        this.factory = factory;
    }

    public String getSQLState() {
        return sqlState;
    }

    public String getMessage() {
        return message;
    }

    public int getErrorCode() {
        return errorCode;
    }

    @Override
    public String toString() {
        return "ERROR " + errorCode + " (" + sqlState + "): " + message;
    }

    public Factory getExceptionFactory() {
        return factory;
    }

    public static interface Factory {
        public static final Factory DEFAULT = new Factory() {

            @Override
            public SQLException newException(SQLExceptionInfo info) {
                return new SQLException(info.toString(), info.getCode().getSQLState(), info.getCode().getErrorCode(), info.getRootCause());
            }
            
        };
        public static final Factory SYNTAX_ERROR = new Factory() {

            @Override
            public SQLException newException(SQLExceptionInfo info) {
                return new PhoenixParserException(info.getMessage(), info.getRootCause());
            }
            
        };
        public SQLException newException(SQLExceptionInfo info);
    }
    
    private static final Map<Integer,SQLExceptionCode> errorCodeMap = Maps.newHashMapWithExpectedSize(SQLExceptionCode.values().length);
    static {
        for (SQLExceptionCode code : SQLExceptionCode.values()) {
            SQLExceptionCode otherCode = errorCodeMap.put(code.getErrorCode(), code);
            if (otherCode != null) {
                throw new IllegalStateException("Duplicate error code for " + code + " and " + otherCode);
            }
        }
    }
    
    public static SQLExceptionCode fromErrorCode(int errorCode) {
        SQLExceptionCode code = errorCodeMap.get(errorCode);
        if (code == null) {
            return SQLExceptionCode.UNKNOWN_ERROR_CODE;
        }
        return code;
    }
}
