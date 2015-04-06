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
import org.apache.phoenix.schema.AmbiguousColumnException;
import org.apache.phoenix.schema.AmbiguousTableException;
import org.apache.phoenix.schema.ColumnAlreadyExistsException;
import org.apache.phoenix.schema.ColumnFamilyNotFoundException;
import org.apache.phoenix.schema.ColumnNotFoundException;
import org.apache.phoenix.schema.ConcurrentTableMutationException;
import org.apache.phoenix.schema.ReadOnlyTableException;
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
 * Various SQLException Information. Including a vender-specific errorcode and a standard SQLState.
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
    MISSING_CHAR_LENGTH(207, "22003", "Missing length for CHAR."),
    NONPOSITIVE_CHAR_LENGTH(208, "22003", "CHAR or VARCHAR must have a positive length."),
    DECIMAL_PRECISION_OUT_OF_RANGE(209, "22003", "Decimal precision outside of range. Should be within 1 and " + PDataType.MAX_PRECISION + "."),
    MISSING_BINARY_LENGTH(210, "22003", "Missing length for BINARY."),
    NONPOSITIVE_BINARY_LENGTH(211, "22003", "BINARY must have a positive length."),
    SERVER_ARITHMETIC_ERROR(212, "22012", "Arithmetic error on server."),
    VALUE_OUTSIDE_RANGE(213,"22003","Value outside range."),
    VALUE_IN_LIST_NOT_CONSTANT(214, "22008", "Values in IN must evaluate to a constant."),
    SINGLE_ROW_SUBQUERY_RETURNS_MULTIPLE_ROWS(215, "22015", "Single-row sub-query returns more than one row."),
    SUBQUERY_RETURNS_DIFFERENT_NUMBER_OF_FIELDS(216, "22016", "Sub-query must return the same number of fields as the left-hand-side expression of 'IN'."),
    AMBIGUOUS_JOIN_CONDITION(217, "22017", "Amibiguous or non-equi join condition specified. Consider using table list with where clause."),
    CONSTRAINT_VIOLATION(218, "22018", "Constraint violatioin."),
    
    /**
     * Constraint Violation (errorcode 03, sqlstate 23)
     */
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
    INVALID_PRIMARY_KEY_CONSTRAINT(512, "42891", "Invalid column reference in primary key constraint"),
    ARRAY_NOT_ALLOWED_IN_PRIMARY_KEY(513, "42892", "Array type not allowed as primary key constraint"),
    COLUMN_EXIST_IN_DEF(514, "42892", "A duplicate column name was detected in the object definition or ALTER TABLE statement.", new Factory() {
        @Override
        public SQLException newException(SQLExceptionInfo info) {
            return new ColumnAlreadyExistsException(info.getSchemaName(), info.getTableName(), info.getColumnName());
        }
    }),
    ORDER_BY_ARRAY_NOT_SUPPORTED(515, "42893", "ORDER BY of an array type is not allowed"),
    NON_EQUALITY_ARRAY_COMPARISON(516, "42894", "Array types may only be compared using = or !="),
    INVALID_NOT_NULL_CONSTRAINT(517, "42895", "Invalid not null constraint on non primary key column"),

    /**
     *  Invalid Transaction State (errorcode 05, sqlstate 25)
     */
     READ_ONLY_CONNECTION(518,"25502","Mutations are not permitted for a read-only connection."),
 
     VARBINARY_ARRAY_NOT_SUPPORTED(519, "42896", "VARBINARY ARRAY is not supported"),
    
     /**
      *  Expression Index exceptions.
      */
     AGGREGATE_EXPRESSION_NOT_ALLOWED_IN_INDEX(520, "42897", "Aggreagaate expression not allowed in an index"),
     NON_DETERMINISTIC_EXPRESSION_NOT_ALLOWED_IN_INDEX(521, "42898", "Non-deterministic expression not allowed in an index"),
     STATELESS_EXPRESSION_NOT_ALLOWED_IN_INDEX(522, "42899", "Stateless expression not allowed in an index"),

     /** 
      * Union All related errors
      */
     SELECT_COLUMN_NUM_IN_UNIONALL_DIFFS(525, "42902", "SELECT column number differs in a Union All query is not allowed"),
     SELECT_COLUMN_TYPE_IN_UNIONALL_DIFFS(526, "42903", "SELECT column types differ in a Union All query is not allowed"),

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
    CANNOT_MODIFY_VIEW_PK(1036, "42J04", "Cannot modify the primary key of a VIEW."),
    BASE_TABLE_COLUMN(1037, "42J04", "Cannot modify columns of base table used by tenant-specific tables."),
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
            +  IndexManagementUtil.INDEX_WAL_EDIT_CODEC_CLASS_NAME + " in the hbase-sites.xml of every region server"),
            
            
    CANNOT_CREATE_TENANT_SPECIFIC_TABLE(1030, "42Y89", "Cannot create table for tenant-specific connection"),
    CANNOT_DEFINE_PK_FOR_VIEW(1031, "42Y90", "Defining PK columns for a VIEW is not allowed."),
    DEFAULT_COLUMN_FAMILY_ONLY_ON_CREATE_TABLE(1034, "42Y93", "Default column family may only be specified when creating a table."),
    INSUFFICIENT_MULTI_TENANT_COLUMNS(1040, "42Y96", "A MULTI_TENANT table must have two or more PK columns with the first column being NOT NULL and of type VARCHAR or CHAR."),
    VIEW_WHERE_IS_CONSTANT(1045, "43A02", "WHERE clause in VIEW should not evaluate to a constant."),
    CANNOT_UPDATE_VIEW_COLUMN(1046, "43A03", "Column updated in VIEW may not differ from value specified in WHERE clause."),
    TOO_MANY_INDEXES(1047, "43A04", "Too many indexes have already been created on the physical table."),
    NO_LOCAL_INDEX_ON_TABLE_WITH_IMMUTABLE_ROWS(1048,"43A05","Local indexes aren't allowed on tables with immutable rows."),
    COLUMN_FAMILY_NOT_ALLOWED_TABLE_PROPERTY(1049, "43A06", "Column family not allowed for table properties."),
    COLUMN_FAMILY_NOT_ALLOWED_FOR_TTL(1050, "43A07", "Setting TTL for a column family not supported. You can only have TTL for the entire table."),
    CANNOT_ALTER_PROPERTY(1051, "43A08", "Property can be specified or changed only when creating a table"),
    CANNOT_SET_PROPERTY_FOR_COLUMN_NOT_ADDED(1052, "43A09", "Property cannot be specified for a column family that is not being added or modified"),
    CANNOT_SET_TABLE_PROPERTY_ADD_COLUMN(1053, "43A10", "Table level property cannot be set when adding a column"),
    
    NO_LOCAL_INDEXES(1054, "43A11", "Local secondary indexes are not supported for HBase versions " + 
        MetaDataUtil.decodeHBaseVersionAsString(PhoenixDatabaseMetaData.MIN_LOCAL_SI_VERSION_DISALLOW) + " through " + MetaDataUtil.decodeHBaseVersionAsString(PhoenixDatabaseMetaData.MAX_LOCAL_SI_VERSION_DISALLOW) + " inclusive."),
    UNALLOWED_LOCAL_INDEXES(1055, "43A12", "Local secondary indexes are configured to not be allowed."),

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
    EMPTY_SEQUENCE_CACHE(1207, "42Z07", "No more cached sequence values"),
    MINVALUE_MUST_BE_CONSTANT(1208, "42Z08", "Sequence MINVALUE must be an integer or long constant."),
    MAXVALUE_MUST_BE_CONSTANT(1209, "42Z09", "Sequence MAXVALUE must be an integer or long constant."),
    MINVALUE_MUST_BE_LESS_THAN_OR_EQUAL_TO_MAXVALUE(1210, "42Z10", "Sequence MINVALUE must be less than or equal to MAXVALUE."),
    STARTS_WITH_MUST_BE_BETWEEN_MIN_MAX_VALUE(1211, "42Z11",
            "STARTS WITH value must be greater than or equal to MINVALUE and less than or equal to MAXVALUE"),
    SEQUENCE_VAL_REACHED_MAX_VALUE(1212, "42Z12", "Reached MAXVALUE of sequence"),
    SEQUENCE_VAL_REACHED_MIN_VALUE(1213, "42Z13", "Reached MINVALUE of sequence"),
    INCREMENT_BY_MUST_NOT_BE_ZERO(1214, "42Z14", "Sequence INCREMENT BY value cannot be zero"),
                    
    /** Parser error. (errorcode 06, sqlState 42P) */
    PARSER_ERROR(601, "42P00", "Syntax error.", Factory.SYTAX_ERROR),
    MISSING_TOKEN(602, "42P00", "Syntax error.", Factory.SYTAX_ERROR),
    UNWANTED_TOKEN(603, "42P00", "Syntax error.", Factory.SYTAX_ERROR),
    MISMATCHED_TOKEN(604, "42P00", "Syntax error.", Factory.SYTAX_ERROR),
    UNKNOWN_FUNCTION(605, "42P00", "Syntax error.", Factory.SYTAX_ERROR),
    
    /**
     * Implementation defined class. Execution exceptions (errorcode 11, sqlstate XCL). 
     */
    RESULTSET_CLOSED(1101, "XCL01", "ResultSet is closed."),
    GET_TABLE_REGIONS_FAIL(1102, "XCL02", "Cannot get all table regions"),
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
    CANNOT_SPLIT_LOCAL_INDEX(1109,"XCL09", "Local index may not be pre-split"),
    CANNOT_SALT_LOCAL_INDEX(1110,"XCL10", "Local index may not be salted"),
    
    /**
     * Implementation defined class. Phoenix internal error. (errorcode 20, sqlstate INT).
     */
    CANNOT_CALL_METHOD_ON_TYPE(2001, "INT01", "Cannot call method on the argument type."),
    CLASS_NOT_UNWRAPPABLE(2002, "INT03", "Class not unwrappable"),
    PARAM_INDEX_OUT_OF_BOUND(2003, "INT04", "Parameter position is out of range."),
    PARAM_VALUE_UNBOUND(2004, "INT05", "Parameter value unbound"),
    INTERRUPTED_EXCEPTION(2005, "INT07", "Interrupted exception."),
    INCOMPATIBLE_CLIENT_SERVER_JAR(2006, "INT08", "Incompatible jars detected between client and server."),
    OUTDATED_JARS(2007, "INT09", "Outdated jars."),
    INDEX_METADATA_NOT_FOUND(2008, "INT10", "Unable to find cached index metadata. "),
    UNKNOWN_ERROR_CODE(2009, "INT11", "Unknown error code"),
    OPERATION_TIMED_OUT(6000, "TIM01", "Operation timed out", new Factory() {
        @Override
        public SQLException newException(SQLExceptionInfo info) {
            return new SQLTimeoutException(OPERATION_TIMED_OUT.getMessage(),
                    OPERATION_TIMED_OUT.getSQLState(), OPERATION_TIMED_OUT.getErrorCode());
        }
    }), 
    ;

    private final int errorCode;
    private final String sqlState;
    private final String message;
    private final Factory factory;

    private SQLExceptionCode(int errorCode, String sqlState, String message) {
        this(errorCode, sqlState, message, Factory.DEFAULTY);
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
        public static final Factory DEFAULTY = new Factory() {

            @Override
            public SQLException newException(SQLExceptionInfo info) {
                return new SQLException(info.toString(), info.getCode().getSQLState(), info.getCode().getErrorCode(), info.getRootCause());
            }
            
        };
        public static final Factory SYTAX_ERROR = new Factory() {

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
    
    public static SQLExceptionCode fromErrorCode(int errorCode) throws SQLException {
        SQLExceptionCode code = errorCodeMap.get(errorCode);
        if (code == null) {
            throw new SQLExceptionInfo.Builder(SQLExceptionCode.UNKNOWN_ERROR_CODE)
            .setMessage(Integer.toString(errorCode)).build().buildException();
        }
        return code;
    }
}
