/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.phoenix.hive.query;

import com.google.common.base.CharMatcher;
import com.google.common.base.Joiner;
import com.google.common.base.Predicate;
import com.google.common.base.Splitter;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import javax.annotation.Nullable;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hive.serde.serdeConstants;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.mapred.JobConf;
import org.apache.phoenix.hive.constants.PhoenixStorageHandlerConstants;
import org.apache.phoenix.hive.ql.index.IndexSearchCondition;
import org.apache.phoenix.hive.util.PhoenixStorageHandlerUtil;
import org.apache.phoenix.hive.util.PhoenixUtil;

/**
 * Query builder. Produces a query depending on the colummn list and conditions
 */

public class PhoenixQueryBuilder {

    private static final Log LOG = LogFactory.getLog(PhoenixQueryBuilder.class);

    private static final String QUERY_TEMPLATE = "select $HINT$ $COLUMN_LIST$ from $TABLE_NAME$";

    private static final PhoenixQueryBuilder QUERY_BUILDER = new PhoenixQueryBuilder();

    private PhoenixQueryBuilder() {
        if (LOG.isInfoEnabled()) {
            LOG.info("PhoenixQueryBuilder created");
        }
    }

    public static PhoenixQueryBuilder getInstance() {
        return QUERY_BUILDER;
    }

    private void addConditionColumnToReadColumn(List<String> readColumnList, List<String>
            conditionColumnList) {
        if (readColumnList.isEmpty()) {
            return;
        }

        for (String conditionColumn : conditionColumnList) {
            if (!readColumnList.contains(conditionColumn)) {
                if (LOG.isDebugEnabled()) {
                    LOG.debug("Condition column " + conditionColumn + " does not exist in " +
                            "read-columns.");
                }

                readColumnList.add(conditionColumn);
            }
        }
    }

    private String makeQueryString(JobConf jobConf, String tableName, List<String>
            readColumnList, String whereClause, String queryTemplate, String hints, Map<String,
            TypeInfo> columnTypeMap) throws IOException {
        StringBuilder sql = new StringBuilder();
        List<String> conditionColumnList = buildWhereClause(jobConf, sql, whereClause,columnTypeMap);

        if (conditionColumnList.size() > 0) {
            addConditionColumnToReadColumn(readColumnList, conditionColumnList);
            sql.insert(0, queryTemplate.replace("$HINT$", hints).replace("$COLUMN_LIST$",
                    getSelectColumns(jobConf, tableName, readColumnList)).replace("$TABLE_NAME$",
                    tableName));
        } else {
            sql.append(queryTemplate.replace("$HINT$", hints).replace("$COLUMN_LIST$",
                    getSelectColumns(jobConf, tableName, readColumnList)).replace("$TABLE_NAME$",
                    tableName));
        }

        if (LOG.isInfoEnabled()) {
            LOG.info("Input query : " + sql.toString());
        }

        return sql.toString();
    }

    private String makeQueryString(JobConf jobConf, String tableName, List<String>
            readColumnList, List<IndexSearchCondition> searchConditions, String queryTemplate,
                                   String hints) throws IOException {
        StringBuilder sql = new StringBuilder();
        List<String> conditionColumnList = buildWhereClause(jobConf, sql, searchConditions);

        if (conditionColumnList.size() > 0) {
            addConditionColumnToReadColumn(readColumnList, conditionColumnList);
            sql.insert(0, queryTemplate.replace("$HINT$", hints).replace("$COLUMN_LIST$",
                    getSelectColumns(jobConf, tableName, readColumnList)).replace("$TABLE_NAME$",
                    tableName));
        } else {
            sql.append(queryTemplate.replace("$HINT$", hints).replace("$COLUMN_LIST$",
                    getSelectColumns(jobConf, tableName, readColumnList)).replace("$TABLE_NAME$",
                    tableName));
        }

        if (LOG.isInfoEnabled()) {
            LOG.info("Input query : " + sql.toString());
        }

        return sql.toString();
    }

    private String getSelectColumns(JobConf jobConf, String tableName, List<String>
            readColumnList) throws IOException {
        String selectColumns = Joiner.on(PhoenixStorageHandlerConstants.COMMA).join(readColumnList);

        if (PhoenixStorageHandlerConstants.EMPTY_STRING.equals(selectColumns)) {
            selectColumns = "*";
        } else {
            if (PhoenixStorageHandlerUtil.isTransactionalTable(jobConf)) {
                List<String> pkColumnList = PhoenixUtil.getPrimaryKeyColumnList(jobConf, tableName);
                StringBuilder pkColumns = new StringBuilder();

                for (String pkColumn : pkColumnList) {
                    String pkColumnName = pkColumn.toLowerCase();

                    if (!readColumnList.contains(pkColumnName)) {
                        pkColumns.append(pkColumnName).append(PhoenixStorageHandlerConstants.COMMA);
                    }
                }

                selectColumns = pkColumns.toString() + selectColumns;
            }
        }

        return selectColumns;
    }

    public String buildQuery(JobConf jobConf, String tableName, List<String> readColumnList,
                             String whereClause, Map<String, TypeInfo> columnTypeMap) throws
            IOException {
        String hints = getHint(jobConf, tableName);

        if (LOG.isDebugEnabled()) {
            LOG.debug("Building query with columns : " + readColumnList + " table name : " +
                    tableName + "  with where conditions : " + whereClause + "  hints : " + hints);
        }

        return makeQueryString(jobConf, tableName, Lists.newArrayList(readColumnList),
                whereClause, QUERY_TEMPLATE, hints, columnTypeMap);
    }

    public String buildQuery(JobConf jobConf, String tableName, List<String> readColumnList,
                             List<IndexSearchCondition> searchConditions) throws IOException {
        String hints = getHint(jobConf, tableName);

        if (LOG.isDebugEnabled()) {
            LOG.debug("Building query with columns : " + readColumnList + "  table name : " +
                    tableName + " search conditions : " + searchConditions + "  hints : " + hints);
        }

        return makeQueryString(jobConf, tableName, Lists.newArrayList(readColumnList),
                searchConditions, QUERY_TEMPLATE, hints);
    }

    private String getHint(JobConf jobConf, String tableName) {
        StringBuilder hints = new StringBuilder("/*+ ");
        if (!jobConf.getBoolean(PhoenixStorageHandlerConstants.HBASE_SCAN_CACHEBLOCKS, Boolean
                .FALSE)) {
            hints.append("NO_CACHE ");
        }

        String queryHint = jobConf.get(tableName + PhoenixStorageHandlerConstants
                .PHOENIX_TABLE_QUERY_HINT);
        if (queryHint != null) {
            hints.append(queryHint);
        }
        hints.append(" */");

        return hints.toString();
    }

    private List<String> buildWhereClause(JobConf jobConf, StringBuilder sql, String whereClause,
                                          Map<String, TypeInfo> columnTypeMap) throws IOException {
        if (whereClause == null || whereClause.isEmpty()) {
            return Collections.emptyList();
        }

        List<String> conditionColumnList = Lists.newArrayList();
        sql.append(" where ");

        whereClause = StringUtils.replaceEach(whereClause, new String[]{"UDFToString"}, new
                String[]{"to_char"});

        for (String columnName : columnTypeMap.keySet()) {
            if (whereClause.contains(columnName)) {
                conditionColumnList.add(columnName);

                if (PhoenixStorageHandlerConstants.DATE_TYPE.equals(
                        columnTypeMap.get(columnName).getTypeName())) {
                    whereClause = applyDateFunctionUsingRegex(whereClause, columnName);
                } else if (PhoenixStorageHandlerConstants.TIMESTAMP_TYPE.equals(
                        columnTypeMap.get(columnName).getTypeName())) {
                    whereClause = applyTimestampFunctionUsingRegex(whereClause, columnName);
                }
            }
        }

        sql.append(whereClause);

        return conditionColumnList;
    }

    private String applyDateFunctionUsingRegex(String whereClause, String columnName) {
        whereClause = applyFunctionForCommonOperator(whereClause, columnName, true);
        whereClause = applyFunctionForBetweenOperator(whereClause, columnName, true);
        whereClause = applyFunctionForInOperator(whereClause, columnName, true);

        return whereClause;
    }

    private String applyTimestampFunctionUsingRegex(String whereClause, String columnName) {
        whereClause = applyFunctionForCommonOperator(whereClause, columnName, false);
        whereClause = applyFunctionForBetweenOperator(whereClause, columnName, false);
        whereClause = applyFunctionForInOperator(whereClause, columnName, false);

        return whereClause;
    }

    private String applyFunctionForCommonOperator(String whereClause, String columnName, boolean
            isDate) {
        String targetPattern = isDate ? PhoenixStorageHandlerConstants.DATE_PATTERN :
                PhoenixStorageHandlerConstants.TIMESTAMP_PATTERN;
        String pattern = StringUtils.replaceEach(PhoenixStorageHandlerConstants
                        .COMMON_OPERATOR_PATTERN,
                new String[]{PhoenixStorageHandlerConstants.COLUMNE_MARKER,
                        PhoenixStorageHandlerConstants.PATERN_MARKER}, new String[]{columnName,
                        targetPattern});

        Matcher matcher = Pattern.compile(pattern).matcher(whereClause);

        while (matcher.find()) {
            String token = matcher.group(1);
            String datePart = matcher.group(3);

            String convertString = token.replace(datePart, applyFunction(isDate ?
                    PhoenixStorageHandlerConstants.DATE_FUNCTION_TEMPLETE :
                    PhoenixStorageHandlerConstants.TIMESTAMP_FUNCTION_TEMPLATE, datePart));
            whereClause = whereClause.replaceAll(StringUtils.replaceEach(token, new String[]{"(",
                    ")"}, new String[]{"\\(", "\\)"}), convertString);
        }

        return whereClause;
    }

    private String applyFunctionForBetweenOperator(String whereClause, String columnName, boolean
            isDate) {
        String targetPattern = isDate ? PhoenixStorageHandlerConstants.DATE_PATTERN :
                PhoenixStorageHandlerConstants.TIMESTAMP_PATTERN;
        String pattern = StringUtils.replaceEach(PhoenixStorageHandlerConstants
                        .BETWEEN_OPERATOR_PATTERN,
                new String[]{PhoenixStorageHandlerConstants.COLUMNE_MARKER,
                        PhoenixStorageHandlerConstants.PATERN_MARKER}, new String[]{columnName,
                        targetPattern});

        Matcher matcher = Pattern.compile(pattern).matcher(whereClause);

        while (matcher.find()) {
            String token = matcher.group(1);
            boolean isNot = matcher.group(2) == null ? false : true;
            String fromDate = matcher.group(3);
            String toDate = matcher.group(4);

            String convertString = StringUtils.replaceEach(token, new String[]{fromDate, toDate},
                    new String[]{applyFunction(isDate ? PhoenixStorageHandlerConstants
                            .DATE_FUNCTION_TEMPLETE : PhoenixStorageHandlerConstants
                            .TIMESTAMP_FUNCTION_TEMPLATE, fromDate),
                            applyFunction(isDate ? PhoenixStorageHandlerConstants
                                    .DATE_FUNCTION_TEMPLETE : PhoenixStorageHandlerConstants
                                    .TIMESTAMP_FUNCTION_TEMPLATE, toDate)});

            whereClause = whereClause.replaceAll(pattern, convertString);
        }

        return whereClause;
    }

    private String applyFunctionForInOperator(String whereClause, String columnName, boolean
            isDate) {
        String targetPattern = isDate ? PhoenixStorageHandlerConstants.DATE_PATTERN :
                PhoenixStorageHandlerConstants.TIMESTAMP_PATTERN;
        String pattern = StringUtils.replaceEach(PhoenixStorageHandlerConstants.IN_OPERATOR_PATTERN,
                new String[]{PhoenixStorageHandlerConstants.COLUMNE_MARKER,
                        PhoenixStorageHandlerConstants.PATERN_MARKER}, new String[]{columnName,
                        targetPattern});
        String itemPattern = "(" + targetPattern + ")";

        Matcher matcher = Pattern.compile(pattern).matcher(whereClause);

        while (matcher.find()) {
            String token = matcher.group(1);
            Matcher itemMatcher = Pattern.compile(itemPattern).matcher(token);
            while (itemMatcher.find()) {
                String item = itemMatcher.group(1);

                token = token.replace(item, applyFunction(isDate ? PhoenixStorageHandlerConstants
                        .DATE_FUNCTION_TEMPLETE : PhoenixStorageHandlerConstants
                        .TIMESTAMP_FUNCTION_TEMPLATE, item));
            }

            whereClause = whereClause.replaceAll(pattern, token);
        }

        return whereClause;
    }

    /**
     * replace value to specific part of pattern.
     * if pattern is to_date($value$) and value is '2016-01-15'. then return to_date('2016-01-15').
     * if pattern is cast($value$ as date) and value is '2016-01-15'. then return cast
     * ('2016-01-15' as date).
     */
    private String applyFunction(String pattern, String value) {
        if (!value.startsWith(PhoenixStorageHandlerConstants.QUOTATION_MARK)) {
            value = PhoenixStorageHandlerConstants.QUOTATION_MARK + value +
                    PhoenixStorageHandlerConstants.QUOTATION_MARK;
        }

        return pattern.replace(PhoenixStorageHandlerConstants.FUNCTION_VALUE_MARKER, value);
    }

    private String getCompareValueForDateAndTimestampFunction(String compareValue) {
        if (compareValue.startsWith(PhoenixStorageHandlerConstants.QUOTATION_MARK)) {
            return compareValue;
        } else {
            return PhoenixStorageHandlerConstants.QUOTATION_MARK + compareValue +
                    PhoenixStorageHandlerConstants.QUOTATION_MARK;
        }
    }

    private String applyDateFunction(String whereClause, String columnName) {
        StringBuilder whereCondition = new StringBuilder();
        for (Iterator<String> iterator = Splitter.on(CharMatcher.WHITESPACE).omitEmptyStrings()
                .split(whereClause).iterator(); iterator.hasNext(); whereCondition.append
                (PhoenixStorageHandlerConstants.SPACE)) {
            String token = iterator.next();
            if (isMyCondition(columnName, token)) {
                whereCondition.append(token);

                String comparator = iterator.next();
                whereCondition.append(PhoenixStorageHandlerConstants.SPACE);
                whereCondition.append(comparator).append(PhoenixStorageHandlerConstants.SPACE);
                if (PhoenixStorageHandlerConstants.BETWEEN_COMPARATOR.equalsIgnoreCase
                        (comparator)) {
                    whereCondition.append("to_date(").append
                            (getCompareValueForDateAndTimestampFunction(iterator.next())).append
                            (") ").append(iterator.next()).append(PhoenixStorageHandlerConstants
                            .SPACE)
                            .append("to_date(");

                    String toCompareValue = iterator.next();
                    if (toCompareValue.endsWith(PhoenixStorageHandlerConstants
                            .RIGHT_ROUND_BRACKET)) {
                        int rightBracketIndex = toCompareValue.indexOf
                                (PhoenixStorageHandlerConstants.RIGHT_ROUND_BRACKET);
                        whereCondition.append(getCompareValueForDateAndTimestampFunction
                                (toCompareValue.substring(0, rightBracketIndex))).append
                                (PhoenixStorageHandlerConstants.RIGHT_ROUND_BRACKET).append
                                (toCompareValue.substring(rightBracketIndex));
                    } else {
                        whereCondition.append(getCompareValueForDateAndTimestampFunction
                                (toCompareValue)).append(PhoenixStorageHandlerConstants
                                .RIGHT_ROUND_BRACKET);
                    }
                } else if (PhoenixStorageHandlerConstants.IN_COMPARATOR.equalsIgnoreCase
                        (comparator)) {
                    while (iterator.hasNext()) {
                        String aToken = iterator.next();
                        if (aToken.equals(PhoenixStorageHandlerConstants.LEFT_ROUND_BRACKET) ||
                                aToken.equals(PhoenixStorageHandlerConstants.COMMA)) {
                            whereCondition.append(aToken);
                        } else if (aToken.equals(PhoenixStorageHandlerConstants
                                .RIGHT_ROUND_BRACKET)) {
                            whereCondition.append(aToken);
                            break;
                        } else if (aToken.endsWith(PhoenixStorageHandlerConstants
                                .RIGHT_ROUND_BRACKET)) {
                            int bracketIndex = aToken.indexOf(PhoenixStorageHandlerConstants
                                    .RIGHT_ROUND_BRACKET);
                            whereCondition.append("to_date(").append
                                    (getCompareValueForDateAndTimestampFunction(aToken.substring
                                            (0, bracketIndex))).append
                                    (PhoenixStorageHandlerConstants.RIGHT_ROUND_BRACKET).append
                                    (aToken.substring(bracketIndex));
                            break;
                        } else if (aToken.endsWith(PhoenixStorageHandlerConstants.COMMA)) {
                            if (aToken.startsWith(PhoenixStorageHandlerConstants
                                    .LEFT_ROUND_BRACKET)) {
                                int bracketIndex = aToken.lastIndexOf
                                        (PhoenixStorageHandlerConstants.LEFT_ROUND_BRACKET);
                                whereCondition.append(aToken.substring(0, bracketIndex + 1))
                                        .append("to_date(").append
                                        (getCompareValueForDateAndTimestampFunction(aToken
                                                .substring(bracketIndex + 1, aToken.length() - 1)
                                        )).append("),");
                            } else {
                                whereCondition.append("to_date(").append
                                        (getCompareValueForDateAndTimestampFunction(aToken
                                                .substring(0, aToken.length() - 1))).append("),");
                            }
                        }

                        whereCondition.append(PhoenixStorageHandlerConstants.SPACE);
                    }
                } else if (PhoenixStorageHandlerConstants.COMMON_COMPARATOR.contains(comparator)) {
                    String compareValue = getCompareValueForDateAndTimestampFunction(iterator
                            .next());
                    whereCondition.append("to_date(");
                    if (compareValue.endsWith(PhoenixStorageHandlerConstants.RIGHT_ROUND_BRACKET)) {
                        int rightBracketIndex = compareValue.indexOf
                                (PhoenixStorageHandlerConstants.RIGHT_ROUND_BRACKET);
                        whereCondition.append(getCompareValueForDateAndTimestampFunction
                                (compareValue.substring(0, rightBracketIndex))).append
                                (PhoenixStorageHandlerConstants.RIGHT_ROUND_BRACKET).append
                                (compareValue.substring(rightBracketIndex));
                    } else {
                        whereCondition.append(getCompareValueForDateAndTimestampFunction
                                (compareValue)).append(PhoenixStorageHandlerConstants
                                .RIGHT_ROUND_BRACKET);
                    }
                }
            } else {
                whereCondition.append(token);
            }
        }

        return whereCondition.toString();
    }

    // Assume timestamp value is yyyy-MM-dd HH:mm:ss.SSS
    private String applyTimestampFunction(String whereClause, String columnName) {
        StringBuilder whereCondition = new StringBuilder();
        for (Iterator<String> iterator = Splitter.on(CharMatcher.WHITESPACE).omitEmptyStrings()
                .split(whereClause).iterator(); iterator.hasNext(); whereCondition.append
                (PhoenixStorageHandlerConstants.SPACE)) {
            String token = iterator.next();
            if (isMyCondition(columnName, token)) {
                whereCondition.append(token);

                String comparator = iterator.next();
                whereCondition.append(PhoenixStorageHandlerConstants.SPACE);
                whereCondition.append(comparator).append(PhoenixStorageHandlerConstants.SPACE);
                if (PhoenixStorageHandlerConstants.BETWEEN_COMPARATOR.equalsIgnoreCase
                        (comparator)) {
                    String fromCompareValue = iterator.next() + PhoenixStorageHandlerConstants
                            .SPACE + iterator.next();
                    whereCondition.append("to_timestamp(").append
                            (getCompareValueForDateAndTimestampFunction(fromCompareValue)).append
                            (PhoenixStorageHandlerConstants.RIGHT_ROUND_BRACKET);
                    whereCondition.append(PhoenixStorageHandlerConstants.SPACE).append(iterator
                            .next()).append(PhoenixStorageHandlerConstants.SPACE);
                    whereCondition.append("to_timestamp(");

                    String toCompareValue = iterator.next() + PhoenixStorageHandlerConstants
                            .SPACE + iterator.next();
                    if (toCompareValue.endsWith(PhoenixStorageHandlerConstants
                            .RIGHT_ROUND_BRACKET)) {
                        int rightBracketIndex = toCompareValue.indexOf
                                (PhoenixStorageHandlerConstants.RIGHT_ROUND_BRACKET);
                        whereCondition.append(getCompareValueForDateAndTimestampFunction
                                (toCompareValue.substring(0, rightBracketIndex))).append
                                (PhoenixStorageHandlerConstants.RIGHT_ROUND_BRACKET).append
                                (toCompareValue.substring(rightBracketIndex));
                    } else {
                        whereCondition.append(getCompareValueForDateAndTimestampFunction
                                (toCompareValue)).append(PhoenixStorageHandlerConstants
                                .RIGHT_ROUND_BRACKET);
                    }
                } else if (PhoenixStorageHandlerConstants.IN_COMPARATOR.equalsIgnoreCase
                        (comparator)) {
                    while (iterator.hasNext()) {
                        String aToken = iterator.next();
                        if (aToken.equals(PhoenixStorageHandlerConstants.LEFT_ROUND_BRACKET) ||
                                aToken.equals(PhoenixStorageHandlerConstants.COMMA)) {
                            whereCondition.append(aToken);
                        } else if (aToken.equals(PhoenixStorageHandlerConstants
                                .RIGHT_ROUND_BRACKET)) {
                            whereCondition.append(aToken);
                            break;
                        } else {
                            String compareValue = aToken + PhoenixStorageHandlerConstants.SPACE +
                                    iterator.next();

                            if (compareValue.startsWith(PhoenixStorageHandlerConstants
                                    .LEFT_ROUND_BRACKET)) {
                                int leftBracketIndex = compareValue.lastIndexOf
                                        (PhoenixStorageHandlerConstants.LEFT_ROUND_BRACKET);
                                whereCondition.append(compareValue.substring(0, leftBracketIndex
                                        + 1)).append("to_timestamp(");

                                if (compareValue.endsWith(PhoenixStorageHandlerConstants
                                        .RIGHT_ROUND_BRACKET)) {
                                    int rightBracketIndex = compareValue.indexOf
                                            (PhoenixStorageHandlerConstants.RIGHT_ROUND_BRACKET);
                                    whereCondition.append
                                            (getCompareValueForDateAndTimestampFunction
                                                    (compareValue.substring(leftBracketIndex + 1,
                                                            rightBracketIndex)))
                                            .append(PhoenixStorageHandlerConstants
                                                    .RIGHT_ROUND_BRACKET).append(compareValue
                                            .substring(rightBracketIndex));
                                } else if (compareValue.endsWith(PhoenixStorageHandlerConstants
                                        .COMMA)) {
                                    whereCondition.append
                                            (getCompareValueForDateAndTimestampFunction
                                                    (compareValue.substring(leftBracketIndex + 1,
                                                            compareValue.length() - 1)))
                                            .append(PhoenixStorageHandlerConstants
                                                    .RIGHT_ROUND_BRACKET).append
                                            (PhoenixStorageHandlerConstants.COMMA);
                                } else {
                                    whereCondition.append
                                            (getCompareValueForDateAndTimestampFunction
                                                    (compareValue.substring(leftBracketIndex + 1)
                                                    )).append(PhoenixStorageHandlerConstants
                                            .RIGHT_ROUND_BRACKET);
                                }
                            } else if (compareValue.endsWith(PhoenixStorageHandlerConstants
                                    .RIGHT_ROUND_BRACKET)) {
                                int rightBracketIndex = compareValue.indexOf
                                        (PhoenixStorageHandlerConstants.RIGHT_ROUND_BRACKET);
                                whereCondition.append("to_timestamp(").append
                                        (getCompareValueForDateAndTimestampFunction(compareValue
                                                .substring(0, rightBracketIndex)))
                                        .append(PhoenixStorageHandlerConstants
                                                .RIGHT_ROUND_BRACKET).append(compareValue
                                        .substring(rightBracketIndex));
                                break;
                            } else if (compareValue.endsWith(PhoenixStorageHandlerConstants
                                    .COMMA)) {
                                whereCondition.append("to_timestamp(").append
                                        (getCompareValueForDateAndTimestampFunction(compareValue
                                                .substring(0, compareValue.length() - 1))).append
                                        ("),");
                            }
                        }

                        whereCondition.append(PhoenixStorageHandlerConstants.SPACE);
                    }
                } else if (PhoenixStorageHandlerConstants.COMMON_COMPARATOR.contains(comparator)) {
                    String timestampValue = iterator.next() + PhoenixStorageHandlerConstants
                            .SPACE + iterator.next();
                    whereCondition.append("to_timestamp(");
                    if (timestampValue.endsWith(PhoenixStorageHandlerConstants
                            .RIGHT_ROUND_BRACKET)) {
                        int rightBracketIndex = timestampValue.indexOf
                                (PhoenixStorageHandlerConstants.RIGHT_ROUND_BRACKET);
                        whereCondition.append(getCompareValueForDateAndTimestampFunction
                                (timestampValue.substring(0, rightBracketIndex))).append
                                (PhoenixStorageHandlerConstants.RIGHT_ROUND_BRACKET).append
                                (timestampValue.substring(rightBracketIndex));
                    } else {
                        whereCondition.append(getCompareValueForDateAndTimestampFunction
                                (timestampValue)).append(PhoenixStorageHandlerConstants
                                .RIGHT_ROUND_BRACKET);
                    }
                }
            } else {
                whereCondition.append(token);
            }
        }

        return whereCondition.toString();
    }

    private boolean isMyCondition(String columnName, String token) {
        boolean itsMine = false;

        if (columnName.equals(token)) {
            itsMine = true;
        } else if (token.startsWith(PhoenixStorageHandlerConstants.LEFT_ROUND_BRACKET) && token
                .substring(token.lastIndexOf(PhoenixStorageHandlerConstants.LEFT_ROUND_BRACKET) +
                        1).equals(columnName)) {
            itsMine = true;
        } else if (token.startsWith(PhoenixStorageHandlerConstants.LEFT_ROUND_BRACKET) && token
                .endsWith(PhoenixStorageHandlerConstants.RIGHT_ROUND_BRACKET)
                && token.substring(token.lastIndexOf(PhoenixStorageHandlerConstants
                .LEFT_ROUND_BRACKET) + 1, token.indexOf(PhoenixStorageHandlerConstants
                .RIGHT_ROUND_BRACKET)).equals(columnName)) {
            itsMine = true;
        }

        return itsMine;
    }

    protected List<String> buildWhereClause(JobConf jobConf, StringBuilder sql,
                                            List<IndexSearchCondition> searchConditions) throws
            IOException {
        if (searchConditions == null || searchConditions.size() == 0) {
            return Collections.emptyList();
        }

        List<String> conditionColumnList = Lists.newArrayList();
        sql.append(" where ");

        boolean firstCondition = true;
        for (IndexSearchCondition condition : searchConditions) {
            String comparisonOp = condition.getComparisonOp();

            if (comparisonOp.endsWith("GenericUDFBetween") || comparisonOp.endsWith
                    ("GenericUDFIn")) {
                if (condition.getConstantDescs() == null) {
                    continue;
                }
            } else if (comparisonOp.endsWith("GenericUDFOPNull") || comparisonOp.endsWith
                    ("GenericUDFOPNotNull")) {
                // keep going
            } else if (comparisonOp.endsWith("GenericUDFOPEqual")) {
                // keep going
            } else {
                if (condition.getConstantDesc().getValue() == null) {
                    continue;
                }
            }

            if (!firstCondition) {
                sql.append(" and ");
            } else {
                firstCondition = false;
            }

            String columnName = condition.getColumnDesc().getColumn();
            String typeName = condition.getColumnDesc().getTypeString();

            if (LOG.isDebugEnabled()) {
                LOG.debug(columnName + " has condition: " + condition);
            }

            conditionColumnList.add(columnName);
            sql.append(columnName);

            String[] constantValues = PhoenixStorageHandlerUtil.getConstantValues(condition,
                    comparisonOp);

            if (comparisonOp.endsWith("UDFOPEqual")) {        // column = 1
                sql.append(" = ").append(createConstantString(typeName, constantValues[0]));
            } else if (comparisonOp.endsWith("UDFOPEqualOrGreaterThan")) {    // column >= 1
                sql.append(" >= ").append(createConstantString(typeName, constantValues[0]));
            } else if (comparisonOp.endsWith("UDFOPGreaterThan")) {        // column > 1
                sql.append(" > ").append(createConstantString(typeName, constantValues[0]));
            } else if (comparisonOp.endsWith("UDFOPEqualOrLessThan")) {    // column <= 1
                sql.append(" <= ").append(createConstantString(typeName, constantValues[0]));
            } else if (comparisonOp.endsWith("UDFOPLessThan")) {    // column < 1
                sql.append(" < ").append(createConstantString(typeName, constantValues[0]));
            } else if (comparisonOp.endsWith("UDFOPNotEqual")) {    // column != 1
                sql.append(" != ").append(createConstantString(typeName, constantValues[0]));
            } else if (comparisonOp.endsWith("GenericUDFBetween")) {
                appendBetweenCondition(jobConf, sql, condition.isNot(), typeName, constantValues);
            } else if (comparisonOp.endsWith("GenericUDFIn")) {
                appendInCondition(sql, condition.isNot(), typeName, constantValues);
            } else if (comparisonOp.endsWith("GenericUDFOPNull")) {
                sql.append(" is null ");
            } else if (comparisonOp.endsWith("GenericUDFOPNotNull")) {
                sql.append(" is not null ");
            }
        }

        return conditionColumnList;
    }

    protected void appendBetweenCondition(JobConf jobConf, StringBuilder sql, boolean isNot,
                                          String typeName, String[] conditionValues) throws
            IOException {
        try {
            Object[] typedValues = PhoenixStorageHandlerUtil.toTypedValues(jobConf, typeName, conditionValues);
            Arrays.sort(typedValues);

            appendIfNot(isNot, sql).append(" between ")
                    .append(Joiner.on(" and ").join(createConstantString(typeName, typedValues[0]),
                    createConstantString(typeName, typedValues[1])));
        } catch (Exception e) {
            throw new IOException(e);
        }
    }

    protected void appendInCondition(StringBuilder sql, boolean isNot, String typeName, String[]
            conditionValues) {
        List<Object> wrappedConstants = Lists.newArrayListWithCapacity(conditionValues.length);
        for (String conditionValue : conditionValues) {
            wrappedConstants.add(createConstantString(typeName, conditionValue));
        }

        appendIfNot(isNot, sql)
                .append(" in (")
                .append(Joiner.on(", ").join(wrappedConstants))
                .append(")");
    }

    private StringBuilder appendIfNot(boolean isNot, StringBuilder sb) {
        return isNot ? sb.append(" not") : sb;
    }

    private static class ConstantStringWrapper {
        private List<String> types;
        private String prefix;
        private String postfix;

        ConstantStringWrapper(String type, String prefix, String postfix) {
            this(Lists.newArrayList(type), prefix, postfix);
        }

        ConstantStringWrapper(List<String> types, String prefix, String postfix) {
            this.types = types;
            this.prefix = prefix;
            this.postfix = postfix;
        }

        public Object apply(final String typeName, Object value) {
            return Iterables.any(types, new Predicate<String>() {

                @Override
                public boolean apply(@Nullable String type) {
                    return typeName.startsWith(type);
                }
            }) ? prefix + value + postfix : value;
        }
    }

    private static final String SINGLE_QUOTATION = "'";
    private static List<ConstantStringWrapper> WRAPPERS = Lists.newArrayList(
            new ConstantStringWrapper(Lists.newArrayList(
                    serdeConstants.STRING_TYPE_NAME, serdeConstants.CHAR_TYPE_NAME,
                    serdeConstants.VARCHAR_TYPE_NAME, serdeConstants.DATE_TYPE_NAME,
                    serdeConstants.TIMESTAMP_TYPE_NAME
            ), SINGLE_QUOTATION, SINGLE_QUOTATION),
            new ConstantStringWrapper(serdeConstants.DATE_TYPE_NAME, "to_date(", ")"),
            new ConstantStringWrapper(serdeConstants.TIMESTAMP_TYPE_NAME, "to_timestamp(", ")")
    );

    private Object createConstantString(String typeName, Object value) {
        for (ConstantStringWrapper wrapper : WRAPPERS) {
            value = wrapper.apply(typeName, value);
        }

        return value;
    }
}
