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
package org.apache.phoenix.expression;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import org.junit.Test;

/**
 * Makes sure that the ordinals of {@link ExpressionType} entries do not change. Clients and servers
 * of different versions serialize expressions by ordinal. Thus, add new entries only at the end of
 * the enum, and do not move or remove a released entry.
 */
public class ExpressionTypeOrdinalTest {

  /** The entries that existed before vector support, in their released order. */
  private static final String[] RELEASED = { "ReverseFunction", "RowKey", "KeyValue",
    "LiteralValue", "RoundFunction", "FloorFunction", "CeilFunction", "RoundDateExpression",
    "FloorDateExpression", "CeilDateExpression", "RoundTimestampExpression",
    "CeilTimestampExpression", "RoundDecimalExpression", "FloorDecimalExpression",
    "CeilDecimalExpression", "TruncFunction", "ToDateFunction", "ToCharFunction",
    "ToNumberFunction", "CoerceFunction", "SubstrFunction", "AndExpression", "OrExpression",
    "ComparisonExpression", "CountAggregateFunction", "SumAggregateFunction",
    "MinAggregateFunction", "MaxAggregateFunction", "StringBasedLikeExpression", "NotExpression",
    "CaseExpression", "InListExpression", "IsNullExpression", "LongSubtractExpression",
    "DateSubtractExpression", "DecimalSubtractExpression", "LongAddExpression",
    "DecimalAddExpression", "DateAddExpression", "LongMultiplyExpression",
    "DecimalMultiplyExpression", "LongDivideExpression", "DecimalDivideExpression",
    "CoalesceFunction", "StringBasedRegexpReplaceFunction", "SQLTypeNameFunction",
    "StringBasedRegexpSubstrFunction", "StringConcatExpression", "LengthFunction", "LTrimFunction",
    "RTrimFunction", "UpperFunction", "LowerFunction", "TrimFunction",
    "DistinctCountAggregateFunction", "PercentileContAggregateFunction",
    "PercentRankAggregateFunction", "StddevPopFunction", "StddevSampFunction",
    "PercentileDiscAggregateFunction", "DoubleAddExpression", "DoubleSubtractExpression",
    "DoubleMultiplyExpression", "DoubleDivideExpression", "RowValueConstructorExpression",
    "MD5Function", "SQLTableTypeFunction", "IndexStateName", "InvertFunction",
    "ProjectedColumnExpression", "TimestampAddExpression", "TimestampSubtractExpression",
    "ArrayIndexFunction", "ArrayLengthFunction", "ArrayConstructorExpression",
    "SQLViewTypeFunction", "ExternalSqlTypeIdFunction", "ConvertTimezoneFunction", "DecodeFunction",
    "TimezoneOffsetFunction", "EncodeFunction", "LpadFunction", "NthValueFunction",
    "FirstValueFunction", "LastValueFunction", "ArrayAnyComparisonExpression",
    "ArrayAllComparisonExpression", "InlineArrayElemRefExpression", "SQLIndexTypeFunction",
    "ModulusExpression", "DistinctValueAggregateFunction", "StringBasedRegexpSplitFunction",
    "RandomFunction", "ToTimeFunction", "ToTimestampFunction", "ByteBasedLikeExpression",
    "ByteBasedRegexpReplaceFunction", "ByteBasedRegexpSubstrFunction",
    "ByteBasedRegexpSplitFunction", "LikeExpression", "RegexpReplaceFunction",
    "RegexpSubstrFunction", "RegexpSplitFunction", "SignFunction", "YearFunction", "MonthFunction",
    "SecondFunction", "WeekFunction", "HourFunction", "NowFunction", "InstrFunction",
    "MinuteFunction", "DayOfMonthFunction", "ArrayAppendFunction", "UDFExpression",
    "ArrayPrependFunction", "SqrtFunction", "AbsFunction", "CbrtFunction", "LnFunction",
    "LogFunction", "ExpFunction", "PowerFunction", "ArrayConcatFunction", "ArrayFillFunction",
    "ArrayToStringFunction", "StringToArrayFunction", "GetByteFunction", "SetByteFunction",
    "GetBitFunction", "SetBitFunction", "OctetLengthFunction", "RoundWeekExpression",
    "RoundMonthExpression", "RoundYearExpression", "FloorWeekExpression", "FloorMonthExpression",
    "FloorYearExpression", "CeilWeekExpression", "CeilMonthExpression", "CeilYearExpression",
    "DayOfWeekFunction", "DayOfYearFunction", "DefaultValueExpression", "ArrayColumnExpression",
    "FirstValuesFunction", "LastValuesFunction", "DistinctCountHyperLogLogAggregateFunction",
    "CollationKeyFunction", "ArrayRemoveFunction", "TransactionProviderNameFunction",
    "MathPIFunction", "SinFunction", "CosFunction", "TanFunction", "RowKeyBytesStringFunction",
    "PhoenixRowTimestampFunction", "JsonValueFunction", "JsonQueryFunction", "JsonExistsFunction",
    "JsonModifyFunction", "BsonConditionExpressionFunction", "BsonUpdateExpressionFunction",
    "BsonValueFunction", "BsonValueTypeFunction", "PartitionIdFunction", "DecodeBinaryFunction",
    "EncodeBinaryFunction", "DecodeViewIdFunction", "SubBinaryFunction", "ScanStartKeyFunction",
    "ScanEndKeyFunction", "TotalSegmentsFunction", "RowSizeFunction", "RawRowSizeFunction",
    "RegexpLikeFunction", "ByteBasedRegexpLikeFunction", "StringBasedRegexpLikeFunction", };

  @Test
  public void testReleasedOrdinalsAreUnchanged() {
    for (int i = 0; i < RELEASED.length; i++) {
      assertEquals("ordinal of " + RELEASED[i], i, ExpressionType.valueOf(RELEASED[i]).ordinal());
    }
  }

  @Test
  public void testVectorExpressionsAreAppended() {
    String[] vectorTypes = { "L2DistanceFunction", "L2DistanceSquaredFunction",
      "CosineDistanceFunction", "InnerProductDistanceFunction", "BsonVectorValueFunction" };
    for (String name : vectorTypes) {
      assertTrue(name + " must follow every released entry",
        ExpressionType.valueOf(name).ordinal() >= RELEASED.length);
    }
  }
}
