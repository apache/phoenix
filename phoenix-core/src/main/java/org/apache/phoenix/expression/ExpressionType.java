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
package org.apache.phoenix.expression;

import java.util.Map;

import org.apache.phoenix.expression.function.ArrayAllComparisonExpression;
import org.apache.phoenix.expression.function.ArrayAnyComparisonExpression;
import org.apache.phoenix.expression.function.ArrayElemRefExpression;
import org.apache.phoenix.expression.function.ArrayIndexFunction;
import org.apache.phoenix.expression.function.ArrayLengthFunction;
import org.apache.phoenix.expression.function.CeilDateExpression;
import org.apache.phoenix.expression.function.CeilDecimalExpression;
import org.apache.phoenix.expression.function.CeilFunction;
import org.apache.phoenix.expression.function.CeilTimestampExpression;
import org.apache.phoenix.expression.function.CoalesceFunction;
import org.apache.phoenix.expression.function.ConvertTimezoneFunction;
import org.apache.phoenix.expression.function.CountAggregateFunction;
import org.apache.phoenix.expression.function.DecodeFunction;
import org.apache.phoenix.expression.function.DistinctCountAggregateFunction;
import org.apache.phoenix.expression.function.DistinctValueAggregateFunction;
import org.apache.phoenix.expression.function.EncodeFunction;
import org.apache.phoenix.expression.function.ExternalSqlTypeIdFunction;
import org.apache.phoenix.expression.function.FirstValueFunction;
import org.apache.phoenix.expression.function.FloorDateExpression;
import org.apache.phoenix.expression.function.FloorDecimalExpression;
import org.apache.phoenix.expression.function.FloorFunction;
import org.apache.phoenix.expression.function.HourFunction;
import org.apache.phoenix.expression.function.IndexStateNameFunction;
import org.apache.phoenix.expression.function.InstrFunction;
import org.apache.phoenix.expression.function.InvertFunction;
import org.apache.phoenix.expression.function.LTrimFunction;
import org.apache.phoenix.expression.function.LastValueFunction;
import org.apache.phoenix.expression.function.LengthFunction;
import org.apache.phoenix.expression.function.LowerFunction;
import org.apache.phoenix.expression.function.LpadFunction;
import org.apache.phoenix.expression.function.MD5Function;
import org.apache.phoenix.expression.function.MaxAggregateFunction;
import org.apache.phoenix.expression.function.MinAggregateFunction;
import org.apache.phoenix.expression.function.MonthFunction;
import org.apache.phoenix.expression.function.NowFunction;
import org.apache.phoenix.expression.function.NthValueFunction;
import org.apache.phoenix.expression.function.PercentRankAggregateFunction;
import org.apache.phoenix.expression.function.PercentileContAggregateFunction;
import org.apache.phoenix.expression.function.PercentileDiscAggregateFunction;
import org.apache.phoenix.expression.function.RTrimFunction;
import org.apache.phoenix.expression.function.RandomFunction;
import org.apache.phoenix.expression.function.RegexpReplaceFunction;
import org.apache.phoenix.expression.function.RegexpSplitFunction;
import org.apache.phoenix.expression.function.RegexpSubstrFunction;
import org.apache.phoenix.expression.function.ReverseFunction;
import org.apache.phoenix.expression.function.RoundDateExpression;
import org.apache.phoenix.expression.function.RoundDecimalExpression;
import org.apache.phoenix.expression.function.RoundFunction;
import org.apache.phoenix.expression.function.RoundTimestampExpression;
import org.apache.phoenix.expression.function.SQLIndexTypeFunction;
import org.apache.phoenix.expression.function.SQLTableTypeFunction;
import org.apache.phoenix.expression.function.SQLViewTypeFunction;
import org.apache.phoenix.expression.function.SecondFunction;
import org.apache.phoenix.expression.function.SignFunction;
import org.apache.phoenix.expression.function.SqlTypeNameFunction;
import org.apache.phoenix.expression.function.StddevPopFunction;
import org.apache.phoenix.expression.function.StddevSampFunction;
import org.apache.phoenix.expression.function.SubstrFunction;
import org.apache.phoenix.expression.function.SumAggregateFunction;
import org.apache.phoenix.expression.function.TimezoneOffsetFunction;
import org.apache.phoenix.expression.function.ToCharFunction;
import org.apache.phoenix.expression.function.ToDateFunction;
import org.apache.phoenix.expression.function.ToNumberFunction;
import org.apache.phoenix.expression.function.ToTimeFunction;
import org.apache.phoenix.expression.function.ToTimestampFunction;
import org.apache.phoenix.expression.function.TrimFunction;
import org.apache.phoenix.expression.function.TruncFunction;
import org.apache.phoenix.expression.function.UpperFunction;
import org.apache.phoenix.expression.function.WeekFunction;
import org.apache.phoenix.expression.function.YearFunction;

import com.google.common.collect.Maps;

/**
 *
 * Enumeration of all Expression types that will be looked up. They may be evaluated on the server-side.
 * Used during serialization and deserialization to pass Expression between client
 * and server.
 *
 *
 *
 * @since 0.1
 */
// Important : When you want to add new Types make sure to add those towards the end, not changing the existing type's
// ordinal
public enum ExpressionType {
    ReverseFunction(ReverseFunction.class),
    RowKey(RowKeyColumnExpression.class),
    KeyValue(KeyValueColumnExpression.class),
    LiteralValue(LiteralExpression.class),
    RoundFunction(RoundFunction.class),
    FloorFunction(FloorFunction.class),
    CeilFunction(CeilFunction.class),
    RoundDateExpression(RoundDateExpression.class),
    FloorDateExpression(FloorDateExpression.class),
    CeilDateExpression(CeilDateExpression.class),
    RoundTimestampExpression(RoundTimestampExpression.class),
    CeilTimestampExpression(CeilTimestampExpression.class),
    RoundDecimalExpression(RoundDecimalExpression.class),
    FloorDecimalExpression(FloorDecimalExpression.class),
    CeilDecimalExpression(CeilDecimalExpression.class),
    TruncFunction(TruncFunction.class),
    ToDateFunction(ToDateFunction.class),
    ToCharFunction(ToCharFunction.class),
    ToNumberFunction(ToNumberFunction.class),
    CoerceFunction(CoerceExpression.class),
    SubstrFunction(SubstrFunction.class),
    AndExpression(AndExpression.class),
    OrExpression(OrExpression.class),
    ComparisonExpression(ComparisonExpression.class),
    CountAggregateFunction(CountAggregateFunction.class),
    SumAggregateFunction(SumAggregateFunction.class),
    MinAggregateFunction(MinAggregateFunction.class),
    MaxAggregateFunction(MaxAggregateFunction.class),
    LikeExpression(LikeExpression.class),
    NotExpression(NotExpression.class),
    CaseExpression(CaseExpression.class),
    InListExpression(InListExpression.class),
    IsNullExpression(IsNullExpression.class),
    LongSubtractExpression(LongSubtractExpression.class),
    DateSubtractExpression(DateSubtractExpression.class),
    DecimalSubtractExpression(DecimalSubtractExpression.class),
    LongAddExpression(LongAddExpression.class),
    DecimalAddExpression(DecimalAddExpression.class),
    DateAddExpression(DateAddExpression.class),
    LongMultiplyExpression(LongMultiplyExpression.class),
    DecimalMultiplyExpression(DecimalMultiplyExpression.class),
    LongDivideExpression(LongDivideExpression.class),
    DecimalDivideExpression(DecimalDivideExpression.class),
    CoalesceFunction(CoalesceFunction.class),
    RegexpReplaceFunction(RegexpReplaceFunction.class),
    SQLTypeNameFunction(SqlTypeNameFunction.class),
    RegexpSubstrFunction(RegexpSubstrFunction.class),
    StringConcatExpression(StringConcatExpression.class),
    LengthFunction(LengthFunction.class),
    LTrimFunction(LTrimFunction.class),
    RTrimFunction(RTrimFunction.class),
    UpperFunction(UpperFunction.class),
    LowerFunction(LowerFunction.class),
    TrimFunction(TrimFunction.class),
    DistinctCountAggregateFunction(DistinctCountAggregateFunction.class),
    PercentileContAggregateFunction(PercentileContAggregateFunction.class),
    PercentRankAggregateFunction(PercentRankAggregateFunction.class),
    StddevPopFunction(StddevPopFunction.class),
    StddevSampFunction(StddevSampFunction.class),
    PercentileDiscAggregateFunction(PercentileDiscAggregateFunction.class),
    DoubleAddExpression(DoubleAddExpression.class),
    DoubleSubtractExpression(DoubleSubtractExpression.class),
    DoubleMultiplyExpression(DoubleMultiplyExpression.class),
    DoubleDivideExpression(DoubleDivideExpression.class),
    RowValueConstructorExpression(RowValueConstructorExpression.class),
    MD5Function(MD5Function.class),
    SQLTableTypeFunction(SQLTableTypeFunction.class),
    IndexStateName(IndexStateNameFunction.class),
    InvertFunction(InvertFunction.class),
    ProjectedColumnExpression(ProjectedColumnExpression.class),
    TimestampAddExpression(TimestampAddExpression.class),
    TimestampSubtractExpression(TimestampSubtractExpression.class),
    ArrayIndexFunction(ArrayIndexFunction.class),
    ArrayLengthFunction(ArrayLengthFunction.class),
    ArrayConstructorExpression(ArrayConstructorExpression.class),
    SQLViewTypeFunction(SQLViewTypeFunction.class),
    ExternalSqlTypeIdFunction(ExternalSqlTypeIdFunction.class),
    ConvertTimezoneFunction(ConvertTimezoneFunction.class),
    DecodeFunction(DecodeFunction.class),
    TimezoneOffsetFunction(TimezoneOffsetFunction.class),
    EncodeFunction(EncodeFunction.class),
    LpadFunction(LpadFunction.class),
    NthValueFunction(NthValueFunction.class),
    FirstValueFunction(FirstValueFunction.class),
    LastValueFunction(LastValueFunction.class),
    ArrayAnyComparisonExpression(ArrayAnyComparisonExpression.class),
    ArrayAllComparisonExpression(ArrayAllComparisonExpression.class),
    InlineArrayElemRefExpression(ArrayElemRefExpression.class),
    SQLIndexTypeFunction(SQLIndexTypeFunction.class),
    ModulusExpression(ModulusExpression.class),
    DistinctValueAggregateFunction(DistinctValueAggregateFunction.class),
    RegexpSplitFunctiond(RegexpSplitFunction.class),
    RandomFunction(RandomFunction.class),
    ToTimeFunction(ToTimeFunction.class),
    ToTimestampFunction(ToTimestampFunction.class),
    SignFunction(SignFunction.class),
    YearFunction(YearFunction.class),
    MonthFunction(MonthFunction.class),
    SecondFunction(SecondFunction.class),
    WeekFunction(WeekFunction.class),
    HourFunction(HourFunction.class),
    NowFunction(NowFunction.class),
    InstrFunction(InstrFunction.class)
    ;

    ExpressionType(Class<? extends Expression> clazz) {
        this.clazz = clazz;
    }

    public Class<? extends Expression> getExpressionClass() {
        return clazz;
    }

    private final Class<? extends Expression> clazz;

    private static final Map<Class<? extends Expression>,ExpressionType> classToEnumMap = Maps.newHashMapWithExpectedSize(3);
    static {
        for (ExpressionType type : ExpressionType.values()) {
            classToEnumMap.put(type.clazz, type);
        }
    }

    /**
     * Return the ExpressionType for a given Expression instance
     */
    public static ExpressionType valueOf(Expression expression) {
        ExpressionType type = classToEnumMap.get(expression.getClass());
        if (type == null) { // FIXME: this exception gets swallowed and retries happen
            throw new IllegalArgumentException("No ExpressionType for " + expression.getClass());
        }
        return type;
    }

    /**
     * Return the ExpressionType for a given Expression instance
     * or null if none exists.
     */
    public static ExpressionType valueOfOrNull(Expression expression) {
        return classToEnumMap.get(expression.getClass());
    }

    /**
     * Instantiates a DataAccessor based on its DataAccessorType
     */
    public Expression newInstance() {
        try {
            return clazz.newInstance();
        } catch (InstantiationException e) {
            throw new RuntimeException(e);
        } catch (IllegalAccessException e) {
            throw new RuntimeException(e);
        }
    }
}
