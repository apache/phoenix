/*
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license
 * agreements. See the NOTICE file distributed with this work for additional information regarding
 * copyright ownership. The ASF licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at http://www.apache.org/licenses/LICENSE-2.0 Unless required by applicable
 * law or agreed to in writing, software distributed under the License is distributed on an "AS IS"
 * BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License
 * for the specific language governing permissions and limitations under the License.
 */
package org.apache.phoenix.trace;

import java.text.DecimalFormat;
import java.text.DecimalFormatSymbols;
import java.util.List;
import java.util.Map.Entry;

import io.opentelemetry.api.common.AttributeKey;
import io.opentelemetry.api.common.AttributeType;
import io.opentelemetry.api.common.Attributes;
import io.opentelemetry.api.internal.OtelEncodingUtils;
import io.opentelemetry.api.trace.Span;
import io.opentelemetry.api.trace.SpanContext;
import io.opentelemetry.api.trace.SpanKind;
import io.opentelemetry.context.Context;
import io.opentelemetry.sdk.trace.data.LinkData;
import io.opentelemetry.sdk.trace.samplers.Sampler;
import io.opentelemetry.sdk.trace.samplers.SamplingResult;

/**
 * This sampler works mostly like "parentbased_traceidratio", and takes much of the code from
 * TraceIdRatioBasedSampler.
 * 
 * It there is a valid parent span, it will use its isSampled flag.
 * 
 * If there is no valid parent span (i.e. this is the root span), then it will honor the
 * "sampling.priority" hint, sampling if its value is 1 or greater, and not otherwise.
 * 
 * If there is no valid parent span, and the hint is not set, it behaves like
 * TraceIdRatioBasedSampler.
 */
public class PhoenixHintableSampler implements Sampler {

    private static final SamplingResult POSITIVE_SAMPLING_RESULT = SamplingResult.recordAndSample();

    private static final SamplingResult NEGATIVE_SAMPLING_RESULT = SamplingResult.drop();

    // We could use Boolean, but we're trying to mimic to the Opentracing hint behaviour
    private static final AttributeKey<Long> SAMPLING_PRIORITY_ATTRIBUTE_KEY =
            AttributeKey.longKey("sampling.priority");

    private final long idUpperBound;
    private final String description;

    static PhoenixHintableSampler create(double ratio) {
        System.err.println("XXXX PhoenixHintableSampler.create ratio:" + ratio);
        if (ratio < 0.0 || ratio > 1.0) {
            throw new IllegalArgumentException("ratio must be in range [0.0, 1.0]");
        }
        long idUpperBound;
        // Special case the limits, to avoid any possible issues with lack of precision across
        // double/long boundaries. For probability == 0.0, we use Long.MIN_VALUE as this guarantees
        // that we will never sample a trace, even in the case where the id == Long.MIN_VALUE, since
        // Math.Abs(Long.MIN_VALUE) == Long.MIN_VALUE.
        if (ratio == 0.0) {
            idUpperBound = Long.MIN_VALUE;
        } else if (ratio == 1.0) {
            idUpperBound = Long.MAX_VALUE;
        } else {
            idUpperBound = (long) (ratio * Long.MAX_VALUE);
        }
        return new PhoenixHintableSampler(ratio, idUpperBound);
    }

    PhoenixHintableSampler(double ratio, long idUpperBound) {
        System.err.println("XXXX PhoenixHintableSampler.create idUpperBound:" + idUpperBound);
        this.idUpperBound = idUpperBound;
        description = "PhoenixHintableSampler{" + decimalFormat(ratio) + "}";
    }

    @Override
    public SamplingResult shouldSample(Context parentContext, String traceId, String name,
            SpanKind spanKind, Attributes attributes, List<LinkData> parentLinks) {
        System.err.println("XXXX PhoenixHintableSampler.shouldSample");

        SpanContext parentSpanContext = Span.fromContext(parentContext).getSpanContext();
        if (parentSpanContext.isValid()) {
            System.err.println("XXXX PhoenixHintableSampler.shouldSample parentSpanContext.isValid:" + parentSpanContext.isValid() );
            if(parentSpanContext.isSampled()) {
                return POSITIVE_SAMPLING_RESULT;
            } else {
                return NEGATIVE_SAMPLING_RESULT;
            }
        }
        for ( Entry<AttributeKey<?>, Object> entry :attributes.asMap().entrySet()) {
            System.err.println("XXXX PhoenixHintableSampler.shouldSample attribute key:" + entry.getKey() + " value:" + entry.getValue() );
        }
        
        Long hint = attributes.get(SAMPLING_PRIORITY_ATTRIBUTE_KEY);
        System.err.println("XXXX PhoenixHintableSampler.shouldSample hint:" + hint );

        if (hint != null) {
            if (hint <= 0) {
                return NEGATIVE_SAMPLING_RESULT;
            } else {
                return POSITIVE_SAMPLING_RESULT;
            }
        }
        System.err.println("XXXX PhoenixHintableSampler.shouldSample Math.abs(getTraceIdRandomPart(traceId)):" + Math.abs(getTraceIdRandomPart(traceId)) );
        // Always sample if we are within probability range. This is true even for child spans (that
        // may have had a different sampling samplingResult made) to allow for different sampling
        // policies,
        // and dynamic increases to sampling probabilities for debugging purposes.
        // Note use of '<' for comparison. This ensures that we never sample for probability == 0.0,
        // while allowing for a (very) small chance of *not* sampling if the id == Long.MAX_VALUE.
        // This is considered a reasonable tradeoff for the simplicity/performance requirements
        // (this
        // code is executed in-line for every Span creation).
        return Math.abs(getTraceIdRandomPart(traceId)) < idUpperBound ? POSITIVE_SAMPLING_RESULT
                : NEGATIVE_SAMPLING_RESULT;
    }

    @Override
    public String getDescription() {
        return description;
    }

    @Override
    public boolean equals(Object obj) {
        if (!(obj instanceof PhoenixHintableSampler)) {
            return false;
        }
        PhoenixHintableSampler that = (PhoenixHintableSampler) obj;
        return idUpperBound == that.idUpperBound;
    }

    @Override
    public int hashCode() {
        return Long.hashCode(idUpperBound);
    }

    @Override
    public String toString() {
        return getDescription();
    }

    // Visible for testing
    long getIdUpperBound() {
        return idUpperBound;
    }

    private static long getTraceIdRandomPart(String traceId) {
        return OtelEncodingUtils.longFromBase16String(traceId, 16);
    }

    private static String decimalFormat(double value) {
        DecimalFormatSymbols decimalFormatSymbols = DecimalFormatSymbols.getInstance();
        decimalFormatSymbols.setDecimalSeparator('.');

        DecimalFormat decimalFormat = new DecimalFormat("0.000000", decimalFormatSymbols);
        return decimalFormat.format(value);
    }

}
