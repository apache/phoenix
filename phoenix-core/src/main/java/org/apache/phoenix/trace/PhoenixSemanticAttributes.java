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
package org.apache.phoenix.trace;

import io.opentelemetry.api.common.AttributeKey;
import io.opentelemetry.semconv.trace.attributes.SemanticAttributes;

/**
 * The constants in this class correspond with the guidance outlined by the OpenTelemetry <a href=
 * "https://github.com/open-telemetry/opentelemetry-specification/tree/main/specification/trace/semantic_conventions">Semantic
 * Conventions</a>.
 */
public class PhoenixSemanticAttributes {

    // These Attributes are also defined by HBase
    //FIXME These are going to be overwritten ? when transitioning to HBase
    // Span attributes are probably not propageted to children, check
    public static final AttributeKey<String> DB_SYSTEM = SemanticAttributes.DB_SYSTEM;
    public static final String DB_SYSTEM_VALUE = "phoenix";
    public static final AttributeKey<String> DB_CONNECTION_STRING =
            SemanticAttributes.DB_CONNECTION_STRING;
    public static final AttributeKey<String> DB_USER = SemanticAttributes.DB_USER;
    public static final AttributeKey<String> DB_NAME = SemanticAttributes.DB_NAME;
    public static final AttributeKey<String> DB_OPERATION = SemanticAttributes.DB_OPERATION;

    // These are not defined by HBase
    public static final AttributeKey<String> DB_JDBC_DRIVER_CLASSNAME =
            SemanticAttributes.DB_JDBC_DRIVER_CLASSNAME;
    public static final AttributeKey<String> DB_STATEMENT = SemanticAttributes.DB_STATEMENT;
    public static final AttributeKey<String> DB_SQL_TABLE = SemanticAttributes.DB_SQL_TABLE;

}
