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
package org.apache.phoenix.expression.util.regex;

import com.google.common.base.Preconditions;

public class RegexPatternFactory {

    static public enum PatternType {
        JAVA, JONI
    }

    static public AbstractBasePattern compile(PatternType type, String patternString) {
        return compile(type, patternString, 0);
    }

    static public AbstractBasePattern compile(PatternType type, String patternString, int flags) {
        Preconditions.checkNotNull(type);
        switch (type) {
        case JAVA:
            return new JavaRegexWrapper.JavaPattern(patternString, flags);
        case JONI:
            return new JONIRegexWrapper.JONIPattern(patternString, flags);
        default:
            // Should never go here
            throw new NullPointerException();
        }
    }
}
