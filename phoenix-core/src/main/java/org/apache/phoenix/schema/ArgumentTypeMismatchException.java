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

import java.sql.SQLException;
import java.util.Arrays;

import com.google.common.base.Function;
import com.google.common.collect.Collections2;
import org.apache.phoenix.exception.SQLExceptionCode;
import org.apache.phoenix.exception.SQLExceptionInfo;
import org.apache.phoenix.schema.types.PDataType;
import org.apache.phoenix.schema.types.PDataTypeFactory;

import javax.annotation.Nullable;

/**
 * Exception thrown when we try to use use an argument that has the wrong type. 
 * 
 * 
 * @since 1.0
 */
public class ArgumentTypeMismatchException extends SQLException {
    private static final long serialVersionUID = 1L;
    private static SQLExceptionCode code = SQLExceptionCode.TYPE_MISMATCH;

    public ArgumentTypeMismatchException(PDataType expected, PDataType actual, String location) {
        super(new SQLExceptionInfo.Builder(code).setMessage("expected: " + expected + " but was: " + actual + " at " + location).build().toString(), code.getSQLState(), code.getErrorCode());
    }

    public ArgumentTypeMismatchException(Class<? extends PDataType>[] expecteds, PDataType actual, String location) {
        this(Arrays.toString(Collections2.transform(Arrays.asList(expecteds),
            new Function<Class<? extends PDataType>, PDataType>() {
              @Nullable @Override
              public PDataType apply(@Nullable Class<? extends PDataType> input) {
                return PDataTypeFactory.getInstance().instanceFromClass(input);
              }
            }).toArray()), actual.toString(), location);
    }

    public ArgumentTypeMismatchException(String expected, String actual, String location) {
        super(new SQLExceptionInfo.Builder(code).setMessage("expected: " + expected + " but was: " + actual + " at " + location).build().toString(), code.getSQLState(), code.getErrorCode());
    }
}
