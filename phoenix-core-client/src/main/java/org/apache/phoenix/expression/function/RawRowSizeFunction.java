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
package org.apache.phoenix.expression.function;

import java.sql.SQLException;
import java.util.List;
import org.apache.phoenix.expression.Expression;
import org.apache.phoenix.parse.FunctionParseNode.BuiltInFunction;
import org.apache.phoenix.parse.RawRowSizeParseNode;

/**
 * Function to return the total size of the all HBase cells versions and delete markers for a given
 * row
 */
@BuiltInFunction(name = RawRowSizeFunction.NAME, nodeClass = RawRowSizeParseNode.class, args = {})
public class RawRowSizeFunction extends RowSizeFunction {

  public static final String NAME = "RAW_ROW_SIZE";

  public RawRowSizeFunction() {
  }

  public RawRowSizeFunction(List<Expression> children) throws SQLException {
    super(children);
  }
}
