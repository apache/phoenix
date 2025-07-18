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
package org.apache.phoenix.parse;

import java.sql.SQLException;
import java.util.Collections;
import java.util.List;
import org.apache.phoenix.compile.ColumnResolver;

/**
 * Parse Node to help determine whether the document field with the given key exists. The key can
 * contain nested fields: nested documents or nested arrays.
 */
public class DocumentFieldExistsParseNode extends CompoundParseNode {

  private final boolean exists;

  DocumentFieldExistsParseNode(ParseNode children, boolean exists) {
    super(Collections.singletonList(children));
    this.exists = exists;
  }

  @Override
  public <T> T accept(ParseNodeVisitor<T> visitor) throws SQLException {
    List<T> l = Collections.emptyList();
    if (visitor.visitEnter(this)) {
      l = acceptChildren(visitor);
    }
    return visitor.visitLeave(this, l);
  }

  @Override
  public void toSQL(ColumnResolver resolver, StringBuilder buf) {
    List<ParseNode> children = getChildren();
    if (exists) {
      buf.append("field_exists(");
    } else {
      buf.append("field_not_exists(");
    }
    children.get(0).toSQL(resolver, buf);
    buf.append(")");
  }

  public boolean isExists() {
    return exists;
  }

}
