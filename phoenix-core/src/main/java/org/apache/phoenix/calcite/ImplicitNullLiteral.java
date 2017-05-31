/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to you under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.phoenix.calcite;

import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexBiVisitor;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexVisitor;

/*
 * Rex node used to represent implicit nulls. This helps to detect unspecified columns in upsert or
 * upsert select.
 */
public class ImplicitNullLiteral extends RexNode {

    final RexNode delegate;
    public ImplicitNullLiteral(RexNode delegate) {
        this.delegate = delegate;
    }
    @Override
    public RelDataType getType() {
        return delegate.getType();
    }

    @Override
    public <R> R accept(RexVisitor<R> visitor) {
        return delegate.accept(visitor);
    }

    @Override
    public <R, P> R accept(RexBiVisitor<R, P> visitor, P arg) {
        return delegate.accept(visitor, arg);
    }

}