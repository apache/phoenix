/*
 * Copyright 2014 Apache Software Foundation.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.phoenix.expression.function;

import java.util.List;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Pair;
import org.apache.phoenix.expression.Expression;

public abstract class UpsertFunctionExpression extends FunctionExpression {

    private final byte[] identifier = new byte[4];

    public UpsertFunctionExpression() {
        setIdentifier();
    }

    public UpsertFunctionExpression(List<Expression> children) {
        super(children);
        setIdentifier();
    }

    public List<Pair<String, byte[]>> getAttributes() {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    /**
     * Identifier of expression instance.
     * It is used to identification of put/delete attributes per statement on server side coprocessors
     * @return
     */
    public byte[] getIdentifier() {
        return identifier;
    }

    private void setIdentifier() {
        Bytes.random(identifier);
    }

}
