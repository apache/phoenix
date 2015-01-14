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

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.phoenix.expression.Expression;
import org.apache.phoenix.util.SizedUtil;

public class DelegateColumn extends DelegateDatum implements PColumn {
    
    public DelegateColumn(PColumn delegate) {
        super(delegate);
    }
    
    @Override
    protected PColumn getDelegate() {
        return (PColumn)super.getDelegate();
    }
    
    @Override
    public PName getName() {
        return getDelegate().getName();
    }
    
    @Override
    public SortOrder getSortOrder() {
    	return getDelegate().getSortOrder();
    }

    @Override
    public PName getFamilyName() {
        return getDelegate().getFamilyName();
    }

    @Override
    public int getPosition() {
        return getDelegate().getPosition();
    }

    @Override
    public Integer getArraySize() {
        return getDelegate().getArraySize();
    }

    @Override
    public byte[] getViewConstant() {
        return getDelegate().getViewConstant();
    }

    @Override
    public int getEstimatedSize() {
        return SizedUtil.OBJECT_SIZE + getDelegate().getEstimatedSize();
    }

    @Override
    public boolean isViewReferenced() {
        return getDelegate().isViewReferenced();
    }
    
    @Override
    public String getExpressionStr() {
        return getDelegate().getExpressionStr();
    }
}
