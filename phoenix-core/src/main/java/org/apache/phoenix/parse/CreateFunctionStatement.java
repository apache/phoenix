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
package org.apache.phoenix.parse;

public class CreateFunctionStatement extends MutableStatement {
    private final PFunction functionInfo;
    private final boolean temporary;
    private final boolean isReplace;

    public CreateFunctionStatement(PFunction functionInfo, boolean temporary, boolean isReplace) {
        this.functionInfo = functionInfo;
        this.temporary = temporary;
        this.isReplace = isReplace;
    }

    @Override
    public int getBindCount() {
        return 0;
    }

    public PFunction getFunctionInfo() {
        return functionInfo;
    }
    
    public boolean isTemporary() {
        return temporary;
    }

    public boolean isReplace() {
        return isReplace;
    }
}
