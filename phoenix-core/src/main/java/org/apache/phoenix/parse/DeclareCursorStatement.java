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

import org.apache.phoenix.jdbc.PhoenixStatement.Operation;
import java.util.*;

public class DeclareCursorStatement implements BindableStatement {
    private final CursorName cursorName;
    private final SelectStatement select;

    public DeclareCursorStatement(CursorName cursorName, SelectStatement select){
        this.cursorName = cursorName;
        this.select = select;
    }

    public String getCursorName(){
        return cursorName.getName();
    }

    public String getQuerySQL(){
        //Check if there are parameters to bind.
        if(select.getBindCount() > 0){

        }
        //TODO: Test if this works
        return select.toString();
    }

    public SelectStatement getSelect(){
    	return select;
    }

    public List<OrderByNode> getSelectOrderBy() {
        return select.getOrderBy();
    }

    public int getBindCount(){
        return select.getBindCount();
    }

    public Operation getOperation(){
        return Operation.UPSERT;
    }
}
