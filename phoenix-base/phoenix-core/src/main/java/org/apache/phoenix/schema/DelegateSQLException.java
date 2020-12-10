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
import java.util.Iterator;

public class DelegateSQLException extends SQLException {
    private final SQLException delegate;
    private final String msg;
    
    public DelegateSQLException(SQLException e, String msg) {
        this.delegate = e;
        this.msg = e.getMessage() + msg;
    }
    
    @Override
    public String getMessage() {
        return msg;
    }
    
    @Override
    public String getSQLState() {
        return delegate.getSQLState();
    }

    @Override
    public int getErrorCode() {
        return delegate.getErrorCode();
    }

    @Override
    public SQLException getNextException() {
        return delegate.getNextException();
    }

    @Override
    public void setNextException(SQLException ex) {
        delegate.setNextException(ex);
    }

    @Override
    public Iterator<Throwable> iterator() {
        return delegate.iterator();
    }

}
