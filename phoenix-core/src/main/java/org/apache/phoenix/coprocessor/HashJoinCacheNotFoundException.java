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
package org.apache.phoenix.coprocessor;

import java.sql.SQLException;

import org.apache.phoenix.exception.SQLExceptionCode;
import org.apache.phoenix.exception.SQLExceptionInfo;

public class HashJoinCacheNotFoundException extends SQLException{
    private static final long serialVersionUID = 1L;
    private Long cacheId;
    private static SQLExceptionCode ERROR_CODE = SQLExceptionCode.HASH_JOIN_CACHE_NOT_FOUND;
    public HashJoinCacheNotFoundException() {
        this(null);
    }

    public HashJoinCacheNotFoundException(Long cacheId) {
        super(new SQLExceptionInfo.Builder(ERROR_CODE).setMessage("joinId: " + cacheId
                + ". The cache might have expired and have been removed.").build().toString(),
                ERROR_CODE.getSQLState(), ERROR_CODE.getErrorCode(), null);
        this.cacheId=cacheId;
    }
    
    public Long getCacheId(){
        return this.cacheId;
    }
    

}
