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
package org.apache.phoenix.flume.serializer;

import static org.apache.phoenix.flume.FlumeConstants.CONFIG_REGULAR_EXPRESSION;
import static org.apache.phoenix.flume.FlumeConstants.IGNORE_CASE_CONFIG;
import static org.apache.phoenix.flume.FlumeConstants.IGNORE_CASE_DEFAULT;
import static org.apache.phoenix.flume.FlumeConstants.REGEX_DEFAULT;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.flume.Context;
import org.apache.flume.Event;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;
import org.apache.phoenix.schema.types.PDataType;

public class RegexEventSerializer extends BaseEventSerializer {

    private static final Logger logger = LoggerFactory.getLogger(RegexEventSerializer.class);
  
    private Pattern inputPattern;
    
    /**
     * 
     */
    @Override
    public void doConfigure(Context context) {
        final String regex    = context.getString(CONFIG_REGULAR_EXPRESSION, REGEX_DEFAULT);
        final boolean regexIgnoreCase = context.getBoolean(IGNORE_CASE_CONFIG,IGNORE_CASE_DEFAULT);
        inputPattern = Pattern.compile(regex, Pattern.DOTALL + (regexIgnoreCase ? Pattern.CASE_INSENSITIVE : 0));
     }

     
    /**
     * 
     */
    @Override
    public void doInitialize() throws SQLException {
        // NO-OP
    }
    
   
    @Override
    public void upsertEvents(List<Event> events) throws SQLException {
       Preconditions.checkNotNull(events);
       Preconditions.checkNotNull(connection);
       Preconditions.checkNotNull(this.upsertStatement);
       
       PreparedStatement colUpsert = connection.prepareStatement(upsertStatement);
       boolean wasAutoCommit = connection.getAutoCommit();
       connection.setAutoCommit(false);
       
       String value = null;
       Integer sqlType = null;
       try {
           for(Event event : events) {
               byte [] payloadBytes = event.getBody();
               if(payloadBytes == null || payloadBytes.length == 0) {
                   continue;
               }
               String payload = new String(payloadBytes);
               Matcher m = inputPattern.matcher(payload.trim());
               
               if (!m.matches()) {
                 logger.debug("payload {} doesn't match the pattern {} ", payload, inputPattern.toString());  
                 continue;
               }
               if (m.groupCount() != colNames.size()) {
                 logger.debug("payload {} size doesn't match the pattern {} ", m.groupCount(), colNames.size());
                 continue;
               }
               int index = 1 ;
               int offset = 0;
               for (int i = 0 ; i <  colNames.size() ; i++,offset++) {
                   if (columnMetadata[offset] == null ) {
                       continue;
                   }
                   
                   value = m.group(i + 1);
                   sqlType = columnMetadata[offset].getSqlType();
                   Object upsertValue = PDataType.fromTypeId(sqlType).toObject(value);
                   if (upsertValue != null) {
                       colUpsert.setObject(index++, upsertValue, sqlType);
                   } else {
                       colUpsert.setNull(index++, sqlType);
                   }
                }
               
               //add headers if necessary
               Map<String,String> headerValues = event.getHeaders();
               for(int i = 0 ; i < headers.size() ; i++ , offset++) {
                
                   String headerName  = headers.get(i);
                   String headerValue = headerValues.get(headerName);
                   sqlType = columnMetadata[offset].getSqlType();
                   Object upsertValue = PDataType.fromTypeId(sqlType).toObject(headerValue);
                   if (upsertValue != null) {
                       colUpsert.setObject(index++, upsertValue, sqlType);
                   } else {
                       colUpsert.setNull(index++, sqlType);
                   }
               }
  
               if(autoGenerateKey) {
                   sqlType = columnMetadata[offset].getSqlType();
                   String generatedRowValue = this.keyGenerator.generate();
                   Object rowkeyValue = PDataType.fromTypeId(sqlType).toObject(generatedRowValue);
                   colUpsert.setObject(index++, rowkeyValue ,sqlType);
               } 
               colUpsert.execute();
           }
           connection.commit();
       } catch(Exception ex){
           logger.error("An error {} occurred during persisting the event ",ex.getMessage());
           throw new SQLException(ex.getMessage());
       }finally {
           if(wasAutoCommit) {
               connection.setAutoCommit(true);
           }
       }
       
    }

}
