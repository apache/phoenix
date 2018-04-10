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
package org.apache.phoenix.log;

import java.sql.SQLException;

import org.apache.hadoop.conf.Configuration;

import com.lmax.disruptor.LifecycleAware;
import com.lmax.disruptor.Sequence;
import com.lmax.disruptor.SequenceReportingEventHandler;


public class QueryLogDetailsEventHandler implements SequenceReportingEventHandler<RingBufferEvent>, LifecycleAware {
    private Sequence sequenceCallback;
    private LogWriter logWriter;

    public QueryLogDetailsEventHandler(Configuration configuration) throws SQLException{
        this.logWriter = new TableLogWriter(configuration);
    }
    
    @Override
    public void setSequenceCallback(final Sequence sequenceCallback) {
        this.sequenceCallback = sequenceCallback;
    }

    @Override
    public void onEvent(final RingBufferEvent event, final long sequence, final boolean endOfBatch) throws Exception {
        logWriter.write(event);
        event.clear();
    }

    @Override
    public void onStart() {
    }

    @Override
    public void onShutdown() {
        try {
            if (logWriter != null) {
                logWriter.close();
            }
        } catch (Exception e) {
            //Ignore
        }
    }

}
