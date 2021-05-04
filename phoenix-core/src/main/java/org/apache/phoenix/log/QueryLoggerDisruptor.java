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

import java.io.Closeable;
import java.io.IOException;
import java.sql.SQLException;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import org.apache.hadoop.conf.Configuration;
import org.apache.phoenix.query.QueryServices;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.phoenix.thirdparty.com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.lmax.disruptor.BlockingWaitStrategy;
import com.lmax.disruptor.EventTranslator;
import com.lmax.disruptor.ExceptionHandler;
import com.lmax.disruptor.TimeoutException;
import com.lmax.disruptor.WaitStrategy;
import com.lmax.disruptor.dsl.Disruptor;
import com.lmax.disruptor.dsl.ProducerType;

public class QueryLoggerDisruptor implements Closeable{
    
    private volatile Disruptor<RingBufferEvent> disruptor;
    private boolean isClosed = false;
    //number of elements to create within the ring buffer.
    private static final int RING_BUFFER_SIZE = 8 * 1024;
    private static final Logger LOGGER = LoggerFactory.getLogger(QueryLoggerDisruptor.class);
    private static final String DEFAULT_WAIT_STRATEGY = BlockingWaitStrategy.class.getName();
    private static final int DEFAULT_AUDIT_LOGGER_PROCESS_COUNT = 1;
    
    public QueryLoggerDisruptor(Configuration configuration) throws SQLException{
        WaitStrategy waitStrategy;
        try {
            waitStrategy = (WaitStrategy)Class
                    .forName(configuration.get(QueryServices.LOG_BUFFER_WAIT_STRATEGY, DEFAULT_WAIT_STRATEGY)).newInstance();
        } catch (InstantiationException | IllegalAccessException | ClassNotFoundException e) {
            throw new SQLException(e); 
        }
        
        ThreadFactory threadFactory = new ThreadFactoryBuilder()
                .setNameFormat("QueryLogger" + "-thread-%s")
                .setDaemon(true)
                .setThreadFactory(new ThreadFactory() {
                    @Override
                    public Thread newThread(Runnable r) {
                        final Thread result = Executors.defaultThreadFactory().newThread(r);
                        result.setContextClassLoader(QueryLoggerDisruptor.class.getClass().getClassLoader());
                        return result;
                    }
                })
                .build();
        disruptor = new Disruptor<RingBufferEvent>(RingBufferEvent.FACTORY,
                configuration.getInt(QueryServices.LOG_BUFFER_SIZE, RING_BUFFER_SIZE), threadFactory, ProducerType.MULTI,
                waitStrategy);
        final ExceptionHandler<RingBufferEvent> errorHandler = new QueryLoggerDefaultExceptionHandler();
        disruptor.setDefaultExceptionHandler(errorHandler);

        /**
         * if LOG_HANDLER_COUNT is 1 it will work as the previous implementation
         * if LOG_HANDLER_COUNT is 2 or more then Multi Thread
         */
        int handlerCount = configuration.getInt(
                QueryServices.LOG_HANDLER_COUNT, DEFAULT_AUDIT_LOGGER_PROCESS_COUNT);

        if (handlerCount <= 0){
            LOGGER.error("Audit Log Handler Count must be greater than 0." +
                    "change to default value, input : " + handlerCount);
            handlerCount = DEFAULT_AUDIT_LOGGER_PROCESS_COUNT;
        }

        QueryLogDetailsWorkHandler[] workHandlers = new QueryLogDetailsWorkHandler[handlerCount];
        for (int i = 0; i < handlerCount; i++){
            workHandlers[i] = new QueryLogDetailsWorkHandler(configuration);
        }
        disruptor.handleEventsWithWorkerPool(workHandlers);

        LOGGER.info("Starting  QueryLoggerDisruptor for with ringbufferSize=" +
                disruptor.getRingBuffer().getBufferSize() + ", waitStrategy=" +
                waitStrategy.getClass().getSimpleName() + ", " + "exceptionHandler="
                + errorHandler + ", handlerCount=" + handlerCount);
        disruptor.start();
        
    }
    
    /**
     * Attempts to publish an event by translating (write) data representations into events claimed from the RingBuffer.
     * @param translator
     * @return
     */
    public boolean tryPublish(final EventTranslator<RingBufferEvent> translator) {
        if(isClosed()){
            return false;
        }
        return disruptor.getRingBuffer().tryPublishEvent(translator);
    }
    

    public boolean isClosed() {
        return isClosed ;
    }

    @Override
    public void close() throws IOException {
        isClosed = true;
        LOGGER.info("Shutting down QueryLoggerDisruptor..");
        try {
            //we can wait for 2 seconds, so that backlog can be committed
            disruptor.shutdown(2, TimeUnit.SECONDS);
        } catch (TimeoutException e) {
            throw new IOException(e);
        }

    }
    
    
}
