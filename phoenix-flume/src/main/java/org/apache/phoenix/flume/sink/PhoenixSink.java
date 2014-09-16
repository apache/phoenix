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
package org.apache.phoenix.flume.sink;

import java.sql.SQLException;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.flume.Channel;
import org.apache.flume.ChannelException;
import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.EventDeliveryException;
import org.apache.flume.Transaction;
import org.apache.flume.conf.Configurable;
import org.apache.flume.instrumentation.SinkCounter;
import org.apache.flume.sink.AbstractSink;
import org.apache.phoenix.flume.FlumeConstants;
import org.apache.phoenix.flume.serializer.EventSerializer;
import org.apache.phoenix.flume.serializer.EventSerializers;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;
import com.google.common.collect.Lists;

public final class PhoenixSink  extends AbstractSink implements Configurable {
    private static final Logger logger = LoggerFactory.getLogger(PhoenixSink.class);
    private static AtomicInteger counter = new AtomicInteger();
    private static final String NAME   = "Phoenix Sink__";
  
    private SinkCounter sinkCounter;
    private Integer    batchSize;
    private EventSerializer serializer;
 
    public PhoenixSink(){
    }
    
    @Override
    public void configure(Context context){
        this.setName(NAME + counter.incrementAndGet());
        this.batchSize = context.getInteger(FlumeConstants.CONFIG_BATCHSIZE, FlumeConstants.DEFAULT_BATCH_SIZE);
        final String eventSerializerType = context.getString(FlumeConstants.CONFIG_SERIALIZER);
        
        Preconditions.checkNotNull(eventSerializerType,"Event serializer cannot be empty, please specify in the configuration file");
        initializeSerializer(context,eventSerializerType);
        this.sinkCounter = new SinkCounter(this.getName());
    }

    /**
     * Initializes the serializer for flume events.
     * @param eventSerializerType
     */
    private void initializeSerializer(final Context context,final String eventSerializerType) {
        
       EventSerializers eventSerializer = null;
       try {
               eventSerializer =  EventSerializers.valueOf(eventSerializerType.toUpperCase());
        } catch(IllegalArgumentException iae) {
               logger.error("An invalid eventSerializer {} was passed. Please specify one of {} ",eventSerializerType,
                       Joiner.on(",").skipNulls().join(EventSerializers.values()));
               Throwables.propagate(iae);
        }
       
       final Context serializerContext = new Context();
       serializerContext.putAll(context.getSubProperties(FlumeConstants.CONFIG_SERIALIZER_PREFIX));
       copyPropertiesToSerializerContext(context,serializerContext);
             
       try {
         @SuppressWarnings("unchecked")
         Class<? extends EventSerializer> clazz = (Class<? extends EventSerializer>) Class.forName(eventSerializer.getClassName());
         serializer = clazz.newInstance();
         serializer.configure(serializerContext);
         
       } catch (Exception e) {
         logger.error("Could not instantiate event serializer." , e);
         Throwables.propagate(e);
       }
    }

    private void copyPropertiesToSerializerContext(Context context, Context serializerContext) {
        
        serializerContext.put(FlumeConstants.CONFIG_TABLE_DDL,context.getString(FlumeConstants.CONFIG_TABLE_DDL));
        serializerContext.put(FlumeConstants.CONFIG_TABLE,context.getString(FlumeConstants.CONFIG_TABLE));
        serializerContext.put(FlumeConstants.CONFIG_ZK_QUORUM,context.getString(FlumeConstants.CONFIG_ZK_QUORUM));
        serializerContext.put(FlumeConstants.CONFIG_JDBC_URL,context.getString(FlumeConstants.CONFIG_JDBC_URL));
        serializerContext.put(FlumeConstants.CONFIG_BATCHSIZE,context.getString(FlumeConstants.CONFIG_BATCHSIZE));
  }

    @Override
    public void start() {
        logger.info("Starting sink {} ",this.getName());
        sinkCounter.start();
        try {
              serializer.initialize();
              sinkCounter.incrementConnectionCreatedCount();
        } catch(Exception ex) {
            sinkCounter.incrementConnectionFailedCount();
            logger.error("Error {} in initializing the serializer.",ex.getMessage());
            Throwables.propagate(ex);
       }
       super.start();
    }
    
    @Override
    public void stop(){
      super.stop();
      try {
          serializer.close();
        } catch (SQLException e) {
            logger.error(" Error while closing connection {} for sink {} ",e.getMessage(),this.getName());
        }
      sinkCounter.incrementConnectionClosedCount();
      sinkCounter.stop();
    }

    @Override
    public Status process() throws EventDeliveryException {
        
        Status status = Status.READY;
        Channel channel = getChannel();
        Transaction transaction = null;
        List<Event>  events = Lists.newArrayListWithExpectedSize(this.batchSize); 
        long startTime = System.nanoTime();
        try {
            transaction = channel.getTransaction();
            transaction.begin();
            
            for(long i = 0; i < this.batchSize; i++) {
                Event event = channel.take();
                if(event == null){
                  status = Status.BACKOFF;
                  if (i == 0) {
                    sinkCounter.incrementBatchEmptyCount();
                  } else {
                    sinkCounter.incrementBatchUnderflowCount();
                  }
                  break;
                } else {
                  events.add(event);
                }
            }
            if (!events.isEmpty()) {
               if (events.size() == this.batchSize) {
                    sinkCounter.incrementBatchCompleteCount();
                }
                else {
                    sinkCounter.incrementBatchUnderflowCount();
                    status = Status.BACKOFF;
                }
                // save to Hbase
                serializer.upsertEvents(events);
                sinkCounter.addToEventDrainSuccessCount(events.size());
            }
            else {
                logger.debug("no events to process ");
                sinkCounter.incrementBatchEmptyCount();
                status = Status.BACKOFF;
            }
            transaction.commit();
        } catch (ChannelException e) {
            transaction.rollback();
            status = Status.BACKOFF;
            sinkCounter.incrementConnectionFailedCount();
        }
        catch (SQLException e) {
            sinkCounter.incrementConnectionFailedCount();
            transaction.rollback();
            logger.error("exception while persisting to Hbase ", e);
            throw new EventDeliveryException("Failed to persist message to Hbase", e);
        }
        catch (Throwable e) {
            transaction.rollback();
            logger.error("exception while processing in Phoenix Sink", e);
            throw new EventDeliveryException("Failed to persist message", e);
        }
        finally {
            logger.info(String.format("Time taken to process [%s] events was [%s] seconds",
                    events.size(),
                    TimeUnit.SECONDS.convert(System.nanoTime() - startTime, TimeUnit.NANOSECONDS)));
            if( transaction != null ) {
                transaction.close();
            }
        }
        return status;
   }

}
