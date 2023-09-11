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

import com.lmax.disruptor.ExceptionHandler;

class QueryLoggerDefaultExceptionHandler implements ExceptionHandler<RingBufferEvent> {

    @Override
    public void handleEventException(Throwable ex, long sequence, RingBufferEvent event) {
        final StringBuilder sb = new StringBuilder(512);
        sb.append("Query Logger error handling event seq=").append(sequence).append(", value='");
        try {
            sb.append(event);
        } catch (final Exception ignored) {
            sb.append("[ERROR calling ").append(event.getClass()).append(".toString(): ");
            sb.append(ignored).append("]");
        }
        sb.append("':");
        System.err.println(sb);
        ex.printStackTrace();
    }

    @Override
    public void handleOnStartException(final Throwable throwable) {
        System.err.println("QueryLogger error starting:");
        throwable.printStackTrace();
    }

    @Override
    public void handleOnShutdownException(final Throwable throwable) {
        System.err.println("QueryLogger error shutting down:");
        throwable.printStackTrace();
    }

}
