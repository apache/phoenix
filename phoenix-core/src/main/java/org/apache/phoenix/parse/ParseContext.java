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

import java.util.List;

import com.google.common.collect.Lists;

public class ParseContext {
    private boolean isAggregate;
    private boolean hasSequences;
    
    public ParseContext() {
    }

    public boolean isAggregate() {
        return isAggregate;
    }

    public void setAggregate(boolean isAggregate) {
        this.isAggregate |= isAggregate;
    }

    public boolean hasSequences() {
        return hasSequences;
    }

    public void hasSequences(boolean hasSequences) {
        this.hasSequences |= hasSequences;
    }

    public static class Stack {
        private final List<ParseContext> stack = Lists.newArrayListWithExpectedSize(5);
        
        public void push(ParseContext context) {
            stack.add(context);
        }
        
        public ParseContext pop() {
            return stack.remove(stack.size()-1);
        }
        
        public ParseContext peek() {
            return stack.get(stack.size()-1);
        }
        
        public boolean isEmpty() {
            return stack.isEmpty();
        }
    }
    
}
