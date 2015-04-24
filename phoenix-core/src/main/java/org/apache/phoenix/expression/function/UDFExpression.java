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
package org.apache.phoenix.expression.function;

import static org.apache.phoenix.query.QueryServices.DYNAMIC_JARS_DIR_KEY;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.util.List;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.locks.Lock;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.util.DynamicClassLoader;
import org.apache.hadoop.hbase.util.KeyLocker;
import org.apache.hadoop.io.WritableUtils;
import org.apache.phoenix.compile.KeyPart;
import org.apache.phoenix.expression.Expression;
import org.apache.phoenix.expression.visitor.ExpressionVisitor;
import org.apache.phoenix.parse.PFunction;
import org.apache.phoenix.schema.PName;
import org.apache.phoenix.schema.PNameFactory;
import org.apache.phoenix.schema.tuple.Tuple;
import org.apache.phoenix.schema.types.PDataType;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.MapMaker;

public class UDFExpression extends ScalarFunction {
    
    private static Configuration config = HBaseConfiguration.create();

    private static final ConcurrentMap<PName, DynamicClassLoader> tenantIdSpecificCls =
            new MapMaker().concurrencyLevel(3).weakValues().makeMap();

    private static final ConcurrentMap<String, DynamicClassLoader> pathSpecificCls =
            new MapMaker().concurrencyLevel(3).weakValues().makeMap();

    public static final Log LOG = LogFactory.getLog(UDFExpression.class);
    
    /**
     * A locker used to synchronize class loader initialization per tenant id.
     */
    private static final KeyLocker<String> locker = new KeyLocker<String>();

    /**
     * A locker used to synchronize class loader initialization per jar path.
     */
    private static final KeyLocker<String> pathLocker = new KeyLocker<String>();

    private PName tenantId;
    private String functionClassName;
    private String jarPath;
    private ScalarFunction udfFunction;
    
    public UDFExpression() {
    }

    public UDFExpression(List<Expression> children,PFunction functionInfo) {
        super(children);
        this.tenantId =
                functionInfo.getTenantId() == null ? PName.EMPTY_NAME : functionInfo.getTenantId();
        this.functionClassName = functionInfo.getClassName();
        this.jarPath = functionInfo.getJarPath();
        constructUDFFunction();
    }

    @Override
    public boolean evaluate(Tuple tuple, ImmutableBytesWritable ptr) {
        return udfFunction.evaluate(tuple, ptr);
    }

    @Override
    public <T> T accept(ExpressionVisitor<T> visitor) {
        return udfFunction.accept(visitor);
    }

    @Override
    public PDataType getDataType() {
        return udfFunction.getDataType();
    }

    @Override
    public String getName() {
        return udfFunction.getName();
    }

    @Override
    public OrderPreserving preservesOrder() {
        return udfFunction.preservesOrder();
    }

    @Override
    public KeyPart newKeyPart(KeyPart childPart) {
        return udfFunction.newKeyPart(childPart);
    }

    @Override
    public int getKeyFormationTraversalIndex() {
        return udfFunction.getKeyFormationTraversalIndex();
    }

    @Override
    public void write(DataOutput output) throws IOException {
        super.write(output);
        WritableUtils.writeString(output, tenantId.getString());
        WritableUtils.writeString(output, this.functionClassName);
        if(this.jarPath == null) {
            WritableUtils.writeString(output, "");
        } else {
            WritableUtils.writeString(output, this.jarPath);
        }
    }
    
    @Override
    public void readFields(DataInput input) throws IOException {
        super.readFields(input);
        this.tenantId = PNameFactory.newName(WritableUtils.readString(input));
        this.functionClassName = WritableUtils.readString(input);
        String str = WritableUtils.readString(input);
        this.jarPath = str.length() == 0 ? null: str;
        constructUDFFunction();
    }

    private void constructUDFFunction() {
        try {
            DynamicClassLoader classLoader = getClassLoader(this.tenantId, this.jarPath);
            Class<?> clazz = classLoader.loadClass(this.functionClassName);
            Constructor<?> constructor = clazz.getConstructor(List.class);
            udfFunction = (ScalarFunction)constructor.newInstance(this.children);
        } catch (ClassNotFoundException | NoSuchMethodException | SecurityException
                | InstantiationException | IllegalAccessException | IllegalArgumentException
                | InvocationTargetException e) {
            throw new RuntimeException(e);
        }
    }

    public static DynamicClassLoader getClassLoader(final PName tenantId, final String jarPath) {
        DynamicClassLoader cl = tenantIdSpecificCls.get(tenantId);
        String parent = null;
        if (cl != null) return cl;
        if(jarPath != null && !jarPath.isEmpty()) {
            cl = pathSpecificCls.get(jarPath);
            if (cl != null) return cl;
            Path path = new Path(jarPath);
            if(jarPath.endsWith(".jar")) {
                parent = path.getParent().toString();
            } else {
                parent = path.toString();
            }
        }
        if (jarPath == null || jarPath.isEmpty() || config.get(DYNAMIC_JARS_DIR_KEY) != null
                && (parent != null && parent.equals(config.get(DYNAMIC_JARS_DIR_KEY)))) {
            Lock lock = locker.acquireLock(tenantId.getString());
            try {
                cl = tenantIdSpecificCls.get(tenantId);
                if (cl == null) {
                    cl = new DynamicClassLoader(config, UDFExpression.class.getClassLoader());
                }
                // Cache class loader as a weak value, will be GC'ed when no reference left
                DynamicClassLoader prev = tenantIdSpecificCls.putIfAbsent(tenantId, cl);
                if (prev != null) {
                    cl = prev;
                }
                return cl;
            } finally {
                lock.unlock();
            }
        } else {
            Lock lock = pathLocker.acquireLock(jarPath);
            try {
                cl = pathSpecificCls.get(jarPath);
                if (cl == null) {
                    Configuration conf = HBaseConfiguration.create(config);
                    conf.set(DYNAMIC_JARS_DIR_KEY, parent);
                    cl = new DynamicClassLoader(conf, UDFExpression.class.getClassLoader());
                }
                // Cache class loader as a weak value, will be GC'ed when no reference left
                DynamicClassLoader prev = pathSpecificCls.putIfAbsent(jarPath, cl);
                if (prev != null) {
                    cl = prev;
                }
                return cl;
            } finally {
                lock.unlock();
            }
        }
    }
    
    @VisibleForTesting
    public static void setConfig(Configuration conf) {
        config = conf;
    }
}
