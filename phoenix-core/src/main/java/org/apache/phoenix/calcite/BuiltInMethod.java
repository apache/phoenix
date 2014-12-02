package org.apache.phoenix.calcite;

import org.apache.calcite.linq4j.tree.Types;
import org.apache.phoenix.compile.QueryPlan;
import org.apache.phoenix.iterate.ResultIterator;

import com.google.common.collect.ImmutableMap;

import java.lang.reflect.Constructor;
import java.lang.reflect.Method;

/**
 * Built-in methods.
 */
public enum BuiltInMethod {
    RESULT_ITERATOR_NEXT(ResultIterator.class, "next"),
    TO_ENUMERABLE(CalciteRuntime.class, "toEnumerable", QueryPlan.class);

    public final Method method;
    public final Constructor constructor;

    public static final ImmutableMap<Method, BuiltInMethod> MAP;

    static {
        final ImmutableMap.Builder<Method, BuiltInMethod> builder =
            ImmutableMap.builder();
        for (BuiltInMethod value : BuiltInMethod.values()) {
            if (value.method != null) {
                builder.put(value.method, value);
            }
        }
        MAP = builder.build();
    }

    BuiltInMethod(Class clazz, String methodName, Class... argumentTypes) {
        this.method = Types.lookupMethod(clazz, methodName, argumentTypes);
        this.constructor = null;
    }

    BuiltInMethod(Class clazz, Class... argumentTypes) {
        this.method = null;
        this.constructor = Types.lookupConstructor(clazz, argumentTypes);
    }
}
