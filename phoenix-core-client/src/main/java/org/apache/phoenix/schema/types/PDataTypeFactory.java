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
package org.apache.phoenix.schema.types;

import java.util.Comparator;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;

/**
 * Factory avoids circular dependency problem in static initializers across types.
 */
public class PDataTypeFactory {

  private static PDataTypeFactory INSTANCE;
  private final PDataType[] orderedTypes;
  private final SortedSet<PDataType> types;
  private final Map<Class<? extends PDataType>, PDataType> classToInstance;
  private final Map<Class, PDataType> javaClassToInstance;
  private final Map<Class, PDataType> javaClassToUnsignedInstance;
  private final SortedSet<PDataType> unsignedtypes;

  public static PDataTypeFactory getInstance() {
    if (INSTANCE == null) {
      INSTANCE = new PDataTypeFactory();
    }
    return INSTANCE;
  }

  private PDataTypeFactory() {
    types = new TreeSet<>(new Comparator<PDataType>() {
      @Override
      public int compare(PDataType o1, PDataType o2) {
        return Integer.compare(o1.ordinal(), o2.ordinal());
      }
    });    // TODO: replace with ServiceLoader or some other plugin system
    unsignedtypes = new TreeSet<>(new Comparator<PDataType>() {
        @Override
        public int compare(PDataType o1, PDataType o2) {
          return Integer.compare(o1.ordinal(), o2.ordinal());
        }
      });    // TODO: replace with ServiceLoader or some other plugin system
    types.add(PBinary.INSTANCE);
    types.add(PBinaryArray.INSTANCE);
    types.add(PChar.INSTANCE);
    types.add(PCharArray.INSTANCE);
    types.add(PDecimal.INSTANCE);
    types.add(PDecimalArray.INSTANCE);
    types.add(PBoolean.INSTANCE);
    types.add(PBooleanArray.INSTANCE);
    types.add(PDate.INSTANCE);
    types.add(PDateArray.INSTANCE);
    types.add(PDouble.INSTANCE);
    types.add(PDoubleArray.INSTANCE);
    types.add(PFloat.INSTANCE);
    types.add(PFloatArray.INSTANCE);
    types.add(PInteger.INSTANCE);
    types.add(PIntegerArray.INSTANCE);
    types.add(PLong.INSTANCE);
    types.add(PLongArray.INSTANCE);
    types.add(PTime.INSTANCE);
    types.add(PTimeArray.INSTANCE);
    types.add(PTimestamp.INSTANCE);
    types.add(PTimestampArray.INSTANCE);
    types.add(PSmallint.INSTANCE);
    types.add(PSmallintArray.INSTANCE);
    types.add(PTinyint.INSTANCE);
    types.add(PTinyintArray.INSTANCE);
    types.add(PUnsignedDate.INSTANCE);
    unsignedtypes.add(PUnsignedDate.INSTANCE);
    types.add(PUnsignedDateArray.INSTANCE);
    unsignedtypes.add(PUnsignedDateArray.INSTANCE);
    types.add(PUnsignedDouble.INSTANCE);
    unsignedtypes.add(PUnsignedDouble.INSTANCE);
    types.add(PUnsignedDoubleArray.INSTANCE);
    unsignedtypes.add(PUnsignedDoubleArray.INSTANCE);
    types.add(PUnsignedFloat.INSTANCE);
    unsignedtypes.add(PUnsignedFloat.INSTANCE);
    types.add(PUnsignedFloatArray.INSTANCE);
    unsignedtypes.add(PUnsignedFloatArray.INSTANCE);
    types.add(PUnsignedInt.INSTANCE);
    unsignedtypes.add(PUnsignedInt.INSTANCE);
    types.add(PUnsignedIntArray.INSTANCE);
    unsignedtypes.add(PUnsignedIntArray.INSTANCE);
    types.add(PUnsignedLong.INSTANCE);
    unsignedtypes.add(PUnsignedLong.INSTANCE);
    types.add(PUnsignedLongArray.INSTANCE);
    unsignedtypes.add(PUnsignedLongArray.INSTANCE);
    types.add(PUnsignedSmallint.INSTANCE);
    unsignedtypes.add(PUnsignedSmallint.INSTANCE);
    types.add(PUnsignedSmallintArray.INSTANCE);
    unsignedtypes.add(PUnsignedSmallintArray.INSTANCE);
    types.add(PUnsignedTime.INSTANCE);
    unsignedtypes.add(PUnsignedTime.INSTANCE);
    types.add(PUnsignedTimeArray.INSTANCE);
    unsignedtypes.add(PUnsignedTimeArray.INSTANCE);
    types.add(PUnsignedTimestamp.INSTANCE);
    unsignedtypes.add(PUnsignedTimestamp.INSTANCE);
    types.add(PUnsignedTimestampArray.INSTANCE);
    unsignedtypes.add(PUnsignedTimestampArray.INSTANCE);
    types.add(PUnsignedTinyint.INSTANCE);
    unsignedtypes.add(PUnsignedTinyint.INSTANCE);
    types.add(PUnsignedTinyintArray.INSTANCE);
    unsignedtypes.add(PUnsignedTinyintArray.INSTANCE);
    types.add(PVarbinary.INSTANCE);
    types.add(PVarbinaryArray.INSTANCE);
    types.add(PVarchar.INSTANCE);
    types.add(PVarcharArray.INSTANCE);

    classToInstance = new HashMap<>(types.size());
    for (PDataType t : types) {
      classToInstance.put(t.getClass(), t);
    }
    javaClassToInstance = new HashMap<>(types.size());
    for (PDataType t : types) {
        Class javaClass = t.getJavaClass();
        // The first match
        javaClassToInstance.putIfAbsent(javaClass, t);
    }
    javaClassToUnsignedInstance = new HashMap<>(types.size());
    for (PDataType t : unsignedtypes) {
        Class javaClass = t.getJavaClass();
        // The first match
        javaClassToInstance.putIfAbsent(javaClass, t);
    }
    orderedTypes = types.toArray(new PDataType[types.size()]);
  }

  public Set<PDataType> getTypes() {
    return types;
  }

  public PDataType[] getOrderedTypes() {
    return orderedTypes;
  }

  public PDataType instanceFromClass(Class<? extends PDataType> clazz) {
    return classToInstance.get(clazz);
  }

  public PDataType instanceFromJavaClass(Class clazz, PDataType actualType) {
      if (unsignedtypes.contains(actualType)) {
          return javaClassToUnsignedInstance.get(clazz);
      } else {
          return javaClassToInstance.get(clazz);
      }
  }
}
