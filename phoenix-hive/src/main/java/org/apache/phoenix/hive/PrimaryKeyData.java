/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to you under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.phoenix.hive;

import java.io.IOException;
import java.io.InputStream;
import java.io.InvalidClassException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.ObjectStreamClass;
import java.io.OutputStream;
import java.io.Serializable;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

/**
 * Wrapper around the primary key data for Hive.
 */
public class PrimaryKeyData implements Serializable{
    public static final PrimaryKeyData EMPTY = new PrimaryKeyData(Collections.<String,Object> emptyMap());
    private static final long serialVersionUID = 1L;

    // Based on https://www.ibm.com/developerworks/library/se-lookahead/. Prevents unexpected
    // deserialization of other objects of an unexpected class.
    private static class LookAheadObjectInputStream extends ObjectInputStream {
        public LookAheadObjectInputStream(InputStream in) throws IOException {
            super(in);
        }

      @Override
      protected Class<?> resolveClass(ObjectStreamClass desc) throws IOException, ClassNotFoundException {
          if (!desc.getName().equals(PrimaryKeyData.class.getName()) &&
                  !desc.getName().startsWith("java.lang.") &&
                  !desc.getName().startsWith("java.util.") &&
                  !desc.getName().startsWith("java.sql.")) {
              throw new InvalidClassException(desc.getName(), "Expected an instance of PrimaryKeyData");
          }
          return super.resolveClass(desc);
      }
  }

    private final HashMap<String,Object> data;

    public PrimaryKeyData(Map<String,Object> data) {
        if (data instanceof HashMap) {
            this.data = (HashMap<String,Object>) data;
        } else {
            this.data = new HashMap<>(Objects.requireNonNull(data));
        }
    }

    public HashMap<String,Object> getData() {
        return data;
    }

    public void serialize(OutputStream output) throws IOException {
        try (ObjectOutputStream oos = new ObjectOutputStream(output)) {
            oos.writeObject(this);
            oos.flush();
        }
    }

    public static PrimaryKeyData deserialize(InputStream input) throws IOException, ClassNotFoundException {
        try (LookAheadObjectInputStream ois = new LookAheadObjectInputStream(input)) {
            Object obj = ois.readObject();
            if (obj instanceof PrimaryKeyData) {
                return (PrimaryKeyData) obj;
            }
            throw new InvalidClassException(obj == null ? "null" : obj.getClass().getName(), "Disallowed serialized class");
        }
    }
}
