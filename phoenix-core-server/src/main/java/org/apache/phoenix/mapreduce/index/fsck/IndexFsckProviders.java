/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.phoenix.mapreduce.index.fsck;

import org.apache.phoenix.mapreduce.index.fsck.ivf.IvfIndexFsckProvider;
import org.apache.phoenix.schema.PTable;
import org.apache.phoenix.schema.PTableType;
import org.apache.phoenix.util.CDCUtil;

/**
 * Factory for resolving the appropriate {@link IndexFsckProvider} for a given secondary index.
 */
public final class IndexFsckProviders {

  private IndexFsckProviders() {
  }

  /**
   * Resolves the provider for the specified index table based on its index type and algorithm.
   * @param indexTable target index PTable
   * @return matching IndexFsckProvider
   * @throws IllegalArgumentException      if indexTable is null or required parameters are missing
   * @throws UnsupportedOperationException if table type, index type, or algorithm is unsupported
   */
  public static IndexFsckProvider forIndex(PTable indexTable) {
    if (indexTable == null) {
      throw new IllegalArgumentException("Index table cannot be null");
    }

    if (CDCUtil.isCDCIndex(indexTable)) {
      throw new UnsupportedOperationException("Unsupported index type for index tooling: CDC");
    }

    if (indexTable.getType() != PTableType.INDEX) {
      throw new UnsupportedOperationException(
        "Unsupported table type for index tooling: " + indexTable.getType());
    }

    PTable.IndexType indexType = indexTable.getIndexType();
    if (indexType == null) {
      throw new UnsupportedOperationException(
        "Index table has null index type: " + indexTable.getName().getString());
    }

    switch (indexType) {
      case GLOBAL:
      case UNCOVERED_GLOBAL:
        return new GlobalIndexFsckProvider();
      case VECTOR_GLOBAL:
        String algorithm = indexTable.getVectorIndexAlgorithm();
        if (algorithm == null || algorithm.trim().isEmpty()) {
          throw new IllegalArgumentException(
            "Vector index '" + indexTable.getName().getString() + "' has no algorithm specified");
        }
        if ("IVF".equalsIgnoreCase(algorithm)) {
          return new IvfIndexFsckProvider();
        } else {
          throw new UnsupportedOperationException(
            "Unsupported vector index algorithm for index tooling: " + algorithm);
        }
      case LOCAL:
        throw new UnsupportedOperationException("Unsupported index type for index tooling: LOCAL");
      default:
        throw new UnsupportedOperationException(
          "Unsupported index type for index tooling: " + indexType);
    }
  }
}
