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
package org.apache.phoenix.expression.function;

import java.util.List;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.phoenix.expression.Expression;
import org.apache.phoenix.parse.FunctionParseNode.Argument;
import org.apache.phoenix.parse.FunctionParseNode.BuiltInFunction;
import org.apache.phoenix.parse.InnerProductDistanceParseNode;
import org.apache.phoenix.schema.SortOrder;
import org.apache.phoenix.schema.types.PVectorDouble;
import org.apache.phoenix.schema.types.PVectorFloat;

/** Computes negated dot product: {@code -dot(a, b)} */
@BuiltInFunction(name = InnerProductDistanceFunction.NAME,
    nodeClass = InnerProductDistanceParseNode.class,
    args = { @Argument(allowedTypes = { PVectorFloat.class, PVectorDouble.class }),
      @Argument(allowedTypes = { PVectorFloat.class, PVectorDouble.class }) })
public class InnerProductDistanceFunction extends DistanceFunction {

  public static final String NAME = "INNER_PRODUCT";

  public InnerProductDistanceFunction() {
  }

  public InnerProductDistanceFunction(List<Expression> children) {
    super(children);
  }

  public InnerProductDistanceFunction(List<Expression> children, double distanceUpperBound) {
    super(children, distanceUpperBound);
  }

  @Override
  public String getName() {
    return NAME;
  }

  @Override
  protected double computeDistance(byte[] buf1, int off1, SortOrder order1, boolean isDouble1,
    byte[] buf2, int off2, SortOrder order2, boolean isDouble2, int dim, double bound) {
    return compute(buf1, off1, order1, isDouble1, buf2, off2, order2, isDouble2, dim, bound);
  }

  public static double innerProductDistance(byte[] a, int aOff, byte[] b, int bOff, int dim) {
    return innerProductDistanceWithBound(a, aOff, b, bOff, dim, Double.MAX_VALUE);
  }

  public static double innerProductDistanceWithBound(byte[] a, int aOff, byte[] b, int bOff,
    int dim, double bound) {
    return VectorDistanceUtil.innerProductDistanceWithBound(a, aOff, b, bOff, dim, bound);
  }

  public static double compute(byte[] buf1, int off1, SortOrder order1, boolean isDouble1,
    byte[] buf2, int off2, SortOrder order2, boolean isDouble2, int dim, double bound) {
    // Fast path: both float, ASC order
    if (!isDouble1 && !isDouble2 && order1 == SortOrder.ASC && order2 == SortOrder.ASC) {
      return VectorDistanceUtil.innerProductDistanceWithBound(buf1, off1, buf2, off2, dim, bound);
    }
    double dot = 0.0;
    if (isDouble1 && isDouble2 && order1 == SortOrder.ASC && order2 == SortOrder.ASC) {
      // Fast path: both double, ASC order
      for (int i = 0; i < dim; i++) {
        double a = Bytes.toDouble(buf1, off1 + i * Bytes.SIZEOF_DOUBLE);
        double b = Bytes.toDouble(buf2, off2 + i * Bytes.SIZEOF_DOUBLE);
        dot += a * b;
      }
    } else {
      // General path
      for (int i = 0; i < dim; i++) {
        double a = isDouble1
          ? PVectorDouble.readElement(buf1, off1, i, order1)
          : PVectorFloat.readElement(buf1, off1, i, order1);
        double b = isDouble2
          ? PVectorDouble.readElement(buf2, off2, i, order2)
          : PVectorFloat.readElement(buf2, off2, i, order2);
        dot += a * b;
      }
    }

    double distance = -dot;
    if (distance > bound) {
      return Double.MAX_VALUE;
    }
    return distance;
  }
}
