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
import org.apache.phoenix.expression.Expression;
import org.apache.phoenix.parse.DistanceFunctionParseNode;
import org.apache.phoenix.parse.FunctionParseNode.Argument;
import org.apache.phoenix.parse.FunctionParseNode.BuiltInFunction;
import org.apache.phoenix.schema.SortOrder;
import org.apache.phoenix.schema.types.PVectorDouble;
import org.apache.phoenix.schema.types.PVectorFloat;

/**
 * Cosine distance metric: {@code 1 - dot(a, b) / (norm(a) * norm(b))}. Zero magnitude vectors yield
 * an orthogonal distance of 1.0.
 */
@BuiltInFunction(name = CosineDistanceFunction.NAME, nodeClass = DistanceFunctionParseNode.class,
    args = { @Argument(allowedTypes = { PVectorFloat.class, PVectorDouble.class }),
      @Argument(allowedTypes = { PVectorFloat.class, PVectorDouble.class }) })
public class CosineDistanceFunction extends DistanceFunction {

  public static final String NAME = "COSINE_DISTANCE";

  public CosineDistanceFunction() {
  }

  public CosineDistanceFunction(List<Expression> children) {
    super(children);
  }

  @Override
  public String getName() {
    return NAME;
  }

  @Override
  protected double computeFloat(byte[] a, int aOff, byte[] b, int bOff, int dim, double bound) {
    return VectorDistanceUtil.cosineDistanceWithBound(a, aOff, b, bOff, dim, bound);
  }

  @Override
  protected double computeGeneric(byte[] a, int aOff, boolean aIsDouble, SortOrder aOrder, byte[] b,
    int bOff, boolean bIsDouble, SortOrder bOrder, int dim, double bound) {
    double dot = 0.0;
    double normA = 0.0;
    double normB = 0.0;
    for (int i = 0; i < dim; i++) {
      double ai = element(a, aOff, aIsDouble, aOrder, i);
      double bi = element(b, bOff, bIsDouble, bOrder, i);
      dot += ai * bi;
      normA += ai * ai;
      normB += bi * bi;
    }
    return VectorDistanceUtil.applyBound(ScalarDistanceKernel.cosineDistance(dot, normA, normB),
      bound);
  }
}
