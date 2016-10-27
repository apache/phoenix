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
package org.apache.phoenix.hive.ppd;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hive.ql.metadata.HiveStoragePredicateHandler.DecomposedPredicate;
import org.apache.hadoop.hive.ql.plan.ExprNodeDesc;
import org.apache.hadoop.hive.ql.plan.ExprNodeGenericFuncDesc;
import org.apache.phoenix.hive.ql.index.IndexPredicateAnalyzer;
import org.apache.phoenix.hive.ql.index.IndexSearchCondition;
import org.apache.phoenix.hive.ql.index.PredicateAnalyzerFactory;

import java.util.ArrayList;
import java.util.List;

/**
 * Supporting class that generate DecomposedPredicate companion to PhoenixHiveStorageHandler
 * basing on search conditions.
 */
public class PhoenixPredicateDecomposer {

    private static final Log LOG = LogFactory.getLog(PhoenixPredicateDecomposer.class);

    private List<String> columnNameList;
    private boolean calledPPD;

    private List<IndexSearchCondition> searchConditionList;

    public static PhoenixPredicateDecomposer create(List<String> columnNameList) {
        return new PhoenixPredicateDecomposer(columnNameList);
    }

    private PhoenixPredicateDecomposer(List<String> columnNameList) {
        this.columnNameList = columnNameList;
    }

    public DecomposedPredicate decomposePredicate(ExprNodeDesc predicate) {
        if (LOG.isDebugEnabled()) {
            LOG.debug("predicate - " + predicate.toString());
        }

        IndexPredicateAnalyzer analyzer = PredicateAnalyzerFactory.createPredicateAnalyzer
                (columnNameList, getFieldValidator());
        DecomposedPredicate decomposed = new DecomposedPredicate();

        List<IndexSearchCondition> conditions = new ArrayList<IndexSearchCondition>();
        decomposed.residualPredicate = (ExprNodeGenericFuncDesc) analyzer.analyzePredicate
                (predicate, conditions);
        if (!conditions.isEmpty()) {
            decomposed.pushedPredicate = analyzer.translateSearchConditions(conditions);
            try {
                searchConditionList = conditions;
                calledPPD = true;
            } catch (Exception e) {
                LOG.warn("Failed to decompose predicates", e);
                return null;
            }
        }

        if (LOG.isDebugEnabled()) {
            LOG.debug("decomposed predicate - residualPredicate: " + decomposed.residualPredicate +
            ", pushedPredicate: " + decomposed.pushedPredicate);
        }

        return decomposed;
    }

    public List<IndexSearchCondition> getSearchConditionList() {
        return searchConditionList;
    }

    public boolean isCalledPPD() {
        return calledPPD;
    }

    protected IndexPredicateAnalyzer.FieldValidator getFieldValidator() {
        return null;
    }
}
