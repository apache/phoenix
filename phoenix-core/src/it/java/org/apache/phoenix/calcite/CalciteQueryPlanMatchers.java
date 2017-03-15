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
package org.apache.phoenix.calcite;

import com.google.common.base.Function;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Ordering;
import org.apache.hadoop.hbase.util.Strings;
import org.apache.phoenix.calcite.rel.PhoenixClientProject;
import org.apache.phoenix.calcite.rel.PhoenixServerProject;
import org.apache.phoenix.calcite.rel.PhoenixTableScan;
import org.apache.phoenix.calcite.rel.PhoenixToEnumerableConverter;
import org.hamcrest.Description;
import org.hamcrest.Factory;
import org.hamcrest.Matcher;
import org.hamcrest.SelfDescribing;
import org.hamcrest.TypeSafeMatcher;

import java.util.List;
import java.util.Map;

import static org.hamcrest.CoreMatchers.equalTo;

/**
 * Utility for interpreting and matching query plans.
 * The matchers in this class work with query plans that have been parsed into
 * {@link PlanRelNode} trees -- these can be parsed from a JSON-based query plan via
 * {@link PlanRelNode#parsePlan(String)}.
 */
public class CalciteQueryPlanMatchers {

    /**
     * Interface for matching a single {@link PlanRelNode}. This is basically a customized
     * version of a {@link TypeSafeMatcher} for {@link PlanRelNode}s.
     */
    public interface PlanNodeMatcher extends SelfDescribing {

        /**
         * Evaluate this matcher against the given {@link PlanRelNode}.
         *
         * @param planRelNode the node to be evaluated by this matcher
         * @return true if this matcher matches the given node, otherwise false
         */
        boolean matchesPlanNode(PlanRelNode planRelNode);
    }

    /**
     * Wrapper around a collection of {@link PlanNodeMatcher} for matching a single
     * {@link PlanRelNode} within a query plan. This class is also a Hamcrest matcher, and
     * it's instances of this class that are used for full matching of a query plan.
     * This is matcher will match the logical conjunction (i.e. the AND) of all
     * of its underlying {@link PlanNodeMatcher}s.
     * Instances of this class are immutable, and can be used as building blocks into
     * larger instances with the "with" methods (e.g. {@link #withChild(CompositePlanNodeMatcher)}).
     */
    public static class CompositePlanNodeMatcher extends TypeSafeMatcher<PlanRelNode> {

        private final List<PlanNodeMatcher> planNodeMatchers;

        public CompositePlanNodeMatcher() {
            this(ImmutableList.<PlanNodeMatcher>of());
        }

        public CompositePlanNodeMatcher(List<PlanNodeMatcher> planNodeMatchers) {
            this.planNodeMatchers = ImmutableList.copyOf(planNodeMatchers);
        }

        @Override
        protected boolean matchesSafely(PlanRelNode planRelNode) {
            return matchesPlanNode(planRelNode);
        }

        public boolean matchesPlanNode(PlanRelNode planRelNode) {
            for (PlanNodeMatcher planNodeMatcher : planNodeMatchers) {
                if (!planNodeMatcher.matchesPlanNode(planRelNode)) {
                    return false;
                }
            }
            return true;
        }

        @Override
        public void describeTo(Description description) {
            describeTo(description, 0);
        }

        /**
         * Prints a matching description which attempts to match the string format of a full
         * {@link PlanRelNode} tree as closely as possible for easier understanding of failing
         * tests.
         *
         * @param description the description to be written to
         * @param indent      current level of indenting
         */
        void describeTo(Description description, int indent) {
            Ordering<PlanNodeMatcher> matcherOrdering = Ordering.natural().onResultOf(
                    new Function<PlanNodeMatcher, Integer>() {
                        @Override
                        public Integer apply(PlanNodeMatcher planNodeMatcher) {
                            if (planNodeMatcher instanceof PlanRelNodeTypeMatcher) {
                                return 0;
                            } else if (planNodeMatcher instanceof DescendantMatcher) {
                                return 2;
                            } else {
                                return 1;
                            }
                        }
                    });
            String indentStr = Strings.repeat(' ', indent * 4);
            boolean currentNodeMatchersDescribed = false;
            for (PlanNodeMatcher planNodeMatcher : matcherOrdering.sortedCopy(planNodeMatchers)) {
                if (planNodeMatcher instanceof DescendantMatcher) {
                    if (!currentNodeMatchersDescribed) {
                        // Make sure we print out something for the current node if there
                        // was nothing to match on it apart from descendants
                        description.appendText(indentStr + "<any node>\n");
                    }
                    DescendantMatcher descendantMatcher = (DescendantMatcher) planNodeMatcher;
                    descendantMatcher.describeTo(description, indent + 1);
                } else if (planNodeMatcher instanceof PlanRelNodeTypeMatcher) {
                    description.appendText(indentStr);
                    planNodeMatcher.describeTo(description);
                    description.appendText("\n");
                    currentNodeMatchersDescribed = true;
                } else {
                    description.appendText(indentStr + "* ");
                    planNodeMatcher.describeTo(description);
                    description.appendText("\n");
                    currentNodeMatchersDescribed = true;
                }
            }
        }

        /**
         * Create a new {@link CompositePlanNodeMatcher} that contains all of the
         * {@link PlanNodeMatcher}s within this instance in addition to a new
         * {@link PlanNodeMatcher}.
         *
         * @param planNodeMatcher the additional matcher to be added to the new instance
         * @return a new instance containing all matcher of this instance as well as the given
         * matcher
         */
        public CompositePlanNodeMatcher with(PlanNodeMatcher planNodeMatcher) {
            return new CompositePlanNodeMatcher(
                    ImmutableList.<PlanNodeMatcher>builder()
                            .addAll(planNodeMatchers)
                            .add(planNodeMatcher)
                            .build()
            );
        }

        /**
         * Create a new instance with the given matcher that will match a direct child input of
         * the {@link PlanRelNode} being evaluated.
         *
         * @param childPlanNodeMatcher the matcher to match a direct child of the node matched
         *                             by this matcher
         * @return a new instance that will match the conditions of this matcher plus the conditions
         * of the child matcher on a direct child input
         */
        public CompositePlanNodeMatcher withChild(CompositePlanNodeMatcher childPlanNodeMatcher) {
            return with(new DescendantMatcher(childPlanNodeMatcher, 1));
        }

        /**
         * Creates a new instance with the given matchers as adjacent child inputs of the
         * {@link PlanRelNode} being matched. This is simply a shortcut for multiple calls to
         * {@link #withChild(CompositePlanNodeMatcher)}.
         *
         * @param childPlanNodeMatchers the child node matchers to be added
         * @return a new instance that will amtch the conditions of this matcher plus the
         * conditions of the given child matchers on direct child inputs
         */
        public CompositePlanNodeMatcher withChildren(CompositePlanNodeMatcher... childPlanNodeMatchers) {
            CompositePlanNodeMatcher matcher = this;
            for (CompositePlanNodeMatcher childPlanNodeMatcher : childPlanNodeMatchers) {
                matcher = matcher.withChild(childPlanNodeMatcher);
            }
            return matcher;
        }

        /**
         * Create a new instance with the given matcher that will match an descendant input of the
         * {@link PlanRelNode} being evaluated.
         *
         * @param descendantPlanNodeMatcher the matcher to match an descendant of the node matched by
         *                                this matcher
         * @return a new instance that will match the conditions of this matcher plus the conditions
         * of the descendant matcher on an descendant node
         */
        public CompositePlanNodeMatcher withDescendant(CompositePlanNodeMatcher descendantPlanNodeMatcher) {
            return with(new DescendantMatcher(descendantPlanNodeMatcher, Integer.MAX_VALUE));
        }

        /**
         * Create a new instance with the given matcher that will match an attribute value directly.
         *
         * @param attributeName name of the attribute to be matched
         * @param expectedValue value to be matched via {@link Object#equals(Object)}
         * @return a new instance that contains all matchers in this class plus an attribute matcher
         * for the given attribute name and value
         */
        public CompositePlanNodeMatcher withValue(String attributeName, Object expectedValue) {
            return with(attributeName, equalTo(expectedValue));
        }

        /**
         * Create a new instance with the given Hamcrest matcher to be evaluated on an attribute
         * value. The returned {@link CompositePlanNodeMatcher} will contain all matchers of
         * this instance as well as the matcher on the given attribute.
         *
         * @param attributeName name of the attribute to be matched
         * @param matcher       hamcrest matcher to be evaluated on the value of the named attribute
         * @return a new instance that contains all matchers in this class plus an attribute
         * matcher for the given attribute name
         */
        public CompositePlanNodeMatcher with(String attributeName, Matcher<Object> matcher) {
            return with(new PlanNodeAttributeMatcher(attributeName, matcher));
        }
    }

    /**
     * A special case matcher which matches descendant nodes within a specified depth of the
     * "current" node being matched.
     */
    private static class DescendantMatcher implements PlanNodeMatcher {

        private final CompositePlanNodeMatcher descendantMatch;
        private final int maxDepth;

        public DescendantMatcher(CompositePlanNodeMatcher descendantMatch, int maxDepth) {
            this.descendantMatch = descendantMatch;
            this.maxDepth = maxDepth;
        }

        @Override
        public boolean matchesPlanNode(PlanRelNode planRelNode) {
            return matchesPlanNodeDescendants(planRelNode, maxDepth - 1);
        }

        private boolean matchesPlanNodeDescendants(PlanRelNode planRelNode, int depthLimit) {
            for (PlanRelNode childInput : planRelNode.getInputs()) {
                if (descendantMatch.matchesPlanNode(childInput)) {
                    return true;
                }
                if (depthLimit > 0 && matchesPlanNodeDescendants(childInput, depthLimit - 1)) {
                    return true;
                }
            }
            return false;
        }

        @Override
        public void describeTo(Description description) {
            describeTo(description, 0);
        }

        void describeTo(Description description, int indent) {
            if (maxDepth > 1) {
                description.appendText(String.format("%s<up to %d levels of descendants>\n",
                        Strings.repeat(' ', indent * 4), maxDepth));
            }
            descendantMatch.describeTo(description, indent);
        }
    }

    /**
     * Matcher that matches a node on the type name of the node. Matching is based on the
     * ending of the name of the node implementation, so it's possible to supply either a
     * fully-qualified class name, a simple class name (omitting the package), or anything
     * in between.
     */
    private static class PlanRelNodeTypeMatcher implements PlanNodeMatcher {

        private final String typeName;

        public PlanRelNodeTypeMatcher(String typeName) {
            this.typeName = typeName;
        }

        @Override
        public boolean matchesPlanNode(PlanRelNode planRelNode) {
            return planRelNode.getNodeClassName().endsWith(typeName);
        }

        @Override
        public void describeTo(Description description) {
            description.appendText(typeName);
        }

    }

    /**
     * Matches a single named attribute value on a {@link PlanRelNode} using a Hamcrest matcher
     * on the attribute value.
     */
    public static class PlanNodeAttributeMatcher implements PlanNodeMatcher {

        private final String attributeName;
        private final Matcher<Object> attributeValueMatcher;

        public PlanNodeAttributeMatcher(String attributeName, Matcher<Object> attributeValueMatcher) {
            this.attributeName = attributeName;
            this.attributeValueMatcher = attributeValueMatcher;
        }

        @Override
        public boolean matchesPlanNode(PlanRelNode planRelNode) {
            return attributeValueMatcher.matches(planRelNode.getAttributes().get(attributeName));
        }

        @Override
        public void describeTo(Description description) {
            description.appendText(attributeName + " ");
            attributeValueMatcher.describeTo(description);
        }
    }

    /**
     * Matches a filter clause attribute in a scan node.
     */
    private static class FilterClauseMatcher implements PlanNodeMatcher {

        private final Map<String, Object> expectedMap;

        public FilterClauseMatcher(int input, String operation, Object value) {
            expectedMap = ImmutableMap.<String, Object>of(
                    "op", operation,
                    "operands", ImmutableList.of(
                            ImmutableMap.of("input", input),
                            value));
        }

        @Override
        public void describeTo(Description description) {
            description.appendText("filter: " + expectedMap.toString());
        }

        @Override
        public boolean matchesPlanNode(PlanRelNode planRelNode) {
            return expectedMap.equals(planRelNode.getAttributes().get("filter"));
        }
    }

    /**
     * Create a matcher for a {@link PlanRelNode} that matches a node with the given name.
     *
     * The name matching is based on the ending of the name of the implementation class of the node,
     * meaning that a fully-qualified class name can be supplied, or the class name without the
     * package.
     *
     * @param typeName type name to be matched against the {@link PlanRelNode}'s implementation
     *                 class name
     * @return the created matcher
     */
    @Factory

    public static CompositePlanNodeMatcher planNode(String typeName) {
        return anyNode().with(new PlanRelNodeTypeMatcher(typeName));
    }

    /**
     * Create a matcher that matches any node.
     * This can be used as a starting point for building
     * up a matcher when the root node of the plan is unimportant, but it is desired to match
     * something lower down in the plan.
     *
     * @return a matcher which will match any {@link PlanRelNode}
     */
    @Factory
    public static CompositePlanNodeMatcher anyNode() {
        return new CompositePlanNodeMatcher();
    }

    /**
     * Creates a matcher that matches the root of a Phoenix-based query plan.
     * This can be used as a typical starting point for matching any Phoenix query plans.
     *
     * @return a matcher which will match the root of a Phoenix query plan
     */
    @Factory
    public static CompositePlanNodeMatcher phoenixPlan() {
        return planNode(PhoenixToEnumerableConverter.class.getName());
    }

    /**
     * Creates a matcher which matches a {@link PhoenixTableScan} node on the given phoenix table,
     * without a schema name.
     *
     * @param tableName name of the Phoenix table to be matched in the created node matcher
     * @return a matcher whicih will match a Phoenix scan node on the given table
     */
    @Factory
    public static CompositePlanNodeMatcher phoenixScan(String tableName) {
        return planNode(PhoenixTableScan.class.getName())
                .withValue("table", ImmutableList.of("phoenix", tableName));
    }

    /**
     * Creates a matcher which matches a {@link PhoenixTableScan} node on the given phoenix table
     * with the given schema name
     *
     * @param schemaName name of the schema in which the given Phoenix table resides
     * @param tableName name of the Phoenix table to be matched in the created node matcher
     * @return a matcher whicih will match a Phoenix scan node on the given table
     */
    @Factory
    public static CompositePlanNodeMatcher phoenixScan(String schemaName, String tableName) {
        return planNode(PhoenixTableScan.class.getName())
                .withValue("table", ImmutableList.of("phoenix", schemaName, tableName));
    }

    /**
     * Creates a {@link PlanNodeMatcher} which matches the "filter" attribute on a node.
     *
     * @param input identifier of the input field for the filter
     * @param operation operation for the filter
     * @param value value parameter for the filter
     * @return the created matcher
     */
    @Factory
    public static PlanNodeMatcher filter(int input, String operation, Object value) {
        return new FilterClauseMatcher(input, operation, value);
    }

    /**
     * Creates a {@link PlanNodeMatcher} which matches a Phoenix server-side projection.
     *
     * @param fields the fields to be projected
     * @return the created matcher for a server-side projection
     */
    @Factory
    public static CompositePlanNodeMatcher serverProject(String... fields) {
        return planNode(PhoenixServerProject.class.getName())
                .withValue("fields", ImmutableList.copyOf(fields));
    }

    /**
     * Creates a {@link PlanNodeMatcher} which matches a Phoenix client-side projection.
     *
     * @param fields the fields to be projected
     * @return the created matcher for a client-side projection
     */
    @Factory
    public static CompositePlanNodeMatcher clientProject(String... fields) {
        return planNode(PhoenixClientProject.class.getName())
                .withValue("fields", ImmutableList.copyOf(fields));
    }

}
