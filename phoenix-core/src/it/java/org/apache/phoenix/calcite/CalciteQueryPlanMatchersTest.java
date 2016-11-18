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

import org.hamcrest.StringDescription;
import org.junit.Test;

import static org.apache.phoenix.calcite.CalciteQueryPlanMatchers.anyNode;
import static org.apache.phoenix.calcite.CalciteQueryPlanMatchers.filter;
import static org.apache.phoenix.calcite.CalciteQueryPlanMatchers.phoenixPlan;
import static org.apache.phoenix.calcite.CalciteQueryPlanMatchers.phoenixScan;
import static org.apache.phoenix.calcite.CalciteQueryPlanMatchers.planNode;
import static org.apache.phoenix.calcite.CalciteQueryPlanMatchers.serverProject;
import static org.hamcrest.CoreMatchers.not;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;

public class CalciteQueryPlanMatchersTest {

    // A general validation and demo of the calcite plan matching
    @Test
    public void testMatching() {
        /*
         * This is a plan for "select entity_id, a_string, organization_id from aTable where a_string = 'a'"
         *
         * The textual plan is as follows:
         *  PhoenixToEnumerableConverter
         *      PhoenixServerProject(ENTITY_ID=[$1], A_STRING=[$2], ORGANIZATION_ID=[$0])
         *          PhoenixTableScan(table=[[phoenix, ATABLE]], filter=[=($2, 'a')])
         */
        final String planJson = "{\n"
                + "  \"rels\": [\n"
                + "    {\n"
                + "      \"id\": \"0\",\n"
                + "      \"relOp\": \"org.apache.phoenix.calcite.rel.PhoenixTableScan\",\n"
                + "      \"table\": [\n"
                + "        \"phoenix\",\n"
                + "        \"ATABLE\"\n"
                + "      ],\n"
                + "      \"filter\": {\n"
                + "        \"op\": \"=\",\n"
                + "        \"operands\": [\n"
                + "          {\n"
                + "            \"input\": 2\n"
                + "          },\n"
                + "          \"a\"\n"
                + "        ]\n"
                + "      },\n"
                + "      \"inputs\": []\n"
                + "    },\n"
                + "    {\n"
                + "      \"id\": \"1\",\n"
                + "      \"relOp\": \"org.apache.phoenix.calcite.rel.PhoenixServerProject\",\n"
                + "      \"fields\": [\n"
                + "        \"ENTITY_ID\",\n"
                + "        \"A_STRING\",\n"
                + "        \"ORGANIZATION_ID\"\n"
                + "      ],\n"
                + "      \"exprs\": [\n"
                + "        {\n"
                + "          \"input\": 1\n"
                + "        },\n"
                + "        {\n"
                + "          \"input\": 2\n"
                + "        },\n"
                + "        {\n"
                + "          \"input\": 0\n"
                + "        }\n"
                + "      ]\n"
                + "    },\n"
                + "    {\n"
                + "      \"id\": \"2\",\n"
                + "      \"relOp\": \"org.apache.phoenix.calcite.rel.PhoenixToEnumerableConverter\"\n"
                + "    }\n"
                + "  ]\n"
                + "}";

        PlanRelNode planRelNode = PlanRelNode.parsePlan(planJson);

        // The planRelNode toString implementation is very similar to the
        // "normal" string representation of a Calcite query plan, which is intended to
        // make it easier to understand failing matches
        assertEquals(
                  "org.apache.phoenix.calcite.rel.PhoenixToEnumerableConverter id 2\n"
                + "    org.apache.phoenix.calcite.rel.PhoenixServerProject id 1\n"
                + "     * exprs=[{input=1}, {input=2}, {input=0}]\n"
                + "     * fields=[ENTITY_ID, A_STRING, ORGANIZATION_ID]\n"
                + "        org.apache.phoenix.calcite.rel.PhoenixTableScan id 0\n"
                + "         * filter={operands=[{input=2}, a], op==}\n"
                + "         * table=[phoenix, ATABLE]", planRelNode.toString());

        // First just do a generic match of the structure
        assertThat(
                planRelNode,
                planNode("PhoenixToEnumerableConverter").withChild(
                        planNode("PhoenixServerProject").withChild(
                                planNode("PhoenixTableScan"))));

        // Now a demo of the more Phoenix-specific matchers
        assertThat(planRelNode, phoenixPlan().withChild(
                serverProject("ENTITY_ID", "A_STRING", "ORGANIZATION_ID").withChild(
                    phoenixScan("ATABLE").with(filter(2, "=", "a")))));

        // Proof that an assertion will fail if we put something in the matcher which
        // isn't there
        assertThat(
                planRelNode,
                not(
                    phoenixPlan().withChild(
                        serverProject("ENTITY_ID", "A_STRING", "ORGANIZATION_ID").withChild(
                            phoenixScan("WRONG_TABLE").with(filter(2, "=", "a"))))));

        // Demo of more generic matching, where we're only really interested in something
        // deeper down in the plan
        assertThat(
                planRelNode,
                anyNode().withDescendant(
                        phoenixScan("ATABLE").with(filter(2, "=", "a"))));

        // The description of a matcher is intended to also be as close as possible to the
        // "standard" string version of the plan, so that we'll have meaningful assertion
        // failure error messages
        StringDescription stringDescription = new StringDescription();
        planNode("PhoenixToEnumerableConverter").withChild(
                serverProject("ENTITY_ID", "A_STRING", "ORGANIZATION_ID").withChild(
                        planNode("PhoenixTableScan"))).describeTo(stringDescription);

        assertEquals(
                "PhoenixToEnumerableConverter\n"
                + "    org.apache.phoenix.calcite.rel.PhoenixServerProject\n"
                + "    * fields <[ENTITY_ID, A_STRING, ORGANIZATION_ID]>\n"
                + "        PhoenixTableScan\n",
                stringDescription.toString());

    }

    // Testing of a plan that has multiple branched inputs into a join
    @Test
    public void testJoinPlan() {
        String joinPlan = "{\n"
                + "  \"rels\": [\n"
                + "    {\n"
                + "      \"id\": \"0\",\n"
                + "      \"relOp\": \"org.apache.phoenix.calcite.rel.PhoenixTableScan\",\n"
                + "      \"table\": [\n"
                + "        \"phoenix\",\n"
                + "        \"Join\",\n"
                + "        \"ItemTable\"\n"
                + "      ],\n"
                + "      \"inputs\": []\n"
                + "    },\n"
                + "    {\n"
                + "      \"id\": \"1\",\n"
                + "      \"relOp\": \"org.apache.phoenix.calcite.rel.PhoenixTableScan\",\n"
                + "      \"table\": [\n"
                + "        \"phoenix\",\n"
                + "        \"Join\",\n"
                + "        \"SupplierTable\"\n"
                + "      ],\n"
                + "      \"filter\": {\n"
                + "        \"op\": \"=\",\n"
                + "        \"operands\": [\n"
                + "          {\n"
                + "            \"op\": \"CAST\",\n"
                + "            \"operands\": [\n"
                + "              {\n"
                + "                \"input\": 1\n"
                + "              }\n"
                + "            ],\n"
                + "            \"type\": {\n"
                + "              \"type\": \"VARCHAR\",\n"
                + "              \"nullable\": true,\n"
                + "              \"precision\": 2\n"
                + "            }\n"
                + "          },\n"
                + "          \"S5\"\n"
                + "        ]\n"
                + "      },\n"
                + "      \"inputs\": []\n"
                + "    },\n"
                + "    {\n"
                + "      \"id\": \"2\",\n"
                + "      \"relOp\": \"org.apache.phoenix.calcite.rel.PhoenixServerProject\",\n"
                + "      \"fields\": [\n"
                + "        \"supplier_id\",\n"
                + "        \"NAME\",\n"
                + "        \"PHONE\",\n"
                + "        \"ADDRESS\",\n"
                + "        \"LOC_ID\",\n"
                + "        \"NAME5\"\n"
                + "      ],\n"
                + "      \"exprs\": [\n"
                + "        {\n"
                + "          \"input\": 0\n"
                + "        },\n"
                + "        {\n"
                + "          \"input\": 1\n"
                + "        },\n"
                + "        {\n"
                + "          \"input\": 2\n"
                + "        },\n"
                + "        {\n"
                + "          \"input\": 3\n"
                + "        },\n"
                + "        {\n"
                + "          \"input\": 4\n"
                + "        },\n"
                + "        {\n"
                + "          \"op\": \"CAST\",\n"
                + "          \"operands\": [\n"
                + "            {\n"
                + "              \"input\": 1\n"
                + "            }\n"
                + "          ],\n"
                + "          \"type\": {\n"
                + "            \"type\": \"VARCHAR\",\n"
                + "            \"nullable\": true,\n"
                + "            \"precision\": 2\n"
                + "          }\n"
                + "        }\n"
                + "      ]\n"
                + "    },\n"
                + "    {\n"
                + "      \"id\": \"3\",\n"
                + "      \"relOp\": \"org.apache.phoenix.calcite.rel.PhoenixServerJoin\",\n"
                + "      \"condition\": {\n"
                + "        \"op\": \"=\",\n"
                + "        \"operands\": [\n"
                + "          {\n"
                + "            \"input\": 5\n"
                + "          },\n"
                + "          {\n"
                + "            \"input\": 7\n"
                + "          }\n"
                + "        ]\n"
                + "      },\n"
                + "      \"joinType\": \"inner\",\n"
                + "      \"inputs\": [\n"
                + "        \"0\",\n"
                + "        \"2\"\n"
                + "      ]\n"
                + "    },\n"
                + "    {\n"
                + "      \"id\": \"4\",\n"
                + "      \"relOp\": \"org.apache.phoenix.calcite.rel.PhoenixClientProject\",\n"
                + "      \"fields\": [\n"
                + "        \"item_id\",\n"
                + "        \"NAME\",\n"
                + "        \"PRICE\",\n"
                + "        \"DISCOUNT1\",\n"
                + "        \"DISCOUNT2\",\n"
                + "        \"supplier_id\",\n"
                + "        \"DESCRIPTION\",\n"
                + "        \"supplier_id0\",\n"
                + "        \"NAME0\",\n"
                + "        \"PHONE\",\n"
                + "        \"ADDRESS\",\n"
                + "        \"LOC_ID\"\n"
                + "      ],\n"
                + "      \"exprs\": [\n"
                + "        {\n"
                + "          \"input\": 0\n"
                + "        },\n"
                + "        {\n"
                + "          \"input\": 1\n"
                + "        },\n"
                + "        {\n"
                + "          \"input\": 2\n"
                + "        },\n"
                + "        {\n"
                + "          \"input\": 3\n"
                + "        },\n"
                + "        {\n"
                + "          \"input\": 4\n"
                + "        },\n"
                + "        {\n"
                + "          \"input\": 5\n"
                + "        },\n"
                + "        {\n"
                + "          \"input\": 6\n"
                + "        },\n"
                + "        {\n"
                + "          \"input\": 7\n"
                + "        },\n"
                + "        {\n"
                + "          \"input\": 8\n"
                + "        },\n"
                + "        {\n"
                + "          \"input\": 9\n"
                + "        },\n"
                + "        {\n"
                + "          \"input\": 10\n"
                + "        },\n"
                + "        {\n"
                + "          \"input\": 11\n"
                + "        }\n"
                + "      ]\n"
                + "    },\n"
                + "    {\n"
                + "      \"id\": \"5\",\n"
                + "      \"relOp\": \"org.apache.phoenix.calcite.rel.PhoenixToEnumerableConverter\"\n"
                + "    }\n"
                + "  ]\n"
                + "}\n";

        PlanRelNode planRelNode = PlanRelNode.parsePlan(joinPlan);

        assertThat(
                planRelNode,
                phoenixPlan().withChild(
                        planNode("PhoenixClientProject").withChild(
                                planNode("PhoenixServerJoin").withValue("joinType", "inner").withChildren(
                                        phoenixScan("Join", "ItemTable"),
                                        serverProject("supplier_id", "NAME", "PHONE", "ADDRESS",
                                                    "LOC_ID", "NAME5").withChild(
                                                phoenixScan("Join", "SupplierTable")
                                        )
                                )
                        ))
        );
    }
}
