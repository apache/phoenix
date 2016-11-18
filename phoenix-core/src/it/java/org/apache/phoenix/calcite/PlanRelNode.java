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

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.google.common.base.Preconditions;
import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Multimap;
import org.apache.hadoop.hbase.util.Strings;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Represents a single node in a query plan. This is parsed directly from a JSON-based query
 * plan.
 *
 * A complete query plan is represented as a root {@link PlanRelNode}. The child (input) nodes
 * of the root node can be accessed via {@link #getInputs()}.
 */
public class PlanRelNode {

    /**
     * Names of attribute in a query plan which have special meaning (and aren't just "normal"
     * attributes).
     */
    private static final Set<String> RESERVED_FIELD_NAMES = ImmutableSet.of("id", "inputs", "relOp");

    private final int id;
    private final String nodeClassName;
    private List<PlanRelNode> inputs;
    private final Map<String, Object> attributes;

    /**
     * Parse a query plan from JSON into a {@link PlanRelNode}. A JSON query plan can be retrieved
     * via <tt>EXPLAIN PLAN AS JSON FOR &lt;sql statement&gt;</tt>.
     *
     * @param jsonQueryPlan the JSON output of <tt>EXPLAIN PLAN AS JSON</tt>
     * @return the root node of the parsed query plan
     */
    public static PlanRelNode parsePlan(String jsonQueryPlan) {
        ObjectMapper objectMapper = new ObjectMapper();
        JsonNode treeRoot;
        try {
            treeRoot = objectMapper.readTree(jsonQueryPlan);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

        Multimap<Integer, Integer> nodeToInputIds = ArrayListMultimap.create();
        Map<Integer, PlanRelNode> idToNode = new HashMap<>();

        PlanRelNode previousNode = null;
        for (JsonNode relNode : treeRoot.get("rels")) {
            int id = relNode.get("id").asInt();

            String nodeClassName = relNode.get("relOp").textValue();
            ArrayNode inputsArray = (ArrayNode) relNode.get("inputs");
            List<PlanRelNode> inputNodes = ImmutableList.of();

            if (inputsArray == null && previousNode != null) {
                inputNodes = ImmutableList.of(previousNode);
            } else if (inputsArray != null) {
                for (JsonNode inputIdNode : inputsArray) {
                    nodeToInputIds.put(id, inputIdNode.asInt());
                }
            }
            Map<String, Object> attributeMap = new HashMap<>();
            Iterator<Map.Entry<String, JsonNode>> fieldItr = relNode.fields();
            while (fieldItr.hasNext()) {
                Map.Entry<String, JsonNode> fieldEntry = fieldItr.next();
                if (RESERVED_FIELD_NAMES.contains(fieldEntry.getKey())) {
                    continue;
                }
                attributeMap.put(fieldEntry.getKey(), deserializeValue(fieldEntry.getValue()));
            }
            previousNode = new PlanRelNode(id, nodeClassName, inputNodes, attributeMap);
            idToNode.put(id, previousNode);
        }
        assert previousNode != null;

        // Now we re-bind all the inputs to the multi-input nodes
        for (Integer nodeId : nodeToInputIds.keySet()) {
            List<PlanRelNode> inputNodes = new ArrayList<>();
            for (Integer childNodeId : nodeToInputIds.get(nodeId)) {
                inputNodes.add(idToNode.get(childNodeId));
            }
            idToNode.get(nodeId).setInputs(inputNodes);
        }

        return previousNode;
    }

    PlanRelNode(int id, String nodeClassName,
            List<PlanRelNode> inputs, Map<String, Object> attributes) {
        this.id = id;
        this.nodeClassName = Preconditions.checkNotNull(nodeClassName);
        this.inputs = ImmutableList.copyOf(inputs);
        this.attributes = ImmutableMap.copyOf(attributes);
    }

    public int getId() {
        return id;
    }

    public String getNodeClassName() {
        return nodeClassName;
    }

    public List<PlanRelNode> getInputs() {
        return inputs;
    }

    public Map<String, Object> getAttributes() {
        return attributes;
    }

    private void setInputs(List<PlanRelNode> inputs) {
        this.inputs = ImmutableList.copyOf(inputs);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        PlanRelNode that = (PlanRelNode) o;

        if (id != that.id) return false;
        if (!nodeClassName.equals(that.nodeClassName)) return false;
        if (!inputs.equals(that.inputs)) return false;
        return attributes.equals(that.attributes);

    }

    @Override
    public int hashCode() {
        int result = id;
        result = 31 * result + nodeClassName.hashCode();
        result = 31 * result + inputs.hashCode();
        result = 31 * result + attributes.hashCode();
        return result;
    }

    @Override
    public String toString() {
        return asString(0);
    }

    private String asString(int indentLevel) {
        String indent = Strings.repeat(' ', indentLevel * 4);
        StringBuilder strBuilder = new StringBuilder();
        strBuilder.append(String.format("%s%s id %d",
                indent,
                nodeClassName, id));
        for (Map.Entry<String, Object> attributeEntry : attributes.entrySet()) {
            strBuilder.append(String.format("\n%s * %s=%s", indent,
                    attributeEntry.getKey(), attributeEntry.getValue()));
        }
        for (PlanRelNode input : inputs) {
            strBuilder.append(String.format("\n%s", input.asString(indentLevel + 1)));
        }
        return strBuilder.toString();
    }

    /**
     * JSON deserialization helper for recursively deserializing attribute values.
     *
     * @param jsonNode attribute value to be deserialized
     * @return the full-deserialized value
     */
    private static Object deserializeValue(JsonNode jsonNode) {
        if (jsonNode.isValueNode()) {
            if (jsonNode.isNumber()) {
                return jsonNode.numberValue();
            } else if (jsonNode.isBoolean()) {
                return jsonNode.booleanValue();
            } else {
                return jsonNode.asText();
            }
        } else if (jsonNode.isArray()) {
            List<Object> list = new ArrayList<>();
            for (JsonNode arrayValue : jsonNode) {
                list.add(deserializeValue(arrayValue));
            }
            return list;
        } else if (jsonNode.isObject()) {
            Map<String, Object> objectMap = new HashMap<>();
            Iterator<Map.Entry<String, JsonNode>> fieldItr = jsonNode.fields();
            while (fieldItr.hasNext()) {
                Map.Entry<String, JsonNode> entry = fieldItr.next();
                objectMap.put(entry.getKey(), deserializeValue(entry.getValue()));
            }
            return objectMap;
        } else {
            throw new RuntimeException("Don't know how to deserialize " + jsonNode);
        }
    }
}
