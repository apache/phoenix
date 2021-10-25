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
package org.apache.phoenix.schema.tool;

import org.apache.phoenix.thirdparty.com.google.common.collect.ArrayListMultimap;
import org.apache.phoenix.thirdparty.com.google.common.collect.ListMultimap;
import org.apache.hadoop.hbase.util.Pair;
import org.apache.phoenix.parse.AddColumnStatement;
import org.apache.phoenix.parse.BindableStatement;
import org.apache.phoenix.parse.ColumnDef;
import org.apache.phoenix.parse.ColumnDefInPkConstraint;
import org.apache.phoenix.parse.ColumnName;
import org.apache.phoenix.parse.CreateIndexStatement;
import org.apache.phoenix.parse.CreateTableStatement;
import org.apache.phoenix.parse.DropColumnStatement;
import org.apache.phoenix.parse.DropIndexStatement;
import org.apache.phoenix.parse.DropTableStatement;
import org.apache.phoenix.parse.PrimaryKeyConstraint;
import org.apache.phoenix.parse.SQLParser;
import org.apache.phoenix.schema.SortOrder;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.apache.phoenix.schema.tool.SchemaSQLUtil.getCreateIndexSQL;
import static org.apache.phoenix.schema.tool.SchemaSQLUtil.getCreateTableSQL;

public class SchemaSynthesisProcessor implements SchemaProcessor {
    public static final String
            ENTITY_NAME_IN_BASE_AND_ALTER_DDL_DON_T_MATCH =
            "Entity name in base and alter DDL don't match";
    public static final String
            UNSUPPORTED_DDL_EXCEPTION =
            "SchemaTool in Synth mode is supported for CREATE TABLE/VIEW/INDEX ddls";
    private final String ddlFile;

    public SchemaSynthesisProcessor(String ddlFile) {
        this.ddlFile = ddlFile;
    }

    @Override
    public String process() throws Exception {
        List <String> allDDL = getQueriesFromFile(ddlFile);
        String ddl = null;
        for(String s : allDDL) {
            ddl = synthesize(ddl, s);
        }
        return ddl == null ? "" :ddl;
    }

    private String synthesize(String baseDDL, String nextDDL) throws Exception {
        if (baseDDL == null && nextDDL != null) {
            BindableStatement bStmt = new SQLParser(nextDDL).parseStatement();
            if (bStmt instanceof CreateTableStatement || bStmt instanceof CreateIndexStatement) {
                return nextDDL;
            }
            throw new Exception(UNSUPPORTED_DDL_EXCEPTION);
        }
        BindableStatement createStatement = new SQLParser(baseDDL).parseStatement();
        BindableStatement alterStatement = new SQLParser(nextDDL).parseStatement();
        if (createStatement instanceof CreateTableStatement) {
            CreateTableStatement newCreateStmt = null;
            CreateTableStatement createStmt = (CreateTableStatement) createStatement;
            if (alterStatement instanceof AddColumnStatement) {
                newCreateStmt =
                        getCreateTableStatement((AddColumnStatement) alterStatement, createStmt);
            } else if (alterStatement instanceof DropColumnStatement) {
                newCreateStmt =
                        getCreateTableStatement((DropColumnStatement) alterStatement, createStmt);
            } else if (alterStatement instanceof DropTableStatement) {
                return null;
            }
            return getCreateTableSQL(newCreateStmt);
        } else if (createStatement instanceof CreateIndexStatement) {
            if (alterStatement instanceof DropIndexStatement) {
                return null;
            }
            CreateIndexStatement newCreateIndexStmt =
                    getCreateIndexStatement(alterStatement, (CreateIndexStatement) createStatement);
            return getCreateIndexSQL(newCreateIndexStmt);
        } else {
            throw new Exception(UNSUPPORTED_DDL_EXCEPTION);
        }
    }

    private CreateIndexStatement getCreateIndexStatement(BindableStatement alterStatement, CreateIndexStatement createStatement) throws Exception {
        CreateIndexStatement newCreateIndexStmt = null;
        String tableName = createStatement.getIndexTableName().toString();
        String tableNameInAlter = ((AddColumnStatement)alterStatement).getTable().toString().trim();
        sanityCheck(tableName, tableNameInAlter);
        AddColumnStatement addStmt = (AddColumnStatement) alterStatement;
        if (addStmt.getColumnDefs() == null) {
            ListMultimap<String, Pair<String, Object>>
                    finalProps =
                    getEffectiveProperties(addStmt, createStatement.getProps());
            newCreateIndexStmt = new CreateIndexStatement(createStatement, finalProps);
        }
        return newCreateIndexStmt;
    }

    private CreateTableStatement getCreateTableStatement(DropColumnStatement alterStatement,
            CreateTableStatement createStmt) throws Exception {
        CreateTableStatement newCreateStmt = null;
        String tableName = createStmt.getTableName().toString();
        String tableNameInAlter = alterStatement.getTable().toString().trim();
        sanityCheck(tableName, tableNameInAlter);
        List<ColumnDef> oldColumnDef = createStmt.getColumnDefs();
        List<ColumnDef> newColumnDef = new ArrayList<>();
        newColumnDef.addAll(oldColumnDef);
        DropColumnStatement dropStmt = alterStatement;
        for(ColumnName cName : dropStmt.getColumnRefs()) {
            for(ColumnDef colDef : oldColumnDef) {
                if (colDef.getColumnDefName().equals(cName)) {
                    newColumnDef.remove(colDef);
                    break;
                }
            }
        }
        newCreateStmt = new CreateTableStatement(createStmt, newColumnDef);
        return newCreateStmt;
    }

    private CreateTableStatement getCreateTableStatement(AddColumnStatement alterStatement,
            CreateTableStatement createStmt) throws Exception {
        CreateTableStatement newCreateStmt = null;
        String tableName = createStmt.getTableName().toString();
        String tableNameInAlter = alterStatement.getTable().toString().trim();
        sanityCheck(tableName, tableNameInAlter);
        AddColumnStatement addStmt = alterStatement;
        List<ColumnDef> oldColDef = createStmt.getColumnDefs();
        List<ColumnDef> newColDef = new ArrayList<>();
        if (addStmt.getColumnDefs() == null) {
            ListMultimap<String, Pair<String, Object>>
                    finalProps = getEffectiveProperties(addStmt, createStmt.getProps());
            newCreateStmt = new CreateTableStatement(createStmt, finalProps, oldColDef);
        } else {
            newColDef.addAll(oldColDef);
            newColDef.addAll(addStmt.getColumnDefs());
            PrimaryKeyConstraint oldPKConstraint = createStmt.getPrimaryKeyConstraint();
            List<ColumnDefInPkConstraint> pkList = new ArrayList<>();
            for(Pair<ColumnName, SortOrder> entry : oldPKConstraint.getColumnNames()) {
                ColumnDefInPkConstraint cd = new
                        ColumnDefInPkConstraint(entry.getFirst(), entry.getSecond(), oldPKConstraint.isColumnRowTimestamp(entry
                        .getFirst()));
                pkList.add(cd);
            }
            for(ColumnDef cd : addStmt.getColumnDefs()) {
                if(cd.isPK()) {
                    ColumnDefInPkConstraint cdpk = new ColumnDefInPkConstraint(cd.getColumnDefName(), cd.getSortOrder(), cd.isRowTimestamp());
                    pkList.add(cdpk);
                }
            }
            PrimaryKeyConstraint pkConstraint = new PrimaryKeyConstraint(oldPKConstraint.getName(), pkList);
            newCreateStmt = new CreateTableStatement(createStmt, pkConstraint, newColDef);
        }
        return newCreateStmt;
    }

    private void sanityCheck(String tableName, String tableNameInAlter) throws Exception {
        if (!tableName.equalsIgnoreCase(tableNameInAlter)) {
            throw new Exception(ENTITY_NAME_IN_BASE_AND_ALTER_DDL_DON_T_MATCH);
        }
    }

    private ListMultimap<String, Pair<String, Object>> getEffectiveProperties(
            AddColumnStatement addStmt, ListMultimap<String, Pair<String, Object>> oldProps) {
        Map<String, Object> oldPropMap = new HashMap();
        Map<String, Object> changePropMap = new HashMap();

        for (Pair<String, Object> value : oldProps.values()) {
            oldPropMap.put(value.getFirst(),value.getSecond());
        }
        for (Pair<String, Object> value : addStmt.getProps().values()) {
            changePropMap.put(value.getFirst(),value.getSecond());
        }

        oldPropMap.putAll(changePropMap);
        ListMultimap<String, Pair<String, Object>>
                finalProps =
                ArrayListMultimap.<String, Pair<String, Object>>create();
        for (Map.Entry<String, Object> entry : oldPropMap.entrySet()) {
            finalProps.put("", Pair.newPair(entry.getKey(), entry.getValue()));
        }
        return finalProps;
    }

    private List<String> getQueriesFromFile(String ddlFile) throws IOException {
        StringBuilder sb = new StringBuilder();
        File file = new File(ddlFile);
        BufferedReader br = new BufferedReader(new InputStreamReader(
            new FileInputStream(file), StandardCharsets.UTF_8));
        String st;
        while ((st = br.readLine()) != null) {
            sb.append(st).append("\n");
        }
        String trimmedQuery = sb.toString().trim();
        if (trimmedQuery.contains("/*") && trimmedQuery.contains("*/")) {
            trimmedQuery = trimmedQuery.substring(trimmedQuery.lastIndexOf("*/") + 2);
        }
        String [] queries = trimmedQuery.split(";");
        List<String> output = new ArrayList<>();
        for(String query: queries) {
            StringBuilder newSb = new StringBuilder(query);
            char lastChar = newSb.charAt(newSb.length() - 1);
            // DDL in the file should not have a ; at the end
            // remove the last char if it is ; or \n
            if (lastChar == '\n' || lastChar == ';') {
                newSb.deleteCharAt(newSb.length() - 1);
            }
            output.add(newSb.toString().trim());
        }
        return output;
    }
}
