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
package org.apache.calcite.sql;

import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlWriter;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.util.SqlVisitor;
import org.apache.calcite.sql.validate.SqlValidator;
import org.apache.calcite.sql.validate.SqlValidatorScope;
import org.apache.calcite.util.Litmus;
import org.apache.phoenix.parse.ColumnDefInPkConstraint;

public class SqlColumnDefInPkConstraintNode extends SqlNode{
	private final ColumnDefInPkConstraint pkConstraint;
	
	public SqlColumnDefInPkConstraintNode(SqlParserPos pos, ColumnDefInPkConstraint pkConstraint) {
		super(pos);
		this.pkConstraint = pkConstraint;
	}

	public ColumnDefInPkConstraint getPkConstraint() {
		return pkConstraint;
	}

	@Override
	public void unparse(SqlWriter writer, int leftPrec, int rightPrec) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void validate(SqlValidator validator, SqlValidatorScope scope) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public <R> R accept(SqlVisitor<R> visitor) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public boolean equalsDeep(SqlNode node, Litmus litmus) {
		// TODO Auto-generated method stub
		return false;
	}
}
