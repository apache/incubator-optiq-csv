/*
// Licensed to Julian Hyde under one or more contributor license
// agreements. See the NOTICE file distributed with this work for
// additional information regarding copyright ownership.
//
// Julian Hyde licenses this file to you under the Apache License,
// Version 2.0 (the "License"); you may not use this file except in
// compliance with the License. You may obtain a copy of the License at:
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
*/
package org.apache.optiq.impl.csv;

import org.apache.optiq.rel.RelNode;
import org.apache.optiq.relopt.RelOptTable;
import org.apache.optiq.reltype.RelProtoDataType;

import java.io.File;

/**
 * Refinement of {@link CsvTable} that plans itself.
 */
class CsvSmartTable extends CsvTable {
  /** Creates a CsvSmartTable. */
  CsvSmartTable(File file, RelProtoDataType protoRowType) {
    super(file, protoRowType);
  }

  public RelNode toRel(
      RelOptTable.ToRelContext context,
      RelOptTable relOptTable) {
    // Request all fields.
    final int fieldCount = relOptTable.getRowType().getFieldCount();
    final int[] fields = CsvEnumerator.identityList(fieldCount);
    return new CsvTableScan(context.getCluster(), relOptTable, this, fields);
  }
}

// End CsvSmartTable.java
