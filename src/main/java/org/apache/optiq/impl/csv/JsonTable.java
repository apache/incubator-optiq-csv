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

import org.apache.optiq.*;
import org.apache.optiq.impl.AbstractTableQueryable;
import org.apache.optiq.impl.enumerable.AbstractQueryableTable;
import org.apache.optiq.impl.enumerable.EnumerableConvention;
import org.apache.optiq.impl.enumerable.JavaRules;
import org.apache.optiq.rel.RelNode;
import org.apache.optiq.relopt.RelOptTable;
import org.apache.optiq.reltype.*;
import org.apache.optiq.sql.type.SqlTypeName;

import org.apache.linq4j.*;

import java.io.*;

/**
 * Table based on a JSON file.
 */
public class JsonTable extends AbstractQueryableTable
    implements TranslatableTable {
  private final File file;

  /** Creates a JsonTable. */
  JsonTable(File file) {
    super(Object[].class);
    this.file = file;
  }

  public String toString() {
    return "JsonTable";
  }

  public RelDataType getRowType(RelDataTypeFactory typeFactory) {
    return typeFactory.builder().add("_MAP",
        typeFactory.createMapType(
            typeFactory.createSqlType(SqlTypeName.VARCHAR),
            typeFactory.createSqlType(SqlTypeName.ANY))).build();
  }

  public Statistic getStatistic() {
    return Statistics.UNKNOWN;
  }

  public <T> Queryable<T> asQueryable(QueryProvider queryProvider,
      SchemaPlus schema, String tableName) {
    return new AbstractTableQueryable<T>(queryProvider, schema, this,
        tableName) {
      public Enumerator<T> enumerator() {
        //noinspection unchecked
        return (Enumerator<T>) new JsonEnumerator(file);
      }
    };
  }

  /** Returns an enumerable over the file. */
  public Enumerable<Object> enumerable() {
    return new AbstractEnumerable<Object>() {
      public Enumerator<Object> enumerator() {
        return new JsonEnumerator(file);
      }
    };
  }

  public RelNode toRel(
      RelOptTable.ToRelContext context,
      RelOptTable relOptTable) {
    return new JavaRules.EnumerableTableAccessRel(
        context.getCluster(),
        context.getCluster().traitSetOf(EnumerableConvention.INSTANCE),
        relOptTable,
        (Class) getElementType());
  }
}

// End JsonTable.java
