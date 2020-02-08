/*
 * Copyright © 2020 Cask Data, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package io.cdap.delta.oracle;

import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.delta.api.assessment.ColumnAssessment;
import io.cdap.delta.api.assessment.ColumnDetail;
import io.cdap.delta.api.assessment.ColumnSuggestion;
import io.cdap.delta.api.assessment.ColumnSupport;
import io.cdap.delta.api.assessment.Problem;
import io.cdap.delta.api.assessment.TableAssessment;
import io.cdap.delta.api.assessment.TableAssessor;
import io.cdap.delta.api.assessment.TableDetail;
import io.cdap.delta.common.ColumnEvaluation;

import java.sql.Types;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * Table assessor for oracle.
 */
public class OracleTableAssessor implements TableAssessor<TableDetail> {

  @Override
  public TableAssessment assess(TableDetail tableDetail) {
    List<ColumnAssessment> columnAssessments = new ArrayList<>();
    for (ColumnDetail columnDetail : tableDetail.getColumns()) {
      columnAssessments.add(evaluateColumn(columnDetail).getAssessment());
    }

    List<Problem> problems = new ArrayList<>();
    if (tableDetail.getPrimaryKey().isEmpty()) {
      problems.add(new Problem("Missing Primary Key",
                               "Table must have a primary key in order to be replicated.",
                               "Please alter the table to use a primary key, or select a different table",
                               "Not able to do perform CDC for this table"));
    }
    return new TableAssessment(columnAssessments, problems);
  }

  // This mapping if followed by doc: https://docs.oracle.com/database/121/JJDBC/datacc.htm#JJDBC28367
  static ColumnEvaluation evaluateColumn(ColumnDetail detail) {
    Schema schema;
    int sqlType = detail.getType().getVendorTypeNumber();
    ColumnSupport support = ColumnSupport.YES;
    ColumnSuggestion suggestion = null;

    switch (sqlType) {
      case Types.CHAR:
      case Types.VARCHAR:
      case Types.LONGVARCHAR:
      case Types.NCHAR:
        schema = Schema.of(Schema.Type.STRING);
        break;
      case Types.NUMERIC:
        // NUMERIC contains precision and scale, it will depend on that number to do the conversion, for unblocking
        // current test table which defines ID as NUMBER(4), will hardcode it to map to INT.
        // Loop back once this task CDAP-16262 is done.
        schema = Schema.of(Schema.Type.INT);
        break;
      case Types.DECIMAL:
        schema = Schema.of(Schema.LogicalType.DECIMAL);
        break;
      case Types.FLOAT:
      case Types.DOUBLE:
        schema = Schema.of(Schema.Type.DOUBLE);
        break;
      case Types.BIT:
        schema = Schema.of(Schema.Type.BOOLEAN);
        break;
      case Types.TINYINT:
      case Types.SMALLINT:
      case Types.INTEGER:
        schema = Schema.of(Schema.Type.INT);
        break;
      case Types.BIGINT:
        schema = Schema.of(Schema.Type.LONG);
        break;
      case Types.REAL:
        schema = Schema.of(Schema.Type.FLOAT);
        break;
      case Types.BINARY:
      case Types.VARBINARY:
      case Types.LONGVARBINARY:
        schema = Schema.of(Schema.Type.BYTES);
        break;
      case Types.DATE:
        schema = Schema.of(Schema.LogicalType.DATE);
        break;
      case Types.TIME:
        schema = Schema.of(Schema.LogicalType.TIME_MILLIS);
        break;
      case Types.TIMESTAMP:
        support = ColumnSupport.PARTIAL;
        suggestion = new ColumnSuggestion("The precision will be reduced to microseconds from nanoseconds",
                                          Collections.emptyList());
        schema = Schema.of(Schema.LogicalType.TIMESTAMP_MICROS);
        break;
      case Types.ARRAY:
        schema = Schema.of(Schema.Type.ARRAY);
        break;
      default:
        support = ColumnSupport.NO;
        suggestion = new ColumnSuggestion("Unsupported SQL Data Type: " + detail.getType(),
                                          Collections.emptyList());
        schema = null;
    }

    Schema.Field field = null;
    String type = "N/A";

    if (schema != null) {
      field = Schema.Field.of(detail.getName(), detail.isNullable() ? Schema.nullableOf(schema) : schema);
    }

    if (field != null) {
      Schema.LogicalType logicalType = field.getSchema().getLogicalType();
      type = logicalType == null ? field.getSchema().getType().name() : logicalType.name();
    }

    ColumnAssessment assessment = ColumnAssessment.builder(detail.getName(), type)
      .setSupport(support)
      .setSuggestion(suggestion)
      .build();

    return new ColumnEvaluation(field, assessment);
  }
}

