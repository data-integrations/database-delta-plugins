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

package io.cdap.delta.sqlserver;

import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.delta.api.assessment.ColumnAssessment;
import io.cdap.delta.api.assessment.ColumnDetail;
import io.cdap.delta.api.assessment.ColumnSuggestion;
import io.cdap.delta.api.assessment.ColumnSupport;
import io.cdap.delta.api.assessment.Problem;
import io.cdap.delta.api.assessment.TableAssessment;
import io.cdap.delta.api.assessment.TableAssessor;
import io.cdap.delta.api.assessment.TableDetail;

import java.sql.Types;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import javax.annotation.Nullable;

/**
 * Sql server table assessor
 */
public class SqlServerTableAssessor implements TableAssessor<TableDetail> {

  @Override
  public TableAssessment assess(TableDetail tableDetail) {
    List<ColumnAssessment> columnAssessments = new ArrayList<>();
    for (ColumnDetail columnDetail : tableDetail.getColumns()) {
      columnAssessments.add(evaluateColumn(columnDetail).getAssessment());
    }

    List<Problem> problems = new ArrayList<>();
    if (tableDetail.getPrimaryKey().isEmpty()) {
      problems.add(new Problem("Missing Primary Key", "Tables must have a primary key in order to be replicated.",
                               "Please alter the table to use a primary key, or select a different table", ""));
    }
    return new TableAssessment(columnAssessments, problems);
  }

  // This is based on https://docs.microsoft.com/en-us/sql/connect/jdbc/using-basic-data-types?view=sql-server-ver15
  static ColumnEvaluation evaluateColumn(ColumnDetail detail) throws IllegalArgumentException {
    Schema schema;
    int sqlType = detail.getType().getVendorTypeNumber();
    ColumnSupport support = ColumnSupport.YES;
    ColumnSuggestion suggestion = null;
    switch (sqlType) {
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
      case Types.FLOAT:
        schema = Schema.of(Schema.Type.FLOAT);
        break;

      case Types.NUMERIC:
      case Types.DECIMAL:
        // TODO: CDAP-16262 Add scale and precision to ColumnDetail to correctly determine the schema type
        schema = Schema.of(Schema.Type.DOUBLE);
        break;

      case Types.DOUBLE:
        schema = Schema.of(Schema.Type.DOUBLE);
        break;

      case Types.DATE:
        schema = Schema.of(Schema.LogicalType.DATE);
        break;
      case Types.TIME:
        support = ColumnSupport.PARTIAL;
        suggestion = new ColumnSuggestion("The precision will be reduced to microseconds from nanoseconds",
                                          Collections.emptyList());
        schema = Schema.of(Schema.LogicalType.TIME_MICROS);
        break;
      case Types.TIMESTAMP:
        support = ColumnSupport.PARTIAL;
        suggestion = new ColumnSuggestion("The precision will be reduced to microseconds from nanoseconds " +
                                            "if the sql server type is Datatime2",
                                          Collections.emptyList());
        schema = Schema.of(Schema.LogicalType.TIMESTAMP_MICROS);
        break;

      case Types.BINARY:
      case Types.VARBINARY:
      case Types.LONGVARBINARY:
        schema = Schema.of(Schema.Type.BYTES);
        break;

      case Types.VARCHAR:
      case Types.CHAR:
      case Types.LONGVARCHAR:
      case Types.LONGNVARCHAR:
      case Types.NCHAR:
      case Types.NVARCHAR:
        schema = Schema.of(Schema.Type.STRING);
        break;

      case Types.SQLXML:
        // this contains microsoft.sql.Types.DATETIMEOFFSET, which is defined in the jdbc jar
      default:
        support = ColumnSupport.NO;
        suggestion = new ColumnSuggestion("Unsupported SQL Type: " + detail.getType(),
                                          Collections.emptyList());
        schema = null;
    }

    Schema.Field field = schema == null ? null :
                           Schema.Field.of(detail.getName(), detail.isNullable() ? Schema.nullableOf(schema) : schema);
    ColumnAssessment assessment = ColumnAssessment.builder(detail.getName(), detail.getType().getName())
                                    .setSupport(support)
                                    .setSuggestion(suggestion)
                                    .build();
    return new ColumnEvaluation(field, assessment);
  }

  static class ColumnEvaluation {
    private final Schema.Field field;
    private final ColumnAssessment assessment;

    ColumnEvaluation(@Nullable Schema.Field field, ColumnAssessment assessment) {
      this.field = field;
      this.assessment = assessment;
    }

    ColumnAssessment getAssessment() {
      return assessment;
    }

    Schema.Field getField() {
      return field;
    }
  }
}
