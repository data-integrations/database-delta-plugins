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

package io.cdap.delta.mysql;

import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.delta.api.assessment.Assessment;
import io.cdap.delta.api.assessment.ColumnAssessment;
import io.cdap.delta.api.assessment.ColumnDetail;
import io.cdap.delta.api.assessment.ColumnSuggestion;
import io.cdap.delta.api.assessment.ColumnSupport;
import io.cdap.delta.api.assessment.Problem;
import io.cdap.delta.api.assessment.TableAssessment;
import io.cdap.delta.api.assessment.TableAssessor;
import io.cdap.delta.api.assessment.TableDetail;
import io.cdap.delta.plugin.common.ColumnEvaluation;
import io.cdap.delta.plugin.common.DriverCleanup;

import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.sql.Types;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

/**
 * MySQL table assessor.
 */
public class MySqlTableAssessor implements TableAssessor<TableDetail> {
  static final String COLUMN_LENGTH = "COLUMN_LENGTH";
  static final String SCALE = "SCALE";

  private final MySqlConfig conf;
  private final DriverCleanup driverCleanup;

  MySqlTableAssessor(MySqlConfig conf, DriverCleanup driverCleanup) {
    this.conf = conf;
    this.driverCleanup = driverCleanup;
  }

  @Override
  public Assessment assess() {
    List<Problem> featureProblems = new ArrayList<>();
    List<String> permissions = new ArrayList<>();
    permissions.add("REPLICATION SLAVE");
    permissions.add("REPLICATION CLIENT");
    checkReplicationPermissions(featureProblems, permissions);
    return new Assessment(featureProblems, Collections.emptyList());
  }

  @Override
  public void close() throws IOException {
    driverCleanup.close();
  }

  @Override
  public TableAssessment assess(TableDetail tableDetail) {
    List<ColumnAssessment> columnAssessments = new ArrayList<>();
    for (ColumnDetail columnDetail : tableDetail.getColumns()) {
      columnAssessments.add(evaluateColumn(columnDetail).getAssessment());
    }

    return new TableAssessment(columnAssessments, tableDetail.getFeatures());
  }

  // This is based on https://dev.mysql.com/doc/connector-j/5.1/en/connector-j-reference-type-conversions.html
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
        Map<String, String> properties = detail.getProperties();
        // For numeric/decimal columns, this 'COLUMN_LENGTH' represents the precision
        int precision = Integer.parseInt(properties.get(COLUMN_LENGTH));
        int scale = Integer.parseInt(properties.get(SCALE));

        // According to the official MySQL doc, precision must be a value from 1 to 65 and scale must
        // be a value from 0 to 30 and no larger than precision. Since the scale is always greater or equal to 0,
        // the precision here is always equal to the number of significant digits, we can just reuse the precision
        // to form cdap decimal logical type.
        // Doc refer: https://dev.mysql.com/doc/refman/5.7/en/precision-math-decimal-characteristics.html
        schema = Schema.decimalOf(precision, scale);
        break;
      case Types.DOUBLE:
        schema = Schema.of(Schema.Type.DOUBLE);
        break;
      case Types.DATE:
        schema = Schema.of(Schema.LogicalType.DATE);
        break;
      case Types.TIME:
        schema = Schema.of(Schema.LogicalType.TIME_MICROS);
        break;
      case Types.TIMESTAMP:
        schema = Schema.of(Schema.LogicalType.TIMESTAMP_MICROS);
        break;
      case Types.BLOB:
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

  private void checkReplicationPermissions(List<Problem> featureProblems, List<String> permissions) {
    String clientHost = "%";
    String query = String.format("SHOW GRANTS FOR '%s'@'%s'", conf.getUser(), clientHost);
    Problem permissionUnknownProblem =
      new Problem("Table Replication Permission Unknown",
                  String.format("Unable to check if '%s' permissions were granted for user '%s'@'%s' or not",
                                String.join(",", permissions), conf.getUser(), clientHost),
                  "Check database connectivity and user permissions",
                  "Change events might fail to be read after the snapshot phase");
    try (Connection connection = DriverManager.getConnection(conf.getJdbcURL(), conf.getConnectionProperties());
         Statement statement = connection.createStatement();
         ResultSet rs = statement.executeQuery(query)) {
      if (rs.next()) {
        String grants = rs.getString(1);

        // Avoid potential NPE in case the connection was accidentally closed by database before the value is read,
        // though it will happen very rarely
        if (grants == null) {
          featureProblems.add(permissionUnknownProblem);
          return;
        }

        if (grants.contains("ALL")) {
          return;
        }

        for (String permission : permissions) {
          if (!grants.contains(permission)) {
            featureProblems.add(
              new Problem("Table Replication Permission Not Granted",
                          String.format("The '%s' permission is not granted for user '%s'@'%s'", permission,
                                        conf.getUser(), clientHost),
                          String.format("Grant '%s' permission to user '%s'@'%s'", permission,
                                        conf.getUser(), clientHost),
                          "Will not be able to read replication events after the initial snapshot completes."));
          }
        }
      }
    } catch (Exception e) {
      featureProblems.add(permissionUnknownProblem);
    }
  }
}
