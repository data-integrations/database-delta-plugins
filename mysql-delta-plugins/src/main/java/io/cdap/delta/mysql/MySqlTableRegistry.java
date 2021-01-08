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
import io.cdap.delta.api.assessment.ColumnDetail;
import io.cdap.delta.api.assessment.ColumnSupport;
import io.cdap.delta.api.assessment.StandardizedTableDetail;
import io.cdap.delta.api.assessment.TableDetail;
import io.cdap.delta.api.assessment.TableList;
import io.cdap.delta.api.assessment.TableNotFoundException;
import io.cdap.delta.api.assessment.TableRegistry;
import io.cdap.delta.api.assessment.TableSummary;
import io.cdap.delta.plugin.common.ColumnEvaluation;
import io.cdap.delta.plugin.common.DriverCleanup;

import java.io.IOException;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.DriverManager;
import java.sql.JDBCType;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * Lists and describes tables.
 */
public class MySqlTableRegistry implements TableRegistry {
  private final MySqlConfig conf;
  private final DriverCleanup driverCleanup;

  public MySqlTableRegistry(MySqlConfig conf, DriverCleanup driverCleanup) {
    this.conf = conf;
    this.driverCleanup = driverCleanup;
  }

  @Override
  public TableList listTables() throws IOException {
    List<TableSummary> tables = new ArrayList<>();
    try (Connection connection = DriverManager.getConnection(getConnectionString(conf.getDatabase()),
                                                             conf.getConnectionProperties())) {
      DatabaseMetaData dbMeta = connection.getMetaData();
      try (ResultSet tableResults = dbMeta.getTables(conf.getDatabase(), null, null, null)) {
        while (tableResults.next()) {
          String tableName = tableResults.getString(3);
          // ignore the total number of columns for listing tables
          tables.add(new TableSummary(conf.getDatabase(), tableName, 0, null));
        }
      }
      return new TableList(tables);
    } catch (SQLException e) {
      throw new IOException(e.getMessage(), e);
    }
  }

  @Override
  public TableDetail describeTable(String db, String table) throws TableNotFoundException, IOException {
    try (Connection connection = DriverManager.getConnection(getConnectionString(db), conf.getConnectionProperties())) {
      DatabaseMetaData dbMeta = connection.getMetaData();
      TableDetail.Builder builder = getTableDetailBuilder(dbMeta, db, table)
        .orElseThrow(() -> new TableNotFoundException(db, table, ""));
      return builder.build();
    } catch (SQLException e) {
      throw new IOException(e.getMessage(), e);
    }
  }

  @Override
  public StandardizedTableDetail standardize(TableDetail tableDetail) {
    List<Schema.Field> columnSchemas = new ArrayList<>();
    for (ColumnDetail detail : tableDetail.getColumns()) {
      ColumnEvaluation evaluation = MySqlTableAssessor.evaluateColumn(detail);
      if (evaluation.getAssessment().getSupport().equals(ColumnSupport.NO)) {
        continue;
      }
      columnSchemas.add(evaluation.getField());
    }
    Schema schema = Schema.recordOf("outputSchema", columnSchemas);
    return new StandardizedTableDetail(tableDetail.getDatabase(), tableDetail.getTable(),
                                       tableDetail.getPrimaryKey(), schema);
  }

  @Override
  public void close() throws IOException {
    driverCleanup.close();
  }

  private Optional<TableDetail.Builder> getTableDetailBuilder(DatabaseMetaData dbMeta, String db, String table)
    throws SQLException {
    List<ColumnDetail> columns = new ArrayList<>();
    try (ResultSet columnResults = dbMeta.getColumns(db, null, table, null)) {
      while (columnResults.next()) {
        Map<String, String> properties = new HashMap<>();
        properties.put(MySqlTableAssessor.COLUMN_LENGTH, columnResults.getString("COLUMN_SIZE"));
        properties.put(MySqlTableAssessor.SCALE, columnResults.getString("DECIMAL_DIGITS"));
        columns.add(new ColumnDetail(columnResults.getString("COLUMN_NAME"),
                                     JDBCType.valueOf(columnResults.getInt("DATA_TYPE")),
                                     columnResults.getBoolean("NULLABLE"),
                                     properties));
      }
    }
    if (columns.isEmpty()) {
      return Optional.empty();
    }
    List<String> primaryKey = new ArrayList<>();
    try (ResultSet keyResults = dbMeta.getPrimaryKeys(db, null, table)) {
      while (keyResults.next()) {
        primaryKey.add(keyResults.getString("COLUMN_NAME"));
      }
    }
    return Optional.of(TableDetail.builder(db, table, null)
                         .setPrimaryKey(primaryKey)
                         .setColumns(columns));
  }

  private String getConnectionString(String db) {
    return String.format("jdbc:mysql://%s:%d/%s", conf.getHost(), conf.getPort(), db);
  }
}
