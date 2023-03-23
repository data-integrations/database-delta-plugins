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
import io.cdap.delta.api.assessment.ColumnDetail;
import io.cdap.delta.api.assessment.ColumnSupport;
import io.cdap.delta.api.assessment.Problem;
import io.cdap.delta.api.assessment.StandardizedTableDetail;
import io.cdap.delta.api.assessment.TableDetail;
import io.cdap.delta.api.assessment.TableList;
import io.cdap.delta.api.assessment.TableNotFoundException;
import io.cdap.delta.api.assessment.TableRegistry;
import io.cdap.delta.api.assessment.TableSummary;
import io.cdap.delta.plugin.common.ColumnEvaluation;
import io.cdap.delta.plugin.common.DriverCleanup;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.DriverManager;
import java.sql.JDBCType;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import javax.annotation.Nullable;

/**
 * Sql Server table registry
 */
public class SqlServerTableRegistry implements TableRegistry {
  private static final Logger LOG = LoggerFactory.getLogger(SqlServerTableRegistry.class);
  private final String jdbcUrl;
  private final SqlServerConfig config;
  private final DriverCleanup driverCleanup;

  public SqlServerTableRegistry(SqlServerConfig config, DriverCleanup driverCleanup) {
    this.jdbcUrl = String.format("jdbc:sqlserver://%s:%d;databaseName=%s;user=%s;password=%s",
                                 config.getHost(), config.getPort(), config.getDatabase(), config.getUser(),
                                 config.getPassword());
    this.config = config;
    this.driverCleanup = driverCleanup;
  }

  @Override
  public TableList listTables() throws IOException {
    List<TableSummary> tables = new ArrayList<>();
    try (Connection connection = DriverManager.getConnection(jdbcUrl)) {
      try (Statement statement = connection.createStatement()) {
        // we have to execute query here since the metadata will return sys tables back, and we cannot filter
        // that, this query only works for SQL server 2005 or above
        String query = String.format("SELECT name FROM [%s].sys.tables where is_ms_shipped = 0", config.getDatabase());
        ResultSet resultSet = statement.executeQuery(query);
        Set<String> tableNames = new HashSet<>();
        while (resultSet.next()) {
          tableNames.add(resultSet.getString("name"));
        }
        DatabaseMetaData dbMeta = connection.getMetaData();
        try (ResultSet tableResults = dbMeta.getTables(config.getDatabase(), null, null, new String[]{"TABLE"})) {
          while (tableResults.next()) {
            String tableName = tableResults.getString("TABLE_NAME");
            if (!tableNames.contains(tableName)) {
              continue;
            }
            String schemaName = tableResults.getString("TABLE_SCHEM");
            // ignore the total number of columns for listing tables
            tables.add(new TableSummary(config.getDatabase(), tableName, 0, schemaName));
          }
        }
        return new TableList(tables);
      }
    } catch (SQLException e) {
      throw new IOException(e.getMessage(), e);
    }
  }


  @Override
  public TableDetail describeTable(String db, String table) throws TableNotFoundException, IOException {
    return describeTable(db, null, table);
  }

  @Override
  public TableDetail describeTable(String db, @Nullable String schema, String table)
    throws TableNotFoundException, IOException {
    try (Connection connection = DriverManager.getConnection(jdbcUrl)) {
      List<Problem> missingFeatures = new ArrayList<>();
      DatabaseMetaData dbMeta = connection.getMetaData();
      TableDetail.Builder builder = getTableDetailBuilder(dbMeta, db, schema, table)
        .orElseThrow(() -> new TableNotFoundException(db, table, ""));


      String query =
        schema == null ? String.format("SELECT is_tracked_by_cdc FROM sys.tables where name = '%s'", table)
                       : String.format("SELECT is_tracked_by_cdc FROM " +
                                         "(select is_tracked_by_cdc, schema_id from sys.tables where name = '%s') as t "
                                         + "join (select schema_id from sys.schemas where name ='%s') as s "
                                         + "on t.schema_id = s.schema_id", table, schema);
      try (Statement statement = connection.createStatement();
           ResultSet rs = statement.executeQuery(query)) {
        // if schema is null, we will check any table matching the table name with any schema
        if (rs.next()) {
          // if cdc is enabled, then the column 'is_tracked_by_cdc' should be 1
          if (rs.getInt("is_tracked_by_cdc") != 1) {
            missingFeatures.add(
              new Problem("Table CDC Feature Not Enabled",
                          String.format("The CDC feature for table '%s' in database '%s' was not enabled.",
                                        schema == null ? table : schema + "." + table, db),
                          "Check the table CDC settings",
                          "Not able to replicate table changes"));
          }
        }
      } catch (Exception e) {
        String msg = String.format("Unable to check if CDC feature for table '%s' in database '%s' was enabled or not",
                                      schema == null ? table : schema + "." + table, db);
        LOG.error(msg, e);
        missingFeatures.add(
          new Problem("Unable To Check If CDC Was Enabled", msg,
                      "Check database connectivity and table information", null));
      }
      return builder.setFeatures(missingFeatures).build();
    } catch (SQLException e) {
      throw new IOException(e.getMessage(), e);
    }
  }

  @Override
  public StandardizedTableDetail standardize(TableDetail tableDetail) {
    List<Schema.Field> columnSchemas = new ArrayList<>();
    for (ColumnDetail detail : tableDetail.getColumns()) {
      ColumnEvaluation evaluation = SqlServerTableAssessor.evaluateColumn(detail);
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

  private Optional<TableDetail.Builder> getTableDetailBuilder(DatabaseMetaData dbMeta, String db,
    @Nullable String schema, String table) throws SQLException {
    List<ColumnDetail> columns = new ArrayList<>();
    // this schema name is needed to construct the full table name, e.g, dbo.test for debizium to fetch records from
    // sql server. The table name is constructed using [schemaName].[tableName]. However, the dbMeta is not able
    // to retrieve any result back if we pass in the full table name, the schema name has to be passed separately
    // in the second parameter.
    // if schema name is null, then any table matching the table name with any schema name
    try (ResultSet columnResults = dbMeta.getColumns(db, schema, table, null)) {
      while (columnResults.next()) {
        Map<String, String> properties = new HashMap<>();
        properties.put(SqlServerTableAssessor.COLUMN_LENGTH, columnResults.getString("COLUMN_SIZE"));
        properties.put(SqlServerTableAssessor.SCALE, columnResults.getString("DECIMAL_DIGITS"));
        properties.put(SqlServerTableAssessor.TYPE_NAME, columnResults.getString("TYPE_NAME"));
        schema = columnResults.getString("TABLE_SCHEM");
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
    LOG.debug("Query primary key for {}.{}.{}", db, schema, table);
    try (ResultSet keyResults = dbMeta.getPrimaryKeys(db, schema, table)) {
      while (keyResults.next()) {
        String pk = keyResults.getString("COLUMN_NAME");
        LOG.debug("Found primary key for {}.{}.{} : {}", db, schema, table, pk);
        primaryKey.add(pk);
      }
    }

    return Optional.of(TableDetail.builder(db, table, schema)
                         .setPrimaryKey(primaryKey)
                         .setColumns(columns));
  }
}
