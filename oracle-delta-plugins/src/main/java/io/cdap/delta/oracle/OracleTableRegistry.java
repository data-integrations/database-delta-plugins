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
import io.cdap.delta.api.assessment.ColumnDetail;
import io.cdap.delta.api.assessment.StandardizedTableDetail;
import io.cdap.delta.api.assessment.TableDetail;
import io.cdap.delta.api.assessment.TableList;
import io.cdap.delta.api.assessment.TableNotFoundException;
import io.cdap.delta.api.assessment.TableRegistry;
import io.cdap.delta.api.assessment.TableSummary;
import io.cdap.delta.common.DriverCleanup;
import io.debezium.connector.oracle.OracleConnection;
import io.debezium.connector.oracle.OracleConnectorConfig;
import io.debezium.jdbc.JdbcConfiguration;
import io.debezium.relational.TableId;

import java.io.IOException;
import java.sql.DatabaseMetaData;
import java.sql.JDBCType;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.Set;

/**
 * Oracle table registry.
 */
public class OracleTableRegistry implements TableRegistry {
  private final String pdbName;
  private final JdbcConfiguration jdbcConfig;
  private final DriverCleanup driverCleanup;

  public OracleTableRegistry(OracleConfig conf, DriverCleanup driverCleanup) {
    this.driverCleanup = driverCleanup;
    this.jdbcConfig = buildJdbcConfig(conf);
    this.pdbName = conf.getPdbName();
  }

  @Override
  public TableList listTables() throws IOException {
    try (OracleConnection oracleConnection = new OracleConnection(jdbcConfig, new OracleDBConnectionFactory())) {
      List<TableSummary> tables = new ArrayList<>();
      OracleConnectorConfig connectorConfig = new OracleConnectorConfig(jdbcConfig);

      // Set session to pdb
      oracleConnection.setSessionToPdb(pdbName);
      Set<TableId> tableIds = oracleConnection.readTableNames(pdbName,
                                                              null,
                                                              null,
                                                              new String[]{"TABLE"});
      for (TableId tableId : tableIds) {
        // Filter out SYS tables and keep customer tables only
        if (connectorConfig.getTableFilters().dataCollectionFilter().isIncluded(tableId)) {
          Optional<TableDetail> tableDetail = getTableDetail(oracleConnection, jdbcConfig.getDatabase(),
                                                             tableId.catalog(), tableId.table());
          if (!tableDetail.isPresent()) {
            // shouldn't happen
            continue;
          }
          tables.add(new TableSummary(jdbcConfig.getDatabase(), tableId.table(), tableDetail.get().getNumColumns(),
                                      tableDetail.get().getSchema()));
        }
      }

      return new TableList(tables);
    } catch (SQLException e) {
      throw new IOException(e.getMessage(), e);
    }
  }

  @Override
  public TableDetail describeTable(String database, String table) throws TableNotFoundException, IOException {
    try (OracleConnection oracleConnection = new OracleConnection(jdbcConfig, new OracleDBConnectionFactory())) {
      // Set session to pdb
      oracleConnection.setSessionToPdb(pdbName);

      return getTableDetail(oracleConnection, database, pdbName, table)
        .orElseThrow(() -> new TableNotFoundException(database, table, ""));
    } catch (SQLException e) {
      throw new IOException(e.getMessage(), e);
    }
  }

  @Override
  public StandardizedTableDetail standardize(TableDetail tableDetail) {
    List<Schema.Field> columnSchemas = new ArrayList<>();
    for (ColumnDetail detail : tableDetail.getColumns()) {
      columnSchemas.add(Records.getSchemaField(detail));
    }
    Schema schema = Schema.recordOf("outputSchema", columnSchemas);
    return new StandardizedTableDetail(tableDetail.getDatabase(), tableDetail.getTable(),
                                       tableDetail.getPrimaryKey(), schema);
  }

  @Override
  public void close() throws IOException {
    driverCleanup.close();
  }

  // build a jdbc specified config from oracle config.
  private JdbcConfiguration buildJdbcConfig(OracleConfig conf) {
    return JdbcConfiguration.create()
      .with("hostname", conf.getHost())
      .with("port", conf.getPort())
      .with("user", conf.getUser())
      .with("password", conf.getPassword())
      .with("dbname", conf.getDbName())
      .build();
  }

  private Optional<TableDetail> getTableDetail(OracleConnection oracleConnection, String db,
                                               String catalog, String table) throws SQLException {
    // oracleConnection.connection() will re-use the same jdbc conn wrapped by it
    DatabaseMetaData dbMeta = oracleConnection.connection().getMetaData();
    List<ColumnDetail> columns = new ArrayList<>();
    String schema = null;
    try (ResultSet columnResults = dbMeta.getColumns(catalog, null, table, null)) {
      while (columnResults.next()) {
        schema = columnResults.getString("TABLE_SCHEM");
        columns.add(new ColumnDetail(columnResults.getString("COLUMN_NAME"),
                                     JDBCType.valueOf(columnResults.getInt("DATA_TYPE")),
                                     columnResults.getBoolean("NULLABLE")));
      }
    }
    if (columns.isEmpty()) {
      return Optional.empty();
    }
    List<String> primaryKey = new ArrayList<>();
    try (ResultSet keyResults = dbMeta.getPrimaryKeys(catalog, null, table)) {
      while (keyResults.next()) {
        primaryKey.add(keyResults.getString("COLUMN_NAME"));
      }
    }
    return Optional.of(new TableDetail(db, table, schema, primaryKey, columns));
  }
}

