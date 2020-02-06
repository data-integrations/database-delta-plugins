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
import io.cdap.cdap.api.data.schema.UnsupportedTypeException;
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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.sql.DatabaseMetaData;
import java.sql.JDBCType;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Types;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.Set;

/**
 * Oracle table registry.
 */
public class OracleTableRegistry implements TableRegistry {
  private static final Logger LOG = LoggerFactory.getLogger(OracleTableRegistry.class);
  private final String pdbName;
  private final JdbcConfiguration jdbcConfig;
  private final DriverCleanup driverCleanup;
  private final OracleConnection oracleConnection;

  public OracleTableRegistry(OracleConfig conf, DriverCleanup driverCleanup) {
    this.driverCleanup = driverCleanup;
    this.jdbcConfig = buildJdbcConfig(conf);
    this.pdbName = conf.getPdbName();
    this.oracleConnection = new OracleConnection(jdbcConfig, new OracleDBConnectionFactory());
  }

  @Override
  public TableList listTables() throws IOException {
    try {
      List<TableSummary> tables = new ArrayList<>();
      OracleConnectorConfig connectorConfig = new OracleConnectorConfig(jdbcConfig);

      // STEP 1: set session to pdb
      oracleConnection.setSessionToPdb(pdbName);
      // STEP 2: read all the tables
      Set<TableId> tableIds = oracleConnection.readTableNames(pdbName,
                                                              null,
                                                              null,
                                                              new String[]{"TABLE"});
      // STEP 3: filter out customer tables only and get detail for them
      DatabaseMetaData dbMeta = oracleConnection.connection().getMetaData();
      for (TableId tableId : tableIds) {
        if (connectorConfig.getTableFilters().dataCollectionFilter().isIncluded(tableId)) {
          Optional<TableDetail> tableDetail = getTableDetail(dbMeta, jdbcConfig.getDatabase(),
                                                             tableId.catalog(), tableId.table());
          if (!tableDetail.isPresent()) {
            // shouldn't happen
            continue;
          }
          tables.add(new TableSummary(jdbcConfig.getDatabase(), tableId.table(), tableDetail.get().getNumColumns()));
        }
      }

      return new TableList(tables);
    } catch (SQLException e) {
      throw new IOException(e.getMessage(), e);
    } finally {
      // reset session to cdb
      oracleConnection.resetSessionToCdb();

      try {
        oracleConnection.close();
      } catch (SQLException e) {
        LOG.error("Failed to close the connection");
      }
    }
  }

  @Override
  public TableDetail describeTable(String database, String table) throws TableNotFoundException, IOException {
    try {
      // STEP 1: set session to pdb
      oracleConnection.setSessionToPdb(pdbName);
      DatabaseMetaData dbMeta = oracleConnection.connection().getMetaData();
      // STEP2: get detail for given table
      return getTableDetail(dbMeta, database, pdbName, table)
        .orElseThrow(() -> new TableNotFoundException(database, table, ""));
    } catch (SQLException e) {
      throw new IOException(e.getMessage(), e);
    } finally {
      // reset session to cdb
      oracleConnection.resetSessionToCdb();

      try {
        oracleConnection.close();
      } catch (SQLException e) {
        LOG.error("Failed to close the connection");
      }
    }
  }

  @Override
  public StandardizedTableDetail standardize(TableDetail tableDetail) {
    List<Schema.Field> columnSchemas = new ArrayList<>();
    for (ColumnDetail detail : tableDetail.getColumns()) {
      columnSchemas.add(getSchemaField(detail));
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

  private Optional<TableDetail> getTableDetail(DatabaseMetaData dbMeta, String db,
                                               String catalog, String table) throws SQLException {
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

  // This mapping if followed by doc: https://docs.oracle.com/database/121/JJDBC/datacc.htm#JJDBC28367
  private Schema.Field getSchemaField(ColumnDetail detail) {
    Schema schema;
    int sqlType = detail.getType().getVendorTypeNumber();

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
        schema = Schema.of(Schema.LogicalType.TIME_MICROS);
        break;
      case Types.TIMESTAMP:
        schema = Schema.of(Schema.LogicalType.TIMESTAMP_MICROS);
        break;
      case Types.ARRAY:
        schema = Schema.of(Schema.Type.ARRAY);
        break;
      default:
        throw new RuntimeException(new UnsupportedTypeException("Unsupported SQL Type: " + sqlType));
    }

    return Schema.Field.of(detail.getName(), detail.isNullable() ? Schema.nullableOf(schema) : schema);
  }
}

