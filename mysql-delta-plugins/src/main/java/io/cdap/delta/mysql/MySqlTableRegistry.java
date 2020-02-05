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

import com.mysql.cj.MysqlType;
import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.delta.api.assessment.ColumnDetail;
import io.cdap.delta.api.assessment.StandardizedTableDetail;
import io.cdap.delta.api.assessment.TableDetail;
import io.cdap.delta.api.assessment.TableList;
import io.cdap.delta.api.assessment.TableNotFoundException;
import io.cdap.delta.api.assessment.TableRegistry;
import io.cdap.delta.api.assessment.TableSummary;
import io.cdap.delta.common.DriverCleanup;

import java.io.IOException;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.Properties;

/**
 * Lists and describes tables.
 */
public class MySqlTableRegistry implements TableRegistry {
  private final MySqlConfig conf;
  private final DriverCleanup driverCleanup;
  private final Properties properties;

  public MySqlTableRegistry(MySqlConfig conf, DriverCleanup driverCleanup) {
    this.conf = conf;
    this.driverCleanup = driverCleanup;
    this.properties = new Properties();
    properties.put("user", conf.getUser());
    properties.put("password", conf.getPassword());;
  }

  @Override
  public TableList listTables() throws IOException {
    List<TableSummary> tables = new ArrayList<>();
    try (Connection connection = DriverManager.getConnection(
      String.format("jdbc:mysql://%s:%d/%s", conf.getHost(), conf.getPort(), conf.getDatabase()), properties)) {
      DatabaseMetaData dbMeta = connection.getMetaData();
      try (ResultSet tableResults = dbMeta.getTables(null, null, null, null)) {
        while (tableResults.next()) {
          String tableName = tableResults.getString(3);
          Optional<TableDetail> tableDetail = getTableDetail(dbMeta, conf.getDatabase(), tableName);
          if (!tableDetail.isPresent()) {
            // shouldn't happen
            continue;
          }
          tables.add(new TableSummary(conf.getDatabase(), tableName, tableDetail.get().getNumColumns()));
        }
      }
      return new TableList(tables);
    } catch (SQLException e) {
      throw new IOException(e.getMessage(), e);
    }
  }

  @Override
  public TableDetail describeTable(String db, String table) throws TableNotFoundException, IOException {
    try (Connection connection = DriverManager.getConnection(
      String.format("jdbc:mysql://%s:%d/%s", conf.getHost(), conf.getPort(), db), properties)) {
      DatabaseMetaData dbMeta = connection.getMetaData();
      return getTableDetail(dbMeta, db, table).orElseThrow(() -> new TableNotFoundException(db, table, ""));
    } catch (SQLException e) {
      throw new IOException(e.getMessage(), e);
    }
  }

  @Override
  public StandardizedTableDetail standardize(TableDetail tableDetail) {
    Schema schema = Schema.recordOf("xyz", Schema.Field.of("x", Schema.of(Schema.Type.INT)));
    return new StandardizedTableDetail(tableDetail.getDatabase(), tableDetail.getTable(),
                                       tableDetail.getPrimaryKey(), schema);
  }

  @Override
  public void close() throws IOException {
    driverCleanup.close();
  }

  private Optional<TableDetail> getTableDetail(DatabaseMetaData dbMeta, String db, String table) throws SQLException {
    List<ColumnDetail> columns = new ArrayList<>();
    try (ResultSet columnResults = dbMeta.getColumns(db, null, table, null)) {
      while (columnResults.next()) {
        columns.add(new ColumnDetail(columnResults.getString("COLUMN_NAME"),
                                     MysqlType.getByJdbcType(columnResults.getInt("DATA_TYPE")),
                                     columnResults.getBoolean("NULLABLE")));
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
    return Optional.of(new TableDetail(db, table, null, primaryKey, columns));
  }
}
