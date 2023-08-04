/*
 * Copyright (c) 2023.
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

package io.cdap.plugin;

import io.cdap.e2e.utils.PluginPropertyUtils;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.TimeZone;
import java.util.concurrent.TimeUnit;

/**
 *  Mssql client.
 */
public class MssqlClient {

    static Connection getMssqlConnection() throws SQLException, ClassNotFoundException {
        TimeZone timezone = TimeZone.getTimeZone("UTC");
        TimeZone.setDefault(timezone);
        Class.forName("com.microsoft.sqlserver.jdbc.SQLServerDriver");
        String databaseName = PluginPropertyUtils.pluginProp("mssqlDatabaseName");
        return DriverManager.getConnection("jdbc:sqlserver://" + System.getenv("MSSQL_HOST")
                        + ":" + System.getenv("MSSQL_PORT") + ";databaseName=" + databaseName,
                System.getenv("MSSQL_USERNAME"), System.getenv("MSSQL_PASSWORD"));

    }

    public static void createTable(String table, String schema, String datatypeColumns)
            throws SQLException, ClassNotFoundException {
        try (Connection connect = getMssqlConnection(); Statement statement = connect.createStatement()) {
            String createTableQuery = "CREATE TABLE " + schema + "." + table + datatypeColumns;
            statement.executeUpdate(createTableQuery);
            // Enabling table level CDC
            String enableCDCSql = "EXEC sys.sp_cdc_enable_table @source_schema = " + schema + " , " +
             "@source_name = " + table + ", @capture_instance = NULL, @role_name = 'cdc_role';";
            statement.execute(enableCDCSql);

            // Insert row1 data.
            String datatypesValues = PluginPropertyUtils.pluginProp("mssqlDatatypeValuesRow1");
            String datatypesColumnsList = PluginPropertyUtils.pluginProp("mssqlDatatypesColumnsList");
            statement.executeUpdate("INSERT INTO " + schema + "." + table + " " + datatypesColumnsList + " " +
                    datatypesValues);
            // Insert row2 data.
            String datatypesValues2 = PluginPropertyUtils.pluginProp("mssqlDatatypeValuesRow2");
            String datatypesColumnsList2 = PluginPropertyUtils.pluginProp("mssqlDatatypesColumnsList");
            statement.executeUpdate("INSERT INTO " + schema + "." + table + " " + datatypesColumnsList2 + " " +
                    datatypesValues2);
        }
    }

    public static void insertRow(String table, String schema, String datatypeValues) throws
            SQLException, ClassNotFoundException {
        try (Connection connect = getMssqlConnection(); Statement statement = connect.createStatement()) {
            // Insert dummy data.
            statement.executeUpdate("INSERT INTO " + schema + "." + table + " " +
                    " VALUES " + datatypeValues);

        }
    }
    public static void deleteRow(String table, String schema, String deleteCondition) throws SQLException,
            ClassNotFoundException {
        try (Connection connect = getMssqlConnection(); Statement statement = connect.createStatement()) {
            // Insert dummy data.
            statement.executeUpdate("DELETE FROM  " + schema + "." + table + " WHERE " + deleteCondition);
        }
    }
    public static void updateRow(String table, String schema, String updateCondition, String updatedValue) throws
            SQLException, ClassNotFoundException {
        try (Connection connect = getMssqlConnection(); Statement statement = connect.createStatement()) {
            // Insert dummy data.
            statement.executeUpdate("UPDATE " + schema + "." + table + " SET " + updatedValue +
                    " WHERE " + updateCondition);
        }
    }

    public static void deleteTable(String schema, String table)
            throws SQLException, ClassNotFoundException {
        try (Connection connect = getMssqlConnection(); Statement statement = connect.createStatement()) {
            String dropTableQuery = "DROP TABLE " + schema + "." + table;
            statement.execute(dropTableQuery);
        }
    }

    public static void waitForFlush() throws InterruptedException {
        int flushInterval = Integer.parseInt(PluginPropertyUtils.pluginProp("loadInterval"));
        TimeUnit time = TimeUnit.SECONDS;
        time.sleep(2 * flushInterval + 60);
    }
}
