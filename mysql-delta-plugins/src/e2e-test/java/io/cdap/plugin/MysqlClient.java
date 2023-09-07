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
import java.util.concurrent.TimeUnit;

/**
 *  MySQL client.
 */
public class MysqlClient {

     private static final String database = PluginPropertyUtils.pluginProp("mysqlDatabaseName");
        static Connection getMysqlConnection() throws SQLException, ClassNotFoundException {
            Class.forName("com.mysql.cj.jdbc.Driver");
            return DriverManager.getConnection("jdbc:mysql://" + System.getenv("MYSQL_HOST") + ":" +
                                                 System.getenv("MYSQL_PORT") + "/" + database +
                       "?tinyInt1isBit=false", System.getenv("MYSQL_USERNAME"), System.getenv("MYSQL_PASSWORD"));
    }

    public static void createTable(String sourceTable, String datatypeColumns)
      throws SQLException, ClassNotFoundException {
        try (Connection connect = getMysqlConnection();
             Statement statement = connect.createStatement()) {
            String createSourceTableQuery = "CREATE TABLE " + sourceTable + " " + datatypeColumns;
            statement.executeUpdate(createSourceTableQuery);
            // Insert row1 data.
            String datatypesValues = PluginPropertyUtils.pluginProp("mysqlDatatypeValuesRow1");
            String datatypesColumnsList = PluginPropertyUtils.pluginProp("mysqlDatatypesColumnsList");
            statement.executeUpdate("INSERT INTO " + sourceTable + " " + datatypesColumnsList + " " +
                                      datatypesValues);
            // Insert row2 data.
            String datatypesValues2 = PluginPropertyUtils.pluginProp("mysqlDatatypeValuesRow2");
            String datatypesColumnsList2 = PluginPropertyUtils.pluginProp("mysqlDatatypesColumnsList");
            statement.executeUpdate("INSERT INTO " + sourceTable + " " + datatypesColumnsList2 + " " +
                                      datatypesValues2);
        }
    }

    public static void insertRow (String table, String datatypeValues) throws
            SQLException, ClassNotFoundException {
        try (Connection connect = getMysqlConnection(); Statement statement = connect.createStatement()) {
            // Insert dummy data.
            statement.executeUpdate("INSERT INTO " + table + " " + " VALUES " + datatypeValues);
        }
    }

    public static void deleteRow(String table, String deleteCondition) throws SQLException,
            ClassNotFoundException {
        try (Connection connect = getMysqlConnection(); Statement statement = connect.createStatement()) {
            // Delete dummy data.
            statement.executeUpdate("DELETE FROM " + table + " WHERE " + deleteCondition);
        }
    }

    public static void updateRow(String table, String updateCondition, String updatedValue) throws
            SQLException, ClassNotFoundException {
        try (Connection connect = getMysqlConnection(); Statement statement = connect.createStatement()) {
            // Update dummy data.
            statement.executeUpdate("UPDATE " + table + " SET " + updatedValue +
                    " WHERE " + updateCondition);
        }
    }

    public static void deleteTable(String table) throws SQLException, ClassNotFoundException {
        try (Connection connect = getMysqlConnection();
             Statement statement = connect.createStatement()) {
            {
                String dropTableQuery = "DROP TABLE " + table;
                statement.executeUpdate(dropTableQuery);
            }
        }
    }

    public static void waitForFlush() throws InterruptedException {
        int flushInterval = Integer.parseInt(PluginPropertyUtils.pluginProp("loadInterval"));
        TimeUnit time = TimeUnit.SECONDS;
        time.sleep(2 * flushInterval + 60);
    }
}
