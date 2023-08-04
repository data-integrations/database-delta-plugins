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

package io.cdap.plugin.hooks;

import com.google.cloud.bigquery.BigQueryException;
import io.cdap.e2e.utils.BigQueryClient;
import io.cdap.e2e.utils.PluginPropertyUtils;
import io.cdap.plugin.MssqlClient;
import io.cucumber.java.After;
import io.cucumber.java.Before;
import org.apache.commons.lang3.RandomStringUtils;
import org.junit.Assert;
import stepsdesign.BeforeActions;

import java.io.IOException;
import java.sql.SQLException;

/**
 * MsSQL test hooks.
 */
public class TestSetUpHooks {

  public static String schemaName = PluginPropertyUtils.pluginProp("mssqlSchema");
  public static String datatypeColumns = PluginPropertyUtils.pluginProp("mssqlDatatypeColumns");

  @Before(order = 1)
  public static void setTableName() {
    String randomString = RandomStringUtils.randomAlphabetic(10);
    String sourceTableName = String.format("SourceTable_%s", randomString);
    PluginPropertyUtils.addPluginProp("mssqlSourceTable", sourceTableName);
  }

  @Before(order = 2, value = "@MSSQL_SOURCE")
  public static void createTable() throws SQLException, ClassNotFoundException {
    MssqlClient.createTable(PluginPropertyUtils.pluginProp("mssqlSourceTable"), schemaName, datatypeColumns);
    BeforeActions.scenario.write("MSSQL Source Table - " +
            PluginPropertyUtils.pluginProp("mssqlSourceTable") + " created successfully");
  }

  @After(order = 2, value = "@MSSQL_DELETE")
  public static void dropTable() throws SQLException, ClassNotFoundException {
    MssqlClient.deleteTable(schemaName, PluginPropertyUtils.pluginProp("mssqlSourceTable"));
    BeforeActions.scenario.write("MSSQL Source Table - " +
            PluginPropertyUtils.pluginProp("mssqlSourceTable") + " deleted successfully");
  }

  @After(order = 1, value = "@BQ_SINK_TEST")
  public static void deleteTempTargetBQTable() throws IOException, InterruptedException {
    String bqTargetTableName = PluginPropertyUtils.pluginProp("mssqlSourceTable");
    try {
      BigQueryClient.dropBqQuery(bqTargetTableName);
      BeforeActions.scenario.write("BQ Target table - " + bqTargetTableName + " deleted successfully");
      PluginPropertyUtils.removePluginProp("mssqlSourceTable");
    } catch (BigQueryException e) {
      if (e.getMessage().contains("Not found: Table")) {
        BeforeActions.scenario.write("BQ Target Table " + bqTargetTableName + " does not exist");
      } else {
        Assert.fail(e.getMessage());
      }
    }
  }
}
