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

package io.cdap.plugin.mssql.actions;

import io.cdap.e2e.pages.locators.CdfPluginPropertiesLocators;
import io.cdap.e2e.utils.ElementHelper;
import io.cdap.e2e.utils.PluginPropertyUtils;
import io.cdap.plugin.MssqlClient;

import java.sql.SQLException;
import java.util.concurrent.TimeUnit;

/**
 * Replication Mssql Actions.
 */
public class MssqlActions {

    private static String tableName = PluginPropertyUtils.pluginProp("mssqlSourceTable");
    private static String schemaName = PluginPropertyUtils.pluginProp("mssqlSchema");

    public static void selectTable() {
        String table = schemaName + "." + PluginPropertyUtils.pluginProp("mssqlSourceTable");
        ElementHelper.clickOnElement(CdfPluginPropertiesLocators.selectReplicationTable(table), 180);
    }

    public static void executeCdcEventsOnSourceTable()
            throws SQLException, ClassNotFoundException {
        String datatypeValues = PluginPropertyUtils.pluginProp("mssqlDatatypeForInsertOperation");
        String deleteCondition = PluginPropertyUtils.pluginProp("mssqlDeleteRowCondition");
        String updateCondition = PluginPropertyUtils.pluginProp("mssqlUpdateRowCondition");
        String updatedValue = PluginPropertyUtils.pluginProp("mssqlUpdatedRow");
        MssqlClient.insertRow(tableName, schemaName, datatypeValues);
        MssqlClient.updateRow(tableName, schemaName, updateCondition, updatedValue);
        MssqlClient.deleteRow(tableName, schemaName, deleteCondition);
    }

    public static void waitTillPipelineIsRunningAndCheckForErrors() throws InterruptedException {
        //wait for datastream to startup
        int defaultTimeout = Integer.parseInt(PluginPropertyUtils.pluginProp("pipelineInitialization"));
        TimeUnit.SECONDS.sleep(defaultTimeout);
        MssqlClient.waitForFlush();
    }

    public static void waitForReplication() throws InterruptedException {
        MssqlClient.waitForFlush();
    }
}
