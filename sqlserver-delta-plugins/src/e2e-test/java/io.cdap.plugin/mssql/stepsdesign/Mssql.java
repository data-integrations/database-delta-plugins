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

package io.cdap.plugin.mssql.stepsdesign;

import io.cdap.e2e.utils.BigQueryClient;
import io.cdap.e2e.utils.CdfHelper;
import io.cdap.e2e.utils.PluginPropertyUtils;
import io.cdap.plugin.BQValidation;
import io.cdap.plugin.mssql.actions.MssqlActions;
import io.cucumber.java.en.And;
import io.cucumber.java.en.Then;
import org.junit.Assert;
import stepsdesign.BeforeActions;

import java.io.IOException;
import java.sql.SQLException;
import java.text.ParseException;

/**
 * Contains Mssql replication test scenarios step definitions.
 */
public class Mssql implements CdfHelper {

  @Then("Select the source table if available")
  public void selectTable() {
    MssqlActions.selectTable();
  }

  @And("Run insert, update and delete CDC events on source table")
  public void executeCdcEvents() throws SQLException, ClassNotFoundException {
    MssqlActions.executeCdcEventsOnSourceTable();
  }

  @Then("Wait till pipeline is in running state and check if no errors occurred")
  public void waitTillPipelineIsInRunningState() throws InterruptedException {
    MssqlActions.waitTillPipelineIsRunningAndCheckForErrors();
  }

  @And("Wait till CDC events are reflected in BQ")
  public void waitForReplicationToFlushEvents() throws InterruptedException {
    MssqlActions.waitForReplication();
  }

  @Then("Validate the values of records transferred to target BigQuery table is equal to the values from MsSQL " +
          "source Table")
  public void validateTheValuesOfRecordsTransferredToTargetBigQuery()
          throws InterruptedException, IOException, SQLException, ClassNotFoundException, ParseException {
    int targetBQRecordsCount = BigQueryClient.countBqQuery(PluginPropertyUtils.pluginProp("mssqlSourceTable"));
    BeforeActions.scenario.write("No of Records Transferred to BigQuery:" + targetBQRecordsCount);
    String bqTargetTableName = PluginPropertyUtils.pluginProp("mssqlSourceTable");
    boolean recordsMatched = BQValidation.validateDBToBQRecordValues(
      PluginPropertyUtils.pluginProp("mssqlSchema"),
      PluginPropertyUtils.pluginProp("mssqlSourceTable"), bqTargetTableName);
    Assert.assertTrue("Value of records transferred to the target table should be equal to the value " +
            "of the records in the source table", recordsMatched);
  }
}
