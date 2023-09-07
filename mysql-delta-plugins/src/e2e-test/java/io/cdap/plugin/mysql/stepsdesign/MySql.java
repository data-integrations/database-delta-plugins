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

package io.cdap.plugin.mysql.stepsdesign;

import io.cdap.e2e.utils.BigQueryClient;
import io.cdap.e2e.utils.CdfHelper;
import io.cdap.e2e.utils.PluginPropertyUtils;
import io.cdap.plugin.BQValidation;
import io.cdap.plugin.mysql.actions.MysqlActions;
import io.cucumber.java.en.And;
import io.cucumber.java.en.Then;
import org.junit.Assert;
import stepsdesign.BeforeActions;

import java.io.IOException;
import java.sql.SQLException;
import java.text.ParseException;

/**
 * Contains MySQL replication test scenarios step definitions.
 */
public class MySql implements CdfHelper {

  @Then("Wait till pipeline is in running state and check if no errors occurred")
  public void waitTillPipelineIsInRunningState() throws InterruptedException {
    MysqlActions.waitTillPipelineIsRunningAndCheckForErrors();
  }

  @And("Wait till CDC events are reflected in BQ")
  public void waitForReplicationToFlushEvents() throws InterruptedException {
    MysqlActions.waitForReplication();
  }

  @Then("Select the source table if available")
  public void selectTable() {
    MysqlActions.selectTable();
  }

  @And("Run insert, update and delete CDC events on source table")
  public void executeCdcEvents() throws SQLException, ClassNotFoundException {
    MysqlActions.executeCdcEventsOnSourceTable();
  }

  @Then("Validate the values of records transferred to target Big Query table is equal to the values from source table")
  public void validateTheValuesOfRecordsTransferredToTargetBigQueryTableIsEqualToTheValuesFromSourceTable()
          throws InterruptedException, IOException, SQLException, ClassNotFoundException, ParseException {
    int targetBQRecordsCount = BigQueryClient.countBqQuery(PluginPropertyUtils.pluginProp("sourceMySqlTable"));
    BeforeActions.scenario.write("No of Records Transferred to BigQuery:" + targetBQRecordsCount);
    boolean recordsMatched = BQValidation.validateBQAndDBRecordValues(
            PluginPropertyUtils.pluginProp("sourceMySqlTable"),
            PluginPropertyUtils.pluginProp("sourceMySqlTable"));
    Assert.assertTrue("Value of records transferred to the target table should be equal to the value " +
            "of the records in the source table", recordsMatched);
  }
}
