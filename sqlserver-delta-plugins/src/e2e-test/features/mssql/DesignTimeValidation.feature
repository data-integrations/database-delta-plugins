#
# Copyright © 2023 Cask Data, Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License"); you may not
# use this file except in compliance with the License. You may obtain a copy of
# the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
# WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
# License for the specific language governing permissions and limitations under
# the License.
#

@Mssql
Feature: Mssql - Verify Mssql source plugin design time validation scenarios

  @Required
  Scenario: To verify validation message when user provides invalid Host
    Given Open DataFusion Project with replication to configure replication job
    When Enter input plugin property: "name" with pipelineName
    And Click on the "Next" button in replication to navigate
    And Select Source plugin: "Microsoft SQLServer" from the replication plugins list
    Then Replace input plugin property: "host" with value: "mssqlInvalidHost" for Credentials and Authorization related fields
    Then Replace input plugin property: "port" with value: "mssqlPort" for Credentials and Authorization related fields
    Then Select dropdown plugin property: "select-jdbcPluginName" with option value: "mssqlDriverName"
    Then Replace input plugin property: "database" with value: "mssqlDatabaseName"
    Then Replace input plugin property: "user" with value: "mssqlUsername" for Credentials and Authorization related fields
    Then Replace input plugin property: "password" with value: "mssqlPassword" for Credentials and Authorization related fields
    And Click on the "Next" button in replication to navigate
    Then Replace input plugin property: "project" with value: "projectId"
    Then Enter input plugin property: "datasetName" with value: "dataset"
    And Click on the "Next" button in replication to navigate
    Then Verify that the Plugin is displaying an error message: "errorMessageInvalidMssqlHost"

  @Required
  Scenario: To verify validation message when user provides invalid Port
    Given Open DataFusion Project with replication to configure replication job
    When Enter input plugin property: "name" with pipelineName
    And Click on the "Next" button in replication to navigate
    And Select Source plugin: "Microsoft SQLServer" from the replication plugins list
    Then Replace input plugin property: "host" with value: "mssqlHost" for Credentials and Authorization related fields
    Then Replace input plugin property: "port" with value: "mssqlInvalidPort" for Credentials and Authorization related fields
    Then Select dropdown plugin property: "select-jdbcPluginName" with option value: "mssqlDriverName"
    Then Replace input plugin property: "database" with value: "mssqlDatabaseName"
    Then Replace input plugin property: "user" with value: "mssqlUsername" for Credentials and Authorization related fields
    Then Replace input plugin property: "password" with value: "mssqlPassword" for Credentials and Authorization related fields
    And Click on the "Next" button in replication to navigate
    Then Replace input plugin property: "project" with value: "projectId"
    Then Enter input plugin property: "datasetName" with value: "dataset"
    And Click on the "Next" button in replication to navigate
    Then Verify that the Plugin is displaying an error message: "errorMessageInvalidMssqlPort"

  Scenario: To verify validation message when user provides invalid Database Name
    Given Open DataFusion Project with replication to configure replication job
    When Enter input plugin property: "name" with pipelineName
    And Click on the "Next" button in replication to navigate
    And Select Source plugin: "Microsoft SQLServer" from the replication plugins list
    Then Replace input plugin property: "host" with value: "mssqlHost" for Credentials and Authorization related fields
    Then Replace input plugin property: "port" with value: "mssqlPort" for Credentials and Authorization related fields
    Then Select dropdown plugin property: "select-jdbcPluginName" with option value: "mssqlDriverName"
    Then Replace input plugin property: "database" with value: "mssqlInvalidDatabaseName"
    Then Replace input plugin property: "user" with value: "mssqlUsername" for Credentials and Authorization related fields
    Then Replace input plugin property: "password" with value: "mssqlPassword" for Credentials and Authorization related fields
    And Click on the "Next" button in replication to navigate
    Then Replace input plugin property: "project" with value: "projectId"
    Then Enter input plugin property: "datasetName" with value: "dataset"
    And Click on the "Next" button in replication to navigate
    Then Verify that the Plugin is displaying an error message: "errorMessageInvalidMssqlDatabase"

  @Required
  Scenario: To verify validation message when user provides invalid user
    Given Open DataFusion Project with replication to configure replication job
    When Enter input plugin property: "name" with pipelineName
    And Click on the "Next" button in replication to navigate
    And Select Source plugin: "Microsoft SQLServer" from the replication plugins list
    Then Replace input plugin property: "host" with value: "mssqlHost" for Credentials and Authorization related fields
    Then Replace input plugin property: "port" with value: "mssqlPort" for Credentials and Authorization related fields
    Then Select dropdown plugin property: "select-jdbcPluginName" with option value: "mssqlDriverName"
    Then Replace input plugin property: "database" with value: "mssqlDatabaseName"
    Then Replace input plugin property: "user" with value: "mssqlInvalidUser" for Credentials and Authorization related fields
    Then Replace input plugin property: "password" with value: "mssqlPassword" for Credentials and Authorization related fields
    And Click on the "Next" button in replication to navigate
    Then Replace input plugin property: "project" with value: "projectId"
    Then Enter input plugin property: "datasetName" with value: "dataset"
    And Click on the "Next" button in replication to navigate
    Then Verify that the Plugin is displaying an error message: "errorMessageInvalidMssqlUser"

  Scenario: To verify validation message when user provides invalid password
    Given Open DataFusion Project with replication to configure replication job
    When Enter input plugin property: "name" with pipelineName
    And Click on the "Next" button in replication to navigate
    And Select Source plugin: "Microsoft SQLServer" from the replication plugins list
    Then Replace input plugin property: "host" with value: "mssqlHost" for Credentials and Authorization related fields
    Then Replace input plugin property: "port" with value: "mssqlPort" for Credentials and Authorization related fields
    Then Select dropdown plugin property: "select-jdbcPluginName" with option value: "mssqlDriverName"
    Then Replace input plugin property: "database" with value: "mssqlDatabaseName"
    Then Replace input plugin property: "user" with value: "mssqlInvalidUser" for Credentials and Authorization related fields
    Then Replace input plugin property: "password" with value: "mssqlInvalidPassword" for Credentials and Authorization related fields
    And Click on the "Next" button in replication to navigate
    Then Replace input plugin property: "project" with value: "projectId"
    Then Enter input plugin property: "datasetName" with value: "dataset"
    And Click on the "Next" button in replication to navigate
    Then Verify that the Plugin is displaying an error message: "errorMessageInvalidMssqlPassword"

  Scenario: To verify validation message when macro enabled for password field
    Given Open DataFusion Project with replication to configure replication job
    When Enter input plugin property: "name" with pipelineName
    And Click on the "Next" button in replication to navigate
    And Select Source plugin: "Microsoft SQLServer" from the replication plugins list
    Then Replace input plugin property: "host" with value: "mssqlHost" for Credentials and Authorization related fields
    Then Replace input plugin property: "port" with value: "mssqlPort" for Credentials and Authorization related fields
    Then Select dropdown plugin property: "select-jdbcPluginName" with option value: "mssqlDriverName"
    Then Replace input plugin property: "database" with value: "mssqlDatabaseName"
    Then Replace input plugin property: "user" with value: "mssqlUsername" for Credentials and Authorization related fields
    Then Click on the Macro button of Property: "password" and set the value to: "mssqlPassword"
    And Click on the "Next" button in replication to navigate
    Then Replace input plugin property: "project" with value: "projectId"
    Then Enter input plugin property: "datasetName" with value: "dataset"
    And Click on the "Next" button in replication to navigate
    Then Verify that the Plugin is displaying an error message: "errorMessageMacroPassword"

  Scenario: To verify validation message when macro enabled for service account key field
    Given Open DataFusion Project with replication to configure replication job
    When Enter input plugin property: "name" with pipelineName
    And Click on the "Next" button in replication to navigate
    And Select Source plugin: "Microsoft SQLServer" from the replication plugins list
    Then Replace input plugin property: "host" with value: "mssqlHost" for Credentials and Authorization related fields
    Then Replace input plugin property: "port" with value: "mssqlPort" for Credentials and Authorization related fields
    Then Select dropdown plugin property: "select-jdbcPluginName" with option value: "mssqlDriverName"
    Then Replace input plugin property: "database" with value: "mssqlDatabaseName"
    Then Replace input plugin property: "user" with value: "mssqlUsername" for Credentials and Authorization related fields
    Then Replace input plugin property: "password" with value: "mssqlPassword" for Credentials and Authorization related fields
    And Click on the "Next" button in replication to navigate
    Then Replace input plugin property: "project" with value: "projectId"
    Then Click on the Macro button of Property: "serviceAccountKey" and set the value to: "ServiceAccountKey"
    Then Enter input plugin property: "datasetName" with value: "dataset"
    And Click on the "Next" button in replication to navigate
    Then Verify that the Plugin is displaying an error message: "errorMessageMacroServiceAccountKey"
