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

package io.cdap.delta.sqlserver;

import io.cdap.cdap.api.annotation.Description;
import io.cdap.cdap.api.annotation.Macro;
import io.cdap.cdap.api.plugin.PluginConfig;

import javax.annotation.Nullable;

/**
 * Plugin configuration for the SqlServer source.
 */
public class SqlServerConfig extends PluginConfig {

  @Description("Hostname or IP address of the SqlServer to read from.")
  private String host;

  @Description("Port to use to connect to the SqlServer.")
  private int port;

  @Description("Username to use to connect to the SqlServer.")
  private String user;

  @Macro
  @Description("Password to use to connect to the SqlServer.")
  private String password;

  @Description("Database to consume events for.")
  private String database;

  @Nullable
  @Description("Timezone of the SqlServer. This is used when converting dates into timestamps.")
  private String serverTimezone;

  @Description("Name of the jdbc plugin to use.")
  private String jdbcPluginName;

  public SqlServerConfig(String host, int port, String user, String password,
                         String database, @Nullable String serverTimezone, String jdbcPluginName) {
    this.host = host;
    this.port = port;
    this.user = user;
    this.password = password;
    this.database = database;
    this.serverTimezone = serverTimezone;
    this.jdbcPluginName = jdbcPluginName;
  }

  public String getDatabase() {
    return database;
  }

  public String getHost() {
    return host;
  }

  public int getPort() {
    return port;
  }

  public String getUser() {
    return user;
  }

  public String getPassword() {
    return password;
  }

  public String getJdbcPluginName() {
    return jdbcPluginName;
  }

  public String getServerTimezone() {
    return serverTimezone == null || serverTimezone.isEmpty() ? "UTC" : serverTimezone;
  }

  public String getJDBCPluginId() {
    return String.format("%s.%s.%s", "sqlserversource", "jbdc", jdbcPluginName);
  }
}
