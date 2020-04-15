/*
 * Copyright © 2019 Cask Data, Inc.
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

import io.cdap.cdap.api.annotation.Description;
import io.cdap.cdap.api.annotation.Macro;
import io.cdap.cdap.api.plugin.PluginConfig;

import java.util.Properties;
import javax.annotation.Nullable;

/**
 * Plugin configuration for the MySQL origin.
 */
public class MySqlConfig extends PluginConfig {
  @Description("Hostname of the MySQL server to read from.")
  private String host;

  @Description("Port to use to connect to the MySQL server.")
  private int port;

  @Description("Username to use to connect to the MySQL server.")
  private String user;

  @Macro
  @Description("Password to use to connect to the MySQL server.")
  private String password;

  @Description("A unique numeric ID to identify this origin as an event consumer.")
  private int consumerID;

  @Description("Database to consume events for.")
  private String database;

  @Nullable
  @Description("Timezone of the MySQL server. This is used when converting dates into timestamps.")
  private String serverTimezone;

  @Description("Name of the jdbc plugin to use.")
  private String jdbcPluginName;

  public MySqlConfig(String host, int port, String user, String password, int consumerID,
                     String database, @Nullable String serverTimezone) {
    this.host = host;
    this.port = port;
    this.user = user;
    this.password = password;
    this.consumerID = consumerID;
    this.database = database;
    this.serverTimezone = serverTimezone;
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

  public int getConsumerID() {
    return consumerID;
  }

  public String getDatabase() {
    return database;
  }

  public String getServerTimezone() {
    return serverTimezone == null || serverTimezone.isEmpty() ? "UTC" : serverTimezone;
  }

  public String getJdbcPluginName() {
    return jdbcPluginName;
  }

  public String getJDBCPluginId() {
    return String.format("%s.%s.%s", "mysqlsource", "jbdc", jdbcPluginName);
  }

  public String getJdbcURL() {
    return String.format("jdbc:mysql://%s:%d/%s", host, port, database);
  }

  public Properties getConnectionProperties() {
    Properties properties = new Properties();
    properties.put("user", user);
    properties.put("password", password);
    properties.put("serverTimezone", getServerTimezone());
    return properties;
  }
}
