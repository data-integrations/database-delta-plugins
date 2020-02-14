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

package io.cdap.delta.oracle;

import io.cdap.cdap.api.annotation.Description;
import io.cdap.cdap.api.plugin.PluginConfig;

import javax.annotation.Nullable;

/**
 * Plugin configuration for the Oracle.
 */
public class OracleConfig extends PluginConfig {
  @Description("Hostname of the Oracle server to read from.")
  private String host;

  @Description("Port to use to connect to the Oracle server.")
  private int port;

  @Description("Username to use to connect to the Oracle server.")
  private String user;

  @Description("Password to use to connect to the Oracle server.")
  private String password;

  @Description("DB name of the Oracle server.")
  private String dbName;

  @Description("PDB name of the Oracle server.")
  private String pdbName;

  @Description("XStream out server name of the Oracle server.")
  private String outServerName;

  @Description("Name of the JDBC plugin to use.")
  private String jdbcPluginName;

  // TODO: implement the proper way load oracle instant client libraries from remote path
  @Description("Path to load oracle instant client libraries from local.")
  private String libPath;

  @Nullable
  @Description("Connection string to use when connecting to the database through JDBC. "
    + "This is required if a JDBC plugin was specified.")
  private String connectionString;

  public OracleConfig(String host, int port, String user, String password,
                      String dbName, String pdbName, String outServerName,
                      String jdbcPluginName, String libPath,
                      @Nullable String connectionString) {
    this.host = host;
    this.port = port;
    this.user = user;
    this.password = password;
    this.dbName = dbName;
    this.pdbName = pdbName;
    this.outServerName = outServerName;
    this.jdbcPluginName = jdbcPluginName;
    this.libPath = libPath;
    this.connectionString = connectionString;
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

  public String getDbName() {
    return dbName;
  }

  public String getPdbName() {
    return pdbName;
  }

  public String getOutServerName() {
    return outServerName;
  }

  public String getJdbcPluginName() {
    return jdbcPluginName;
  }

  public String getLibPath() {
    return libPath;
  }

  public String getConnectionString() {
    return connectionString == null ? "jdbc:oracle:oci:@" + host + ":" + port + "/" + dbName : connectionString;
  }
}
