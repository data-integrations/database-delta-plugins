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

import io.debezium.jdbc.JdbcConfiguration;
import io.debezium.jdbc.JdbcConnection;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

/**
 * This oracle db connection factory is used for table listing and assessment process.
 * In order to get rid of the jdbc native library loading process, we will use 'thin' driver to build the connection.
 */
public class OracleDBConnectionFactory implements JdbcConnection.ConnectionFactory {
  @Override
  public Connection connect(JdbcConfiguration config) throws SQLException {
    String hostName = config.getHostname();
    int port = config.getPort();
    String database = config.getDatabase();
    String user = config.getUser();
    String password = config.getPassword();

    return DriverManager.getConnection(
      "jdbc:oracle:thin:@" + hostName + ":" + port + "/" + database, user, password);
  }
}
