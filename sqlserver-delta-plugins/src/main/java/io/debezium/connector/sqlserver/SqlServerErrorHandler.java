/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.sqlserver;

import io.debezium.connector.base.ChangeEventQueue;
import io.debezium.pipeline.ErrorHandler;

/**
 * Error handler for SQL Server.
 *
 * @author Chris Cranford
 */
public class SqlServerErrorHandler extends ErrorHandler {
  // This class is copied from debezium. driverClassLoader variable is added to the
  // original class. This class loader is the jdbc plugin class loader and is required
  // to load the SQLServerException class from the user uploaded jdbc jar rather than looking
  // into the debezium connector jar.
  public static ClassLoader driverClassLoader;
  public SqlServerErrorHandler(SqlServerConnectorConfig connectorConfig, ChangeEventQueue<?> queue) {
    super(SqlServerConnector.class, connectorConfig, queue);
  }

  @Override
  protected boolean isRetriable(Throwable throwable) {
    Class<?> sqlServerExceptionClass = null;
    try {
      sqlServerExceptionClass = driverClassLoader.loadClass("com.microsoft.sqlserver.jdbc.SQLServerException");
    } catch (ClassNotFoundException e) {
      throw new RuntimeException("Failed to load the SQLServerException class.", e);
    }


    if (!(sqlServerExceptionClass.isInstance(throwable)) && sqlServerExceptionClass.isInstance(throwable.getCause())) {
      throwable = throwable.getCause();
    }

    return sqlServerExceptionClass.isInstance(throwable)
      && (throwable.getMessage().contains("Connection timed out (Read failed)")
      || throwable.getMessage().contains("Connection timed out (Write failed)")
      || throwable.getMessage().contains("The connection has been closed.")
      || throwable.getMessage().contains("The connection is closed.")
      || throwable.getMessage().contains("The login failed.")
      || throwable.getMessage().contains("Server is in script upgrade mode.")
      || throwable.getMessage().contains("Try the statement later.")
      || throwable.getMessage().contains("Connection reset")
      || throwable.getMessage().contains("Socket closed")
      || throwable.getMessage().contains("SHUTDOWN is in progress")
      || throwable.getMessage().contains("The server failed to resume the transaction")
      || throwable.getMessage().contains("Verify the connection properties")
      || throwable.getMessage().contains("Broken pipe (Write failed)")
      || throwable.getMessage()
      .startsWith("An insufficient number of arguments were supplied for the procedure or " +
                    "function cdc.fn_cdc_get_all_changes_")
      || throwable.getMessage()
      .endsWith("was deadlocked on lock resources with another process and has been chosen as the deadlock victim. " +
                  "Rerun the transaction."));
  }
}
