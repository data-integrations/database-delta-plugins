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
  public SqlServerErrorHandler(String logicalName, ChangeEventQueue<?> queue) {
    super(SqlServerConnector.class, logicalName, queue);
  }

  @Override
  protected boolean isRetriable(Throwable throwable) {
    try {
      Class<?> sqlServerExceptionClass = driverClassLoader.loadClass("com.microsoft.sqlserver.jdbc.SQLServerException");
      return sqlServerExceptionClass.isInstance(throwable)
        && (throwable.getMessage().contains("Connection timed out (Read failed)")
        || throwable.getMessage().contains("The connection has been closed.")
        || throwable.getMessage().contains("Connection reset")
        || throwable.getMessage().contains("SHUTDOWN is in progress"));

    } catch (ClassNotFoundException e) {
      throw new RuntimeException("Failed to load the SQLServerException class.", e);
    }
  }
}
