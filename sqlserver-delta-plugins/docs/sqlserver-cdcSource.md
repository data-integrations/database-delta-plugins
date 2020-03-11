# SQL Server CDC Source


Description
-----------
Microsoft SQL Server CDC Source will replicate all of the row-level changes in the databases on Microsoft SQL server.

Setting up SQL Server
-----------
#### Enable CDC On Database
Enable CDC for database 'mydb' will look similar like this:
```
-- ====
-- Enable Database for CDC template
-- ====
USE mydb
GO
EXEC sys.sp_cdc_enable_db
GO
```
Note that CDC cannot be enabled for master database.

#### Enable CDC On Table
Enable CDC for the table you want will look similar like this:
```
-- =========
-- Enable a Table Specifying Filegroup Option Template
-- If ther is no 'role_name', you can set it as NULL.
-- =========
USE MyDB
GO

EXEC sys.sp_cdc_enable_table
@source_schema = N'dbo',
@source_name   = N'MyTable',
@role_name     = N'MyRole',
@supports_net_changes = 1
GO
```

#### Verify Table CDC Accessibility
Run following query to make sure your table has CDC assess.
```
-- =========
-- Verify the user of the connector have access, this query should not have empty result
-- =========

EXEC sys.sp_cdc_help_change_data_capture
GO
```

Setting up JDBC Driver
-----------
In order to set up JDBC driver, just need to upload downloaded SQL Server JDBC driver jar and configure it like this:
```
Name: DriverName
Class name: com.microsoft.sqlserver.jdbc.SQLServerDriver
Version: 8.0
Description: This is a jdbc driver for SQL Server.
```

Plugin Properties
-----------
**Host:** Hostname of the SQL server to read from.

**Port:** Port to use to connect to the SQL server.

**Server Timezone:** Timezone of the SQL server. This is used when converting dates into timestamps.

**User:** Username to use to connect to the SQL server.

**Password:** Password to use to connect to the SQL server.

**Database:** Database to consume events for.

**JDBC Plugin Name:** Name of the jdbc driver to use.

Example
----------
```
{
    "host": "localhost",
    "port": "1433",
    "database": "mydb",
    "jdbcPluginName": "MSJDBC",
    "user": "test",
    "password": "test",
    "serverTimezone": "PST"
}
```