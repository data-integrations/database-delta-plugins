# SQL Server CDC Source


Description
-----------
Microsoft SQL Server CDC Source will replicate all of the row-level changes in the databases on Microsoft SQL server.

Setting up Microsoft SQL Server
-----------
#### Enable CDC On Database
* Make sure you have the required permissions by following this [link](https://docs.microsoft.com/en-us/sql/integration-services/change-data-capture/sql-server-connection-required-permissions-for-the-cdc-designer?view=sql-server-ver15).

* Enable CDC for database 'MyDB' will look similar like this:
```
-- ====
-- Enable Database for CDC template
-- ====
USE MyDB
GO
EXEC sys.sp_cdc_enable_db
GO
```
Note that CDC cannot be enabled for master database.

#### Enable CDC On Table
Enable CDC for the table 'MyTable' will look similar like this:
```
-- =========
-- If there is no 'role_name', you can set it as NULL.
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
If it is not already installed, instructions for installing the Microsoft SQL Server JDBC driver can be found on the 
Hub.

Plugin Properties
-----------
**Host:** Hostname or IP address of the SQL server to read from.

**Port:** Port to use to connect to the SQL server.

**Server Timezone:** Timezone of the SQL server. This is used when converting dates into timestamps.

**User:** Username to use to connect to the SQL server.

**Password:** Password to use to connect to the SQL server.

**Database:** Database to replicate data from.

**JDBC Plugin Name:** Identifier for the SQLServer JDBC driver, which is the name used while uploading the SQLServer JDBC driver.
