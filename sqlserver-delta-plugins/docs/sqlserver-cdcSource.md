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
@source_schema  = N'dbo',
@source_name    = N'MyTable',
@role_name      = N'MyRole',
@filegroup_name = N'MyDB_CT',
@supports_net_changes = 0
GO
```

#### Verify Table CDC Accessibility
Run following query to make sure your table has CDC assess.
```
-- =========
-- Verify the user (you specify in the replicator to connect to the source DB) has access, this query should not have empty result
-- =========

EXEC sys.sp_cdc_help_change_data_capture
GO
```

### Grant permission on user defined type
If the tables to be replicated contain columns of user defined types, the table owner must grant EXECUTE permissions on
the custom data types to the database user specified in the replication job.
``````
GRANT EXECUTE ON TYPE::YOUR_TYPE to YOUR_USER
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

**Replicate Existing Data:** Whether to replicate existing data from the source database. By default, pipeline will 
replicate the existing data from source tables. If set to false, any existing data in the source tables will be 
ignored and only changes happening after the pipeline started will be replicated.
