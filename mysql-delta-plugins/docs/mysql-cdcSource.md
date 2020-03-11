# MySQL CDC Source


Description
-----------
MySQL CDC Source will replicate all of the row-level changes in the databases on MySQL server.

Setting up MySQL
-----------
#### Enable the binlog
This is done in the MySQL server configuration file and will look similar like this:
```
server-id         = 1
log_bin           = mysql-bin
binlog_format     = row
binlog_row_image  = full
expire_logs_days  = 10
```

#### Enable GTIDs (optional)
The MySQL server can be configured to use GTID-based replication. Using GTIDs greatly simplifies replication and makes 
it possible to easily confirm whether masters and slaves are consistent. Note that if you’re using an earlier version 
before MySQL 5.6.5, you will not be able to enable GTIDs.
```
gtid_mode                 = on
enforce_gtid_consistency  = on
```

#### Enable Query Log Events (optional)
Starting with MySQL 5.6 row based replication can be configured to include the original SQL statement with each 
binlog event. Note that if you’re using an earlier version of MySQL, you will not be able to enable this feature.
```
binlog_rows_query_log_events = on
```

#### Set Up Session Timeouts (optional)
When initial snapshots of very large databases are executed then it is possible that an established connection will 
timeout while reading the content of the database tables. We can increase following configs to deal with that.
```
interactive_timeout = <duration-in-seconds>
wait_timeout        = <duration-in-seconds>
```

#### Create A MySQL User
A MySQL user must be defined with all the following permissions on any database which wants to be replicated:
**SELECT**, **RELOAD**, **SHOW DATABASES**, **REPLICATION SLAVE** and **REPLICATION CLIENT**.

Setting up JDBC Driver
-----------
In order to set up JDBC driver, just need to upload downloaded MySQL JDBC driver jar and configure it like this:
```
Name: DriverName
Class name: com.mysql.cj.jdbc.Driver
Version: 8.0
Description: This is a jdbc driver for MySQL.
```

Plugin Properties
-----------
**Host:** Hostname of the MySQL server to read from.

**Port:** Port to use to connect to the MySQL server.

**Consumer ID:** An unique numeric ID to identify this origin as an event consumer.

**Server Timezone:** Timezone of the MySQL server. This is used when converting dates into timestamps.

**User:** Username to use to connect to the MySQL server.

**Password:** Password to use to connect to the MySQL server.

**Database:** Database to consume events for.

**JDBC Plugin Name:** Name of the jdbc driver to use.

Example
----------
```
{
    "host": "localhost",
    "port": "3306",
    "consumerID": "1",
    "serverTimezone": "PST",
    "user": "xyz",
    "password": "xyz",
    "database": "mydb",
    "jdbcPluginName": "MYJDBC"
}
```