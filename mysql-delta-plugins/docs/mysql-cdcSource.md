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
```
mysql> CREATE USER 'user'@'%' IDENTIFIED BY 'password';
mysql> GRANT SELECT, RELOAD, SHOW DATABASES, REPLICATION SLAVE, REPLICATION CLIENT ON *.* TO 'user'@'%' IDENTIFIED WITH mysql_native_password BY 'password';
mysql> FLUSH PRIVILEGES;
```

If using a hosted option such as Amazon RDS or Amazon Aurora that do not allow a global read lock, table-level locks are
used to create the consistent snapshot. In this case, you need to also grant LOCK_TABLES permissions to the user that
you create.

Setting up JDBC Driver
-----------
If it is not already installed, instructions for installing the MySQL JDBC driver can be found on the Hub. The MySQL
JDBC driver installed should be version 8 or above.

Plugin Properties
-----------
**Host:** Hostname of the MySQL server to read from.

**Port:** Port to use to connect to the MySQL server.

**Consumer ID:** Optional unique numeric ID to identify this origin as an event consumer. This number cannot be the 
same as another Delta Replicator that is reading from the server, and it cannot be the same as the server-id for any
 MySQL slave that is replicating from the server. By default, random number will be used. 

**Server Timezone:** Timezone of the MySQL server. This is used when converting dates into timestamps.

**User:** Username to use to connect to the MySQL server. Actual account used by the source while connecting 
to the MySQL server will be of the form 'user_name'@'%' where user_name is this field.

**Password:** Password to use to connect to the MySQL server.

**Database:** Database to replicate data from.

**JDBC Plugin Name:** Identifier for the MySQL JDBC driver, which is the name used while uploading the MySQL JDBC driver.

**Replicate Existing Data:** Whether to replicate existing data from the source database. By default, pipeline will 
replicate the existing data from source tables. If set to false, any existing data in the source tables will be 
ignored and only changes happening after the pipeline started will be replicated.

Troubleshooting
-----------
If the replicator is able to start snapshotting the data, but fails when it switches over to read from the 
binlog with errors in the log like:

```
Caused by: com.github.shyiko.mysql.binlog.network.AuthenticationException: Client does not support authentication protocol requested by server; consider upgrading MySQL client
```

This is most likely caused by the replication user using an incompatible password type. This is especially
common starting from MySQL 8 on up. To fix this, run the following command:

```
ALTER USER '[user]'@'[host]' IDENTIFIED WITH mysql_native_password BY '[password]'
```

to change the user to use a MySQL native password.
