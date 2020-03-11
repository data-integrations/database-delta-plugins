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
mysql> CREATE USER 'user'@'localhost' IDENTIFIED BY 'password';
mysql> GRANT SELECT, RELOAD, SHOW DATABASES, REPLICATION SLAVE, REPLICATION CLIENT ON *.* TO 'user' IDENTIFIED BY 'password';
mysql> FLUSH PRIVILEGES;
```

Setting up JDBC Driver
-----------
If it is not already installed, instructions for installing the MySQL JDBC driver can be found on the Hub.

Plugin Properties
-----------
**Host:** Hostname of the MySQL server to read from.

**Port:** Port to use to connect to the MySQL server.

**Consumer ID:** An unique numeric ID to identify this origin as an event consumer. This number cannot be the same as 
another Delta Replicator that is reading from the server, and it cannot be the same as the server-id for any MySQL 
slave that is replicating from the server.

**Server Timezone:** Timezone of the MySQL server. This is used when converting dates into timestamps.

**User:** Username to use to connect to the MySQL server.

**Password:** Password to use to connect to the MySQL server.

**Database:** Database to consume events for.

**JDBC Plugin Name:** Name of the jdbc driver to use.