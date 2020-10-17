# Accessing MySQL with Greenplum PXF

PXF supports accessing external systems through the
[External Table Framework](https://gpdb.docs.pivotal.io/latest/ref_guide/sql_commands/CREATE_EXTERNAL_TABLE.html).

This section provides examples on how to access data residing on MySQL using
[External Tables](#external-tables).

The examples includes the syntax and the configuration required to access MySQL.

## Prerequisites

- Ensure the PXF Server has been initialized and started
- Docker Engine (to run MySQL)
- MySQL JDBC driver

## Running MySQL on Docker

For this example, we will provision a MySQL server using Docker.

```shell script
$ docker run -p 3306:3306 \
  --name mysql-test \
  -e MYSQL_ROOT_PASSWORD=my-secret-pw \
  -d mysql:latest
```

Running this command will automatically pull the latest MySQL image from docker
(if you don't have it locally), and it will start up a MySQL server. The `-p`
flag will expose port `3306` to the host. We'll use `my-secret-pw` as the
password for mysql.

## Create a table in MySQL

Next, we will create a table in MySQL that we will then use to access it from
Greenplum PXF. We need to connect to the docker image running MySQL. You will
need to type in the password you provided earlier. In this example, we'll need
to type `my-secret-pw`.

```shell script
$ docker exec -it mysql-test mysql -u root -p
```

Let's go ahead and create a table in MySQL, we'll use the `mysql` database.

```mysql
mysql> USE mysql;
Database changed

mysql> CREATE TABLE names (id int, name varchar(64), last varchar(64));
Query OK, 0 rows affected (0.03 sec)
```

Now, let's go ahead and add some data to our table.

```mysql
mysql> INSERT INTO names values (1, 'Francisco', 'Guerrero'), (2, 'Alex', 'Denissov');
Query OK, 2 rows affected (0.00 sec)

mysql> SELECT * from names;
+------+-----------+----------+
| id   | name      | last     |
+------+-----------+----------+
|    1 | Francisco | Guerrero |
|    2 | Alex      | Denissov |
+------+-----------+----------+
2 rows in set (0.00 sec)
```

That's all we need to do on the MySQL side. Now let's go ahead and configure
MySQL for Greenplum PXF.

## Configure MySQL server definition for Greenplum PXF

Create a server definition, this operation happens at the Greenplum master
server filesystem level (requires `gpadmin` access). First, identify your
`PXF_CONF` path on the filesystem.

```shell script
$ mkdir -p $PXF_CONF/servers/mysql
$ cp $PXF_CONF/templates/jdbc-site.xml $PXF_CONF/servers/mysql
$ vi $PXF_CONF/servers/mysql/jdbc-site.xml
```

Fill in the `jdbc.driver`, `jdbc.url`, `jdbc.user` and `jdbc.password`
properties. For the `jdbc.driver` use `com.mysql.jdbc.Driver`, the
`jdbc.url` use `jdbc:mysql://localhost:3306/mysql`, `jdbc.user` is `root` and
`jdbc.password` is `my-secret-pw`.

Your `$PXF_CONF/servers/mysql/jdbc-site.xml` will look like this:

```xml
<?xml version="1.0" encoding="UTF-8"?>
<configuration>
    <property>
        <name>jdbc.driver</name>
        <value>com.mysql.jdbc.Driver</value>
        <description>Class name of the JDBC driver (e.g. org.postgresql.Driver)</description>
    </property>
    <property>
        <name>jdbc.url</name>
        <value>jdbc:mysql://localhost:3306/mysql</value>
        <description>The URL that the JDBC driver can use to connect to the database (e.g. jdbc:postgresql://localhost/postgres)</description>
    </property>
    <property>
        <name>jdbc.user</name>
        <value>root</value>
        <description>User name for connecting to the database (e.g. postgres)</description>
    </property>
    <property>
        <name>jdbc.password</name>
        <value>my-secret-pw</value>
        <description>Password for connecting to the database (e.g. postgres)</description>
    </property>
    ...
    ...
    ...

</configuration>  
```

Finally, let's synchronize the MySQL server configuration across all segment
hosts.

```shell script
$ pxf cluster sync
```

## Configure MySQL JDBC driver for Greenplum PXF

PXF needs a MySQL JDBC driver to be able to access data residing on MySQL.
You can download a MySQL JDBC driver from maven central or your preferred 
download location. For example, you can run the following command:

```shell script
$ cd $PXF_CONF/lib
$ wget https://repo1.maven.org/maven2/mysql/mysql-connector-java/8.0.21/mysql-connector-java-8.0.21.jar
```

Finally, we will need to synchronize the MySQL JDBC driver across all segment
hosts, and we will need to restart PXF.

```shell script
$ pxf cluster sync
$ pxf cluster restart
```

## Create the PXF External Table inside Greenplum

Let's go ahead and create a database for this tutorial and let's connect to 
`psql`.

```shell script
$ createdb mysql-pxf
$ psql mysql-pxf
```

Ensure that the `pxf` extension is installed.

```sql
mysql-pxf=> CREATE EXTENSION pxf;
```

Next, create the external table definition.

```sql
mysql-pxf=> CREATE EXTERNAL TABLE names_in_mysql (id int, name text, last text)
LOCATION('pxf://names?PROFILE=jdbc&SERVER=mysql')
FORMAT 'CUSTOM' (formatter='pxfwritable_import');
```

The `LOCATION` specifies three important pieces of information:

1. The table name `names`
2. The `PROFILE` specifies the access protocol, in this case `jdbc`
3. The `SERVER` specifies the server name configuration that we previously
   configured

Now, let's issue a query against our `names` table residing in MySQL.

```sql
mysql-pxf=> SELECT * FROM names_in_mysql;
 id |   name    |   last   
----+-----------+----------
  1 | Francisco | Guerrero
  2 | Alex      | Denissov
(2 rows)
```

Finally, let's create a `WRITABLE` external table and let's insert some data
in MySQL from Greenplum PXF.

```sql
mysql-pxf=> CREATE WRITABLE EXTERNAL TABLE names_in_mysql_w (id int, name text, last text)
LOCATION('pxf://names?PROFILE=jdbc&SERVER=mysql')
FORMAT 'CUSTOM' (formatter='pxfwritable_export');

mysql-pxf=> INSERT INTO names_in_mysql_w VALUES (3, 'Ashuka', 'Xue');
INSERT 0 1
```

And let's query the table once more:

```sql
mysql-pxf=> SELECT * FROM names_in_mysql;
 id |   name    |   last   
----+-----------+----------
  1 | Francisco | Guerrero
  2 | Alex      | Denissov
  3 | Ashuka    | Xue
(3 rows)

```

There we have it! We can read and write data from MySQL using Greenplum PXF.
