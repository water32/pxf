# Accessing S3 with Greenplum PXF

PXF supports accessing external systems through the [External Table Framework](https://gpdb.docs.pivotal.io/latest/ref_guide/sql_commands/CREATE_EXTERNAL_TABLE.html)
and through the [Foreign Data Wrapper Framework](https://gpdb.docs.pivotal.io/latest/ref_guide/sql_commands/CREATE_FOREIGN_DATA_WRAPPER.html).
This section provides examples on how to access external data residing on S3 using:

- [Foreign Data Wrappers](#foreign-data-wrappers)
- [External Tables](#external-tables)

The examples include syntax and the configuration parameters required to access S3.

## Prerequisites

- Ensure the PXF Server has been initialized and started
- An [Amazon Web Services S3](https://aws.amazon.com/s3/) Account
- An AWS Access Key and an AWS Secret Key. [Click here](https://aws.amazon.com/premiumsupport/knowledge-center/create-access-key/) for details on how to generate a access and secret key
- An AWS [S3 Bucket](https://docs.aws.amazon.com/AmazonS3/latest/dev/UsingBucket.html)

## Foreign Data Wrappers

The foreign data wrapper framework is the newest framework supported by PXF, and
it is currently only available in the master branch of Greenplum. Ensure that the
`pxf_fdw` extension is installed.

~~~
CREATE EXTENSION pxf_fdw;
~~~

First, we need to [create a server](https://www.postgresql.org/docs/9.4/sql-createserver.html) specifying the access and secret keys.

~~~
CREATE SERVER s3
    FOREIGN DATA WRAPPER s3_pxf_fdw
    OPTIONS ( accesskey 'MY_AWS_ACCESS_KEY', secretkey 'MY_AWS_SECRET_KEY' );
~~~

Next, we will [create a foreign table](https://www.postgresql.org/docs/9.4/sql-createforeigntable.html) definition, in this example we are accessing
[TPC-H lineitem](http://www.tpc.org/tpch/default5.asp) data, that has been
stored in [Apache Parquet](https://parquet.apache.org/) format.

~~~
CREATE FOREIGN TABLE lineitem_parquet_s3 (
    l_orderkey BIGINT NOT NULL,
    l_partkey BIGINT NOT NULL,
    l_suppkey BIGINT NOT NULL,
    l_linenumber BIGINT NOT NULL,
    l_quantity DECIMAL(15, 2) NOT NULL,
    l_extendedprice DECIMAL(15, 2) NOT NULL,
    l_discount DECIMAL(15, 2) NOT NULL,
    l_tax DECIMAL(15, 2) NOT NULL,
    l_returnflag CHAR(1) NOT NULL,
    l_linestatus CHAR(1) NOT NULL,
    l_shipdate DATE NOT NULL,
    l_commitdate DATE NOT NULL,
    l_receiptdate DATE NOT NULL,
    l_shipinstruct CHAR(25) NOT NULL,
    l_shipmode CHAR(10) NOT NULL,
    l_comment VARCHAR(44) NOT NULL)
    SERVER s3
    OPTIONS ( resource '/bucket/path/to/parquet/data', format 'parquet' );
~~~

Finally, we need to allow users to access the `s3` SERVER by providing a
[user mapping](https://www.postgresql.org/docs/9.4/sql-createusermapping.html).

~~~
CREATE USER MAPPING FOR CURRENT_USER
    SERVER s3;
~~~

Now we can issue query statements against our data residing in S3.

~~~
SELECT * FROM lineitem_parquet_s3;
~~~

## External Tables

Create a server definition, this operation happens at the Greenplum master
server filesystem level (requires `gpadmin` access). First, identify your
`PXF_CONF` path on the filesystem.

~~~
mkdir -p $PXF_CONF/servers/s3
cp $PXF_CONF/templates/s3-site.xml $PXF_CONF/servers/s3
vi $PXF_CONF/servers/s3/s3-site.xml
~~~

Fill in the `YOUR_AWS_ACCESS_KEY_ID` and `YOUR_AWS_SECRET_ACCESS_KEY` values in
the `$PXF_CONF/servers/s3/s3-site.xml` file with your S3 access and secret key
values. If you run Greenplum on a cluster, synchronize your server configuration
by issuing the following command.

~~~
$GPHOME/pxf/bin/pxf cluster sync
~~~

Ensure that the `pxf` extension is installed.

~~~
CREATE EXTENSION pxf;
~~~

Next, create the external table definition.

~~~
CREATE EXTERNAL TABLE lineitem_parquet_s3_r (
    l_orderkey BIGINT,
    l_partkey BIGINT,
    l_suppkey BIGINT,
    l_linenumber BIGINT,
    l_quantity DECIMAL (15, 2),
    l_extendedprice DECIMAL (15, 2),
    l_discount DECIMAL (15, 2),
    l_tax DECIMAL (15, 2),
    l_returnflag CHAR (1),
    l_linestatus CHAR (1),
    l_shipdate DATE,
    l_commitdate DATE,
    l_receiptdate DATE,
    l_shipinstruct CHAR (25),
    l_shipmode CHAR (10),
    l_comment VARCHAR (44))
    LOCATION('pxf://bucket/path/to/parquet/data?PROFILE=s3:parquet&SERVER=s3')
    FORMAT 'CUSTOM' (formatter='pxfwritable_import');
~~~

Again, we can issue query statements against our data residing in S3, this time
using the external table framework:

~~~
SELECT * FROM lineitem_parquet_s3_r;
~~~
