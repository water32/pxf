# PXF Foreign Data Wrapper for Greenplum and PostgreSQL

This Greenplum extension implements a Foreign Data Wrapper (FDW) for PXF.

PXF is a query federation engine that accesses data residing in external systems
such as Hadoop, Hive, HBase, relational databases, S3, Google Cloud Storage,
among other external systems.

### Development

## Compile

To compile the PXF foreign data wrapper, we need a Greenplum 6+ installation and libcurl.

    export PATH=/usr/local/greenplum-db/bin/:$PATH

    make

## Install

    make install

## Regression

    make installcheck

## Using the Demo Profile

PXF ships a `demo` profile that can be used to test that PXF is functional. To use the `demo` profile, the PXF
Server component must be started (i.e `pxf [cluster] start`).

To use the `demo` profile, run the following commands in a `psql` prompt:

```sql
CREATE EXTENSION IF NOT EXISTS pxf_fdw;

CREATE SERVER demo_server
    FOREIGN DATA WRAPPER demo_pxf_fdw;

CREATE USER MAPPING FOR CURRENT_USER SERVER demo_server;

CREATE FOREIGN TABLE pxf_read_demo (a TEXT, b TEXT, c TEXT)
    SERVER demo_server
    OPTIONS ( resource 'tmp/dummy1' );
```

After the foreign table has been created, you can test the PXF service by running a select query:

```sql
SELECT * FROM pxf_read_demo;
```