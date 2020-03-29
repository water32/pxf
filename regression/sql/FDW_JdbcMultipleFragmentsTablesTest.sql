-- start_ignore
DROP TABLE IF EXISTS gpdb_types4 CASCADE;

CREATE TABLE gpdb_types4
(
    t1   text,
    t2   text,
    num1 int,
    dub1 double precision,
    dec1 numeric,
    tm   timestamp,
    r    real,
    bg   bigint,
    b    boolean,
    tn   smallint,
    sml  smallint,
    dt   date,
    vc1  varchar(5),
    c1   char(3),
    bin  bytea
) DISTRIBUTED BY (t1);

\COPY gpdb_types4 FROM '{{ WORKING_DIR }}/resources/data/gpdb/gpdb_types.txt' DELIMITER E'\t' NULL as E'\\N' CSV;
-- end_ignore

-- sets the date style and bytea output to the expected by the tests
SET datestyle='ISO, MDY';
SET bytea_output='escape';

CREATE SERVER jdbc_test_multiple_fragments
    FOREIGN DATA WRAPPER jdbc_pxf_fdw
    OPTIONS ( jdbc_driver 'org.postgresql.Driver', db_url 'jdbc:postgresql://{{ PGHOST }}:{{ PGPORT }}/regression' );

CREATE USER MAPPING FOR CURRENT_USER SERVER jdbc_test_multiple_fragments;

DROP FOREIGN TABLE IF EXISTS pxf_jdbc_multiple_fragments_by_int_1 CASCADE;

CREATE FOREIGN TABLE pxf_jdbc_multiple_fragments_by_int_1
(
    t1   text,
    t2   text,
    num1 int,
    dub1 double precision,
    dec1 numeric,
    tm   timestamp,
    r    real,
    bg   bigint,
    b    boolean,
    tn   smallint,
    sml  smallint,
    dt   date,
    vc1  varchar(5),
    c1   char(3),
    bin  bytea
) SERVER jdbc_test_multiple_fragments
    OPTIONS ( resource 'gpdb_types4', partition_by 'num1:int', range '1:6', interval '1' );

-- @description query01 for JDBC query with int by partitioning
SELECT * FROM pxf_jdbc_multiple_fragments_by_int_1 ORDER BY t1;

DROP FOREIGN TABLE IF EXISTS pxf_jdbc_multiple_fragments_by_date_1 CASCADE;

CREATE FOREIGN TABLE pxf_jdbc_multiple_fragments_by_date_1
(
    t1   text,
    t2   text,
    num1 int,
    dub1 double precision,
    dec1 numeric,
    tm   timestamp,
    r    real,
    bg   bigint,
    b    boolean,
    tn   smallint,
    sml  smallint,
    dt   date,
    vc1  varchar(5),
    c1   char(3),
    bin  bytea
) SERVER jdbc_test_multiple_fragments
    OPTIONS ( resource 'gpdb_types4', partition_by 'dt:date', range '2015-03-06:2015-03-20', interval '1:DAY' );

-- @description query01 for JDBC query with date by partitioning
SELECT * FROM pxf_jdbc_multiple_fragments_by_date_1 ORDER BY t1;

DROP FOREIGN TABLE IF EXISTS pxf_jdbc_multiple_fragments_by_enum_1 CASCADE;

CREATE FOREIGN TABLE pxf_jdbc_multiple_fragments_by_enum_1
(
    t1   text,
    t2   text,
    num1 int,
    dub1 double precision,
    dec1 numeric,
    tm   timestamp,
    r    real,
    bg   bigint,
    b    boolean,
    tn   smallint,
    sml  smallint,
    dt   date,
    vc1  varchar(5),
    c1   char(3),
    bin  bytea
) SERVER jdbc_test_multiple_fragments
    OPTIONS ( resource 'gpdb_types4', partition_by 'c1:enum', range 'USD:UAH', interval '1' );

-- @description query01 for JDBC query with enum by partitioning
SELECT * FROM pxf_jdbc_multiple_fragments_by_enum_1 ORDER BY t1;

-- start_ignore
{{ CLEAN_UP }}-- clean up JDBC and local disk
{{ CLEAN_UP }} DROP FOREIGN TABLE IF EXISTS pxf_jdbc_multiple_fragments_by_enum_1 CASCADE;
{{ CLEAN_UP }} DROP FOREIGN TABLE IF EXISTS pxf_jdbc_multiple_fragments_by_date_1 CASCADE;
{{ CLEAN_UP }} DROP FOREIGN TABLE IF EXISTS pxf_jdbc_multiple_fragments_by_int_1 CASCADE;
{{ CLEAN_UP }} DROP USER MAPPING IF EXISTS FOR CURRENT_USER SERVER jdbc_test_multiple_fragments;
{{ CLEAN_UP }} DROP SERVER IF EXISTS jdbc_test_multiple_fragments CASCADE;
{{ CLEAN_UP }} DROP TABLE IF EXISTS gpdb_types4 CASCADE;
-- end_ignore
