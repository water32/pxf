-- @description query01 for JDBC query without partitioning
-- start_ignore
DROP TABLE IF EXISTS gpdb_types6 CASCADE;

CREATE TABLE gpdb_types6
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

\COPY gpdb_types6 FROM '{{ WORKING_DIR }}/resources/data/gpdb/gpdb_types.txt' DELIMITER E'\t' NULL as E'\\N' CSV;
-- end_ignore

-- sets the date style and bytea output to the expected by the tests
SET datestyle='ISO, MDY';
SET bytea_output='escape';

CREATE SERVER jdbc_test_single_fragment
    FOREIGN DATA WRAPPER jdbc_pxf_fdw
    OPTIONS ( jdbc_driver 'org.postgresql.Driver', db_url 'jdbc:postgresql://{{ PGHOST }}:{{ PGPORT }}/regression' );

CREATE USER MAPPING FOR CURRENT_USER SERVER jdbc_test_single_fragment;

DROP FOREIGN TABLE IF EXISTS pxf_jdbc_single_fragment CASCADE;

CREATE FOREIGN TABLE pxf_jdbc_single_fragment
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
) SERVER jdbc_test_single_fragment
    OPTIONS ( resource 'gpdb_types6' );

SELECT * FROM pxf_jdbc_single_fragment ORDER BY t1;

-- start_ignore
{{ CLEAN_UP }}-- clean up JDBC and local disk
{{ CLEAN_UP }} DROP FOREIGN TABLE IF EXISTS pxf_jdbc_single_fragment CASCADE;
{{ CLEAN_UP }} DROP USER MAPPING IF EXISTS FOR CURRENT_USER SERVER jdbc_test_single_fragment;
{{ CLEAN_UP }} DROP SERVER IF EXISTS jdbc_test_single_fragment CASCADE;
{{ CLEAN_UP }} DROP TABLE IF EXISTS gpdb_types6 CASCADE;
-- end_ignore