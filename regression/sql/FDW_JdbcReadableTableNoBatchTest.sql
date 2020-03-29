-- @description query01 for JDBC query with batch size 0 (as good as infinity)
-- start_ignore
DROP TABLE IF EXISTS gpdb_types3 CASCADE;

CREATE TABLE gpdb_types3
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

\COPY gpdb_types3 FROM '{{ WORKING_DIR }}/resources/data/gpdb/gpdb_types.txt' DELIMITER E'\t' NULL as E'\\N' CSV;
-- end_ignore

-- sets the date style and bytea output to the expected by the tests
SET datestyle='ISO, MDY';
SET bytea_output='escape';

CREATE SERVER jdbc_test_no_batch
    FOREIGN DATA WRAPPER jdbc_pxf_fdw
    OPTIONS ( jdbc_driver 'org.postgresql.Driver', db_url 'jdbc:postgresql://{{ PGHOST }}:{{ PGPORT }}/regression' );

CREATE USER MAPPING FOR CURRENT_USER SERVER jdbc_test_no_batch;

DROP FOREIGN TABLE IF EXISTS pxf_jdbc_readable_nobatch CASCADE;

CREATE FOREIGN TABLE pxf_jdbc_readable_nobatch
    (
        t1 text,
        t2 text,
        num1 int,
        dub1 double precision,
        dec1 numeric,
        tm timestamp,
        r real,
        bg bigint,
        b boolean,
        tn smallint,
        sml smallint,
        dt date,
        vc1 varchar(5),
        c1 char(3),
        bin bytea
        ) SERVER jdbc_test_no_batch
    OPTIONS ( resource 'gpdb_types3', fetch_size '0');

SELECT * FROM pxf_jdbc_readable_nobatch ORDER BY t1;

-- start_ignore
{{ CLEAN_UP }}-- clean up JDBC and local disk
{{ CLEAN_UP }} DROP FOREIGN TABLE IF EXISTS pxf_jdbc_readable_nobatch CASCADE;
{{ CLEAN_UP }} DROP USER MAPPING IF EXISTS FOR CURRENT_USER SERVER jdbc_test_no_batch;
{{ CLEAN_UP }} DROP SERVER IF EXISTS jdbc_test_no_batch CASCADE;
{{ CLEAN_UP }} DROP TABLE IF EXISTS gpdb_types3 CASCADE;
-- end_ignore