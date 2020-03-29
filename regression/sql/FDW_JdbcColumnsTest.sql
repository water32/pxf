-- start_ignore
-- data prep
DROP TABLE IF EXISTS gpdb_columns2 CASCADE;

CREATE TABLE gpdb_columns2
(
    t       text,
    "num 1" int,
    "n@m2"  int
) DISTRIBUTED BY (t);

\COPY gpdb_columns2 FROM '{{ WORKING_DIR }}/resources/data/gpdb/gpdb_columns.txt' DELIMITER E'\t' NULL as E'\\N' CSV ;
-- end_ignore

CREATE SERVER jdbc_test_columns
    FOREIGN DATA WRAPPER jdbc_pxf_fdw
    OPTIONS ( jdbc_driver 'org.postgresql.Driver', db_url 'jdbc:postgresql://{{ PGHOST }}:{{ PGPORT }}/regression' );

CREATE USER MAPPING FOR CURRENT_USER SERVER jdbc_test_columns;

DROP FOREIGN TABLE IF EXISTS pxf_jdbc_columns CASCADE;

CREATE FOREIGN TABLE pxf_jdbc_columns
(
    t       text,
    "num 1" int,
    "n@m2"  int
) SERVER jdbc_test_columns
    OPTIONS ( resource 'gpdb_columns2' );

-- @description query01 for JDBC query with special column names
SELECT * FROM pxf_jdbc_columns ORDER BY t;

DROP FOREIGN TABLE IF EXISTS pxf_jdbc_columns2 CASCADE;

CREATE FOREIGN TABLE pxf_jdbc_columns2
    (
        t       text,
        num1 int OPTIONS ( column_name 'num 1' ),
        num2  int OPTIONS ( column_name 'n@m2' )
        ) SERVER jdbc_test_columns
    OPTIONS ( resource 'gpdb_columns2' );

-- @description query02 for JDBC query with special column names as column options
SELECT * FROM pxf_jdbc_columns2 ORDER BY t;

-- start_ignore
{{ CLEAN_UP }}-- clean up JDBC and local disk
{{ CLEAN_UP }} DROP FOREIGN TABLE IF EXISTS pxf_jdbc_columns2 CASCADE;
{{ CLEAN_UP }} DROP FOREIGN TABLE IF EXISTS pxf_jdbc_columns CASCADE;
{{ CLEAN_UP }} DROP USER MAPPING IF EXISTS FOR CURRENT_USER SERVER pxf_jdbc_columns;
{{ CLEAN_UP }} DROP SERVER IF EXISTS pxf_jdbc_columns CASCADE;
{{ CLEAN_UP }} DROP TABLE IF EXISTS gpdb_columns2 CASCADE;
-- end_ignore