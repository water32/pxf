-- start_ignore
-- data prep
DROP TABLE IF EXISTS gpdb_types_1 CASCADE;

CREATE TABLE gpdb_types_1
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

\COPY gpdb_types_1 FROM '{{ WORKING_DIR }}/resources/data/gpdb/gpdb_types.txt' DELIMITER E'\t' NULL as E'\\N' CSV;

DROP TABLE IF EXISTS gpdb_columns_1 CASCADE;

CREATE TABLE gpdb_columns_1
(
    t       text,
    "num 1" int,
    "n@m2"  int
) DISTRIBUTED BY (t);

\COPY gpdb_columns_1 FROM '{{ WORKING_DIR }}/resources/data/gpdb/gpdb_columns.txt' DELIMITER E'\t' NULL as E'\\N' CSV;
-- end_ignore

-- sets the date style and bytea output to the expected by the tests
SET datestyle='ISO, MDY';
SET bytea_output='escape';

CREATE SERVER jdbc_test_column_projection
    FOREIGN DATA WRAPPER jdbc_pxf_fdw
    OPTIONS ( jdbc_driver 'org.postgresql.Driver', db_url 'jdbc:postgresql://{{ PGHOST }}:{{ PGPORT }}/regression' );

CREATE USER MAPPING FOR CURRENT_USER SERVER jdbc_test_column_projection;

DROP FOREIGN TABLE IF EXISTS pxf_jdbc_multiple_fragments_by_int CASCADE;

CREATE FOREIGN TABLE pxf_jdbc_multiple_fragments_by_int
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
        ) SERVER jdbc_test_column_projection
    OPTIONS ( resource 'gpdb_types_1', partition_by 'num1:int', range '1:6', interval '1' );

DROP FOREIGN TABLE IF EXISTS pxf_jdbc_multiple_fragments_by_date CASCADE;

CREATE FOREIGN TABLE pxf_jdbc_multiple_fragments_by_date
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
        ) SERVER jdbc_test_column_projection
    OPTIONS ( resource 'gpdb_types_1', partition_by 'dt:date', range '2015-03-06:2015-03-20', interval '1:DAY' );

DROP FOREIGN TABLE IF EXISTS pxf_jdbc_subset_of_fields_diff_order CASCADE;

CREATE FOREIGN TABLE pxf_jdbc_subset_of_fields_diff_order
    (
        "n@m2" int,
        "num 1" int
        ) SERVER jdbc_test_column_projection
    OPTIONS ( resource 'gpdb_columns_1' );

DROP FOREIGN TABLE IF EXISTS pxf_jdbc_superset_of_fields CASCADE;

CREATE FOREIGN TABLE pxf_jdbc_superset_of_fields
    (
        t text,
        "does_not_exist_on_source" text,
        "num 1" int,
        num2 int OPTIONS ( column_name 'n@m2' )
        ) SERVER jdbc_test_column_projection
    OPTIONS ( resource 'gpdb_columns_1' );

-- @description query01 for JDBC query with int by partitioning
SELECT t2, tm, sqrt(sml), c1 FROM pxf_jdbc_multiple_fragments_by_int ORDER BY t1;

-- @description query02 for JDBC query with date by partitioning
SELECT t2, dec1, b::int, bin FROM pxf_jdbc_multiple_fragments_by_date WHERE num1 >= 5 ORDER BY t1;

-- @description query03 for JDBC query with subset of columns in different order than the source table
SELECT "n@m2", "num 1" FROM pxf_jdbc_subset_of_fields_diff_order ORDER BY "num 1";

-- @description query04 for JDBC query with superset of columns in different order
SELECT num2, "t", "num 1" FROM pxf_jdbc_superset_of_fields ORDER BY "t";

-- pxf_jdbc_superset_of_fields table is a superset of the source table
-- this will error out
-- start_matchsubs
--
-- # create a match/subs
--
-- m/(ERROR|WARNING):.*remote component error.*\(\d+\).*from.*'\d+\.\d+\.\d+\.\d+:\d+'.*/
-- s/'\d+\.\d+\.\d+\.\d+:\d+'/'SOME_IP:SOME_PORT'/
-- m/(ERROR|WARNING):.*remote component error.*\(\d+\).*from.*'(([0-9a-fA-F]{1,4}:){7,7}[0-9a-fA-F]{1,4}|([0-9a-fA-F]{1,4}:){1,7}:|([0-9a-fA-F]{1,4}:){1,6}:[0-9a-fA-F]{1,4}|([0-9a-fA-F]{1,4}:){1,5}(:[0-9a-fA-F]{1,4}){1,2}|([0-9a-fA-F]{1,4}:){1,4}(:[0-9a-fA-F]{1,4}){1,3}|([0-9a-fA-F]{1,4}:){1,3}(:[0-9a-fA-F]{1,4}){1,4}|([0-9a-fA-F]{1,4}:){1,2}(:[0-9a-fA-F]{1,4}){1,5}|[0-9a-fA-F]{1,4}:((:[0-9a-fA-F]{1,4}){1,6})|:((:[0-9a-fA-F]{1,4}){1,7}|:)|fe80:(:[0-9a-fA-F]{0,4}){0,4}%[0-9a-zA-Z]{1,}|::(ffff(:0{1,4}){0,1}:){0,1}((25[0-5]|(2[0-4]|1{0,1}[0-9]){0,1}[0-9])\.){3,3}(25[0-5]|(2[0-4]|1{0,1}[0-9]){0,1}[0-9])|([0-9a-fA-F]{1,4}:){1,4}:((25[0-5]|(2[0-4]|1{0,1}[0-9]){0,1}[0-9])\.){3,3}(25[0-5]|(2[0-4]|1{0,1}[0-9]){0,1}[0-9])):\d+'.*/
-- s/'(([0-9a-fA-F]{1,4}:){7,7}[0-9a-fA-F]{1,4}|([0-9a-fA-F]{1,4}:){1,7}:|([0-9a-fA-F]{1,4}:){1,6}:[0-9a-fA-F]{1,4}|([0-9a-fA-F]{1,4}:){1,5}(:[0-9a-fA-F]{1,4}){1,2}|([0-9a-fA-F]{1,4}:){1,4}(:[0-9a-fA-F]{1,4}){1,3}|([0-9a-fA-F]{1,4}:){1,3}(:[0-9a-fA-F]{1,4}){1,4}|([0-9a-fA-F]{1,4}:){1,2}(:[0-9a-fA-F]{1,4}){1,5}|[0-9a-fA-F]{1,4}:((:[0-9a-fA-F]{1,4}){1,6})|:((:[0-9a-fA-F]{1,4}){1,7}|:)|fe80:(:[0-9a-fA-F]{0,4}){0,4}%[0-9a-zA-Z]{1,}|::(ffff(:0{1,4}){0,1}:){0,1}((25[0-5]|(2[0-4]|1{0,1}[0-9]){0,1}[0-9])\.){3,3}(25[0-5]|(2[0-4]|1{0,1}[0-9]){0,1}[0-9])|([0-9a-fA-F]{1,4}:){1,4}:((25[0-5]|(2[0-4]|1{0,1}[0-9]){0,1}[0-9])\.){3,3}(25[0-5]|(2[0-4]|1{0,1}[0-9]){0,1}[0-9])):\d+'/'SOME_IP:SOME_PORT'/
--
-- m/\/gpdb\/v\d+\//
-- s/v\d+/SOME_VERSION/
--
-- m/file:.*;/
-- s/file:.*; lineNumber: \d+; columnNumber: \d+;/SOME_ERROR_LOCATION/g
--
-- m/Exception report.*/
-- s/report.*/SOME_EXCEPTION/
--
-- m/DETAIL/
-- s/DETAIL/CONTEXT/
--
-- m/pxf:\/\/(.*)\/pxf_automation_data/
-- s/pxf:\/\/.*\/pxf_automation_data/pxf:\/\/pxf_automation_data/
--
-- m/CONTEXT:.*line.*/
-- s/line \d* of //g
--
-- end_matchsubs
SELECT * FROM pxf_jdbc_superset_of_fields ORDER BY "t";

-- start_ignore
{{ CLEAN_UP }}-- clean up JDBC and local disk
{{ CLEAN_UP }} DROP FOREIGN TABLE IF EXISTS pxf_jdbc_multiple_fragments_by_int CASCADE;
{{ CLEAN_UP }} DROP FOREIGN TABLE IF EXISTS pxf_jdbc_multiple_fragments_by_date CASCADE;
{{ CLEAN_UP }} DROP FOREIGN TABLE IF EXISTS pxf_jdbc_subset_of_fields_diff_order CASCADE;
{{ CLEAN_UP }} DROP FOREIGN TABLE IF EXISTS pxf_jdbc_superset_of_fields CASCADE;
{{ CLEAN_UP }} DROP USER MAPPING IF EXISTS FOR CURRENT_USER SERVER jdbc_test_column_projection;
{{ CLEAN_UP }} DROP SERVER IF EXISTS jdbc_test_column_projection CASCADE;
{{ CLEAN_UP }} DROP TABLE IF EXISTS gpdb_types_1 CASCADE;
{{ CLEAN_UP }} DROP TABLE IF EXISTS gpdb_columns_1 CASCADE;
-- end_ignore