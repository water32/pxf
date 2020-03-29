-- start_ignore

-- Create database server and copy the jdbc-site.xml template and testuser-user.xml template.
-- Replace Driver, JDBC URL, user name, password configurations in jdbc-site.xml
-- Copy the report.sql file to the server
\!mkdir -p {{ PXF_CONF }}/servers/database
\!if [ ! -f {{ PXF_CONF }}/servers/database/jdbc-site.xml ]; then cp {{ PXF_CONF }}/templates/jdbc-site.xml {{ PXF_CONF }}/servers/database/; sed {{ SED_OPTS }} -e "s|YOUR_DATABASE_JDBC_DRIVER_CLASS_NAME|org.postgresql.Driver|" -e "s|YOUR_DATABASE_JDBC_URL|jdbc:postgresql://{{ PGHOST }}:{{ PGPORT }}/regression|" -e "s|YOUR_DATABASE_JDBC_USER||" -e "s|YOUR_DATABASE_JDBC_PASSWORD||" {{ PXF_CONF }}/servers/database/jdbc-site.xml; fi
\!if [ ! -f {{ PXF_CONF }}/servers/database/testuser-user.xml ]; then  cp {{ PXF_CONF }}/servers/database/jdbc-site.xml {{ PXF_CONF }}/servers/database/testuser-user.xml; sed {{ SED_OPTS }} "s|regression|template1|" {{ PXF_CONF }}/servers/database/testuser-user.xml; fi
\!cp {{ WORKING_DIR }}/resources/data/jdbc/report.sql {{ PXF_CONF }}/servers/database

DROP TABLE IF EXISTS gpdb_types5 CASCADE;

CREATE TABLE gpdb_types5
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

\COPY gpdb_types5 FROM '{{ WORKING_DIR }}/resources/data/gpdb/gpdb_types.txt' DELIMITER E'\t' NULL as E'\\N' CSV;
-- end_ignore

-- sets the date style and bytea output to the expected by the tests
SET datestyle='ISO, MDY';
SET bytea_output='escape';

CREATE SERVER jdbc_test_server_config
    FOREIGN DATA WRAPPER jdbc_pxf_fdw
    OPTIONS ( config 'database' );

CREATE USER MAPPING FOR CURRENT_USER SERVER jdbc_test_server_config;

DROP FOREIGN TABLE IF EXISTS pxf_jdbc_read_server_config_all CASCADE;

CREATE FOREIGN TABLE pxf_jdbc_read_server_config_all
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
) SERVER jdbc_test_server_config
    OPTIONS ( resource 'gpdb_types5', partition_by 'num1:int', range '1:6', interval '1' );

-- @description query01 for JDBC query with server config
SELECT * FROM pxf_jdbc_read_server_config_all ORDER BY t1;

-- start_ignore
{{ CLEAN_UP }}-- clean up JDBC and local disk
{{ CLEAN_UP }} DROP FOREIGN TABLE IF EXISTS pxf_jdbc_read_server_config_all CASCADE;
{{ CLEAN_UP }} DROP USER MAPPING IF EXISTS FOR CURRENT_USER SERVER jdbc_test_server_config;
{{ CLEAN_UP }} DROP SERVER IF EXISTS jdbc_test_server_config CASCADE;
{{ CLEAN_UP }} DROP TABLE IF EXISTS gpdb_types5 CASCADE;
-- end_ignore
