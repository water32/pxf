-- start_ignore
{{ GPDB_REMOTE }}\!ssh {{ PGHOST }} mkdir -p {{ TEST_LOCATION }}
{{ GPDB_REMOTE }}-- if GPDB is remote, will need to scp file down from there for beeline
{{ GPDB_REMOTE }}\!scp {{ PGHOST }}:{{ TEST_LOCATION }}/data.csv {{ TEST_LOCATION }}
\!{{ HCFS_CMD }} --config {{ PXF_CONF }}/servers/{{ SERVER_CONFIG }}/ dfs -mkdir -p '{{ HCFS_SCHEME }}{{ HCFS_BUCKET }}{{ TEST_LOCATION }}/parquet/'
\!{{ HCFS_CMD }} --config {{ PXF_CONF }}/servers/{{ SERVER_CONFIG }}/ dfs -copyFromLocal '{{ WORKING_DIR }}/resources/data/parquet/parquet_primitive_types' '{{ HCFS_SCHEME }}{{ HCFS_BUCKET }}{{ TEST_LOCATION }}/parquet/parquet_primitive_types'
-- end_ignore

-- sets the date style and bytea output to the expected by the tests
SET datestyle='ISO, MDY';
SET bytea_output='escape';

CREATE SERVER parquet_test_parquet_primitive_types
    FOREIGN DATA WRAPPER {{ HCFS_PROTOCOL }}_pxf_fdw
    OPTIONS (config '{{ SERVER_CONFIG }}');

CREATE USER MAPPING FOR CURRENT_USER SERVER parquet_test_parquet_primitive_types;

DROP FOREIGN TABLE IF EXISTS pxf_parquet_primitive_types CASCADE;

CREATE FOREIGN TABLE pxf_parquet_primitive_types
(
    s1  TEXT,
    s2  TEXT,
    n1  INTEGER,
    d1  DOUBLE PRECISION,
    dc1 NUMERIC,
    tm  TIMESTAMP,
    f   REAL,
    bg  BIGINT,
    b   BOOLEAN,
    tn  SMALLINT,
    vc1 VARCHAR(5),
    sml SMALLINT,
    c1  CHAR(3),
    bin BYTEA
) SERVER parquet_test_parquet_primitive_types
    OPTIONS (resource '{{ HCFS_BUCKET }}{{ TEST_LOCATION }}/parquet/parquet_primitive_types', format 'parquet');

-- @description test for primitive Parquet data types
-- Parquet data has been generated using PDT timezone, so we need to shift tm field on difference between PDT and current timezone
SELECT s1, s2, n1, d1, dc1, tm, f, bg, b, tn, sml, vc1, c1, bin FROM pxf_parquet_primitive_types ORDER BY s1;

-- start_ignore
{{ CLEAN_UP }}-- clean up HCFS and local disk
{{ CLEAN_UP }} DROP FOREIGN TABLE IF EXISTS pxf_parquet_primitive_types CASCADE;
{{ CLEAN_UP }} DROP USER MAPPING IF EXISTS FOR CURRENT_USER SERVER parquet_test_parquet_primitive_types;
{{ CLEAN_UP }} DROP SERVER IF EXISTS parquet_test_parquet_primitive_types CASCADE;
{{ CLEAN_UP }}\!{{ HCFS_CMD }} --config {{ PXF_CONF }}/servers/{{ SERVER_CONFIG }}/ dfs -rm -r -f {{ HCFS_SCHEME }}{{ HCFS_BUCKET }}{{ TEST_LOCATION }}/parquet/parquet_primitive_types
{{ CLEAN_UP }}{{ GPDB_REMOTE }}\!ssh {{ PGHOST }} rm -rf {{ TEST_LOCATION }}
-- end_ignore