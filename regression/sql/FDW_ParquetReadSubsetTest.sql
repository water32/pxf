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

CREATE SERVER parquet_test_parquet_subset
    FOREIGN DATA WRAPPER {{ HCFS_PROTOCOL }}_pxf_fdw
    OPTIONS (config '{{ SERVER_CONFIG }}');

CREATE USER MAPPING FOR CURRENT_USER SERVER parquet_test_parquet_subset;

DROP FOREIGN TABLE IF EXISTS pxf_parquet_subset CASCADE;

CREATE FOREIGN TABLE pxf_parquet_subset
(
    s1  TEXT,
    n1  INTEGER,
    d1  DOUBLE PRECISION,
    f   REAL,
    b   BOOLEAN,
    vc1 VARCHAR(5),
    bin BYTEA
) SERVER parquet_test_parquet_subset
    OPTIONS (resource '{{ HCFS_BUCKET }}{{ TEST_LOCATION }}/parquet/parquet_primitive_types', format 'parquet');

-- @description test Parquet with Greenplum table as a subset of the parquet file
SELECT s1, n1, d1, f, b, vc1, bin FROM pxf_parquet_subset ORDER BY s1;

-- s1, d1, vc1 are projected columns
SELECT d1, vc1 FROM pxf_parquet_subset ORDER BY s1;

-- start_ignore
{{ CLEAN_UP }}-- clean up HCFS and local disk
{{ CLEAN_UP }} DROP FOREIGN TABLE IF EXISTS pxf_parquet_subset CASCADE;
{{ CLEAN_UP }} DROP USER MAPPING IF EXISTS FOR CURRENT_USER SERVER parquet_test_parquet_subset;
{{ CLEAN_UP }} DROP SERVER IF EXISTS parquet_test_parquet_subset CASCADE;
{{ CLEAN_UP }}\!{{ HCFS_CMD }} --config {{ PXF_CONF }}/servers/{{ SERVER_CONFIG }}/ dfs -rm -r -f {{ HCFS_SCHEME }}{{ HCFS_BUCKET }}{{ TEST_LOCATION }}/parquet/parquet_primitive_types
{{ CLEAN_UP }}{{ GPDB_REMOTE }}\!ssh {{ PGHOST }} rm -rf {{ TEST_LOCATION }}
-- end_ignore