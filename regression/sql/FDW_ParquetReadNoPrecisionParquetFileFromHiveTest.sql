-- start_ignore
{{ GPDB_REMOTE }}\!ssh {{ PGHOST }} mkdir -p {{ TEST_LOCATION }}
{{ GPDB_REMOTE }}-- if GPDB is remote, will need to scp file down from there for beeline
{{ GPDB_REMOTE }}\!scp {{ PGHOST }}:{{ TEST_LOCATION }}/data.csv {{ TEST_LOCATION }}
\!{{ HCFS_CMD }} --config {{ PXF_BASE }}/servers/{{ SERVER_CONFIG }}/ dfs -mkdir -p '{{ HCFS_SCHEME }}{{ HCFS_BUCKET }}{{ TEST_LOCATION }}/parquet/'
\!{{ HCFS_CMD }} --config {{ PXF_BASE }}/servers/{{ SERVER_CONFIG }}/ dfs -copyFromLocal '{{ WORKING_DIR }}/resources/data/parquet/undefined_precision_numeric.parquet' '{{ HCFS_SCHEME }}{{ HCFS_BUCKET }}{{ TEST_LOCATION }}/parquet/undefined_precision_numeric.parquet'
-- end_ignore

CREATE SERVER parquet_test_parquet_read_undefined_precision_numeric
    FOREIGN DATA WRAPPER {{ HCFS_PROTOCOL }}_pxf_fdw
    OPTIONS (config '{{ SERVER_CONFIG }}');

CREATE USER MAPPING FOR CURRENT_USER SERVER parquet_test_parquet_read_undefined_precision_numeric;

DROP FOREIGN TABLE IF EXISTS pxf_parquet_read_undefined_precision_numeric CASCADE;

CREATE FOREIGN TABLE pxf_parquet_read_undefined_precision_numeric
(
    description text,
    value       numeric
) SERVER parquet_test_parquet_read_undefined_precision_numeric
    OPTIONS (resource '{{ HCFS_BUCKET }}{{ TEST_LOCATION }}/parquet/undefined_precision_numeric.parquet', format 'parquet');

-- @description parquet test for undefined precision numeric
SELECT * FROM pxf_parquet_read_undefined_precision_numeric ORDER BY description;

-- start_ignore
{{ CLEAN_UP }}-- clean up HCFS and local disk
{{ CLEAN_UP }} DROP FOREIGN TABLE IF EXISTS pxf_parquet_read_undefined_precision_numeric CASCADE;
{{ CLEAN_UP }} DROP USER MAPPING IF EXISTS FOR CURRENT_USER SERVER parquet_test_parquet_read_undefined_precision_numeric;
{{ CLEAN_UP }} DROP SERVER IF EXISTS parquet_test_parquet_read_undefined_precision_numeric CASCADE;
{{ CLEAN_UP }}\!{{ HCFS_CMD }} --config {{ PXF_BASE }}/servers/{{ SERVER_CONFIG }}/ dfs -rm -r -f {{ HCFS_SCHEME }}{{ HCFS_BUCKET }}{{ TEST_LOCATION }}/parquet/undefined_precision_numeric.parquet
{{ CLEAN_UP }}{{ GPDB_REMOTE }}\!ssh {{ PGHOST }} rm -rf {{ TEST_LOCATION }}
-- end_ignore