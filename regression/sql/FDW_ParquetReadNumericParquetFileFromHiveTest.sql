-- start_ignore
{{ GPDB_REMOTE }}\!ssh {{ PGHOST }} mkdir -p {{ TEST_LOCATION }}
{{ GPDB_REMOTE }}-- if GPDB is remote, will need to scp file down from there for beeline
{{ GPDB_REMOTE }}\!scp {{ PGHOST }}:{{ TEST_LOCATION }}/data.csv {{ TEST_LOCATION }}
\!{{ HCFS_CMD }} --config {{ PXF_BASE }}/servers/{{ SERVER_CONFIG }}/ dfs -mkdir -p '{{ HCFS_SCHEME }}{{ HCFS_BUCKET }}{{ TEST_LOCATION }}/parquet/'
\!{{ HCFS_CMD }} --config {{ PXF_BASE }}/servers/{{ SERVER_CONFIG }}/ dfs -copyFromLocal '{{ WORKING_DIR }}/resources/data/parquet/numeric.parquet' '{{ HCFS_SCHEME }}{{ HCFS_BUCKET }}{{ TEST_LOCATION }}/parquet/numeric.parquet'
-- end_ignore

CREATE SERVER parquet_test_parquet_read_numeric
    FOREIGN DATA WRAPPER {{ HCFS_PROTOCOL }}_pxf_fdw
    OPTIONS (config '{{ SERVER_CONFIG }}');

CREATE USER MAPPING FOR CURRENT_USER SERVER parquet_test_parquet_read_numeric;

DROP FOREIGN TABLE IF EXISTS pxf_parquet_read_numeric CASCADE;

CREATE FOREIGN TABLE pxf_parquet_read_numeric
    (
        description TEXT,
        a DECIMAL(5, 2),
        b DECIMAL(12, 2),
        c DECIMAL(18, 18),
        d DECIMAL(24, 16),
        e DECIMAL(30, 5),
        f DECIMAL(34, 30),
        g DECIMAL(38, 10),
        h DECIMAL(38, 38)
        ) SERVER parquet_test_parquet_read_numeric
    OPTIONS (resource '{{ HCFS_BUCKET }}{{ TEST_LOCATION }}/parquet/numeric.parquet', format 'parquet');

-- @description test numeric with precision and scale defined
SELECT a, b, c, d, e, f, g, h FROM pxf_parquet_read_numeric ORDER BY description;

-- start_ignore
{{ CLEAN_UP }}-- clean up HCFS and local disk
{{ CLEAN_UP }} DROP FOREIGN TABLE IF EXISTS pxf_parquet_read_numeric CASCADE;
{{ CLEAN_UP }} DROP USER MAPPING IF EXISTS FOR CURRENT_USER SERVER parquet_test_parquet_read_numeric;
{{ CLEAN_UP }} DROP SERVER IF EXISTS parquet_test_parquet_read_numeric CASCADE;
{{ CLEAN_UP }}\!{{ HCFS_CMD }} --config {{ PXF_BASE }}/servers/{{ SERVER_CONFIG }}/ dfs -rm -r -f {{ HCFS_SCHEME }}{{ HCFS_BUCKET }}{{ TEST_LOCATION }}/parquet/numeric.parquet
{{ CLEAN_UP }}{{ GPDB_REMOTE }}\!ssh {{ PGHOST }} rm -rf {{ TEST_LOCATION }}
-- end_ignore