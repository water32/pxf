-- start_ignore
-- data prep
{{ GPDB_REMOTE }}\!ssh {{ PGHOST }} mkdir -p {{ TEST_LOCATION }}
{{ GPDB_REMOTE }}-- if GPDB is remote, will need to scp file down from there for beeline
{{ GPDB_REMOTE }}\!scp {{ PGHOST }}:{{ TEST_LOCATION }}/data.csv {{ TEST_LOCATION }}
\!{{ HCFS_CMD }} --config {{ PXF_CONF }}/servers/{{ SERVER_CONFIG }}/ dfs -mkdir -p '{{ HCFS_SCHEME }}{{ HCFS_BUCKET }}{{ TEST_LOCATION }}/parquet/'
\!{{ HCFS_CMD }} --config {{ PXF_CONF }}/servers/{{ SERVER_CONFIG }}/ dfs -copyFromLocal '{{ WORKING_DIR }}/resources/data/parquet/parquet_types.parquet' '{{ HCFS_SCHEME }}{{ HCFS_BUCKET }}{{ TEST_LOCATION }}/parquet/parquet_types.parquet'
-- end_ignore

-- sets the date style and bytea output to the expected by the tests
SET datestyle='ISO, MDY';
SET bytea_output='escape';

CREATE SERVER parquet_test_pushdown_server
    FOREIGN DATA WRAPPER {{ HCFS_PROTOCOL }}_pxf_fdw
    OPTIONS (config '{{ SERVER_CONFIG }}');

CREATE USER MAPPING FOR CURRENT_USER SERVER parquet_test_pushdown_server;

DROP FOREIGN TABLE IF EXISTS parquet_types_hcfs_r CASCADE;

CREATE FOREIGN TABLE parquet_types_hcfs_r
(
    id    integer,
    name  text,
    cdate date,
    amt   double precision,
    grade text,
    b     boolean,
    tm    timestamp without time zone,
    bg    bigint,
    bin   bytea,
    sml   smallint,
    r     real,
    vc1   character varying(5),
    c1    character(3),
    dec1  numeric,
    dec2  numeric(5, 2),
    dec3  numeric(13, 5),
    num1  integer
) SERVER parquet_test_pushdown_server
    OPTIONS (resource '{{ HCFS_BUCKET }}{{ TEST_LOCATION }}/parquet/parquet_types.parquet', format 'parquet');

-- no filter
select id, name, cdate, amt, grade, b, tm, bg, bin, sml, r, vc1, c1, dec1, dec2, dec3, num1 from parquet_types_hcfs_r;

-- filter by integer
select id, name, cdate, amt, grade, b, tm, bg, bin, sml, r, vc1, c1, dec1, dec2, dec3, num1 from parquet_types_hcfs_r where num1 = 11;

select id, name, cdate, amt, grade, b, tm, bg, bin, sml, r, vc1, c1, dec1, dec2, dec3, num1 from parquet_types_hcfs_r where num1 < 11;

select id, name, cdate, amt, grade, b, tm, bg, bin, sml, r, vc1, c1, dec1, dec2, dec3, num1 from parquet_types_hcfs_r where num1 > 11;

select id, name, cdate, amt, grade, b, tm, bg, bin, sml, r, vc1, c1, dec1, dec2, dec3, num1 from parquet_types_hcfs_r where num1 <= 11;

select id, name, cdate, amt, grade, b, tm, bg, bin, sml, r, vc1, c1, dec1, dec2, dec3, num1 from parquet_types_hcfs_r where num1 >= 11;

select id, name, cdate, amt, grade, b, tm, bg, bin, sml, r, vc1, c1, dec1, dec2, dec3, num1 from parquet_types_hcfs_r where num1 <> 11;

select id, name, cdate, amt, grade, b, tm, bg, bin, sml, r, vc1, c1, dec1, dec2, dec3, num1 from parquet_types_hcfs_r where num1 is null;

select id, name, cdate, amt, grade, b, tm, bg, bin, sml, r, vc1, c1, dec1, dec2, dec3, num1 from parquet_types_hcfs_r where num1 is not null;

-- filter by bigint
select id, name, cdate, amt, grade, b, tm, bg, bin, sml, r, vc1, c1, dec1, dec2, dec3, num1 from parquet_types_hcfs_r where bg = 2147483655;

select id, name, cdate, amt, grade, b, tm, bg, bin, sml, r, vc1, c1, dec1, dec2, dec3, num1 from parquet_types_hcfs_r where bg < 0;

select id, name, cdate, amt, grade, b, tm, bg, bin, sml, r, vc1, c1, dec1, dec2, dec3, num1 from parquet_types_hcfs_r where bg > 2147483655;

select id, name, cdate, amt, grade, b, tm, bg, bin, sml, r, vc1, c1, dec1, dec2, dec3, num1 from parquet_types_hcfs_r where bg <= -2147483643;

select id, name, cdate, amt, grade, b, tm, bg, bin, sml, r, vc1, c1, dec1, dec2, dec3, num1 from parquet_types_hcfs_r where bg >= 2147483655;

select id, name, cdate, amt, grade, b, tm, bg, bin, sml, r, vc1, c1, dec1, dec2, dec3, num1 from parquet_types_hcfs_r where bg <> -1;

select id, name, cdate, amt, grade, b, tm, bg, bin, sml, r, vc1, c1, dec1, dec2, dec3, num1 from parquet_types_hcfs_r where bg is null;

select id, name, cdate, amt, grade, b, tm, bg, bin, sml, r, vc1, c1, dec1, dec2, dec3, num1 from parquet_types_hcfs_r where bg is not null;

-- filter by real
select id, name, cdate, amt, grade, b, tm, bg, bin, sml, r, vc1, c1, dec1, dec2, dec3, num1 from parquet_types_hcfs_r where r = 8.7;

select id, name, cdate, amt, grade, b, tm, bg, bin, sml, r, vc1, c1, dec1, dec2, dec3, num1 from parquet_types_hcfs_r where r < 8.7;

select id, name, cdate, amt, grade, b, tm, bg, bin, sml, r, vc1, c1, dec1, dec2, dec3, num1 from parquet_types_hcfs_r where r > 8.7;

select id, name, cdate, amt, grade, b, tm, bg, bin, sml, r, vc1, c1, dec1, dec2, dec3, num1 from parquet_types_hcfs_r where r <= 8.7;

select id, name, cdate, amt, grade, b, tm, bg, bin, sml, r, vc1, c1, dec1, dec2, dec3, num1 from parquet_types_hcfs_r where r >= 8.7;

select id, name, cdate, amt, grade, b, tm, bg, bin, sml, r, vc1, c1, dec1, dec2, dec3, num1 from parquet_types_hcfs_r where r <> 8.7;

select id, name, cdate, amt, grade, b, tm, bg, bin, sml, r, vc1, c1, dec1, dec2, dec3, num1 from parquet_types_hcfs_r where r is null;

select id, name, cdate, amt, grade, b, tm, bg, bin, sml, r, vc1, c1, dec1, dec2, dec3, num1 from parquet_types_hcfs_r where r is not null;

-- filter by text
select id, name, cdate, amt, grade, b, tm, bg, bin, sml, r, vc1, c1, dec1, dec2, dec3, num1 from parquet_types_hcfs_r where grade = 'excellent';

select id, name, cdate, amt, grade, b, tm, bg, bin, sml, r, vc1, c1, dec1, dec2, dec3, num1 from parquet_types_hcfs_r where grade < 'excellent';

select id, name, cdate, amt, grade, b, tm, bg, bin, sml, r, vc1, c1, dec1, dec2, dec3, num1 from parquet_types_hcfs_r where grade > 'excellent';

select id, name, cdate, amt, grade, b, tm, bg, bin, sml, r, vc1, c1, dec1, dec2, dec3, num1 from parquet_types_hcfs_r where grade <= 'excellent';

select id, name, cdate, amt, grade, b, tm, bg, bin, sml, r, vc1, c1, dec1, dec2, dec3, num1 from parquet_types_hcfs_r where grade >= 'excellent';

select id, name, cdate, amt, grade, b, tm, bg, bin, sml, r, vc1, c1, dec1, dec2, dec3, num1 from parquet_types_hcfs_r where grade <> 'excellent';

select id, name, cdate, amt, grade, b, tm, bg, bin, sml, r, vc1, c1, dec1, dec2, dec3, num1 from parquet_types_hcfs_r where grade is null;

select id, name, cdate, amt, grade, b, tm, bg, bin, sml, r, vc1, c1, dec1, dec2, dec3, num1 from parquet_types_hcfs_r where grade is not null;

-- filter by varchar
select id, name, cdate, amt, grade, b, tm, bg, bin, sml, r, vc1, c1, dec1, dec2, dec3, num1 from parquet_types_hcfs_r where vc1 = 's_16';

select id, name, cdate, amt, grade, b, tm, bg, bin, sml, r, vc1, c1, dec1, dec2, dec3, num1 from parquet_types_hcfs_r where vc1 < 's_10';

select id, name, cdate, amt, grade, b, tm, bg, bin, sml, r, vc1, c1, dec1, dec2, dec3, num1 from parquet_types_hcfs_r where vc1 > 's_168';

select id, name, cdate, amt, grade, b, tm, bg, bin, sml, r, vc1, c1, dec1, dec2, dec3, num1 from parquet_types_hcfs_r where vc1 <= 's_10';

select id, name, cdate, amt, grade, b, tm, bg, bin, sml, r, vc1, c1, dec1, dec2, dec3, num1 from parquet_types_hcfs_r where vc1 >= 's_168';

select id, name, cdate, amt, grade, b, tm, bg, bin, sml, r, vc1, c1, dec1, dec2, dec3, num1 from parquet_types_hcfs_r where vc1 <> 's_16';

select id, name, cdate, amt, grade, b, tm, bg, bin, sml, r, vc1, c1, dec1, dec2, dec3, num1 from parquet_types_hcfs_r where vc1 IS NULL;

select id, name, cdate, amt, grade, b, tm, bg, bin, sml, r, vc1, c1, dec1, dec2, dec3, num1 from parquet_types_hcfs_r where vc1 IS NOT NULL;

-- filter by char
select id, name, cdate, amt, grade, b, tm, bg, bin, sml, r, vc1, c1, dec1, dec2, dec3, num1 from parquet_types_hcfs_r where c1 = 'EUR';

select id, name, cdate, amt, grade, b, tm, bg, bin, sml, r, vc1, c1, dec1, dec2, dec3, num1 from parquet_types_hcfs_r where c1 < 'USD';

select id, name, cdate, amt, grade, b, tm, bg, bin, sml, r, vc1, c1, dec1, dec2, dec3, num1 from parquet_types_hcfs_r where c1 > 'EUR';

select id, name, cdate, amt, grade, b, tm, bg, bin, sml, r, vc1, c1, dec1, dec2, dec3, num1 from parquet_types_hcfs_r where c1 <= 'EUR';

select id, name, cdate, amt, grade, b, tm, bg, bin, sml, r, vc1, c1, dec1, dec2, dec3, num1 from parquet_types_hcfs_r where c1 >= 'USD';

select id, name, cdate, amt, grade, b, tm, bg, bin, sml, r, vc1, c1, dec1, dec2, dec3, num1 from parquet_types_hcfs_r where c1 <> 'USD';

select id, name, cdate, amt, grade, b, tm, bg, bin, sml, r, vc1, c1, dec1, dec2, dec3, num1 from parquet_types_hcfs_r where c1 IS NULL;

select id, name, cdate, amt, grade, b, tm, bg, bin, sml, r, vc1, c1, dec1, dec2, dec3, num1 from parquet_types_hcfs_r where c1 IS NOT NULL;

-- filter by smallint
select id, name, cdate, amt, grade, b, tm, bg, bin, sml, r, vc1, c1, dec1, dec2, dec3, num1 from parquet_types_hcfs_r where sml = 1000;

select id, name, cdate, amt, grade, b, tm, bg, bin, sml, r, vc1, c1, dec1, dec2, dec3, num1 from parquet_types_hcfs_r where sml < -1000;

select id, name, cdate, amt, grade, b, tm, bg, bin, sml, r, vc1, c1, dec1, dec2, dec3, num1 from parquet_types_hcfs_r where sml > 31000;

select id, name, cdate, amt, grade, b, tm, bg, bin, sml, r, vc1, c1, dec1, dec2, dec3, num1 from parquet_types_hcfs_r where sml <= 0;

select id, name, cdate, amt, grade, b, tm, bg, bin, sml, r, vc1, c1, dec1, dec2, dec3, num1 from parquet_types_hcfs_r where sml >= 0;

select id, name, cdate, amt, grade, b, tm, bg, bin, sml, r, vc1, c1, dec1, dec2, dec3, num1 from parquet_types_hcfs_r where sml <> 0;

select id, name, cdate, amt, grade, b, tm, bg, bin, sml, r, vc1, c1, dec1, dec2, dec3, num1 from parquet_types_hcfs_r where sml IS NULL;

select id, name, cdate, amt, grade, b, tm, bg, bin, sml, r, vc1, c1, dec1, dec2, dec3, num1 from parquet_types_hcfs_r where sml IS NOT NULL;

-- filter by date
select id, name, cdate, amt, grade, b, tm, bg, bin, sml, r, vc1, c1, dec1, dec2, dec3, num1 from parquet_types_hcfs_r where cdate = '2019-12-04';

select id, name, cdate, amt, grade, b, tm, bg, bin, sml, r, vc1, c1, dec1, dec2, dec3, num1 from parquet_types_hcfs_r where cdate < '2019-12-04';

select id, name, cdate, amt, grade, b, tm, bg, bin, sml, r, vc1, c1, dec1, dec2, dec3, num1 from parquet_types_hcfs_r where cdate > '2019-12-20';

select id, name, cdate, amt, grade, b, tm, bg, bin, sml, r, vc1, c1, dec1, dec2, dec3, num1 from parquet_types_hcfs_r where cdate <= '2019-12-06';

select id, name, cdate, amt, grade, b, tm, bg, bin, sml, r, vc1, c1, dec1, dec2, dec3, num1 from parquet_types_hcfs_r where cdate >= '2019-12-15';

select id, name, cdate, amt, grade, b, tm, bg, bin, sml, r, vc1, c1, dec1, dec2, dec3, num1 from parquet_types_hcfs_r where cdate <> '2019-12-15';

select id, name, cdate, amt, grade, b, tm, bg, bin, sml, r, vc1, c1, dec1, dec2, dec3, num1 from parquet_types_hcfs_r where cdate IS NULL;

select id, name, cdate, amt, grade, b, tm, bg, bin, sml, r, vc1, c1, dec1, dec2, dec3, num1 from parquet_types_hcfs_r where cdate IS NOT NULL;

-- filter by float8
select id, name, cdate, amt, grade, b, tm, bg, bin, sml, r, vc1, c1, dec1, dec2, dec3, num1 from parquet_types_hcfs_r where amt = 1200;

select id, name, cdate, amt, grade, b, tm, bg, bin, sml, r, vc1, c1, dec1, dec2, dec3, num1 from parquet_types_hcfs_r where amt < 1500;

select id, name, cdate, amt, grade, b, tm, bg, bin, sml, r, vc1, c1, dec1, dec2, dec3, num1 from parquet_types_hcfs_r where amt > 2500;

select id, name, cdate, amt, grade, b, tm, bg, bin, sml, r, vc1, c1, dec1, dec2, dec3, num1 from parquet_types_hcfs_r where amt <= 1500;

select id, name, cdate, amt, grade, b, tm, bg, bin, sml, r, vc1, c1, dec1, dec2, dec3, num1 from parquet_types_hcfs_r where amt >= 2550;

select id, name, cdate, amt, grade, b, tm, bg, bin, sml, r, vc1, c1, dec1, dec2, dec3, num1 from parquet_types_hcfs_r where amt <> 1200;

select id, name, cdate, amt, grade, b, tm, bg, bin, sml, r, vc1, c1, dec1, dec2, dec3, num1 from parquet_types_hcfs_r where amt IS NULL;

select id, name, cdate, amt, grade, b, tm, bg, bin, sml, r, vc1, c1, dec1, dec2, dec3, num1 from parquet_types_hcfs_r where amt IS NOT NULL;

-- filter by bytea
select id, name, cdate, amt, grade, b, tm, bg, bin, sml, r, vc1, c1, dec1, dec2, dec3, num1 from parquet_types_hcfs_r where bin = '1';

select id, name, cdate, amt, grade, b, tm, bg, bin, sml, r, vc1, c1, dec1, dec2, dec3, num1 from parquet_types_hcfs_r where bin < '1';

select id, name, cdate, amt, grade, b, tm, bg, bin, sml, r, vc1, c1, dec1, dec2, dec3, num1 from parquet_types_hcfs_r where bin > '1';

select id, name, cdate, amt, grade, b, tm, bg, bin, sml, r, vc1, c1, dec1, dec2, dec3, num1 from parquet_types_hcfs_r where bin <= '1';

select id, name, cdate, amt, grade, b, tm, bg, bin, sml, r, vc1, c1, dec1, dec2, dec3, num1 from parquet_types_hcfs_r where bin >= '1';

select id, name, cdate, amt, grade, b, tm, bg, bin, sml, r, vc1, c1, dec1, dec2, dec3, num1 from parquet_types_hcfs_r where bin <> '1';

select id, name, cdate, amt, grade, b, tm, bg, bin, sml, r, vc1, c1, dec1, dec2, dec3, num1 from parquet_types_hcfs_r where bin IS NULL;

select id, name, cdate, amt, grade, b, tm, bg, bin, sml, r, vc1, c1, dec1, dec2, dec3, num1 from parquet_types_hcfs_r where bin IS NOT NULL;

-- filter by id column with projection
select id from parquet_types_hcfs_r where id = 5;

select name, cdate, amt, sml, num1 from parquet_types_hcfs_r where id = 8 or (id > 10 and grade = 'bad');

select bin, bg, tm from parquet_types_hcfs_r where id = 15;

-- filter by date and amt
select id, name, cdate, amt, grade, b, tm, bg, bin, sml, r, vc1, c1, dec1, dec2, dec3, num1 from parquet_types_hcfs_r where cdate > '2019-12-02' and cdate < '2019-12-12' and amt > 1500;

-- filter by date with or and amt
select id, name, cdate, amt, grade, b, tm, bg, bin, sml, r, vc1, c1, dec1, dec2, dec3, num1 from parquet_types_hcfs_r where cdate > '2019-12-19' OR ( cdate <= '2019-12-15' and amt > 2000);

-- filter by date with or and amt using column projection
select id, amt, b from parquet_types_hcfs_r where cdate > '2019-12-19' OR ( cdate <= '2019-12-15' and amt > 2000);

-- filter by date or amt
select id, name, cdate, amt, grade, b, tm, bg, bin, sml, r, vc1, c1, dec1, dec2, dec3, num1 from parquet_types_hcfs_r where cdate > '2019-12-20' OR amt < 1500;

-- filter by timestamp (not pushed)
select id, name, cdate, amt, grade, b, tm, bg, bin, sml, r, vc1, c1, dec1, dec2, dec3, num1 from parquet_types_hcfs_r where CAST(tm AS TIMESTAMP WITH TIME ZONE) AT TIME ZONE 'PDT' = '2013-07-23 21:00:00';

-- filter by decimal (not pushed)
select id, name, cdate, amt, grade, b, tm, bg, bin, sml, r, vc1, c1, dec1, dec2, dec3, num1 from parquet_types_hcfs_r where dec2 = 0;

-- filter by in (not pushed)
select id, name, cdate, amt, grade, b, tm, bg, bin, sml, r, vc1, c1, dec1, dec2, dec3, num1 from parquet_types_hcfs_r where num1 in (11, 12);

-- start_ignore
{{ CLEAN_UP }}-- clean up HCFS and local disk
{{ CLEAN_UP }} DROP FOREIGN TABLE IF EXISTS parquet_types_hcfs_r CASCADE;
{{ CLEAN_UP }} DROP USER MAPPING IF EXISTS FOR CURRENT_USER SERVER parquet_test_pushdown_server;
{{ CLEAN_UP }} DROP SERVER IF EXISTS parquet_test_pushdown_server CASCADE;
{{ CLEAN_UP }}\!{{ HCFS_CMD }} --config {{ PXF_CONF }}/servers/{{ SERVER_CONFIG }}/ dfs -rm -r -f {{ HCFS_SCHEME }}{{ HCFS_BUCKET }}{{ TEST_LOCATION }}/parquet/parquet_types.parquet
{{ CLEAN_UP }}{{ GPDB_REMOTE }}\!ssh {{ PGHOST }} rm -rf {{ TEST_LOCATION }}
-- end_ignore