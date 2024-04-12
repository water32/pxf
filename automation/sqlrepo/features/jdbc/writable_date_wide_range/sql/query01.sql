-- @description query01 for JDBC writable query
SET timezone='utc';

INSERT INTO pxf_jdbc_datetime_writable_date_wide_range_on SELECT * FROM gpdb_types_with_date_wide_range;

INSERT INTO pxf_jdbc_datetime_writable_date_wide_range_off SELECT * FROM gpdb_types_with_date_wide_range;

SELECT tm, dt, tmz FROM datetime_writable_with_date_wide_range_on ORDER BY t1;

SELECT tm, dt, tmz FROM datetime_writable_with_date_wide_range_off ORDER BY t1;
