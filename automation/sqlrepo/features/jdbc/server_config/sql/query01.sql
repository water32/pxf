-- @description query01 for JDBC query with server config
SET timezone='utc';

SELECT * FROM pxf_jdbc_read_server_config_all ORDER BY t1;

