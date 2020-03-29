-- @description query01 for JDBC query from view
-- start_ignore

-- Create database server and copy the jdbc-site.xml template and testuser-user.xml template.
-- Replace Driver, JDBC URL, user name, password configurations in jdbc-site.xml
-- Copy the report.sql file to the server
\!mkdir -p {{ PXF_CONF }}/servers/database
\!if [ ! -f {{ PXF_CONF }}/servers/database/jdbc-site.xml ]; then cp {{ PXF_CONF }}/templates/jdbc-site.xml {{ PXF_CONF }}/servers/database/; sed {{ SED_OPTS }} -e "s|YOUR_DATABASE_JDBC_DRIVER_CLASS_NAME|org.postgresql.Driver|" -e "s|YOUR_DATABASE_JDBC_URL|jdbc:postgresql://{{ PGHOST }}:{{ PGPORT }}/regression|" -e "s|YOUR_DATABASE_JDBC_USER||" -e "s|YOUR_DATABASE_JDBC_PASSWORD||" {{ PXF_CONF }}/servers/database/jdbc-site.xml; fi
\!if [ ! -f {{ PXF_CONF }}/servers/database/testuser-user.xml ]; then  cp {{ PXF_CONF }}/servers/database/jdbc-site.xml {{ PXF_CONF }}/servers/database/testuser-user.xml; sed {{ SED_OPTS }} "s|regression|template1|" {{ PXF_CONF }}/servers/database/testuser-user.xml; fi
\!cp {{ WORKING_DIR }}/resources/data/jdbc/report.sql {{ PXF_CONF }}/servers/database

-- Create db-session-params database and copy the jdbc-site.xml template.
-- Replace Driver, JDBC URL, user name, password configurations in jdbc-site.xml
-- Add client_min_messages, default_statistics_target session properties
\!mkdir -p {{ PXF_CONF }}/servers/db-session-params
\!if [ ! -f {{ PXF_CONF }}/servers/db-session-params/jdbc-site.xml ]; then cp {{ PXF_CONF }}/templates/jdbc-site.xml {{ PXF_CONF }}/servers/db-session-params/; sed {{ SED_OPTS }} -e "s|YOUR_DATABASE_JDBC_DRIVER_CLASS_NAME|org.postgresql.Driver|" -e "s|YOUR_DATABASE_JDBC_URL|jdbc:postgresql://{{ PGHOST }}:{{ PGPORT }}/regression|" -e "s|YOUR_DATABASE_JDBC_USER||" -e "s|YOUR_DATABASE_JDBC_PASSWORD||" -e "s|</configuration>|<property><name>jdbc.session.property.client_min_messages</name><value>debug1</value></property></configuration>|" -e "s|</configuration>|<property><name>jdbc.session.property.default_statistics_target</name><value>123</value></property></configuration>|" {{ PXF_CONF }}/servers/db-session-params/jdbc-site.xml; fi

-- end_ignore

CREATE SERVER jdbc_test_no_params
    FOREIGN DATA WRAPPER jdbc_pxf_fdw
    OPTIONS ( config 'database' );

CREATE USER MAPPING FOR CURRENT_USER SERVER jdbc_test_no_params;

CREATE SERVER jdbc_test_session_params
    FOREIGN DATA WRAPPER jdbc_pxf_fdw
    OPTIONS ( config 'db-session-params' );

CREATE USER MAPPING FOR CURRENT_USER SERVER jdbc_test_session_params;

DROP FOREIGN TABLE IF EXISTS pxf_jdbc_read_view_no_params CASCADE;

CREATE FOREIGN TABLE pxf_jdbc_read_view_no_params
(
    name    text,
    setting text
) SERVER jdbc_test_no_params
    OPTIONS ( resource 'pg_settings' );

SELECT * FROM pxf_jdbc_read_view_no_params WHERE name='client_min_messages' OR name='default_statistics_target' ORDER BY name;

DROP FOREIGN TABLE IF EXISTS pxf_jdbc_read_view_session_params CASCADE;

CREATE FOREIGN TABLE pxf_jdbc_read_view_session_params
(
    name    text,
    setting text
) SERVER jdbc_test_session_params
    OPTIONS ( resource 'pg_settings' );

SELECT * FROM pxf_jdbc_read_view_session_params WHERE name='client_min_messages' OR name='default_statistics_target' ORDER BY name;

-- start_ignore
{{ CLEAN_UP }}-- clean up JDBC and local disk
{{ CLEAN_UP }} DROP FOREIGN TABLE IF EXISTS pxf_jdbc_read_view_session_params CASCADE;
{{ CLEAN_UP }} DROP FOREIGN TABLE IF EXISTS pxf_jdbc_read_view_no_params CASCADE;
{{ CLEAN_UP }} DROP USER MAPPING IF EXISTS FOR CURRENT_USER SERVER jdbc_test_no_params;
{{ CLEAN_UP }} DROP SERVER IF EXISTS jdbc_test_no_params CASCADE;
{{ CLEAN_UP }} DROP USER MAPPING IF EXISTS FOR CURRENT_USER SERVER jdbc_test_session_params;
{{ CLEAN_UP }} DROP SERVER IF EXISTS jdbc_test_session_params CASCADE;
-- end_ignore