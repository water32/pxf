-- @description query01 for JDBC named queries
-- start_ignore

-- Create database server and copy the jdbc-site.xml template and testuser-user.xml template.
-- Replace Driver, JDBC URL, user name, password configurations in jdbc-site.xml
-- Copy the report.sql file to the server
\!mkdir -p {{ PXF_CONF }}/servers/database
\!if [ ! -f {{ PXF_CONF }}/servers/database/jdbc-site.xml ]; then cp {{ PXF_CONF }}/templates/jdbc-site.xml {{ PXF_CONF }}/servers/database/; sed {{ SED_OPTS }} -e "s|YOUR_DATABASE_JDBC_DRIVER_CLASS_NAME|org.postgresql.Driver|" -e "s|YOUR_DATABASE_JDBC_URL|jdbc:postgresql://{{ PGHOST }}:{{ PGPORT }}/regression|" -e "s|YOUR_DATABASE_JDBC_USER||" -e "s|YOUR_DATABASE_JDBC_PASSWORD||" {{ PXF_CONF }}/servers/database/jdbc-site.xml; fi
\!if [ ! -f {{ PXF_CONF }}/servers/database/testuser-user.xml ]; then  cp {{ PXF_CONF }}/servers/database/jdbc-site.xml {{ PXF_CONF }}/servers/database/testuser-user.xml; sed {{ SED_OPTS }} "s|regression|template1|" {{ PXF_CONF }}/servers/database/testuser-user.xml; fi
\!cp {{ WORKING_DIR }}/resources/data/jdbc/report.sql {{ PXF_CONF }}/servers/database

DROP TABLE IF EXISTS gpdb_dept CASCADE;

CREATE TABLE gpdb_dept
(
    name text,
    id   int
) DISTRIBUTED BY (name);

INSERT INTO gpdb_dept
VALUES (E'sales', E'1'),
       (E'finance', E'2'),
       (E'it', E'3');

DROP TABLE IF EXISTS gpdb_emp CASCADE;

CREATE TABLE gpdb_emp
(
    name    text,
    dept_id int,
    salary  int
) DISTRIBUTED BY (name);

INSERT INTO gpdb_emp
VALUES (E'alice', E'1', E'115'),
       (E'bob', E'1', E'120'),
       (E'charli', E'1', E'93'),
       (E'daniel', E'2', E'87'),
       (E'emma', E'2', E'100'),
       (E'frank', E'2', E'103'),
       (E'george', E'2', E'90'),
       (E'henry', E'3', E'96'),
       (E'ivanka', E'3', E'70');

-- end_ignore

SELECT gpdb_dept.name, count(*), max(gpdb_emp.salary)
FROM gpdb_dept JOIN gpdb_emp
                    ON gpdb_dept.id = gpdb_emp.dept_id
GROUP BY gpdb_dept.name;

CREATE SERVER jdbc_test_named_query
    FOREIGN DATA WRAPPER jdbc_pxf_fdw
    OPTIONS ( config 'database' );

CREATE USER MAPPING FOR CURRENT_USER SERVER jdbc_test_named_query;

DROP FOREIGN TABLE IF EXISTS pxf_jdbc_read_named_query CASCADE;

CREATE FOREIGN TABLE pxf_jdbc_read_named_query
(
    name  text,
    count int,
    max   int
) SERVER jdbc_test_named_query 
    OPTIONS ( resource 'query:report' );

SELECT * FROM pxf_jdbc_read_named_query ORDER BY name;

SELECT name, count FROM pxf_jdbc_read_named_query WHERE max > 100 ORDER BY name;

SELECT max(max) FROM pxf_jdbc_read_named_query;

DROP FOREIGN TABLE IF EXISTS pxf_jdbc_read_named_query_partitioned CASCADE;

CREATE FOREIGN TABLE pxf_jdbc_read_named_query_partitioned
(
    name  text,
    count int,
    max   int
) SERVER jdbc_test_named_query 
    OPTIONS ( resource 'query:report', partition_by 'count:int', range '1:5', interval '1' );

SELECT * FROM pxf_jdbc_read_named_query_partitioned ORDER BY name;

SELECT name, count FROM pxf_jdbc_read_named_query_partitioned WHERE count > 2 ORDER BY name;

-- start_ignore
{{ CLEAN_UP }}-- clean up JDBC and local disk
{{ CLEAN_UP }} DROP FOREIGN TABLE IF EXISTS pxf_jdbc_read_named_query_partitioned CASCADE;
{{ CLEAN_UP }} DROP FOREIGN TABLE IF EXISTS pxf_jdbc_read_named_query CASCADE;
{{ CLEAN_UP }} DROP USER MAPPING IF EXISTS FOR CURRENT_USER SERVER jdbc_test_named_query;
{{ CLEAN_UP }} DROP SERVER IF EXISTS jdbc_test_named_query CASCADE;
{{ CLEAN_UP }} DROP TABLE IF EXISTS gpdb_emp CASCADE;
{{ CLEAN_UP }} DROP TABLE IF EXISTS gpdb_dept CASCADE;
-- end_ignore
