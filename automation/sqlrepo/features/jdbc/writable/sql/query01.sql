-- @description query01 for JDBC writable query
SET timezone='America/Los_Angeles';

ALTER TABLE gpdb_types_target ADD CONSTRAINT gpdb_types_target_t1_key UNIQUE (t1);

INSERT INTO pxf_jdbc_writable SELECT * FROM gpdb_types;

SELECT * FROM gpdb_types_target ORDER BY t1;

-- start_matchsubs
--
-- # create a match/subs
--
-- m/\/gpdb\/v\d+\//
-- s/v\d+/SOME_VERSION/
--
-- m/file:.*;/
-- s/file:.*; lineNumber: \d+; columnNumber: \d+;/SOME_ERROR_LOCATION/g
--
-- m/DETAIL/
-- s/DETAIL/CONTEXT/
--
-- m/pxf:\/\/(.*)\/pxf_automation_data/
-- s/pxf:\/\/.*\/pxf_automation_data/pxf:\/\/pxf_automation_data/
--
-- m/CONTEXT:.*line.*/
-- s/line \d* of //g
--
-- end_matchsubs
INSERT INTO pxf_jdbc_writable SELECT * FROM gpdb_types;
