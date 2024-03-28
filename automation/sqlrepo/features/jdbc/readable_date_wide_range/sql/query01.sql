-- @description query01 for JDBC query with date wide range on
--
-- start_matchsubs
--
-- # create a match/subs
--
-- m/(.*)ERROR:/
-- s/(.*)ERROR:/ERROR:/
--
-- m/DETAIL/
-- s/DETAIL/CONTEXT/
--
-- m/CONTEXT:.*line.*/
-- s/line \d* of //g
--
-- end_matchsubs

SET timezone='utc';

SELECT * FROM pxf_jdbc_readable_date_wide_range_on ORDER BY t1;

SET timezone='America/Los_Angeles';

SELECT tmz FROM pxf_jdbc_readable_date_wide_range_on ORDER BY t1;

SELECT tm, dt, tmz FROM pxf_jdbc_readable_date_wide_range_off ORDER BY t1;
