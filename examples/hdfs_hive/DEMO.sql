----------------------------------------------------------------------------------------------------------
----  READING HDFS ---------------------------------------------------------------------------------------
----------------------------------------------------------------------------------------------------------

-- do data prep
\! hdfs dfs -mkdir -p /tmp/data/hdfs
\! hdfs dfs -rm /tmp/data/hdfs/sales.csv
\! hdfs dfs -copyFromLocal ~/workspace/pxf/examples/hdfs_hive/sales.csv /tmp/data/hdfs

DROP EXTERNAL TABLE IF EXISTS salesinfo_hdfsprofile;
CREATE EXTERNAL TABLE salesinfo_hdfsprofile(location text, month text, num_orders int, total_sales float8)
	LOCATION ('pxf:///tmp/data/hdfs/sales.csv?profile=hdfs:csv')
	FORMAT 'CSV';
SELECT * FROM salesinfo_hdfsprofile;

-- USE HEAP TABLE
DROP TABLE IF EXISTS heap_table;
SELECT * INTO heap_table FROM salesinfo_hdfsprofile;

SELECT * FROM heap_table;

----------------------------------------------------------------------------------------------------------
----  READING HIVE ---------------------------------------------------------------------------------------
----------------------------------------------------------------------------------------------------------

\! hive -f examples/hdfs_hive/hive_script.sql

DROP EXTERNAL TABLE IF EXISTS salesinfo_hiveprofile;
CREATE EXTERNAL TABLE salesinfo_hiveprofile(location text, month text, num_orders int, total_sales float8)
	LOCATION ('pxf://default.hive_demo_data_table?profile=Hive')
	FORMAT 'custom' (FORMATTER='pxfwritable_import'); -- note the different format
SELECT * FROM salesinfo_hiveprofile;

----------------------------------------------------------------------------------------------------------
----  WRITING TO HDFS  -----------------------------------------------------------------------------------
----------------------------------------------------------------------------------------------------------

-- do data prep
\! hdfs dfs -rm -r -f /tmp/data/hdfs/write_demo
\! hdfs dfs -mkdir -p /tmp/data/hdfs/write_demo

DROP EXTERNAL TABLE IF EXISTS salesinfo_writehdfsprofile;
CREATE WRITABLE EXTERNAL TABLE salesinfo_writehdfsprofile(location text, month text, num_orders int, total_sales float8)
	LOCATION ('pxf:///tmp/data/hdfs/write_demo?profile=hdfs:csv')
	FORMAT 'CSV';
INSERT INTO salesinfo_writehdfsprofile VALUES
	('Mountain View','Feb',1000,3.14159),
	('New York','Mar',20000,31.4159),
	('Berlin','Apr',30,314.159),
	('Tokyo','Apr',30,314.159),
	('Irving','May',3000,31415.9),
	('Cairo','Aug',30,314.159),
	('London','May',400,3141.59),
	('Hackensack','Sep',100,3.14159),
	('Atlanta','Dec',10,3.14159),
	('Washington','Nov',25,314.159),
	('Hong Kong','Jun',50000,31415.9);

\! hdfs dfs -ls /tmp/data/hdfs
\! hdfs dfs -ls /tmp/data/hdfs/write_demo
\! rm -rf /tmp/write_demo
\! hdfs dfs -copyToLocal /tmp/data/hdfs/write_demo /tmp/write_demo

DROP EXTERNAL TABLE IF EXISTS salesinfo_fromwrite;
CREATE EXTERNAL TABLE salesinfo_fromwrite(location text, month text, num_orders int, total_sales float8)
	LOCATION ('pxf:///tmp/data/hdfs/write_demo?profile=hdfs:csv')
	FORMAT 'CSV';
SELECT * FROM salesinfo_fromwrite;

DROP EXTERNAL TABLE IF EXISTS salesinfo_fromwrite;
CREATE EXTERNAL TABLE salesinfo_fromwrite(location text, month text, num_orders int, total_sales float8)
	LOCATION ('pxf:///tmp/data/hdfs?profile=hdfs:csv')
	FORMAT 'CSV';
SELECT * FROM salesinfo_fromwrite;

----------------------------------------------------------------------------------------------------------
----  S3  ------------------------------------------------------------------------------------------------
----------------------------------------------------------------------------------------------------------

\! aws s3 rm --recursive s3://gpdb-ud-scratch/tmp/data/hdfs/write_demo
\! aws s3 ls s3://gpdb-ud-scratch/tmp/data/hdfs/write_demo/
\! aws s3 cp ~/workspace/pxf/examples/hdfs_hive/sales.csv s3://gpdb-ud-scratch/tmp/data/hdfs/write_demo/

DROP EXTERNAL TABLE IF EXISTS salesinfo_froms3write;
CREATE EXTERNAL TABLE salesinfo_froms3write(location text, month text, num_orders int, total_sales float8)
	LOCATION ('pxf://gpdb-ud-scratch/tmp/data/hdfs/write_demo?profile=s3:csv&server=s3')
	FORMAT 'CSV';
SELECT * FROM salesinfo_froms3write;

DROP EXTERNAL TABLE IF EXISTS salesinfo_writes3profile;
CREATE WRITABLE EXTERNAL TABLE salesinfo_writes3profile(location text, month text, num_orders int, total_sales float8)
	LOCATION ('pxf://gpdb-ud-scratch/tmp/data/hdfs/write_demo?profile=s3:csv&server=s3')
	FORMAT 'CSV';
INSERT INTO salesinfo_writes3profile VALUES
	('Mountain View','Feb',1000,3.14159),
	('New York','Mar',20000,31.4159),
	('Berlin','Apr',30,314.159),
	('Tokyo','Apr',30,314.159),
	('Irving','May',3000,31415.9),
	('Cairo','Aug',30,314.159),
	('London','May',400,3141.59),
	('Hackensack','Sep',100,3.14159),
	('Atlanta','Dec',10,3.14159),
	('Washington','Nov',25,314.159),
	('Hong Kong','Jun',50000,31415.9);

----------------------------------------------------------------------------------------------------------
----  S3 AND PREDICATES ----------------------------------------------------------------------------------
----------------------------------------------------------------------------------------------------------

SELECT * FROM salesinfo_froms3write WHERE month = 'Dec';

DROP EXTERNAL TABLE IF EXISTS salesinfo_s3select;
CREATE EXTERNAL TABLE salesinfo_s3select(location text, month text, num_orders int, total_sales float8)
	LOCATION ('pxf://gpdb-ud-scratch/tmp/data/hdfs/write_demo?profile=s3:csv&server=s3&s3_select=on')
	FORMAT 'CSV';
SELECT * FROM salesinfo_s3select WHERE month = 'Dec'; -- select happens on S3





























----------------------------------------------------------------------------------------------------------
----  AVRO  ----------------------------------------------------------------------------------------------
----------------------------------------------------------------------------------------------------------

\! hdfs dfs -rm -r -f /tmp/data/avro
\! hdfs dfs -mkdir -p /tmp/data/avro
\! hdfs dfs -copyFromLocal sql/gpdb6/avro_demo/schema*.av* /tmp/data/avro
\! hdfs dfs -ls /tmp/data/avro
\! hive -f examples/hdfs_hive/hive_avro_script.sql

DROP EXTERNAL TABLE IF EXISTS salesinfo_hiveavroprofile;
CREATE EXTERNAL TABLE salesinfo_hiveavroprofile(id INT, name TEXT, user_id TEXT, favorite_music TEXT)
	LOCATION ('pxf://default.hive_avro_demo_data_table?profile=Hive')
	FORMAT 'custom' (FORMATTER='pxfwritable_import');
SELECT * FROM salesinfo_hiveavroprofile;
