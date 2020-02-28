DROP TABLE IF EXISTS hive_demo_data_table;

-- Create Hive table
CREATE TABLE hive_demo_data_table (
	location string,
	month string,
	number_of_orders int,
	total_sales double
	)
	ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
	STORED AS textfile;

-- Load data into new Hive table
LOAD DATA LOCAL INPATH 'examples/hdfs_hive/sales.csv'
	INTO TABLE  hive_demo_data_table;
