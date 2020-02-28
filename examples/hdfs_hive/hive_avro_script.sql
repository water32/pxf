DROP TABLE IF EXISTS hive_avro_demo_data_table;
CREATE TABLE hive_avro_demo_data_table (
	id INT,
	name STRING,
	user_id STRING,
	favorite_music STRING
	)
	ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.avro.AvroSerDe'
	STORED AS INPUTFORMAT 'org.apache.hadoop.hive.ql.io.avro.AvroContainerInputFormat'
	OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.avro.AvroContainerOutputFormat'
	TBLPROPERTIES ('avro.schema.url'='hdfs://0.0.0.0:8020/tmp/data/avro/schema1.avsc');
LOAD DATA LOCAL INPATH 'examples/hdfs_hive/schema1.avro'
	INTO TABLE hive_avro_demo_data_table;
