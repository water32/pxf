# Accessing Hive text format with PXF

This is mostly just following the Greenplum PXF docs for [HDFS](https://gpdb.docs.pivotal.io/6-4/pxf/hdfs_text.html) and [Hive](https://gpdb.docs.pivotal.io/6-4/pxf/hive_pxf.html), the commands are a bit more eng-friendly.

## Prep data and processes

Start up Hive, HDFS, Greenplum, and PXF:

```bash
~/workspace/singlecluster/bin/start-hdfs.sh
~/workspace/singlecluster/bin/start-hive.sh
gpstart -a
pxf start
```

There is some sample data in `~/workspace/pxf/examples/hdfs_hive/sales.csv`:

```bash
cat ~/workspace/pxf/examples/hdfs_hive/sales.csv
Prague,Jan,101,4875.33
Rome,Mar,87,1557.39
Bangalore,May,317,8936.99
Beijing,Jul,411,11600.67
San Francisco,Sept,156,6846.34
Paris,Nov,159,7134.56
San Francisco,Jan,113,5397.89
Prague,Dec,333,9894.77
Bangalore,Jul,271,8320.55
Beijing,Dec,100,4248.41
```

------------------------

## HDFS

```bash
~/workspace/singlecluster/bin/hdfs dfs -copyFromLocal ~/workspace/pxf/examples/hdfs_hive/sales.csv /tmp/data.csv
```

In Greenplum, create the external table using PXF protocol, and query the HDFS data.

```SQL
DROP EXTERNAL TABLE IF EXISTS salesinfo_hdfsprofile;
CREATE EXTERNAL TABLE salesinfo_hdfsprofile(location text, month text, num_orders int, total_sales float8)
	LOCATION ('pxf:///tmp/data.csv?profile=hdfs:csv')
	FORMAT 'CSV';
SELECT * FROM salesinfo_hdfsprofile;
   location    | month | num_orders | total_sales
---------------+-------+------------+-------------
 Prague        | Jan   |        101 |     4875.33
 Rome          | Mar   |         87 |     1557.39
 Bangalore     | May   |        317 |     8936.99
 Beijing       | Jul   |        411 |    11600.67
 San Francisco | Sept  |        156 |     6846.34
 Paris         | Nov   |        159 |     7134.56
 San Francisco | Jan   |        113 |     5397.89
 Prague        | Dec   |        333 |     9894.77
 Bangalore     | Jul   |        271 |     8320.55
 Beijing       | Dec   |        100 |     4248.41
(10 rows)
```

------------------------

## Hive

Look at this SQL file in `~/workspace/pxf/examples/hdfs_hive/hive_script.sql`, you can run this inside the `hive` interpreter:

```sql
CREATE TABLE hive_demo_data_table (
	location string,
	month string,
	number_of_orders int,
	total_sales double
	)
	ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
	STORED AS textfile;
LOAD DATA LOCAL INPATH 'examples/hdfs_hive/sales.csv'
	INTO TABLE hive_demo_data_table;
```

Run the Hive script:

```bash
cd ~/workspace/pxf # script has a relative path, so must be run from here
~/workspace/singlecluster/bin/hive -f ~/workspace/pxf/examples/hdfs_hive/hive_script.sql
```

In Greenplum, create the external table using PXF protocol, but this time use Hive profile instead of `hdfs:csv`, referencing the Hive table:

```SQL
DROP EXTERNAL TABLE IF EXISTS salesinfo_hiveprofile;
CREATE EXTERNAL TABLE salesinfo_hiveprofile(location text, month text, num_orders int, total_sales float8)
	LOCATION ('pxf://default.hive_demo_data_table?profile=Hive')
	FORMAT 'custom' (FORMATTER='pxfwritable_import');
SELECT * FROM salesinfo_hiveprofile;
   location    | month | num_orders | total_sales
---------------+-------+------------+-------------
 Prague        | Jan   |        101 |     4875.33
 Rome          | Mar   |         87 |     1557.39
 Bangalore     | May   |        317 |     8936.99
 Beijing       | Jul   |        411 |    11600.67
 San Francisco | Sept  |        156 |     6846.34
 Paris         | Nov   |        159 |     7134.56
 San Francisco | Jan   |        113 |     5397.89
 Prague        | Dec   |        333 |     9894.77
 Bangalore     | Jul   |        271 |     8320.55
 Beijing       | Dec   |        100 |     4248.41
(10 rows)
```

------------------------

## Hive Avro

The SQL file to create a Hive table based on the binary Avro file provided in `~/workspace/examples/hdfs_hive/schema1.avro`:

```SQL
DROP TABLE IF EXISTS hive_avro_demo_data_table;
CREATE TABLE hive_avro_demo_data_table (
	id INT,
	name STRING,
	user_id STRING,
	favorite_music STRING
	)
	ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.avro.AvroSerDe'
	STORED AS INPUTFORMAT 'org.apache.hadoop.hive.ql.io.avro.AvroContainerInputFormat'
	OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.avro.AvroContainerOutputFormat';
LOAD DATA LOCAL INPATH 'examples/hdfs_hive/schema1.avro'
	INTO TABLE hive_avro_demo_data_table;
```

Run the Hive script:

```bash
cd ~/workspace/pxf # script has a relative path, so must be run from here
~/workspace/singlecluster/bin/hive -f ~/workspace/pxf/examples/hdfs_hive/hive_avro_script.sql
```

In Greenplum:

```SQL
DROP EXTERNAL TABLE IF EXISTS salesinfo_hiveavroprofile;
CREATE EXTERNAL TABLE salesinfo_hiveavroprofile(id INT, name TEXT, user_id TEXT, favorite_music TEXT)
	LOCATION ('pxf://default.hive_avro_demo_data_table?profile=Hive')
	FORMAT 'custom' (FORMATTER='pxfwritable_import');
SELECT * FROM salesinfo_hiveavroprofile;
 id |   name    |  user_id  | favorite_music
----+-----------+-----------+----------------
  1 | alex      | adenissov | mozart
  4 | venkatesh | vraghavan | amy winehouse
(2 rows)
```
