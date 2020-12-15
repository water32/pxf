/* fdw/pxf_fdw--1.0.sql */

-- complain if script is sourced in psql, rather than via CREATE EXTENSION
\echo Use "CREATE EXTENSION pxf_fdw" to load this file. \quit

CREATE FUNCTION pxf_fdw_handler()
RETURNS fdw_handler
AS 'MODULE_PATHNAME'
LANGUAGE C STRICT;

CREATE FUNCTION pxf_fdw_validator(text[], oid)
RETURNS void
AS 'MODULE_PATHNAME'
LANGUAGE C STRICT;

CREATE FUNCTION pxf_fdw_version()
RETURNS pg_catalog.text STRICT
AS 'MODULE_PATHNAME'
LANGUAGE C;

CREATE TABLE pxf_config
(
    segment_id   smallint   NOT NULL,
    pxf_host     text       NOT NULL,
    pxf_port     smallint   NOT NULL,
    pxf_protocol varchar(5) NOT NULL,
    PRIMARY KEY (segment_id)
);

COMMENT ON COLUMN pxf_config.segment_id IS 'The identifier of the segment';
COMMENT ON COLUMN pxf_config.pxf_host IS 'Hostname for the PXF Service that the segment will access';
COMMENT ON COLUMN pxf_config.pxf_port IS 'Port number for the PXF Service that the segment will access';
COMMENT ON COLUMN pxf_config.pxf_protocol IS 'Protocol for the PXF Service (i.e HTTP or HTTPS)';

INSERT INTO pxf_config
SELECT content, 'localhost', 5888, 'http' FROM gp_segment_configuration WHERE role = 'p' AND content > -1;

-- Mark the pxf_config table as a configuration table, which will cause
-- pg_dump to include the table's contents (not its definition) in dumps.
-- Only include non-default configurations.
SELECT pg_catalog.pg_extension_config_dump('pxf_config',
    'WHERE pxf_host <> ''localhost'' AND pxf_port <> 5888 AND pxf_protocol <> ''http''');

CREATE FOREIGN DATA WRAPPER jdbc_pxf_fdw
    HANDLER pxf_fdw_handler
    VALIDATOR pxf_fdw_validator
    OPTIONS ( protocol 'jdbc', mpp_execute 'all segments' );

CREATE FOREIGN DATA WRAPPER hdfs_pxf_fdw
    HANDLER pxf_fdw_handler
    VALIDATOR pxf_fdw_validator
    OPTIONS ( protocol 'hdfs', mpp_execute 'all segments' );

CREATE FOREIGN DATA WRAPPER hive_pxf_fdw
    HANDLER pxf_fdw_handler
    VALIDATOR pxf_fdw_validator
    OPTIONS ( protocol 'hive', mpp_execute 'all segments' );

CREATE FOREIGN DATA WRAPPER hbase_pxf_fdw
    HANDLER pxf_fdw_handler
    VALIDATOR pxf_fdw_validator
    OPTIONS ( protocol 'hbase', mpp_execute 'all segments' );

CREATE FOREIGN DATA WRAPPER s3_pxf_fdw
    HANDLER pxf_fdw_handler
    VALIDATOR pxf_fdw_validator
    OPTIONS ( protocol 's3', mpp_execute 'all segments' );

CREATE FOREIGN DATA WRAPPER gs_pxf_fdw
    HANDLER pxf_fdw_handler
    VALIDATOR pxf_fdw_validator
    OPTIONS ( protocol 'gs', mpp_execute 'all segments' );

CREATE FOREIGN DATA WRAPPER adl_pxf_fdw
    HANDLER pxf_fdw_handler
    VALIDATOR pxf_fdw_validator
    OPTIONS ( protocol 'adl', mpp_execute 'all segments' );

CREATE FOREIGN DATA WRAPPER wasbs_pxf_fdw
    HANDLER pxf_fdw_handler
    VALIDATOR pxf_fdw_validator
    OPTIONS ( protocol 'wasbs', mpp_execute 'all segments' );

CREATE FOREIGN DATA WRAPPER file_pxf_fdw
    HANDLER pxf_fdw_handler
    VALIDATOR pxf_fdw_validator
    OPTIONS ( protocol 'file', mpp_execute 'all segments' );

CREATE FOREIGN DATA WRAPPER demo_pxf_fdw
    HANDLER pxf_fdw_handler
    VALIDATOR pxf_fdw_validator
    OPTIONS ( protocol 'demo', mpp_execute 'all segments' );

COMMENT ON FOREIGN DATA WRAPPER jdbc_pxf_fdw
    IS 'PXF JDBC foreign data wrapper';

COMMENT ON FOREIGN DATA WRAPPER hdfs_pxf_fdw
    IS 'PXF HDFS foreign data wrapper';

COMMENT ON FOREIGN DATA WRAPPER hive_pxf_fdw
    IS 'PXF Hive foreign data wrapper';

COMMENT ON FOREIGN DATA WRAPPER hbase_pxf_fdw
    IS 'PXF HBase foreign data wrapper';

COMMENT ON FOREIGN DATA WRAPPER s3_pxf_fdw
    IS 'PXF AWS S3 foreign data wrapper';

COMMENT ON FOREIGN DATA WRAPPER gs_pxf_fdw
    IS 'PXF Google Cloud Storage (GS) foreign data wrapper';

COMMENT ON FOREIGN DATA WRAPPER adl_pxf_fdw
    IS 'PXF Azure Data Lake (ADL) foreign data wrapper';

COMMENT ON FOREIGN DATA WRAPPER wasbs_pxf_fdw
    IS 'PXF Windows Azure Storage Blob (WASB) foreign data wrapper';

COMMENT ON FOREIGN DATA WRAPPER file_pxf_fdw
    IS 'PXF File foreign data wrapper';

COMMENT ON FOREIGN DATA WRAPPER demo_pxf_fdw
    IS 'PXF Demo foreign data wrapper. Quickly test PXF without configuring any external access';

COMMENT ON FUNCTION pxf_fdw_version()
    IS 'Displays the version of pxf_fdw';
