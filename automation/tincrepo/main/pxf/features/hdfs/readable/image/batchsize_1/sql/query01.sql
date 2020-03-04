-- @description query01 for PXF HDFS Readable images

SELECT image_test_batchsize_1.names, image_test_batchsize_1.directories FROM image_test_batchsize_1, compare_table_batchsize_1
    WHERE image_test_batchsize_1.fullpaths = compare_table_batchsize_1.fullpaths
    AND image_test_batchsize_1.directories = compare_table_batchsize_1.directories
    AND image_test_batchsize_1.names = compare_table_batchsize_1.names
    AND image_test_batchsize_1.images = compare_table_batchsize_1.images
    ORDER BY names;
