-- @description query01 for PXF HDFS Readable images

SELECT image_test_large_batchsize.names, image_test_large_batchsize.directories FROM image_test_large_batchsize, compare_table_large_batchsize
    WHERE image_test_large_batchsize.fullpaths = compare_table_large_batchsize.fullpaths
    AND image_test_large_batchsize.directories = compare_table_large_batchsize.directories
    AND image_test_large_batchsize.names = compare_table_large_batchsize.names
    AND image_test_large_batchsize.images = compare_table_large_batchsize.images
    ORDER BY names;
