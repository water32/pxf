-- @description query01 for PXF HDFS Readable images

SELECT image_test_small_batchsize.names, image_test_small_batchsize.directories FROM image_test_small_batchsize, compare_table_small_batchsize
    WHERE image_test_small_batchsize.fullpaths = compare_table_small_batchsize.fullpaths
    AND image_test_small_batchsize.directories = compare_table_small_batchsize.directories
    AND image_test_small_batchsize.names = compare_table_small_batchsize.names
    AND image_test_small_batchsize.images = compare_table_small_batchsize.images
    ORDER BY names;
