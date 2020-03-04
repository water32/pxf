-- @description query01 for PXF HDFS Readable images

SELECT image_test_single_fragment.names, image_test_single_fragment.directories FROM image_test_single_fragment, compare_table_single_fragment
    WHERE image_test_single_fragment.fullpaths = compare_table_single_fragment.fullpaths
    AND image_test_single_fragment.directories = compare_table_single_fragment.directories
    AND image_test_single_fragment.names = compare_table_single_fragment.names
    AND image_test_single_fragment.images = compare_table_single_fragment.images
    ORDER BY names;
