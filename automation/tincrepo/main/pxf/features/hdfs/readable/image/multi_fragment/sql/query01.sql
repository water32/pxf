-- @description query01 for PXF HDFS Readable images

SELECT image_test_multi_fragment.names, image_test_multi_fragment.directories FROM image_test_multi_fragment, compare_table_multi_fragment
    WHERE image_test_multi_fragment.fullpaths = compare_table_multi_fragment.fullpaths
    AND image_test_multi_fragment.directories = compare_table_multi_fragment.directories
    AND image_test_multi_fragment.names = compare_table_multi_fragment.names
    AND image_test_multi_fragment.images = compare_table_multi_fragment.images
    ORDER BY names;
