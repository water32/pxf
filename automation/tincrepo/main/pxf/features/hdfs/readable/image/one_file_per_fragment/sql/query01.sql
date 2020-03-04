-- @description query01 for PXF HDFS Readable images

SELECT image_test_one_file_per_fragment.names, image_test_one_file_per_fragment.directories FROM image_test_one_file_per_fragment, compare_table_one_file_per_fragment
    WHERE image_test_one_file_per_fragment.fullpaths = compare_table_one_file_per_fragment.fullpaths
    AND image_test_one_file_per_fragment.directories = compare_table_one_file_per_fragment.directories
    AND image_test_one_file_per_fragment.names = compare_table_one_file_per_fragment.names
    AND image_test_one_file_per_fragment.images = compare_table_one_file_per_fragment.images
    ORDER BY names;
