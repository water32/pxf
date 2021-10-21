package org.greenplum.pxf.plugins.hive;

import lombok.Getter;
import lombok.NoArgsConstructor;
import org.apache.hadoop.mapred.FileSplit;
import org.greenplum.pxf.plugins.hdfs.HcfsFragmentMetadata;

import java.util.Properties;

/**
 * Fragment Metadata for Hive
 */
@NoArgsConstructor
public class HiveFragmentMetadata extends HcfsFragmentMetadata {

    /**
     * Properties needed for SerDe initialization
     */
    @Getter
    private Properties properties;
    @Getter
    private String  schemaEvolutionColumns;
    @Getter
    private String  schemaEvolutionColumnTypes;

    /**
     * Default constructor for JSON serialization
     */
    public HiveFragmentMetadata(long start, long length, Properties properties) {
        super(start, length);
        this.properties = properties;
    }

    /**
     * Constructs a {@link HiveFragmentMetadata} object with the given
     * {@code fileSplit} and the {@code properties}.
     *
     * @param fileSplit  the {@link FileSplit} object.
     * @param properties the properties
     */
    public HiveFragmentMetadata(FileSplit fileSplit, Properties properties) {
        this(fileSplit, properties, null, null);
    }

    /**
     * Constructs a {@link HiveFragmentMetadata} object with the given
     * {@code fileSplit} and the {@code properties}.
     *
     * @param fileSplit  the {@link FileSplit} object.
     * @param properties the properties
     */
    public HiveFragmentMetadata(FileSplit fileSplit, Properties properties, String schemaEvolutionColumns, String schemaEvolutionColumnTypes) {
        super(fileSplit);
        this.properties = properties;
        this.schemaEvolutionColumns = schemaEvolutionColumns;
        this.schemaEvolutionColumnTypes = schemaEvolutionColumnTypes;
    }
}
