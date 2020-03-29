package org.greenplum.pxf.plugins.jdbc;

import lombok.Getter;
import org.greenplum.pxf.api.model.DataSplit;
import org.greenplum.pxf.plugins.jdbc.partitioning.JdbcFragmentMetadata;

/**
 * A query split holds information about a JDBC data split.
 * {@link JdbcDataSplitter} iterates over splits for a query slice.
 */
public class JdbcDataSplit extends DataSplit {

    /**
     * Jdbc's metadata about the fragment
     */
    @Getter
    private final JdbcFragmentMetadata fragmentMetadata;

    /**
     * Constructs a JdbcDataSplit with a given table name
     *
     * @param tableName the table name
     */
    public JdbcDataSplit(String tableName) {
        this(tableName, null);
    }

    /**
     * Constructs a JdbcDataSplit with a given tableName and
     * {@link JdbcFragmentMetadata}
     *
     * @param tableName        the table name
     * @param fragmentMetadata metadata about the fragment
     */
    public JdbcDataSplit(String tableName, JdbcFragmentMetadata fragmentMetadata) {
        super(tableName);
        this.fragmentMetadata = fragmentMetadata;
    }

    /**
     * Returns a unique resource name for the given split
     *
     * @return a unique resource name for the given split
     */
    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder(getResource());

        if (fragmentMetadata != null)
            sb.append(":").append(fragmentMetadata);

        return sb.toString();
    }
}
