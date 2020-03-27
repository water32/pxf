package org.greenplum.pxf.plugins.jdbc;

import org.apache.commons.lang.SerializationUtils;
import org.apache.hadoop.conf.Configuration;
import org.greenplum.pxf.api.model.BaseDataSplitter;
import org.greenplum.pxf.api.model.DataSplit;
import org.greenplum.pxf.api.model.DataSplitter;
import org.greenplum.pxf.api.model.Plugin;
import org.greenplum.pxf.api.model.RequestContext;
import org.greenplum.pxf.plugins.jdbc.partitioning.JdbcFragmentMetadata;
import org.greenplum.pxf.plugins.jdbc.partitioning.PartitionType;

import java.util.List;
import java.util.NoSuchElementException;

/**
 * JDBC Data Splitter
 *
 * <p>Splits the query to allow multiple simultaneous SELECTs
 */
public class JdbcDataSplitter extends BaseDataSplitter {

    private PartitionType partitionType;
    private String column;
    private String range;
    private String interval;
    private DataSplit nextSplit;
    private List<JdbcFragmentMetadata> metadata;
    private int currentSplit = 0;
    private int totalSplits = 1; // assume at least one split

    /**
     * Constructs a {@link DataSplitter} and initializes the {@link Plugin}
     *
     * @param context       the request context for the given query
     * @param configuration the configuration for the server we are accessing
     */
    public JdbcDataSplitter(RequestContext context, Configuration configuration) {
        super(context, configuration);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void initialize(RequestContext context, Configuration configuration) {
        super.initialize(context, configuration);

        String partitionByOption = context.getOption("PARTITION_BY");
        if (partitionByOption == null) return;

        try {
            String[] partitionBy = partitionByOption.split(":");
            column = partitionBy[0];
            partitionType = PartitionType.of(partitionBy[1]);
        } catch (ArrayIndexOutOfBoundsException e) {
            throw new IllegalArgumentException("The parameter 'PARTITION_BY' has incorrect format. The correct format is '<column_name>:{int|date|enum}'");
        }

        range = context.getOption("RANGE");
        interval = context.getOption("INTERVAL");
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean hasNext() {

        if (nextSplit == null && currentSplit < totalSplits) {
            nextSplit = getNext();
        }

        return nextSplit != null;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public DataSplit next() {
        if (currentSplit >= totalSplits)
            throw new NoSuchElementException();

        if (nextSplit == null) {
            nextSplit = getNext();
        }

        DataSplit n = nextSplit;
        currentSplit++;
        nextSplit = null;

        return n;
    }

    /**
     * Create {@link DataSplit} from byte array.
     *
     * @param fragmentMetadata metadata for the DataSplit
     * @return {@link DataSplit}
     */
    private DataSplit createDataSplit(byte[] fragmentMetadata) {
        return new DataSplit(context.getDataSource(), fragmentMetadata);
    }

    /**
     * Returns the next {@link DataSplit}. If there is no query partitioning,
     * return a single split. Otherwise, initialize a list of fragment metadata
     * during the first call, and return the current DataSplit
     *
     * @return the next DataSplit
     */
    private DataSplit getNext() {
        if (partitionType == null) {
            totalSplits = 1;
            return createDataSplit(null);
        } else {
            if (metadata == null) {
                metadata = partitionType.getFragmentsMetadata(column, range, interval);
                totalSplits = metadata.size();
            }
            return createDataSplit(SerializationUtils.serialize(metadata.get(currentSplit)));
        }
    }
}
