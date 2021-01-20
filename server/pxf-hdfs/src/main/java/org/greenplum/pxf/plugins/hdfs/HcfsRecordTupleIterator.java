package org.greenplum.pxf.plugins.hdfs;

import lombok.SneakyThrows;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.InputFormat;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordReader;
import org.greenplum.pxf.api.model.DataSplit;
import org.greenplum.pxf.api.model.QuerySession;
import org.greenplum.pxf.api.model.RequestContext;
import org.greenplum.pxf.api.model.TupleIterator;
import org.greenplum.pxf.plugins.hdfs.utilities.HdfsUtilities;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.NoSuchElementException;

/**
 * Base {@link TupleIterator} for reading a splittable Hadoop Compatible File
 * System data source. HCFS will divide the file into splits based on an
 * internal decision (by default, the block size is also the split size).
 * <p>
 * {@link TupleIterator}s that require such base functionality should extend
 * this class.
 */
public abstract class HcfsRecordTupleIterator<K, V, M> implements TupleIterator<V, M> {

    protected Logger LOG = LoggerFactory.getLogger(this.getClass());

    protected final RecordReader<K, V> reader;
    protected final K key;
    protected final V value;
    protected final InputFormat<K, V> inputFormat;
    protected final JobConf jobConf;
    protected final RequestContext context;
    protected final FileSplit fileSplit;
    protected final HcfsType hcfsType;

    private V result;

    protected HcfsRecordTupleIterator(QuerySession<V, M> querySession, DataSplit split) throws IOException {
        this(null, querySession, split);
    }

    protected HcfsRecordTupleIterator(InputFormat<K, V> inputFormat, QuerySession<V, M> querySession, DataSplit split) throws IOException {
        this.inputFormat = inputFormat;
        context = querySession.getContext();

        // variable required for the splits iteration logic
        jobConf = new JobConf(context.getConfiguration(), HdfsSplittableDataAccessor.class);

        // Check if the underlying configuration is for HDFS
        hcfsType = HcfsType.getHcfsType(context);

        Path file = new Path(context.getDataSource() + split.getResource());

        // Build the fileSplit from metadata
        fileSplit = HdfsUtilities.parseFileSplit(file, split.getMetadata());

        reader = getReader(jobConf, fileSplit);
        key = reader.createKey();
        value = reader.createValue();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    @SneakyThrows
    public boolean hasNext() {
        if (result == null) {
            readNext();
        }
        return result != null;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    @SneakyThrows
    public V next() {
        if (result == null) {
            readNext();

            if (result == null)
                throw new NoSuchElementException();
        }

        V tuple = result;
        result = null;
        return tuple;
    }

    /**
     * When finished reading the file, it closes the RecordReader
     */
    @Override
    public void cleanup() throws IOException {
        if (reader != null) {
            reader.close();
        }
    }

    /**
     * Specialized {@link TupleIterator}s will override this method and
     * implement their own recordReader. For example, a plain delimited text
     * {@link TupleIterator} may want to return a
     * {@link org.apache.hadoop.mapred.LineRecordReader}.
     *
     * @param jobConf the hadoop jobconf to use for the selected InputFormat
     * @param split   the input split to be read by the iterator
     * @return a recordReader to be used for reading the data records of the
     * split
     * @throws IOException when the recordReader creation fails
     */
    abstract protected RecordReader<K, V> getReader(JobConf jobConf, InputSplit split)
            throws IOException;

    /**
     * Reads the next value and stores it in the {@code result} variable, or
     * stores null if the reader has been exhausted.
     *
     * @throws IOException when an error occurs during read
     */
    private void readNext() throws IOException {
        if (reader.next(key, value)) {
            result = value;
        } else {
            result = null;
        }
    }
}
