package org.greenplum.pxf.plugins.hdfs;

import org.apache.commons.lang3.tuple.MutablePair;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.InputFormat;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordReader;
import org.greenplum.pxf.api.model.BaseProcessor;
import org.greenplum.pxf.api.model.DataSplit;
import org.greenplum.pxf.api.model.DataSplitter;
import org.greenplum.pxf.api.model.FragmentMetadata;
import org.greenplum.pxf.api.model.RequestContext;
import org.greenplum.pxf.api.model.TupleIterator;
import org.greenplum.pxf.plugins.hdfs.splitter.HcfsDataSplitter;
import org.greenplum.pxf.plugins.hdfs.utilities.HdfsUtilities;


import java.io.IOException;
import java.nio.ByteBuffer;
import java.time.Duration;
import java.time.Instant;
import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.concurrent.TimeUnit;

/**
 * Processor for accessing splittable Hadoop Compatible File System (HCFS) data
 * sources. HCFS will divide the files into splits based on an internal
 * decision (by default, the block size is the split size).
 *
 * <p> Processors that require file splitting functionality can extend this
 * class.
 * K: the type of the key that the record reader returns
 * V: the type of the value that the record reader returns
 * S: the type of the schema used by the Processor
 */
public abstract class HcfsSplittableDataProcessor<K, V, S> extends BaseProcessor<Pair<K, V>, S> {

    protected JobConf jobConf;
    protected final InputFormat<K, V> inputFormat;
    HcfsType hcfsType;

    /**
     * Constructs an HcfsSplittableDataProcessor
     *
     * @param inputFormat the HCFS {@link InputFormat} the caller wants to use
     */
    protected HcfsSplittableDataProcessor(InputFormat<K, V> inputFormat) {
        this.inputFormat = inputFormat;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void initialize(RequestContext context, Configuration configuration) {
        super.initialize(context, configuration);

        // variable required for the splits iteration logic
        jobConf = new JobConf(configuration, HcfsSplittableDataProcessor.class);

        // Check if the underlying configuration is for HDFS
        hcfsType = HcfsType.getHcfsType(configuration, context);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public TupleIterator<Pair<K, V>> getTupleIterator(DataSplit split) throws IOException {
        return new RecordTupleItr(split);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public DataSplitter getDataSplitter() {
        return new HcfsDataSplitter(context, configuration);
    }

    /**
     * Specialized processors will override this method and implement their own
     * recordReader. For example, a plain delimited text accessor may want to
     * return a LineRecordReader.
     *
     * @param conf  the hadoop jobconf to use for the selected InputFormat
     * @param split the input split to be read by the accessor
     * @return a RecordReader to be used for reading the data records of the
     * split
     * @throws IOException if RecordReader could not be created
     */
    abstract protected RecordReader<K, V> getReader(JobConf conf, InputSplit split) throws IOException;

    protected class RecordTupleItr implements TupleIterator<Pair<K, V>> {

        protected final K key;
        protected final V value;

        /**
         * The next key on the iterator
         */
        protected K nextKey;

        /**
         * The next value on the iterator
         */
        protected V nextValue;

        protected RecordReader<K, V> reader;

        /**
         * Records the total number of rows read from this iterator
         */
        protected long totalRowsRead;

        /**
         * Records the total read time in nanoseconds
         */
        protected long totalReadTimeInNanos;

        /**
         * Wraps the pair result
         */
        private final MutablePair<K, V> result;

        /**
         * Creates a new tuple iterator for record splits
         *
         * @param split the record split
         */
        public RecordTupleItr(DataSplit split) {
            this(split, null, null);
        }

        public RecordTupleItr(DataSplit split, K key, V value) {
            Path file = new Path(hcfsType.getDataUri(configuration, context.getDataSource() + split.getResource()));
            FragmentMetadata metadata = deserializeFragmentMetadata(split.getMetadata());
            InputSplit fileSplit = new FileSplit(file, metadata.getStart(), metadata.getEnd(), (String[]) null);

            try {
                this.reader = getReader(jobConf, fileSplit);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
            this.key = key != null ? key : reader.createKey();
            this.value = value != null ? value : reader.createValue();
            this.result = new MutablePair<>();
        }

        /**
         * {@inheritDoc}
         */
        @Override
        public boolean hasNext() {
            if (nextValue == null && reader != null) {
                readNext();
            }
            return nextValue != null;
        }

        /**
         * {@inheritDoc}
         */
        @Override
        public Pair<K, V> next() {
            if (nextValue == null) {
                readNext();

                if (nextValue == null)
                    throw new NoSuchElementException();
            }

            totalRowsRead++;
            result.setLeft(nextKey);
            result.setRight(nextValue);
            nextKey = null;
            nextValue = null;
            return result;
        }

        /**
         * {@inheritDoc}
         */
        @Override
        public void cleanup() throws IOException {
            closeForRead();
        }

        /**
         * Reads the next key/value pair from the reader
         *
         * @throws NoSuchElementException when the reader has already been closed
         * @throws RuntimeException       when an IOException occurs, it gets wrapped in a RuntimeException
         */
        protected void readNext() throws RuntimeException {
            if (reader == null) throw new NoSuchElementException();
            try {
                Instant start = Instant.now();
                if (reader.next(key, value)) {
                    totalReadTimeInNanos += Duration.between(start, Instant.now()).toNanos();
                    nextKey = key;
                    nextValue = value;
                } else {
                    closeForRead();
                }
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }

        /**
         * Closes the RecordReader when finished reading the file
         */
        protected void closeForRead() throws IOException {
            if (reader != null) {
                reader.close();
                reader = null;
            }
            if (LOG.isDebugEnabled()) {
                final long millis = TimeUnit.NANOSECONDS.toMillis(totalReadTimeInNanos);
                long average = totalRowsRead > 0 ? totalReadTimeInNanos / totalRowsRead : 0;
                LOG.debug("{}-{}: Read TOTAL of {} rows from file {} on server {} in {} ms. Average speed: {} nanoseconds/row",
                    context.getTransactionId(),
                    context.getSegmentId(),
                    totalRowsRead,
                    context.getDataSource(),
                    context.getServerName(),
                    millis,
                    average);
            }
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean isThreadSafe() {
        return HdfsUtilities.isThreadSafe(
            configuration,
            context.getDataSource(),
            context.getOption("COMPRESSION_CODEC"));
    }

    // TODO: deduplicate this code (also found in ParquetProcessor)
    protected FragmentMetadata deserializeFragmentMetadata(byte[] bytes) {
        ByteBuffer buffer = ByteBuffer.allocate(2 * Long.BYTES);
        buffer.put(bytes);
        buffer.flip(); // need flip
        return new FragmentMetadata(buffer.getLong(), buffer.getLong());
    }
}
