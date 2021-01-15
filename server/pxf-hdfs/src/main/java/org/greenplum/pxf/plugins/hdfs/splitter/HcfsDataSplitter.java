package org.greenplum.pxf.plugins.hdfs.splitter;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.greenplum.pxf.api.model.BaseDataSplitter;
import org.greenplum.pxf.api.model.DataSplit;
import org.greenplum.pxf.api.model.QuerySession;
import org.greenplum.pxf.plugins.hdfs.HcfsFragmentMetadata;
import org.greenplum.pxf.plugins.hdfs.HcfsType;
import org.greenplum.pxf.plugins.hdfs.utilities.PxfInputFormat;

import java.io.IOException;
import java.util.Iterator;
import java.util.NoSuchElementException;

public class HcfsDataSplitter extends BaseDataSplitter {

    protected JobConf jobConf;
    protected HcfsType hcfsType;

    protected Iterator<InputSplit> inputSplitIterator;
    protected int basePathLength;

    public HcfsDataSplitter(QuerySession<?, ?> querySession) {
        super(querySession);

        // Check if the underlying configuration is for HDFS
        hcfsType = HcfsType.getHcfsType(context);
        jobConf = new JobConf(configuration, this.getClass());
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean hasNext() {
        if (inputSplitIterator == null) {
            Path path = new Path(hcfsType.getDataUri(context));
            basePathLength = path.toString().length();
            try {
                inputSplitIterator = getSplits(path);
            } catch (IOException e) {
                throw new RuntimeException(String.format("Unable to retrieve splits for path %s", path.toString()), e);
            }

            if (inputSplitIterator == null) {
                // There are no splits
                return false;
            }
        }
        return inputSplitIterator.hasNext();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public DataSplit next() {
        FileSplit fsp = (FileSplit) inputSplitIterator.next();
        String resource = fsp.getPath().toString().substring(basePathLength);

        /* metadata information includes: file split's start and length */
        return new DataSplit(resource, new HcfsFragmentMetadata(fsp));
    }

    /**
     * Returns an {@link InputSplit} iterator for the given path
     *
     * @param path the path
     * @return an {@link InputSplit} iterator
     * @throws IOException when {@link FileInputFormat#getSplits(JobConf, int)} returns an IOException
     */
    protected Iterator<InputSplit> getSplits(Path path) throws IOException {
        PxfInputFormat pxfInputFormat = new PxfInputFormat();
        PxfInputFormat.setInputPaths(jobConf, path);
        final InputSplit[] splits = pxfInputFormat.getSplits(jobConf, 1);

        if (splits == null) {
            return null;
        }

        LOG.info("{}-{}: {}-- Total number of splits = {}",
                context.getTransactionId(), context.getSegmentId(),
                context.getDataSource(), splits.length);

        return new Iterator<InputSplit>() {
            private int currentSplit = 0;

            @Override
            public boolean hasNext() {
                return currentSplit < splits.length;
            }

            @Override
            public InputSplit next() {
                if (currentSplit >= splits.length)
                    throw new NoSuchElementException();
                return splits[currentSplit++];
            }
        };
    }
}
