package org.greenplum.pxf.plugins.hdfs.avro;

import org.apache.avro.mapred.AvroRecordReader;
import org.apache.avro.mapred.AvroWrapper;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapred.RecordReader;

import java.io.IOException;

/**
 * An AvroRecordReader that respects the Key/Value semantics of the reader, by
 * wrapping a {@link AvroRecordReader} and inversing the order of the key/value
 * of the original implementation.
 */
public class PxfAvroRecordReader<T>
        implements RecordReader<NullWritable, AvroWrapper<T>> {

    private final AvroRecordReader<T> reader;

    /**
     * Constructs a {@link PxfAvroRecordReader} from an {@link AvroRecordReader}
     *
     * @param reader the AvroRecord Reader
     */
    public PxfAvroRecordReader(AvroRecordReader<T> reader) {
        this.reader = reader;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean next(NullWritable ignore, AvroWrapper<T> wrapper) throws IOException {
        return reader.next(wrapper, ignore);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public NullWritable createKey() {
        return reader.createValue();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public AvroWrapper<T> createValue() {
        return reader.createKey();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public long getPos() throws IOException {
        return reader.getPos();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void close() throws IOException {
        reader.close();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public float getProgress() throws IOException {
        return reader.getProgress();
    }
}
