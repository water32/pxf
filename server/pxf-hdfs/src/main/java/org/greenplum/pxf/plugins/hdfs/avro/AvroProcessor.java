package org.greenplum.pxf.plugins.hdfs.avro;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.mapred.AvroJob;
import org.apache.avro.mapred.AvroRecordReader;
import org.apache.avro.mapred.AvroWrapper;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordReader;
import org.greenplum.pxf.api.model.DataSplit;
import org.greenplum.pxf.api.model.DataSplitter;
import org.greenplum.pxf.api.model.Processor;
import org.greenplum.pxf.api.model.QuerySession;
import org.greenplum.pxf.api.model.RequestContext;
import org.greenplum.pxf.api.model.TupleIterator;
import org.greenplum.pxf.api.serializer.TupleSerializer;
import org.greenplum.pxf.plugins.hdfs.HcfsRecordTupleIterator;
import org.greenplum.pxf.plugins.hdfs.HcfsType;
import org.greenplum.pxf.plugins.hdfs.splitter.HcfsDataSplitter;
import org.springframework.stereotype.Component;

import java.io.IOException;

/**
 * A PXF Processor to support Avro file records
 */
@Component
public class AvroProcessor implements Processor<GenericRecord, Schema> {

    private final AvroUtilities avroUtilities;
    private final AvroTupleSerializer avroTupleSerializer;

    public AvroProcessor(AvroUtilities avroUtilities, AvroTupleSerializer avroTupleSerializer) {
        this.avroUtilities = avroUtilities;
        this.avroTupleSerializer = avroTupleSerializer;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public DataSplitter getDataSplitter(QuerySession<GenericRecord, Schema> querySession) {
        return new HcfsDataSplitter(querySession);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public TupleIterator<GenericRecord, Schema> getTupleIterator(QuerySession<GenericRecord, Schema> querySession, DataSplit split) throws IOException {
        return new AvroTupleItr(querySession, split);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public TupleSerializer<GenericRecord, Schema> tupleSerializer(QuerySession<GenericRecord, Schema> querySession) {
        return avroTupleSerializer;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean canProcessRequest(QuerySession<GenericRecord, Schema> querySession) {
        RequestContext context = querySession.getContext();
        return StringUtils.equalsIgnoreCase("avro", context.getFormat()) &&
                HcfsType.fromString(context.getProtocol().toUpperCase()) != HcfsType.CUSTOM;
    }

    /**
     * AvroTupleItr wraps a {@link AvroTupleItrInternal}, but
     * {@link java.util.Iterator#next()} produces a {@link GenericRecord},
     * which is the internal datum for the {@link AvroWrapper} object that is
     * returned by {@link AvroTupleItrInternal#next()}. Since these values
     * will be batched before being consumed, we need to produce
     * {@link GenericRecord}s as opposed to {@link AvroWrapper}. This is
     * because the {@link AvroWrapper} object returned by
     * {@link AvroTupleItrInternal} is created once, and reused on each
     * iteration. For that reason, we cannot directly consume
     * {@link AvroTupleItrInternal}, and we need to unwrap the
     * {@link GenericRecord} objects.
     */
    private class AvroTupleItr implements TupleIterator<GenericRecord, Schema> {

        private final AvroTupleItrInternal internalItr;

        @SuppressWarnings("unchecked")
        public AvroTupleItr(QuerySession<GenericRecord, Schema> querySession, DataSplit split) throws IOException {
            // this is a hacky way to pass the QuerySession, maybe there's a better way?
            internalItr = new AvroTupleItrInternal((QuerySession<AvroWrapper<GenericRecord>, Schema>) (QuerySession<?, ?>) querySession, split);
        }

        /**
         * {@inheritDoc}
         */
        @Override
        public Schema getMetadata() {
            return internalItr.getMetadata();
        }

        /**
         * {@inheritDoc}
         */
        @Override
        public boolean hasNext() {
            return internalItr.hasNext();
        }

        /**
         * {@inheritDoc}
         */
        @Override
        public GenericRecord next() {
            return internalItr.next().datum();
        }

        /**
         * {@inheritDoc}
         */
        @Override
        public void cleanup() throws IOException {
            internalItr.cleanup();
        }

        /**
         * An internal AvroTupleItr that extends from
         * {@link HcfsRecordTupleIterator}. It uses the {@link AvroWrapper}
         * object for the reader. However, we cannot consume {@link AvroWrapper}
         * objects, but rather we need to consume the
         * {@link AvroWrapper#datum()}.
         */
        private class AvroTupleItrInternal extends HcfsRecordTupleIterator<NullWritable, AvroWrapper<GenericRecord>, Schema> {

            private final Schema schema;

            public AvroTupleItrInternal(QuerySession<AvroWrapper<GenericRecord>, Schema> querySession, DataSplit split) throws IOException {
                super(querySession, split);

                // Obtain the schema for the file
                schema = avroUtilities.obtainSchema(context, hcfsType);

                // Pass the schema to the AvroInputFormat
                AvroJob.setInputSchema(jobConf, schema);
            }

            /**
             * {@inheritDoc}
             */
            @Override
            public Schema getMetadata() {
                return schema;
            }

            /**
             * {@inheritDoc}
             */
            @Override
            protected RecordReader<NullWritable, AvroWrapper<GenericRecord>> getReader(JobConf jobConf, InputSplit split) throws IOException {
                return new PxfAvroRecordReader<>(new AvroRecordReader<>(jobConf, (FileSplit) split));
            }
        }
    }
}
