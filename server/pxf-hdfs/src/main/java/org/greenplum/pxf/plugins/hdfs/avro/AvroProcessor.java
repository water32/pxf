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
public class AvroProcessor implements Processor<AvroWrapper<GenericRecord>, Schema> {

    private final AvroUtilities avroUtilities;

    public AvroProcessor(AvroUtilities avroUtilities) {
        this.avroUtilities = avroUtilities;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public DataSplitter getDataSplitter(QuerySession<AvroWrapper<GenericRecord>, Schema> querySession) {
        return new HcfsDataSplitter(querySession);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public TupleIterator<AvroWrapper<GenericRecord>, Schema> getTupleIterator(QuerySession<AvroWrapper<GenericRecord>, Schema> querySession, DataSplit split) throws IOException {
        return new AvroTupleItr(querySession, split);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public TupleSerializer<AvroWrapper<GenericRecord>, Schema> tupleSerializer(QuerySession<AvroWrapper<GenericRecord>, Schema> querySession) {
        return null;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean canProcessRequest(QuerySession<AvroWrapper<GenericRecord>, Schema> querySession) {
        RequestContext context = querySession.getContext();
        return StringUtils.equalsIgnoreCase("avro", context.getFormat()) &&
                HcfsType.fromString(context.getProtocol().toUpperCase()) != HcfsType.CUSTOM;
    }

    private class AvroTupleItr extends HcfsRecordTupleIterator<NullWritable, AvroWrapper<GenericRecord>, Schema> {

        private final Schema schema;

        public AvroTupleItr(QuerySession<AvroWrapper<GenericRecord>, Schema> querySession, DataSplit split) throws IOException {
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
        protected RecordReader<NullWritable, AvroWrapper<GenericRecord>> getReader(JobConf jobConf, InputSplit split)
                throws IOException {
            return new PxfAvroRecordReader<>(new AvroRecordReader<>(jobConf, (FileSplit) split));
        }
    }
}
