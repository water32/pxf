//package org.greenplum.pxf.plugins.hdfs.avro;
//
//import org.apache.avro.Schema;
//import org.apache.avro.generic.GenericData;
//import org.apache.avro.generic.GenericRecord;
//import org.apache.avro.mapred.AvroInputFormat;
//import org.apache.avro.mapred.AvroJob;
//import org.apache.avro.mapred.AvroRecordReader;
//import org.apache.avro.mapred.AvroWrapper;
//import org.apache.commons.lang3.tuple.Pair;
//import org.apache.hadoop.io.NullWritable;
//import org.apache.hadoop.mapred.FileSplit;
//import org.apache.hadoop.mapred.InputSplit;
//import org.apache.hadoop.mapred.JobConf;
//import org.apache.hadoop.mapred.RecordReader;
//import org.greenplum.pxf.api.model.QuerySplit;
//import org.greenplum.pxf.api.model.RequestContext;
//import org.greenplum.pxf.api.utilities.ColumnDescriptor;
//import org.greenplum.pxf.api.utilities.Utilities;
//import org.greenplum.pxf.plugins.hdfs.avro.AvroUtilities;
//
//import java.io.IOException;
//import java.nio.ByteBuffer;
//import java.util.Iterator;
//import java.util.LinkedList;
//import java.util.List;
//import java.util.Map;
//import java.util.NoSuchElementException;
//
//import static org.apache.commons.lang3.ObjectUtils.defaultIfNull;
//
///**
// * A PXF Processor to support Avro file records
// */
//public class AvroProcessor extends HcfsSplittableDataProcessor<AvroWrapper<GenericRecord>, NullWritable, Schema> {
//
//    private static final String DEFAULT_MAP_KEY_DELIM = ":";
//    private static final String DEFAULT_RECORD_KEY_DELIM = ":";
//    private static final String DEFAULT_COLLECTION_DELIM = ",";
//
//    private String collectionDelim;
//    private String mapKeyDelim;
//    private String recordKeyDelim;
//    private final AvroUtilities avroUtilities;
//    private int recordKeyIndex;
//    private Schema readSchema;
//
//    /**
//     * Constructs a new instance of AvroProcessor
//     */
//    public AvroProcessor() {
//        super(new AvroInputFormat<>());
//        avroUtilities = AvroUtilities.getInstance();
//    }
//
//    @Override
//    public void initialize(RequestContext context) {
//        super.initialize(context);
//
//        recordKeyIndex = context.getRecordkeyColumn() != null ? context.getRecordkeyColumn().columnIndex() : -1;
//        collectionDelim = defaultIfNull(context.getOption("COLLECTION_DELIM"), DEFAULT_COLLECTION_DELIM);
//        mapKeyDelim = defaultIfNull(context.getOption("MAPKEY_DELIM"), DEFAULT_MAP_KEY_DELIM);
//        recordKeyDelim = defaultIfNull(context.getOption("RECORDKEY_DELIM"), DEFAULT_RECORD_KEY_DELIM);
//    }
//
//    /**
//     * {@inheritDoc}
//     */
//    @Override
//    protected RecordReader<AvroWrapper<GenericRecord>, NullWritable> getReader(JobConf conf, InputSplit split) throws IOException {
//        return new AvroRecordReader<>(jobConf, (FileSplit) split);
//    }
//
//    /**
//     * {@inheritDoc}
//     */
//    @Override
//    public Iterator<Pair<AvroWrapper<GenericRecord>, NullWritable>> getTupleIterator(QuerySplit split) {
//        ensureReadSchemaInitialized(split);
//        return new AvroTupleItr(split);
//    }
//
//    /**
//     * {@inheritDoc}
//     */
//    @Override
//    public Iterator<Object> getFields(Pair<AvroWrapper<GenericRecord>, NullWritable> tuple) throws IOException {
//        /*
//         * In some cases, a thread processes no splits, but the thread still
//         * processes tuples. For example, in the case of a segment with 3 hosts
//         * and a single split. Only 1 host will process the split, but all 3
//         * hosts will process the resulting tuples from the split. For that
//         * reason, we need to ensure the schema has been initialized, otherwise
//         * a NullPointerException will occur when trying to read the fields
//         * from the schema.
//         */
//        ensureReadSchemaInitialized(null);
//        return new FieldItr(tuple.getLeft().datum());
//    }
//
//    /**
//     * Makes sure the schema has been initialized from the context. Stores
//     * the schema in the QuerySession metadata and caches it.
//     *
//     * @param split the query split to process
//     */
//    private void ensureReadSchemaInitialized(QuerySplit split) {
//        if (readSchema != null) return;
//
//        Schema schema = querySession.getMetadata();
//        if (schema == null) {
//            if (split == null)
//                throw new RuntimeException("Avro schema is unavailable");
//
//            synchronized (querySession) {
//                if ((schema = querySession.getMetadata()) == null) {
//                    schema = avroUtilities.obtainSchema(context, configuration, hcfsType);
//                    querySession.setMetadata(schema);
//                }
//            }
//        }
//        readSchema = schema;
//    }
//
//    private class AvroTupleItr extends RecordTupleItr {
//
//        public AvroTupleItr(QuerySplit split) {
//            super(split, new AvroWrapper<>(), NullWritable.get());
//            // Pass the schema to the AvroInputFormat
//            AvroJob.setInputSchema(jobConf, readSchema);
//        }
//
//        /**
//         * The Avro tuple iterator is currently the only specialized iterator
//         * because the special AvroRecordReader.next() semantics (uses the
//         * AvroWrapper).
//         */
//        @Override
//        protected void readNext() throws RuntimeException {
//            /*
//             * Reset datum to null to avoid stale bytes to be padded from the
//             * previous tuple's datum
//             * */
//            key.datum(null);
//            super.readNext();
//        }
//    }
//
//    private class FieldItr implements Iterator<Object> {
//
//        private int currentIndex;
//        private int currentField;
//        private final int totalFields;
//        private final GenericRecord record;
//        private final List<Schema.Field> fields;
//        private final StringBuilder reusableSb;
//
//        public FieldItr(GenericRecord record) {
//            currentIndex = 0;
//            currentField = 0;
//            this.record = record;
//            this.fields = readSchema.getFields();
//            this.reusableSb = new StringBuilder();
//            totalFields = this.fields.size();
//        }
//
//        @Override
//        public boolean hasNext() {
//            return currentField < totalFields;
//        }
//
//        @Override
//        public Object next() {
//            if (currentField >= totalFields)
//                throw new NoSuchElementException();
//
//            Schema.Field field = fields.get(currentField);
//            ColumnDescriptor columnDescriptor = context.getTupleDescription().get(currentField++);
//            Object result;
//
//            if (!columnDescriptor.isProjected()) {
//                result = null;
//            } else {
//                if (currentIndex == recordKeyIndex) {
//                    /* Add the record key if exists */
//                    // TODO: fix this
////                    currentIndex += recordkeyAdapter.appendRecordkeyField(record, context, row);
//                }
//                result = resolveField(record.get(field.name()), field.schema());
//            }
//            return result;
//        }
//
//        @SuppressWarnings("unchecked")
//        private Object resolveField(Object value, Schema schema) {
//
//            if (value == null) {
//                currentIndex++;
//                return null;
//            }
//
//            Object result = null;
//            Schema.Type fieldType = schema.getType();
//            switch (fieldType) {
//                case ARRAY:
//                    result = serializeArrayField((GenericData.Array<?>) value, schema);
//                    break;
//                case MAP:
//                    result = serializeMapField((Map<String, ?>) value, schema);
//                    break;
//                case RECORD:
//                    result = serializeRecordField(((GenericData.Record) value), schema);
//                    break;
//                case BOOLEAN:
//                case DOUBLE:
//                case ENUM:
//                case INT:
//                case FLOAT:
//                case LONG:
//                    result = value;
//                    currentIndex++;
//                    break;
//                case BYTES:
//                case FIXED:
//                    if (value instanceof ByteBuffer) {
//                        result = ((ByteBuffer) value).array();
//                    } else {
//                        /*
//                         * Entry point when the underlying ByteArray
//                         * is from GenericData.Fixed data
//                         */
//                        result = ((GenericData.Fixed) value).bytes();
//                    }
//                    currentIndex++;
//                    break;
//                case STRING:
//                    result = value.toString();
//                    currentIndex++;
//                    break;
//                case UNION:
//                    /*
//                     * When an Avro field is a union, we resolve the type of
//                     * the union element, and delegate the record update via
//                     * recursion
//                     */
//                    int unionIndex = GenericData.get().resolveUnion(schema, value);
//                    result = resolveField(value, schema.getTypes().get(unionIndex));
//                    break;
//                default:
//                    break;
//            }
//            return result;
//        }
//
//        private String serializeRecordField(GenericData.Record record, Schema schema) {
//            Schema fieldKeySchema = Schema.create(Schema.Type.STRING);
//            reusableSb.setLength(0);
//            reusableSb.append("{");
//            for (Schema.Field field : schema.getFields()) {
//                // TODO: fix this
//                Schema fieldSchema = field.schema();
//                Object fieldValue = record.get(field.name());
//                List<OneField> complexRecord = new LinkedList<>();
//                serializeField(resolveField(field.name(), fieldKeySchema), fieldKeySchema.getType());
//                serializeField(resolveField(fieldValue, fieldSchema), fieldKeySchema.getType());
////                addOneFieldToRecord(this.record, DataType.TEXT,
////                        HdfsUtilities.toString(complexRecord, recordkeyDelim));
//                currentIndex++;
//            }
//            reusableSb.append("}");
//            return reusableSb.toString();
//        }
//
//        /**
//         * Serializes an Avro array into a string representation. Each element
//         * in the array is resolved into a field, and then added the the
//         * resulting string. Supports nested complex types.
//         *
//         * @param array  the Avro array
//         * @param schema the schema for the field
//         * @return the string representation of the Avro array
//         */
//        private String serializeArrayField(GenericData.Array<?> array, Schema schema) {
//            Schema typeSchema = schema.getElementType();
//            reusableSb.setLength(0);
//            reusableSb.append("[");
//            for (int i = 0; i < array.size(); i++) {
//                if (i > 0) reusableSb.append(recordKeyDelim);
//                serializeField(resolveField(array.get(i), typeSchema), typeSchema.getType());
//            }
//            reusableSb.append("]");
//            return reusableSb.toString();
//        }
//
//        /**
//         * Serializes an Avro map into a string representation of the map. Each
//         * key/value pair int he map is resolved into a field, and then
//         * serialized into a string representation. Supports nested complex
//         * types.
//         *
//         * @param avroMap the Avro map
//         * @param schema  the schema for the field
//         * @return the string representation of the Avro map
//         */
//        private String serializeMapField(Map<String, ?> avroMap, Schema schema) {
//            Schema keySchema = Schema.create(Schema.Type.STRING);
//            Schema valueSchema = schema.getValueType();
//            reusableSb.setLength(0);
//            reusableSb.append("{");
//            for (Map.Entry<String, ?> entry : avroMap.entrySet()) {
//                // TODO: fix this
////                if (i > 0) reusableSb.append(mapKeyDelim);
//                serializeField(resolveField(entry.getKey(), keySchema), keySchema.getType());
//                serializeField(resolveField(entry.getValue(), valueSchema), valueSchema.getType());
//            }
//            reusableSb.append("}");
//            return reusableSb.toString();
//        }
//
//        private void serializeField(Object v, Schema.Type type) {
//            if (type == Schema.Type.BYTES || type == Schema.Type.FIXED) {
//                Utilities.byteArrayToOctalString((byte[]) v, reusableSb);
//            } else {
//                reusableSb.append(v);
//            }
//        }
//    }
//}
