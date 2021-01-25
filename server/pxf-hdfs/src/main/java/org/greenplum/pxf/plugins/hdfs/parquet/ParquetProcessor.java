package org.greenplum.pxf.plugins.hdfs.parquet;

import lombok.SneakyThrows;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.parquet.HadoopReadOptions;
import org.apache.parquet.ParquetReadOptions;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.filter2.compat.FilterCompat;
import org.apache.parquet.format.converter.ParquetMetadataConverter;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.hadoop.ParquetReader;
import org.apache.parquet.hadoop.example.GroupReadSupport;
import org.apache.parquet.hadoop.metadata.FileMetaData;
import org.apache.parquet.hadoop.util.HadoopInputFile;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.Type;
import org.greenplum.pxf.api.filter.FilterParser;
import org.greenplum.pxf.api.filter.InOperatorTransformer;
import org.greenplum.pxf.api.filter.Node;
import org.greenplum.pxf.api.filter.Operator;
import org.greenplum.pxf.api.filter.TreeTraverser;
import org.greenplum.pxf.api.filter.TreeVisitor;
import org.greenplum.pxf.api.model.DataSplit;
import org.greenplum.pxf.api.model.DataSplitter;
import org.greenplum.pxf.api.model.Processor;
import org.greenplum.pxf.api.model.QuerySession;
import org.greenplum.pxf.api.model.RequestContext;
import org.greenplum.pxf.api.model.TupleIterator;
import org.greenplum.pxf.api.serializer.TupleSerializer;
import org.greenplum.pxf.api.utilities.ColumnDescriptor;
import org.greenplum.pxf.plugins.hdfs.HcfsType;
import org.greenplum.pxf.plugins.hdfs.filter.BPCharOperatorTransformer;
import org.greenplum.pxf.plugins.hdfs.splitter.HcfsDataSplitter;
import org.greenplum.pxf.plugins.hdfs.utilities.HdfsUtilities;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import java.io.IOException;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static org.apache.parquet.hadoop.api.ReadSupport.PARQUET_READ_SCHEMA;

/**
 * A PXF Processor to support Parquet file records
 */
@Component
public class ParquetProcessor implements Processor<Group, MessageType> {

    public static final EnumSet<Operator> SUPPORTED_OPERATORS = EnumSet.of(
            Operator.NOOP,
            Operator.LESS_THAN,
            Operator.GREATER_THAN,
            Operator.LESS_THAN_OR_EQUAL,
            Operator.GREATER_THAN_OR_EQUAL,
            Operator.EQUALS,
            Operator.NOT_EQUALS,
            Operator.IS_NULL,
            Operator.IS_NOT_NULL,
            // Operator.IN,
            Operator.OR,
            Operator.AND,
            Operator.NOT
    );

    private static final TreeTraverser TRAVERSER = new TreeTraverser();
    private static final TreeVisitor IN_OPERATOR_TRANSFORMER = new InOperatorTransformer();

    private final ParquetTupleSerializer tupleSerializer;

    public ParquetProcessor(ParquetTupleSerializer tupleSerializer) {
        this.tupleSerializer = tupleSerializer;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public DataSplitter getDataSplitter(QuerySession<Group, MessageType> querySession) {
        return new HcfsDataSplitter(querySession);
    }

    @Override
    public TupleIterator<Group, MessageType> getTupleIterator(QuerySession<Group, MessageType> querySession, DataSplit split) throws IOException {
        return new ParquetTupleItr(querySession, split);
    }

    @Override
    public TupleSerializer<Group, MessageType> tupleSerializer(QuerySession<Group, MessageType> querySession) {
        return tupleSerializer;
    }

    @Override
    public boolean canProcessRequest(QuerySession<Group, MessageType> querySession) {
        RequestContext context = querySession.getContext();
        return StringUtils.equalsIgnoreCase("parquet", context.getFormat()) &&
                HcfsType.fromString(context.getProtocol().toUpperCase()) != HcfsType.CUSTOM;
    }

    static class ParquetTupleItr implements TupleIterator<Group, MessageType> {

        protected Logger LOG = LoggerFactory.getLogger(this.getClass());

        private final RequestContext context;
        private final Configuration configuration;
        private final ParquetReader<Group> fileReader;
        private final MessageType readSchema;

        private Group group = null;

        private ParquetTupleItr(QuerySession<Group, MessageType> querySession, DataSplit split) throws IOException {
            this.context = querySession.getContext();
            this.configuration = context.getConfiguration();
            Path file = new Path(context.getDataSource() + split.getResource());

            FileSplit fileSplit = HdfsUtilities.parseFileSplit(file, split.getMetadata());

            // Read the original schema from the parquet file
            MessageType originalSchema = getSchema(file, fileSplit);
            // Get a map of the column name to Types for the given schema
            Map<String, Type> originalFieldsMap = getOriginalFieldsMap(originalSchema);
            // Get the read schema. This is either the full set or a subset (in
            // case of column projection) of the greenplum schema.
            readSchema = buildReadSchema(originalFieldsMap, originalSchema);
            // Get the record filter in case of predicate push-down
            FilterCompat.Filter recordFilter = getRecordFilter(context.getFilterString(), originalFieldsMap);

            // add column projection
            configuration.set(PARQUET_READ_SCHEMA, readSchema.toString());

            fileReader = ParquetReader.builder(new GroupReadSupport(), file)
                    .withConf(configuration)
                    // Create reader for a given split, read a range in file
                    .withFileRange(fileSplit.getStart(), fileSplit.getStart() + fileSplit.getLength())
                    .withFilter(recordFilter)
                    .build();
        }

        /**
         * {@inheritDoc}
         */
        @Override
        @SneakyThrows
        public boolean hasNext() {
            if (group == null) {
                readNext();
            }
            return group != null;
        }

        /**
         * {@inheritDoc}
         */
        @Override
        @SneakyThrows
        public Group next() {
            if (group == null) {
                readNext();

                if (group == null)
                    throw new NoSuchElementException();
            }

            Group tuple = group;
            group = null;
            return tuple;
        }

        @Override
        public MessageType getMetadata() {
            return readSchema;
        }

        /**
         * {@inheritDoc}
         */
        @Override
        public void cleanup() throws IOException {
            if (fileReader != null) {
                fileReader.close();
            }
        }

        /**
         * Reads the original schema from the parquet file.
         *
         * @param parquetFile the path to the parquet file
         * @param fileSplit   the file split we are accessing
         * @return the original schema from the parquet file
         * @throws IOException when there's an IOException while reading the schema
         */
        private MessageType getSchema(Path parquetFile, FileSplit fileSplit) throws IOException {

            final long then = System.nanoTime();
            ParquetMetadataConverter.MetadataFilter filter = ParquetMetadataConverter.range(
                    fileSplit.getStart(), fileSplit.getStart() + fileSplit.getLength());
            ParquetReadOptions parquetReadOptions = HadoopReadOptions
                    .builder(configuration)
                    .withMetadataFilter(filter)
                    .build();
            HadoopInputFile inputFile = HadoopInputFile.fromPath(parquetFile, configuration);
            try (ParquetFileReader parquetFileReader =
                         ParquetFileReader.open(inputFile, parquetReadOptions)) {
                FileMetaData metadata = parquetFileReader.getFileMetaData();
                if (LOG.isDebugEnabled()) {
                    LOG.debug("{}-{}: Reading file {} with {} records in {} RowGroups",
                            context.getTransactionId(), context.getSegmentId(),
                            parquetFile.getName(), parquetFileReader.getRecordCount(),
                            parquetFileReader.getRowGroups().size());
                }
                final long millis = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - then);
                LOG.debug("{}-{}: Read schema in {} ms", context.getTransactionId(),
                        context.getSegmentId(), millis);
                return metadata.getSchema();
            } catch (Exception e) {
                throw new IOException(e);
            }
        }

        /**
         * Builds a map of names to Types from the original schema, the map allows
         * easy access from a given column name to the schema {@link Type}.
         *
         * @param originalSchema the original schema of the parquet file
         * @return a map of field names to types
         */
        private Map<String, Type> getOriginalFieldsMap(MessageType originalSchema) {
            Map<String, Type> originalFields = new HashMap<>(originalSchema.getFieldCount() * 2);

            // We need to add the original name and lower cased name to
            // the map to support mixed case where in GPDB the column name
            // was created with quotes i.e "mIxEd CaSe". When quotes are not
            // used to create a table in GPDB, the name of the column will
            // always come in lower-case
            originalSchema.getFields().forEach(t -> {
                String columnName = t.getName();
                originalFields.put(columnName, t);
                originalFields.put(columnName.toLowerCase(), t);
            });

            return originalFields;
        }

        /**
         * Generates a read schema when there is column projection
         *
         * @param originalFields a map of field names to types
         * @param originalSchema the original read schema
         */
        private MessageType buildReadSchema(Map<String, Type> originalFields, MessageType originalSchema) {
            List<Type> projectedFields = context.getTupleDescription().stream()
                    .filter(ColumnDescriptor::isProjected)
                    .map(c -> {
                        Type t = originalFields.get(c.columnName());
                        if (t == null) {
                            throw new IllegalArgumentException(
                                    String.format("Column %s is missing from parquet schema", c.columnName()));
                        }
                        return t;
                    })
                    .collect(Collectors.toList());
            return new MessageType(originalSchema.getName(), projectedFields);
        }

        /**
         * Returns the parquet record filter for the given filter string
         *
         * @param filterString      the filter string
         * @param originalFieldsMap a map of field names to types
         * @return the parquet record filter for the given filter string
         */
        private FilterCompat.Filter getRecordFilter(String filterString, Map<String, Type> originalFieldsMap) {
            if (org.apache.commons.lang.StringUtils.isBlank(filterString)) {
                return FilterCompat.NOOP;
            }

            List<ColumnDescriptor> tupleDescription = context.getTupleDescription();
            ParquetRecordFilterBuilder filterBuilder = new ParquetRecordFilterBuilder(
                    tupleDescription, originalFieldsMap);
            TreeVisitor pruner = new ParquetOperatorPruner(
                    tupleDescription, originalFieldsMap, SUPPORTED_OPERATORS);
            TreeVisitor bpCharTransformer = new BPCharOperatorTransformer(tupleDescription);

            try {
                // Parse the filter string into a expression tree Node
                Node root = new FilterParser().parse(filterString);
                // Transform IN operators into a chain of ORs, then
                // prune the parsed tree with valid supported operators and then
                // traverse the pruned tree with the ParquetRecordFilterBuilder to
                // produce a record filter for parquet
                TRAVERSER.traverse(root, IN_OPERATOR_TRANSFORMER, pruner, bpCharTransformer, filterBuilder);
                return filterBuilder.getRecordFilter();
            } catch (Exception e) {
                LOG.error(String.format("%s-%d: %s--%s Unable to generate Parquet Record Filter for filter",
                        context.getTransactionId(),
                        context.getSegmentId(),
                        context.getDataSource(),
                        context.getFilterString()), e);
                return FilterCompat.NOOP;
            }
        }

        private void readNext() throws IOException {
            group = fileReader.read();
        }
    }
}
