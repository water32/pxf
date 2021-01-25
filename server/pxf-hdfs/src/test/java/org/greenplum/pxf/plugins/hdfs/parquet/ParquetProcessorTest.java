package org.greenplum.pxf.plugins.hdfs.parquet;

import org.apache.hadoop.conf.Configuration;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.schema.MessageType;
import org.greenplum.pxf.api.model.DataSplit;
import org.greenplum.pxf.api.model.Processor;
import org.greenplum.pxf.api.model.QuerySession;
import org.greenplum.pxf.api.model.RequestContext;
import org.greenplum.pxf.api.model.TupleIterator;
import org.greenplum.pxf.plugins.hdfs.HcfsFragmentMetadata;
import org.greenplum.pxf.plugins.hdfs.splitter.HcfsDataSplitter;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.Objects;

import static org.assertj.core.api.Assertions.assertThat;

class ParquetProcessorTest {

    ParquetTupleSerializer tupleSerializer;
    Processor<Group, MessageType> processor;
    QuerySession<Group, MessageType> querySession;
    RequestContext context;

    @BeforeEach
    void setup() {
        String path = Objects.requireNonNull(getClass().getClassLoader().getResource("parquet/")).getPath();

        Configuration configuration = new Configuration();
        configuration.set("pxf.fs.basePath", path);

        context = new RequestContext();
        context.setConfiguration(configuration);
        context.setConfig("fakeConfig");
        context.setServerName("fakeServerName");
        context.setUser("fakeUser");
        context.setUser("test-user");
        context.setProfileScheme("file");
        context.setDataSource(path);
        context.setFormat("parquet");

        querySession = new QuerySession<>(context, null, null, null);
        tupleSerializer = new ParquetTupleSerializer();
        processor = new ParquetProcessor(tupleSerializer);
    }

    @Test
    void getDataSplitter() {
        assertThat(processor.getDataSplitter(querySession)).isInstanceOf(HcfsDataSplitter.class);
    }

    @Test
    void getTupleIterator() throws IOException {
        DataSplit split = new DataSplit("parquet_types.parquet", new HcfsFragmentMetadata(0, 5));
        TupleIterator<Group, MessageType> tupleIterator = processor.getTupleIterator(querySession, split);
        assertThat(tupleIterator).isInstanceOf(ParquetProcessor.ParquetTupleItr.class);
        tupleIterator.cleanup();
    }

    @Test
    void tupleSerializer() {
        assertThat(processor.tupleSerializer(querySession)).isSameAs(tupleSerializer);
    }

    @Test
    void canProcessRequest() {
        assertThat(processor.canProcessRequest(querySession)).isEqualTo(true);

        context.setFormat("foo");
        assertThat(processor.canProcessRequest(querySession)).isEqualTo(false);
    }
}