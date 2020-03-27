package org.greenplum.pxf.plugins.jdbc;

import org.apache.commons.lang.SerializationUtils;
import org.apache.hadoop.conf.Configuration;
import org.greenplum.pxf.api.factory.ProducerTaskFactory;
import org.greenplum.pxf.api.factory.SerializerFactory;
import org.greenplum.pxf.api.model.DataSplit;
import org.greenplum.pxf.api.model.DataSplitter;
import org.greenplum.pxf.api.model.RequestContext;
import org.greenplum.pxf.api.task.ProducerTask;
import org.greenplum.pxf.plugins.jdbc.partitioning.PartitionType;
import org.greenplum.pxf.plugins.jdbc.utils.ConnectionManager;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.mock.mockito.MockBean;

import java.io.File;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
@SpringBootTest(
    classes = JdbcTestConfig.class,
    webEnvironment = SpringBootTest.WebEnvironment.NONE
)
public class JdbcProcessorTest {

    @Autowired
    private JdbcProcessor processor;

    /* Dependency for JdbcProcessor */
    @MockBean
    private SerializerFactory serializerFactory;

    @MockBean
    private ProducerTaskFactory<?, ?> producerTaskFactory;

    private DataSplit dataSplit;
    private RequestContext context;
    private Configuration configuration;

    @MockBean
    private ConnectionManager mockConnectionManager;
    @Mock
    private DatabaseMetaData mockMetaData;
    @Mock
    private Connection mockConnection;
    @Mock
    private Statement mockStatement;
    @Mock
    private ResultSet mockResultSet;

    @BeforeEach
    @SuppressWarnings("unchecked")
    public void setup() throws SQLException {
        configuration = new Configuration();
        configuration.set("jdbc.driver", "org.greenplum.pxf.plugins.jdbc.FakeJdbcDriver");
        configuration.set("jdbc.url", "test-url");
        context = new RequestContext();
        context.setConfig("default");
        context.setDataSource("test-table");
        context.setUser("test-user");
        context.setTotalSegments(1);
        dataSplit = new DataSplit("test-table");

        when(mockConnectionManager.getConnection(any(), any(), any(), anyBoolean(), any(), any())).thenReturn(mockConnection);
        when(mockConnection.getMetaData()).thenReturn(mockMetaData);
        when(producerTaskFactory.getProducerTask(any())).thenReturn(mock(ProducerTask.class));
    }

//    @Test
//    public void testWriteFailsWhenQueryIsSpecified() throws Exception {
//        Exception ex = assertThrows(IllegalArgumentException.class,
//        ()->);
//        assertEquals("specifying query name in data path is not supported for JDBC writable external tables", ex.getMessage());
//        context.setDataSource("query:foo");
//        accessor.initialize(context, configuration);
//        accessor.openForWrite();
//    }

    @Test
    public void testReadFromQueryFailsWhenServerDirectoryIsNotSpecified() throws Exception {
        context.setServerName("unknown");
        context.setDataSource("query:foo");
        processor.initialize(context, configuration);
        Exception ex = assertThrows(IllegalStateException.class,
            () -> processor.getTupleIterator(dataSplit));
        assertEquals("No server configuration directory found for server unknown", ex.getMessage());
    }

    @Test
    public void testReadFromQueryFailsWhenServerDirectoryDoesNotExist() throws Exception {
        configuration.set("pxf.config.server.directory", "/non-existing-directory");
        context.setDataSource("query:foo");
        processor.initialize(context, configuration);
        Exception ex = assertThrows(RuntimeException.class,
            () -> processor.getTupleIterator(dataSplit));
        assertEquals("Failed to read text of query foo : File '/non-existing-directory/foo.sql' does not exist", ex.getMessage());
    }

    @Test
    public void testReadFromQueryFailsWhenQueryFileIsNotFoundInExistingDirectory() {
        configuration.set("pxf.config.server.directory", "/tmp/");
        context.setDataSource("query:foo");
        processor.initialize(context, configuration);
        Exception ex = assertThrows(RuntimeException.class,
            () -> processor.getTupleIterator(dataSplit));
        assertEquals("Failed to read text of query foo : File '/tmp/foo.sql' does not exist", ex.getMessage());
    }

    @Test
    public void testReadFromQueryFailsWhenQueryFileIsEmpty() throws Exception {
        String serversDirectory = new File(this.getClass().getClassLoader().getResource("servers").toURI()).getCanonicalPath();
        configuration.set("pxf.config.server.directory", serversDirectory + File.separator + "test-server");
        context.setDataSource("query:emptyquery");
        processor.initialize(context, configuration);
        Exception ex = assertThrows(RuntimeException.class,
            () -> processor.getTupleIterator(dataSplit));
        assertEquals("Query text file is empty for query emptyquery", ex.getMessage());
    }

    @Test
    public void testReadFromQuery() throws Exception {
        String serversDirectory = new File(this.getClass().getClassLoader().getResource("servers").toURI()).getCanonicalPath();
        configuration.set("pxf.config.server.directory", serversDirectory + File.separator + "test-server");
        context.setDataSource("query:testquery");
        dataSplit = new DataSplit("query:testquery");
        ArgumentCaptor<String> queryPassed = ArgumentCaptor.forClass(String.class);
        when(mockStatement.executeQuery(queryPassed.capture())).thenReturn(mockResultSet);
        when(mockConnection.createStatement()).thenReturn(mockStatement);
        when(mockMetaData.getDatabaseProductName()).thenReturn("Greenplum");
        when(mockMetaData.getExtraNameCharacters()).thenReturn("");

        processor.initialize(context, configuration);
        processor.getTupleIterator(dataSplit);

        String expected = "SELECT  FROM (SELECT dept.name, count(), max(emp.salary)\n" +
            "FROM dept JOIN emp\n" +
            "ON dept.id = emp.dept_id\n" +
            "GROUP BY dept.name) pxfsubquery";
        assertEquals(expected, queryPassed.getValue());
    }

    @Test
    public void testReadFromQueryEndingInSemicolon() throws Exception {
        String serversDirectory = new File(this.getClass().getClassLoader().getResource("servers").toURI()).getCanonicalPath();
        configuration.set("pxf.config.server.directory", serversDirectory + File.separator + "test-server");
        context.setDataSource("query:testquerywithsemicolon");
        dataSplit = new DataSplit("query:testquerywithsemicolon");
        ArgumentCaptor<String> queryPassed = ArgumentCaptor.forClass(String.class);
        when(mockStatement.executeQuery(queryPassed.capture())).thenReturn(mockResultSet);
        when(mockConnection.createStatement()).thenReturn(mockStatement);
        when(mockMetaData.getDatabaseProductName()).thenReturn("Greenplum");
        when(mockMetaData.getExtraNameCharacters()).thenReturn("");

        processor.initialize(context, configuration);
        processor.getTupleIterator(dataSplit);

        String expected = "SELECT  FROM (SELECT dept.name, count(), max(emp.salary)\n" +
            "FROM dept JOIN emp\n" +
            "ON dept.id = emp.dept_id\n" +
            "GROUP BY dept.name) pxfsubquery";
        assertEquals(expected, queryPassed.getValue());
    }

    @Test
    public void testReadFromQueryWithValidSemicolon() throws Exception {
        String serversDirectory = new File(this.getClass().getClassLoader().getResource("servers").toURI()).getCanonicalPath();
        configuration.set("pxf.config.server.directory", serversDirectory + File.separator + "test-server");
        context.setDataSource("query:testquerywithvalidsemicolon");
        dataSplit = new DataSplit("query:testquerywithvalidsemicolon");
        ArgumentCaptor<String> queryPassed = ArgumentCaptor.forClass(String.class);
        when(mockStatement.executeQuery(queryPassed.capture())).thenReturn(mockResultSet);
        when(mockConnection.createStatement()).thenReturn(mockStatement);
        when(mockMetaData.getDatabaseProductName()).thenReturn("Greenplum");
        when(mockMetaData.getExtraNameCharacters()).thenReturn("");

        processor.initialize(context, configuration);
        processor.getTupleIterator(dataSplit);

        String expected = "SELECT  FROM (SELECT dept.name, count(), max(emp.salary)\n" +
            "FROM dept JOIN emp\n" +
            "ON dept.id = emp.dept_id\n" +
            "WHERE dept.name LIKE '%;%'\n" +
            "GROUP BY dept.name) pxfsubquery";
        assertEquals(expected, queryPassed.getValue());
    }

    @Test
    public void testReadFromQueryWithPartitions() throws Exception {
        String serversDirectory = new File(this.getClass().getClassLoader().getResource("servers").toURI()).getCanonicalPath();
        configuration.set("pxf.config.server.directory", serversDirectory + File.separator + "test-server");
        context.setDataSource("query:testquery");
        context.addOption("PARTITION_BY", "count:int");
        context.addOption("RANGE", "1:10");
        context.addOption("INTERVAL", "1");
        context.setFragmentMetadata(SerializationUtils.serialize(PartitionType.INT.getFragmentsMetadata("count", "1:10", "1").get(2)));
        dataSplit = new DataSplit("query:testquery");
        ArgumentCaptor<String> queryPassed = ArgumentCaptor.forClass(String.class);
        when(mockStatement.executeQuery(queryPassed.capture())).thenReturn(mockResultSet);
        when(mockConnection.createStatement()).thenReturn(mockStatement);
        when(mockMetaData.getDatabaseProductName()).thenReturn("Greenplum");
        when(mockMetaData.getExtraNameCharacters()).thenReturn("");

        processor.initialize(context, configuration);
        DataSplitter splitter = processor.getDataSplitter();

        for (int i = 0; i <= 2; i++) {
            assertTrue(splitter.hasNext());
            assertNotNull((dataSplit = splitter.next()));
        }
        processor.getTupleIterator(dataSplit);

        String expected = "SELECT  FROM (SELECT dept.name, count(), max(emp.salary)\n" +
            "FROM dept JOIN emp\n" +
            "ON dept.id = emp.dept_id\n" +
            "GROUP BY dept.name) pxfsubquery WHERE count >= 1 AND count < 2";
        assertEquals(expected, queryPassed.getValue());
    }

    @Test
    public void testReadFromQueryWithWhereWithPartitions() throws Exception {
        String serversDirectory = new File(this.getClass().getClassLoader().getResource("servers").toURI()).getCanonicalPath();
        configuration.set("pxf.config.server.directory", serversDirectory + File.separator + "test-server");
        context.setDataSource("query:testquerywithwhere");
        context.addOption("PARTITION_BY", "count:int");
        context.addOption("RANGE", "1:10");
        context.addOption("INTERVAL", "1");
        context.setFragmentMetadata(SerializationUtils.serialize(PartitionType.INT.getFragmentsMetadata("count", "1:10", "1").get(2)));
        dataSplit = new DataSplit("query:testquerywithwhere");
        ArgumentCaptor<String> queryPassed = ArgumentCaptor.forClass(String.class);
        when(mockStatement.executeQuery(queryPassed.capture())).thenReturn(mockResultSet);
        when(mockConnection.createStatement()).thenReturn(mockStatement);
        when(mockMetaData.getDatabaseProductName()).thenReturn("Greenplum");
        when(mockMetaData.getExtraNameCharacters()).thenReturn("");

        processor.initialize(context, configuration);
        DataSplitter splitter = processor.getDataSplitter();

        for (int i = 0; i <= 2; i++) {
            assertTrue(splitter.hasNext());
            assertNotNull((dataSplit = splitter.next()));
        }

        processor.getTupleIterator(dataSplit);

        String expected = "SELECT  FROM (SELECT dept.name, count(), max(emp.salary)\n" +
            "FROM dept JOIN emp\n" +
            "ON dept.id = emp.dept_id\n" +
            "WHERE dept.id < 10\n" +
            "GROUP BY dept.name) pxfsubquery WHERE count >= 1 AND count < 2";
        assertEquals(expected, queryPassed.getValue());
    }

    @Test
    public void testGetFragmentsAndReadFromQueryWithPartitions() throws Exception {
        String serversDirectory = new File(this.getClass().getClassLoader().getResource("servers").toURI()).getCanonicalPath();
        configuration.set("pxf.config.server.directory", serversDirectory + File.separator + "test-server");
        context.setDataSource("query:testquery");
        context.addOption("PARTITION_BY", "count:int");
        context.addOption("RANGE", "1:10");
        context.addOption("INTERVAL", "1");

        processor.initialize(context, configuration);
        DataSplitter splitter = processor.getDataSplitter();

        for (int i = 0; i <= 2; i++) {
            assertTrue(splitter.hasNext());
            assertNotNull((dataSplit = splitter.next()));
        }

        ArgumentCaptor<String> queryPassed = ArgumentCaptor.forClass(String.class);
        when(mockConnection.createStatement()).thenReturn(mockStatement);
        when(mockStatement.executeQuery(queryPassed.capture())).thenReturn(mockResultSet);
        when(mockMetaData.getDatabaseProductName()).thenReturn("Greenplum");
        when(mockMetaData.getExtraNameCharacters()).thenReturn("");

        processor.getTupleIterator(dataSplit);

        String expected = "SELECT  FROM (SELECT dept.name, count(), max(emp.salary)\n" +
            "FROM dept JOIN emp\n" +
            "ON dept.id = emp.dept_id\n" +
            "GROUP BY dept.name) pxfsubquery WHERE count >= 1 AND count < 2";
        assertEquals(expected, queryPassed.getValue());
    }
}
