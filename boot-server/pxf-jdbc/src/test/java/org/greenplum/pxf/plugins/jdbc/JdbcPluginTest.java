package org.greenplum.pxf.plugins.jdbc;

/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

import org.apache.hadoop.conf.Configuration;
import org.greenplum.pxf.api.factory.ProducerTaskFactory;
import org.greenplum.pxf.api.factory.SerializerFactory;
import org.greenplum.pxf.api.model.RequestContext;
import org.greenplum.pxf.api.task.ProducerTask;
import org.greenplum.pxf.plugins.jdbc.utils.ConnectionManager;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.mock.mockito.MockBean;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Properties;

import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
@SpringBootTest(
    classes = JdbcTestConfig.class,
    webEnvironment = SpringBootTest.WebEnvironment.NONE
)
public class JdbcPluginTest {

    @MockBean
    private ConnectionManager mockConnectionManager;
    @Mock
    private DatabaseMetaData mockMetaData;
    @Mock
    private Connection mockConnection;
    @Mock
    private PreparedStatement mockStatement;
    @Autowired
    private JdbcProcessor plugin;

    /* Dependency for JdbcProcessor */
    @MockBean
    private SerializerFactory serializerFactory;

    @MockBean
    private ProducerTaskFactory<?, ?> producerTaskFactory;

    private SQLException exception = new SQLException("some error");
    private RequestContext context;
    private Configuration configuration;
    private Properties poolProps;

    @BeforeEach
    @SuppressWarnings("unchecked")
    public void before() {
        configuration = new Configuration();
        context = new RequestContext();
        context.setConfig("default");
        context.setDataSource("test-table");
        context.setUser("test-user");
        context.setTotalSegments(1);

        poolProps = new Properties();
        poolProps.setProperty("maximumPoolSize", "5");
        poolProps.setProperty("connectionTimeout", "30000");
        poolProps.setProperty("idleTimeout", "30000");
        poolProps.setProperty("minimumIdle", "0");
        when(producerTaskFactory.getProducerTask(any())).thenReturn(mock(ProducerTask.class));
    }

    @Test
    public void testCloseConnectionWithCommit() throws Exception {
        when(mockMetaData.supportsTransactions()).thenReturn(true);
        when(mockConnection.getMetaData()).thenReturn(mockMetaData);
        when(mockConnection.isClosed()).thenReturn(false);
        when(mockConnection.getAutoCommit()).thenReturn(false);
        Mockito.doNothing().when(mockConnection).commit();
        Mockito.doNothing().when(mockConnection).close();
        when(mockStatement.getConnection()).thenReturn(mockConnection);
        Mockito.doNothing().when(mockStatement).close();

        JdbcProcessor.closeStatementAndConnection(mockStatement);

        verify(mockStatement, times(1)).close();
        verify(mockConnection, times(1)).close();
        verify(mockConnection, times(1)).commit();
    }

    @Test
    public void testCloseConnectionWithoutCommit() throws Exception {
        when(mockMetaData.supportsTransactions()).thenReturn(true);
        when(mockConnection.getMetaData()).thenReturn(mockMetaData);
        when(mockConnection.isClosed()).thenReturn(false);
        when(mockConnection.getAutoCommit()).thenReturn(true);
        Mockito.doNothing().when(mockConnection).close();
        when(mockStatement.getConnection()).thenReturn(mockConnection);
        Mockito.doNothing().when(mockStatement).close();

        JdbcProcessor.closeStatementAndConnection(mockStatement);

        verify(mockStatement, times(1)).close();
        verify(mockConnection, times(1)).close();
    }

    @Test
    public void testCloseConnectionWithoutTransactions() throws Exception {
        when(mockMetaData.supportsTransactions()).thenReturn(false);
        when(mockConnection.getMetaData()).thenReturn(mockMetaData);
        when(mockConnection.isClosed()).thenReturn(false);
        Mockito.doNothing().when(mockConnection).close();
        when(mockStatement.getConnection()).thenReturn(mockConnection);
        Mockito.doNothing().when(mockStatement).close();

        JdbcProcessor.closeStatementAndConnection(mockStatement);

        verify(mockStatement, times(1)).close();
        verify(mockConnection, times(1)).close();
    }

    @Test
    public void testCloseConnectionClosed() throws Exception {
        when(mockConnection.isClosed()).thenReturn(true);
        Mockito.doNothing().when(mockConnection).close();
        when(mockStatement.getConnection()).thenReturn(mockConnection);
        Mockito.doNothing().when(mockStatement).close();

        JdbcProcessor.closeStatementAndConnection(mockStatement);

        verify(mockStatement, times(1)).close();
    }

    @Test
    public void testCloseConnectionWithExceptionDatabaseMetaData() throws Exception {
        when(mockStatement.getConnection()).thenReturn(mockConnection);
        Mockito.doNothing().when(mockStatement).close();
        when(mockConnection.isClosed()).thenReturn(false);
        Mockito.doNothing().when(mockConnection).close();
        when(mockConnection.getMetaData()).thenReturn(mockMetaData);
        doThrow(exception).when(mockMetaData).supportsTransactions();

        Exception e = assertThrows(Exception.class,
            () -> JdbcProcessor.closeStatementAndConnection(mockStatement),
            "SQLException must have been thrown");
        assertSame(exception, e);

        verify(mockStatement, times(1)).close();
        verify(mockConnection, times(1)).close();
    }

    @Test
    public void testCloseConnectionWithExceptionConnectionOnCommit() throws Exception {
        when(mockStatement.getConnection()).thenReturn(mockConnection);
        Mockito.doNothing().when(mockStatement).close();
        when(mockConnection.getMetaData()).thenReturn(mockMetaData);
        when(mockMetaData.supportsTransactions()).thenReturn(true);
        when(mockConnection.isClosed()).thenReturn(false);
        when(mockConnection.getAutoCommit()).thenReturn(false);
        doThrow(exception).when(mockConnection).commit();
        Mockito.doNothing().when(mockConnection).close();

        Exception e = assertThrows(SQLException.class,
            () -> JdbcProcessor.closeStatementAndConnection(mockStatement),
            "SQLException must have been thrown");
        assertSame(exception, e);

        verify(mockStatement, times(1)).close();
        verify(mockConnection, times(1)).close();
    }

    @Test
    public void testCloseConnectionWithExceptionConnectionOnClose() throws Exception {
        when(mockStatement.getConnection()).thenReturn(mockConnection);
        Mockito.doNothing().when(mockStatement).close();
        when(mockConnection.getMetaData()).thenReturn(mockMetaData);
        when(mockMetaData.supportsTransactions()).thenReturn(false);
        when(mockConnection.isClosed()).thenReturn(false);
        doThrow(exception).when(mockConnection).close();

        JdbcProcessor.closeStatementAndConnection(mockStatement);

        verify(mockStatement, times(1)).close();
    }

    @Test
    public void testCloseConnectionWithExceptionStatementOnClose() throws Exception {
        when(mockStatement.getConnection()).thenReturn(mockConnection);
        when(mockConnection.getMetaData()).thenReturn(mockMetaData);
        when(mockMetaData.supportsTransactions()).thenReturn(false);
        when(mockConnection.isClosed()).thenReturn(false);
        Mockito.doNothing().when(mockConnection).close();
        doThrow(exception).when(mockStatement).close();

        Exception e = assertThrows(SQLException.class,
            () -> JdbcProcessor.closeStatementAndConnection(mockStatement),
            "SQLException must have been thrown");
        assertSame(exception, e);

        verify(mockConnection, times(1)).close();
    }

    @Test
    public void testCloseConnectionWithExceptionStatementOnGetConnection() throws Exception {
        doThrow(exception).when(mockStatement).getConnection();
        Mockito.doNothing().when(mockStatement).close();

        Exception e = assertThrows(SQLException.class,
            () -> JdbcProcessor.closeStatementAndConnection(mockStatement),
            "SQLException must have been thrown");
        assertSame(exception, e);

        verify(mockStatement, times(1)).close();
    }

    @Test
    public void testTransactionIsolationNotSetByUser() throws SQLException {
        configuration.set("jdbc.driver", "org.greenplum.pxf.plugins.jdbc.FakeJdbcDriver");
        configuration.set("jdbc.url", "test-url");

        when(mockConnectionManager.getConnection(any(), any(), any(), anyBoolean(), any(), any())).thenReturn(mockConnection);
        when(mockConnection.getMetaData()).thenReturn(mockMetaData);

        plugin.initialize(context, configuration);
        Connection conn = plugin.getConnection();

        verify(conn, never()).setTransactionIsolation(anyInt());
    }

    @Test
    public void testTransactionIsolationSetByUserToInvalidValue() throws SQLException {
        configuration.set("jdbc.driver", "org.greenplum.pxf.plugins.jdbc.FakeJdbcDriver");
        configuration.set("jdbc.url", "test-url");
        configuration.set("jdbc.connection.transactionIsolation", "foobarValue");

        when(mockConnectionManager.getConnection(any(), any(), any(), anyBoolean(), any(), any())).thenReturn(mockConnection);

        assertThrows(IllegalArgumentException.class,
            () -> plugin.initialize(context, configuration));
    }

    @Test
    public void testTransactionIsolationSetByUserToUnsupportedValue() throws SQLException {
        configuration.set("jdbc.driver", "org.greenplum.pxf.plugins.jdbc.FakeJdbcDriver");
        configuration.set("jdbc.url", "test-url");
        configuration.set("jdbc.connection.transactionIsolation", "READ_UNCOMMITTED");

        when(mockConnectionManager.getConnection(any(), any(), any(), anyBoolean(), any(), any())).thenReturn(mockConnection);
        when(mockConnection.getMetaData()).thenReturn(mockMetaData);
        // READ_UNCOMMITTED is level 1
        when(mockMetaData.supportsTransactionIsolationLevel(1)).thenReturn(false);

        plugin.initialize(context, configuration);

        assertThrows(SQLException.class,
            () -> plugin.getConnection(),
            "Transaction isolation level READ_UNCOMMITTED is not supported");
    }

    @Test
    public void testTransactionIsolationSetByUserToValidValue() throws SQLException {
        configuration.set("jdbc.driver", "org.greenplum.pxf.plugins.jdbc.FakeJdbcDriver");
        configuration.set("jdbc.url", "test-url");
        configuration.set("jdbc.connection.transactionIsolation", "READ_COMMITTED");

        when(mockConnectionManager.getConnection(any(), any(), any(), anyBoolean(), any(), any())).thenReturn(mockConnection);
        when(mockConnection.getMetaData()).thenReturn(mockMetaData);
        // READ_COMMITTED is level 2
        when(mockMetaData.supportsTransactionIsolationLevel(2)).thenReturn(true);

        plugin.initialize(context, configuration);
        Connection conn = plugin.getConnection();

        // READ_COMMITTED is level 2
        verify(conn).setTransactionIsolation(2);
    }

    @Test
    public void testTransactionIsolationSetByUserFailedToGetMetadata() throws SQLException {
        configuration.set("jdbc.driver", "org.greenplum.pxf.plugins.jdbc.FakeJdbcDriver");
        configuration.set("jdbc.url", "test-url");

        when(mockConnectionManager.getConnection(any(), any(), any(), anyBoolean(), any(), any())).thenReturn(mockConnection);
        doThrow(new SQLException("")).when(mockConnection).getMetaData();

        plugin.initialize(context, configuration);
        assertThrows(SQLException.class,
            () -> plugin.getConnection());
    }

//    @Test
//    public void testGetPreparedStatementSetsQueryTimeoutIfSpecified() throws SQLException {
//        configuration.set("jdbc.driver", "org.greenplum.pxf.plugins.jdbc.FakeJdbcDriver");
//        configuration.set("jdbc.url", "test-url");
//        configuration.set("jdbc.statement.queryTimeout", "173");
//
//        when(mockConnectionManager.getConnection(any(), any(), any(), anyBoolean(), any(), any())).thenReturn(mockConnection);
//        when(mockConnection.getMetaData()).thenReturn(mockMetaData);
//        when(mockConnection.prepareStatement(any())).thenReturn(mockStatement);
//
//        plugin.initialize(context, configuration);
//        plugin.getPreparedStatement(mockConnection, "foo");
//
//        verify(mockStatement).setQueryTimeout(173);
//    }
//
//    @Test
//    public void testGetPreparedStatementDoesNotSetQueryTimeoutIfNotSpecified() throws SQLException {
//        configuration.set("jdbc.driver", "org.greenplum.pxf.plugins.jdbc.FakeJdbcDriver");
//        configuration.set("jdbc.url", "test-url");
//
//        when(mockConnectionManager.getConnection(anyString(), anyString(), any(), anyBoolean(), any(), anyString())).thenReturn(mockConnection);
//        when(mockConnection.getMetaData()).thenReturn(mockMetaData);
//        when(mockConnection.prepareStatement(anyString())).thenReturn(mockStatement);
//
//        plugin.initialize(context, configuration);
//        plugin.getPreparedStatement(mockConnection, "foo");
//
//        verify(mockStatement, never()).setQueryTimeout(anyInt());
//    }

    @Test
    public void testGetConnectionNoConnPropsPoolDisabled() throws SQLException {
        context.setServerName("test-server");
        configuration.set("jdbc.driver", "org.greenplum.pxf.plugins.jdbc.FakeJdbcDriver");
        configuration.set("jdbc.url", "test-url");
        configuration.set("jdbc.pool.enabled", "false");

        when(mockConnectionManager.getConnection(any(), any(), any(), anyBoolean(), any(), any())).thenReturn(mockConnection);
        when(mockConnection.getMetaData()).thenReturn(mockMetaData);

        plugin.initialize(context, configuration);
        Connection conn = plugin.getConnection();

        assertSame(mockConnection, conn);

        verify(mockConnectionManager).getConnection("test-server", "test-url", new Properties(), false, null, null);
    }

    @Test
    public void testGetConnectionConnPropsPoolDisabled() throws SQLException {
        context.setServerName("test-server");

        Properties connProps = new Properties();
        connProps.setProperty("foo", "foo-val");
        connProps.setProperty("bar", "bar-val");

        when(mockConnectionManager.getConnection("test-server", "test-url", connProps, false, null, null)).thenReturn(mockConnection);
        when(mockConnection.getMetaData()).thenReturn(mockMetaData);

        configuration.set("jdbc.driver", "org.greenplum.pxf.plugins.jdbc.FakeJdbcDriver");
        configuration.set("jdbc.url", "test-url");
        configuration.set("jdbc.connection.property.foo", "foo-val");
        configuration.set("jdbc.connection.property.bar", "bar-val");
        configuration.set("jdbc.pool.enabled", "false");

        plugin.initialize(context, configuration);
        Connection conn = plugin.getConnection();

        assertSame(mockConnection, conn);

        verify(mockConnectionManager).getConnection("test-server", "test-url", connProps, false, null, null);
    }

    @Test
    public void testGetConnectionConnPropsPoolEnabledNoPoolProps() throws SQLException {
        context.setServerName("test-server");

        configuration.set("jdbc.driver", "org.greenplum.pxf.plugins.jdbc.FakeJdbcDriver");
        configuration.set("jdbc.url", "test-url");
        configuration.set("jdbc.connection.property.foo", "foo-val");
        configuration.set("jdbc.connection.property.bar", "bar-val");

        // pool is enabled by default
        when(mockConnectionManager.getConnection(any(), any(), any(), anyBoolean(), any(), any())).thenReturn(mockConnection);
        when(mockConnection.getMetaData()).thenReturn(mockMetaData);

        plugin.initialize(context, configuration);
        Connection conn = plugin.getConnection();

        assertSame(mockConnection, conn);

        Properties connProps = new Properties();
        connProps.setProperty("foo", "foo-val");
        connProps.setProperty("bar", "bar-val");

        verify(mockConnectionManager).getConnection("test-server", "test-url", connProps, true, poolProps, null);
    }

    @Test
    public void testGetConnectionConnPropsPoolEnabledWithQualifier() throws SQLException {
        context.setServerName("test-server");

        configuration.set("jdbc.driver", "org.greenplum.pxf.plugins.jdbc.FakeJdbcDriver");
        configuration.set("jdbc.url", "test-url");
        configuration.set("jdbc.connection.property.foo", "foo-val");
        configuration.set("jdbc.connection.property.bar", "bar-val");
        // pool is enabled by default

        configuration.set("jdbc.pool.qualifier", "qual");

        when(mockConnectionManager.getConnection(any(), any(), any(), anyBoolean(), any(), any())).thenReturn(mockConnection);
        when(mockConnection.getMetaData()).thenReturn(mockMetaData);

        plugin.initialize(context, configuration);
        Connection conn = plugin.getConnection();

        assertSame(mockConnection, conn);

        Properties connProps = new Properties();
        connProps.setProperty("foo", "foo-val");
        connProps.setProperty("bar", "bar-val");

        verify(mockConnectionManager).getConnection("test-server", "test-url", connProps, true, poolProps, "qual");
    }

    @Test
    public void testGetConnectionConnPropsPoolEnabledPoolProps() throws SQLException {
        context.setServerName("test-server");

        configuration.set("jdbc.driver", "org.greenplum.pxf.plugins.jdbc.FakeJdbcDriver");
        configuration.set("jdbc.url", "test-url");
        configuration.set("jdbc.connection.property.foo", "foo-val");
        configuration.set("jdbc.connection.property.bar", "bar-val");
        configuration.set("jdbc.pool.enabled", "true");
        configuration.set("jdbc.pool.property.abc", "abc-val");
        configuration.set("jdbc.pool.property.xyz", "xyz-val");
        configuration.set("jdbc.pool.property.maximumPoolSize", "99"); // overwrite default

        when(mockConnectionManager.getConnection(any(), any(), any(), anyBoolean(), any(), any())).thenReturn(mockConnection);
        when(mockConnection.getMetaData()).thenReturn(mockMetaData);

        plugin.initialize(context, configuration);
        Connection conn = plugin.getConnection();

        assertSame(mockConnection, conn);

        Properties connProps = new Properties();
        connProps.setProperty("foo", "foo-val");
        connProps.setProperty("bar", "bar-val");

        poolProps.setProperty("abc", "abc-val");
        poolProps.setProperty("xyz", "xyz-val");
        poolProps.setProperty("maximumPoolSize", "99");

        verify(mockConnectionManager).getConnection("test-server", "test-url", connProps, true, poolProps, null);
    }

    @Test
    public void testGetConnectionConnPropsPoolDisabledPoolProps() throws SQLException {
        context.setServerName("test-server");

        configuration.set("jdbc.driver", "org.greenplum.pxf.plugins.jdbc.FakeJdbcDriver");
        configuration.set("jdbc.url", "test-url");
        configuration.set("jdbc.connection.property.foo", "foo-val");
        configuration.set("jdbc.connection.property.bar", "bar-val");
        configuration.set("jdbc.pool.enabled", "false");
        configuration.set("jdbc.pool.property.abc", "abc-val");
        configuration.set("jdbc.pool.property.xyz", "xyz-val");

        when(mockConnectionManager.getConnection(any(), any(), any(), anyBoolean(), any(), any())).thenReturn(mockConnection);
        when(mockConnection.getMetaData()).thenReturn(mockMetaData);

        plugin.initialize(context, configuration);
        Connection conn = plugin.getConnection();

        assertSame(mockConnection, conn);

        Properties connProps = new Properties();
        connProps.setProperty("foo", "foo-val");
        connProps.setProperty("bar", "bar-val");

        verify(mockConnectionManager).getConnection("test-server", "test-url", connProps, false, null, null);
    }
}
