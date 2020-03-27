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
import org.greenplum.pxf.api.io.DataType;
import org.greenplum.pxf.api.model.ColumnDescriptor;
import org.greenplum.pxf.api.model.RequestContext;
import org.greenplum.pxf.api.task.ProducerTask;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.internal.util.reflection.FieldReader;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.mock.mockito.MockBean;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
@SpringBootTest(
    classes = JdbcTestConfig.class,
    webEnvironment = SpringBootTest.WebEnvironment.NONE
)
public class JdbcPluginTestInitialize {

    private static final String DATA_SOURCE = "t";
    private static final String JDBC_DRIVER = "java.lang.Object";  // we cannot mock Class.forName()
    private static final String JDBC_URL = "jdbc:postgresql://localhost/postgres";
    private static final List<ColumnDescriptor> COLUMNS;

    static {
        COLUMNS = new ArrayList<>();
        COLUMNS.add(new ColumnDescriptor("c1", DataType.INTEGER.getOID(), 1, null, null, true));
        COLUMNS.add(new ColumnDescriptor("c2", DataType.VARCHAR.getOID(), 2, null, null, true));
    }

    private static final String OPTION_POOL_SIZE = "POOL_SIZE";
    private static final String OPTION_QUOTE_COLUMNS = "QUOTE_COLUMNS";
    private static final String CONFIG_SESSION_KEY_PREFIX = "jdbc.session.property.";
    private static final String CONFIG_CONNECTION_KEY_PREFIX = "jdbc.connection.property.";
    private static final String[] CONFIG_PROPERTIES_KEYS = {"k1", "k2"};
    private static final String CONFIG_USER = "jdbc.user";
    private static final String CONFIG_PASSWORD = "jdbc.password";

    @Autowired
    private JdbcProcessor plugin;

    /* Dependency for JdbcProcessor */
    @MockBean
    private SerializerFactory serializerFactory;

    @MockBean
    private ProducerTaskFactory<?, ?> producerTaskFactory;

    @BeforeEach
    @SuppressWarnings("unchecked")
    public void beforeEach() {
        when(producerTaskFactory.getProducerTask(any())).thenReturn(mock(ProducerTask.class));
    }

    /**
     * Create and prepare {@link RequestContext}
     */
    private RequestContext makeContext() {
        RequestContext context = new RequestContext();
        context.setDataSource(DATA_SOURCE);
        context.setTupleDescription(COLUMNS);
        context.setRequestType(RequestContext.RequestType.WRITE_BRIDGE);
        context.setTotalSegments(1);
        return context;
    }

    /**
     * Create and prepare {@link RequestContext}
     */
    private RequestContext makeContextWithDataSource(String datasource) {
        RequestContext context = new RequestContext();
        context.setDataSource(datasource);
        context.setTupleDescription(COLUMNS);
        return context;
    }

    /**
     * Create and prepare {@link Configuration}
     */
    private Configuration makeConfiguration() {
        Configuration configuration = new Configuration();
        configuration.set("jdbc.driver", JDBC_DRIVER);
        configuration.set("jdbc.url", JDBC_URL);
        return configuration;
    }

    @Test
    public void testMinimumSettings() throws Exception {
        // Configuration
        Configuration configuration = makeConfiguration();

        // Context
        RequestContext context = makeContext();

        // Initialize plugin
        plugin.initialize(context, configuration);

        // Checks
        Class.forName(JDBC_DRIVER);
        assertEquals(JDBC_URL, getInternalState(plugin, "jdbcUrl"));
        assertEquals(COLUMNS, getInternalState(plugin, "columns"));

        assertEquals(getInternalState(plugin, "DEFAULT_BATCH_SIZE"), getInternalState(plugin, "batchSize"));
        assertEquals(getInternalState(plugin, "DEFAULT_POOL_SIZE"), getInternalState(plugin, "poolSize"));
        assertNull(getInternalState(plugin, "quoteColumns"));
        assertEquals(getInternalState(plugin, "DEFAULT_FETCH_SIZE"), getInternalState(plugin, "fetchSize"));
        assertNull(getInternalState(plugin, "queryTimeout"));
    }

    @Test
    public void testBatchSize0() throws Exception {
        // Configuration
        Configuration configuration = makeConfiguration();
        configuration.set("jdbc.statement.batchSize", "0");

        // Initialize plugin
        plugin.initialize(makeContext(), configuration);

        // Checks
        assertEquals(1, getInternalState(plugin, "batchSize"));
        assertTrue((boolean) getInternalState(plugin, "batchSizeIsSetByUser"));
    }

    @Test
    public void testBatchSize1() throws Exception {
        // Configuration
        Configuration configuration = makeConfiguration();
        configuration.set("jdbc.statement.batchSize", "1");

        // Initialize plugin
        plugin.initialize(makeContext(), configuration);

        // Checks
        assertEquals(1, getInternalState(plugin, "batchSize"));
        assertTrue((boolean) getInternalState(plugin, "batchSizeIsSetByUser"));
    }

    @Test
    public void testBatchSize2() throws Exception {
        // Configuration
        Configuration configuration = makeConfiguration();
        configuration.set("jdbc.statement.batchSize", "2");

        // Initialize plugin
        plugin.initialize(makeContext(), configuration);

        // Checks
        assertEquals(2, getInternalState(plugin, "batchSize"));
        assertTrue((boolean) getInternalState(plugin, "batchSizeIsSetByUser"));
    }

    @Test
    public void testBatchSizeOnRead() {
        // Configuration
        Configuration configuration = makeConfiguration();
        configuration.set("jdbc.statement.batchSize", "foobar");

        // Initialize plugin
        RequestContext context = makeContext();
        context.setRequestType(RequestContext.RequestType.READ_BRIDGE);
        plugin.initialize(context, configuration);

        // should not error because we don't validate this on the READ path
    }

    @Test
    public void testBatchSizeOnWrite() {
        // Configuration
        Configuration configuration = makeConfiguration();
        configuration.set("jdbc.statement.batchSize", "foobar");

        // Initialize plugin
        Exception ex = assertThrows(NumberFormatException.class,
            () -> plugin.initialize(makeContext(), configuration));
        assertEquals("For input string: \"foobar\"", ex.getMessage());
    }

    @Test
    public void testBatchSizeNegative() {
        // Configuration
        Configuration configuration = makeConfiguration();
        configuration.set("jdbc.statement.batchSize", "-1");
        Exception ex = assertThrows(IllegalArgumentException.class,
            () -> plugin.initialize(makeContext(), configuration));
        assertEquals("Property jdbc.statement.batchSize has incorrect value -1 : must be a non-negative integer", ex.getMessage());
    }

    @Test
    public void testPoolSize1() throws Exception {
        // Configuration
        Configuration configuration = makeConfiguration();

        // Context
        RequestContext context = makeContext();
        context.addOption(OPTION_POOL_SIZE, "1");

        // Initialize plugin
        plugin.initialize(context, configuration);

        // Checks
        assertEquals(1, getInternalState(plugin, "poolSize"));
    }

    @Test
    public void testPoolSizeNegative() throws Exception {
        // Configuration
        Configuration configuration = makeConfiguration();

        // Context
        RequestContext context = makeContext();
        context.addOption(OPTION_POOL_SIZE, "-1");

        // Initialize plugin
        plugin.initialize(context, configuration);

        // Checks
        assertEquals(-1, getInternalState(plugin, "poolSize"));
    }

    @Test
    public void testFetchSize() throws Exception {
        // Configuration
        Configuration configuration = makeConfiguration();
        configuration.set("jdbc.statement.fetchSize", "4");

        // Initialize plugin
        plugin.initialize(makeContext(), configuration);

        // Checks
        assertEquals(4, getInternalState(plugin, "fetchSize"));
    }

    @Test
    public void testQueryTimeout() throws Exception {
        // Configuration
        Configuration configuration = makeConfiguration();
        configuration.set("jdbc.statement.queryTimeout", "200");

        // Initialize plugin
        plugin.initialize(makeContext(), configuration);

        // Checks
        assertEquals(200, getInternalState(plugin, "queryTimeout"));
    }

    @Test
    public void testInvalidStringQueryTimeout() {
        // Configuration
        Configuration configuration = makeConfiguration();
        configuration.set("jdbc.statement.queryTimeout", "foo");
        Exception ex = assertThrows(IllegalArgumentException.class,
            () -> plugin.initialize(makeContext(), configuration));
        assertEquals("Property jdbc.statement.queryTimeout has incorrect value foo : must be a non-negative integer", ex.getMessage());
    }

    @Test
    public void testInvalidNegativeQueryTimeout() {
        // Configuration
        Configuration configuration = makeConfiguration();
        configuration.set("jdbc.statement.queryTimeout", "-1");
        // Initialize plugin
        Exception ex = assertThrows(IllegalArgumentException.class,
            () -> plugin.initialize(makeContext(), configuration));
        assertEquals("Property jdbc.statement.queryTimeout has incorrect value -1 : must be a non-negative integer", ex.getMessage());
    }

    @Test
    public void testQuoteColumnsFalse() throws Exception {
        // Configuration
        Configuration configuration = makeConfiguration();

        // Context
        RequestContext context = makeContext();
        context.addOption(OPTION_QUOTE_COLUMNS, "false");

        // Initialize plugin
        plugin.initialize(context, configuration);

        // Checks
        assertFalse((Boolean) getInternalState(plugin, "quoteColumns"));
    }

    @Test
    public void testQuoteColumnsTrue() throws Exception {
        // Configuration
        Configuration configuration = makeConfiguration();

        // Context
        RequestContext context = makeContext();
        context.addOption(OPTION_QUOTE_COLUMNS, "true");

        // Initialize plugin
        plugin.initialize(context, configuration);

        // Checks
        assertTrue((Boolean) getInternalState(plugin, "quoteColumns"));
    }

    @Test
    public void testQuoteColumnsOther() throws Exception {
        // Configuration
        Configuration configuration = makeConfiguration();

        // Context
        RequestContext context = makeContext();
        context.addOption(OPTION_QUOTE_COLUMNS, "some_other_value");

        // Initialize plugin
        plugin.initialize(context, configuration);

        // Checks
        assertFalse((Boolean) getInternalState(plugin, "quoteColumns"));
    }

    @Test
    @SuppressWarnings("unchecked")
    public void testSessionConfiguration() throws Exception {
        // Configuration
        Configuration configuration = makeConfiguration();
        configuration.set(CONFIG_SESSION_KEY_PREFIX + CONFIG_PROPERTIES_KEYS[0], "v1");
        configuration.set(CONFIG_SESSION_KEY_PREFIX + CONFIG_PROPERTIES_KEYS[1], "v2");

        // Context
        RequestContext context = makeContext();

        // Initialize plugin
        plugin.initialize(context, configuration);

        // Checks
        Map<String, String> expected = new HashMap<>();
        expected.put(CONFIG_PROPERTIES_KEYS[0], "v1");
        expected.put(CONFIG_PROPERTIES_KEYS[1], "v2");
        assertEquals(expected.entrySet(), ((Map<String, String>) getInternalState(plugin, "sessionConfiguration")).entrySet());
    }

    @Test
    public void testSessionConfigurationForbiddenSymbols() {
        // Configuration
        Configuration configuration = makeConfiguration();
        configuration.set(CONFIG_SESSION_KEY_PREFIX + CONFIG_PROPERTIES_KEYS[0], "v1");
        configuration.set(CONFIG_SESSION_KEY_PREFIX + CONFIG_PROPERTIES_KEYS[1], "v2; SELECT * FROM secrets; ");

        // Context
        RequestContext context = makeContext();

        // Initialize plugin
        Exception ex = assertThrows(IllegalArgumentException.class,
            () -> plugin.initialize(context, configuration));
        assertEquals("Some session configuration parameter contains forbidden characters", ex.getMessage());
    }

    @Test
    public void testConnectionConfiguration() throws Exception {
        // Configuration
        Configuration configuration = makeConfiguration();
        configuration.set(CONFIG_CONNECTION_KEY_PREFIX + CONFIG_PROPERTIES_KEYS[0], "v1");
        configuration.set(CONFIG_CONNECTION_KEY_PREFIX + CONFIG_PROPERTIES_KEYS[1], "v2");

        // Context
        RequestContext context = makeContext();

        // Initialize plugin
        plugin.initialize(context, configuration);

        // Checks
        // Note password and user are not set, thus configuration will be equal to the expected one
        Properties expected = new Properties();
        expected.setProperty(CONFIG_PROPERTIES_KEYS[0], "v1");
        expected.setProperty(CONFIG_PROPERTIES_KEYS[1], "v2");
        assertEquals(expected.entrySet(), ((Properties) getInternalState(plugin, "connectionConfiguration")).entrySet());
    }

    @Test
    public void testUser() throws Exception {
        // Configuration
        Configuration configuration = makeConfiguration();
        configuration.set(CONFIG_USER, "user");

        // Context
        RequestContext context = makeContext();

        // Initialize plugin
        plugin.initialize(context, configuration);

        // Checks
        Properties expected = new Properties();
        expected.setProperty("user", "user");
        assertEquals(expected.entrySet(), ((Properties) getInternalState(plugin, "connectionConfiguration")).entrySet());
    }

    @Test
    public void testUserWithImpersonation() throws Exception {
        // Configuration
        Configuration configuration = makeConfiguration();
        configuration.set("pxf.service.user.impersonation", "true");

        // Context
        RequestContext context = makeContext();
        context.setUser("proxy");

        // Initialize plugin
        plugin.initialize(context, configuration);

        // Checks
        Properties expected = new Properties();
        expected.setProperty("user", "proxy");
        assertEquals(expected.entrySet(), ((Properties) getInternalState(plugin, "connectionConfiguration")).entrySet());
    }

    @Test
    public void testUserWithImpersonationOverwrite() throws Exception {
        // Configuration
        Configuration configuration = makeConfiguration();
        configuration.set(CONFIG_USER, "user");
        configuration.set("pxf.service.user.impersonation", "true");

        // Context
        RequestContext context = makeContext();
        context.setUser("proxy");

        // Initialize plugin
        plugin.initialize(context, configuration);

        // Checks
        Properties expected = new Properties();
        expected.setProperty("user", "proxy");
        assertEquals(expected.entrySet(), ((Properties) getInternalState(plugin, "connectionConfiguration")).entrySet());
    }

    @Test
    public void testUserWithoutImpersonationNoOverwrite() throws Exception {
        // Configuration
        Configuration configuration = makeConfiguration();
        configuration.set(CONFIG_USER, "user");
        configuration.set("pxf.service.user.impersonation", "false");

        // Context
        RequestContext context = makeContext();
        context.setUser("proxy");

        // Initialize plugin
        plugin.initialize(context, configuration);

        // Checks
        Properties expected = new Properties();
        expected.setProperty("user", "user");
        assertEquals(expected.entrySet(), ((Properties) getInternalState(plugin, "connectionConfiguration")).entrySet());
    }

    @Test
    public void testUserDefaultImpersonationNoOverwrite() throws Exception {
        // Configuration
        Configuration configuration = makeConfiguration();
        configuration.set(CONFIG_USER, "user");

        // Context
        RequestContext context = makeContext();
        context.setUser("proxy");

        // Initialize plugin
        plugin.initialize(context, configuration);

        // Checks
        Properties expected = new Properties();
        expected.setProperty("user", "user");
        assertEquals(expected.entrySet(), ((Properties) getInternalState(plugin, "connectionConfiguration")).entrySet());
    }


    @Test
    public void testUserPassword() throws Exception {
        // Configuration
        Configuration configuration = makeConfiguration();
        configuration.set(CONFIG_USER, "user");
        configuration.set(CONFIG_PASSWORD, "password");

        // Context
        RequestContext context = makeContext();

        // Initialize plugin
        plugin.initialize(context, configuration);

        // Checks
        Properties expected = new Properties();
        expected.setProperty("user", "user");
        expected.setProperty("password", "password");
        assertEquals(expected.entrySet(), ((Properties) getInternalState(plugin, "connectionConfiguration")).entrySet());
    }

    @Test
    public void testPassword() throws Exception {
        // Configuration
        Configuration configuration = makeConfiguration();
        configuration.set(CONFIG_PASSWORD, "password");

        // Context
        RequestContext context = makeContext();

        // Initialize plugin
        plugin.initialize(context, configuration);

        // Checks
        Properties expected = new Properties();
        assertEquals(expected.entrySet(), ((Properties) getInternalState(plugin, "connectionConfiguration")).entrySet());
    }

    @Test
    public void testDatasourceIsTable() throws Exception {
        plugin.initialize(makeContextWithDataSource("foo"), makeConfiguration());

        assertEquals("foo", getInternalState(plugin, "tableName"));
        assertNull(getInternalState(plugin, "queryName"));
    }

    @Test
    public void testDatasourceIsQuery() throws Exception {
        plugin.initialize(makeContextWithDataSource("query:foo"), makeConfiguration());


        assertEquals("foo", getInternalState(plugin, "queryName"));
        assertNull(getInternalState(plugin, "tableName"));
    }

    @Test
    public void testInitializationFailsWhenDatasourceIsEmptyQuery() {
        Exception ex = assertThrows(IllegalArgumentException.class,
            () -> plugin.initialize(makeContextWithDataSource("query:"), makeConfiguration()));
        assertEquals("Query name is not provided in data source [query:]", ex.getMessage());
    }

    @Test
    public void testConnectionPoolEnabledPropertyNotDefined() throws Exception {
        Configuration configuration = makeConfiguration();

        plugin.initialize(makeContext(), configuration);

        Properties poolConfiguration = (Properties) getInternalState(plugin, "poolConfiguration");
        assertNotNull(poolConfiguration);
        assertEquals(4, poolConfiguration.size());
        assertEquals("5", poolConfiguration.getProperty("maximumPoolSize"));
        assertEquals("30000", poolConfiguration.getProperty("connectionTimeout"));
        assertEquals("30000", poolConfiguration.getProperty("idleTimeout"));
        assertEquals("0", poolConfiguration.getProperty("minimumIdle"));
    }

    @Test
    public void testConnectionPoolNotEnabledPropertyDefined() throws Exception {
        Configuration configuration = makeConfiguration();
        configuration.set("jdbc.pool.enabled", "false");

        plugin.initialize(makeContext(), configuration);

        assertNull(getInternalState(plugin, "poolConfiguration"));
    }

    @Test
    public void testConnectionPoolEnabledPropertyDefined() throws Exception {
        Configuration configuration = makeConfiguration();
        configuration.set("jdbc.pool.enabled", "true");
        configuration.set("jdbc.pool.property.foo", "include-foo");
        configuration.set("jdbc.pool.property.bar", "include-bar");
        configuration.set("jdbc.whatever", "exclude-whatever");

        plugin.initialize(makeContext(), configuration);

        Properties poolProps = (Properties) getInternalState(plugin, "poolConfiguration");
        assertNotNull(poolProps);
        assertEquals(6, poolProps.size());

        Properties expectedProps = new Properties();
        expectedProps.setProperty("maximumPoolSize", "5");
        expectedProps.setProperty("connectionTimeout", "30000");
        expectedProps.setProperty("idleTimeout", "30000");
        expectedProps.setProperty("minimumIdle", "0");
        expectedProps.setProperty("foo", "include-foo");
        expectedProps.setProperty("bar", "include-bar");
        assertEquals(expectedProps, poolProps);
    }

    private Object getInternalState(Object target, String fieldName) throws NoSuchFieldException {
        Class<?> clazz = target.getClass();
        if (clazz.getName().contains("$")) {
            clazz = clazz.getSuperclass();
        }
        Field field = clazz.getDeclaredField(fieldName);
        return new FieldReader(target, field).read();
    }
}
