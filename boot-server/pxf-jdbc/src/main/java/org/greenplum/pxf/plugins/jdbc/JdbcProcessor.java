package org.greenplum.pxf.plugins.jdbc;

import lombok.SneakyThrows;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.greenplum.pxf.api.factory.ConfigurationFactory;
import org.greenplum.pxf.api.io.DataType;
import org.greenplum.pxf.api.model.BaseProcessor;
import org.greenplum.pxf.api.model.ColumnDescriptor;
import org.greenplum.pxf.api.model.DataSplit;
import org.greenplum.pxf.api.model.DataSplitter;
import org.greenplum.pxf.api.model.RequestContext;
import org.greenplum.pxf.api.model.TupleIterator;
import org.greenplum.pxf.api.utilities.Utilities;
import org.greenplum.pxf.plugins.jdbc.utils.ConnectionManager;
import org.greenplum.pxf.plugins.jdbc.utils.DbProduct;
import org.greenplum.pxf.plugins.jdbc.utils.HiveJdbcUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;

import java.io.File;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import static org.greenplum.pxf.api.security.SecureLogin.CONFIG_KEY_SERVICE_USER_IMPERSONATION;

/**
 * JDBC tables processor
 *
 * <p>The SELECT queries are processed by {@link java.sql.Statement}
 */
@Component
@Scope("prototype")
public class JdbcProcessor extends BaseProcessor<ResultSet, Void> {

    private static final Logger LOG = LoggerFactory.getLogger(JdbcProcessor.class);

    // '100' is a recommended value: https://docs.oracle.com/cd/E11882_01/java.112/e16548/oraperf.htm#JJDBC28754
    private static final int DEFAULT_BATCH_SIZE = 100;
    private static final int DEFAULT_FETCH_SIZE = 1000;
    private static final int DEFAULT_POOL_SIZE = 1;

    // configuration parameter names
    private static final String JDBC_DRIVER_PROPERTY_NAME = "jdbc.driver";
    private static final String JDBC_URL_PROPERTY_NAME = "jdbc.url";
    private static final String JDBC_USER_PROPERTY_NAME = "jdbc.user";
    private static final String JDBC_PASSWORD_PROPERTY_NAME = "jdbc.password";
    private static final String JDBC_SESSION_PROPERTY_PREFIX = "jdbc.session.property.";
    private static final String JDBC_CONNECTION_PROPERTY_PREFIX = "jdbc.connection.property.";

    // connection parameter names
    private static final String JDBC_CONNECTION_TRANSACTION_ISOLATION = "jdbc.connection.transactionIsolation";

    // statement properties
    private static final String JDBC_STATEMENT_BATCH_SIZE_PROPERTY_NAME = "jdbc.statement.batchSize";
    private static final String JDBC_STATEMENT_FETCH_SIZE_PROPERTY_NAME = "jdbc.statement.fetchSize";
    private static final String JDBC_STATEMENT_QUERY_TIMEOUT_PROPERTY_NAME = "jdbc.statement.queryTimeout";

    // connection pool properties
    private static final String JDBC_CONNECTION_POOL_ENABLED_PROPERTY_NAME = "jdbc.pool.enabled";
    private static final String JDBC_CONNECTION_POOL_PROPERTY_PREFIX = "jdbc.pool.property.";
    private static final String JDBC_POOL_QUALIFIER_PROPERTY_NAME = "jdbc.pool.qualifier";

    // DDL option names
    private static final String JDBC_DRIVER_OPTION_NAME = "JDBC_DRIVER";
    private static final String JDBC_URL_OPTION_NAME = "DB_URL";

    private static final String FORBIDDEN_SESSION_PROPERTY_CHARACTERS = ";\n\b\0";
    private static final String QUERY_NAME_PREFIX = "query:";
    private static final int QUERY_NAME_PREFIX_LENGTH = QUERY_NAME_PREFIX.length();

    private static final String HIVE_URL_PREFIX = "jdbc:hive2://";
    private static final String HIVE_DEFAULT_DRIVER_CLASS = "org.apache.hive.jdbc.HiveDriver";

    private enum TransactionIsolation {
        READ_UNCOMMITTED(1),
        READ_COMMITTED(2),
        REPEATABLE_READ(4),
        SERIALIZABLE(8),
        NOT_PROVIDED(-1);

        private int isolationLevel;

        TransactionIsolation(int transactionIsolation) {
            isolationLevel = transactionIsolation;
        }

        public int getLevel() {
            return isolationLevel;
        }

        public static TransactionIsolation typeOf(String str) {
            return valueOf(str);
        }
    }

    // Whether JDBC connection properties have been initialized
    private boolean isJdbcInitialized = false;

    // JDBC parameters from config file or specified in DDL
    private String jdbcUrl;

    protected String tableName;

    // Write batch size
    protected int batchSize;
    protected boolean batchSizeIsSetByUser = false;

    // Read batch size
    protected int fetchSize;

    // Thread pool size
    protected int poolSize;

    // Query timeout.
    protected Integer queryTimeout;

    // Quote columns setting set by user (three values are possible)
    protected Boolean quoteColumns = null;

    // Environment variables to SET before query execution
    protected Map<String, String> sessionConfiguration = new HashMap<String, String>();

    // Properties object to pass to JDBC Driver when connection is created
    protected Properties connectionConfiguration = new Properties();

    // Transaction isolation level that a user can configure
    private TransactionIsolation transactionIsolation = TransactionIsolation.NOT_PROVIDED;

    // Columns description
    protected List<ColumnDescriptor> columns = null;

    // Name of query to execute for read flow (optional)
    protected String queryName;

    // connection pool fields
    private boolean isConnectionPoolUsed;
    private Properties poolConfiguration;
    private String poolQualifier;

    private final ConnectionManager connectionManager;

    static {
        // Deprecated as of Oct 22, 2019 in version 5.9.2+
        Configuration.addDeprecation("pxf.impersonation.jdbc",
            CONFIG_KEY_SERVICE_USER_IMPERSONATION,
            "The property \"pxf.impersonation.jdbc\" has been deprecated in favor of \"pxf.service.user.impersonation\".");
    }

    public JdbcProcessor(ConnectionManager connectionManager) {
        this.connectionManager = connectionManager;
    }

    @Override
    public void initialize(RequestContext context, Configuration configuration) {
        super.initialize(context, configuration);
        initializeJdbcProperties();
    }

    /**
     * {@inheritDoc}
     */
    @SneakyThrows
    @Override
    public TupleIterator<ResultSet> getTupleIterator(DataSplit split) {
        if (!(split instanceof JdbcDataSplit))
            throw new IllegalArgumentException("A JdbcDataSplit is required");
        ensureJdbcInitialized();
        return new JdbcTupleItr((JdbcDataSplit) split);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Iterator<Object> getFields(ResultSet tuple) {
        ensureJdbcInitialized();
        return new ResultSetItr(tuple);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean canProcessRequest(RequestContext context) {
        return StringUtils.equalsIgnoreCase("jdbc", context.getProtocol()) &&
            StringUtils.isEmpty(context.getFormat());
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public DataSplitter getDataSplitter() {
        return new JdbcDataSplitter(context, configuration);
    }

    private class JdbcTupleItr implements TupleIterator<ResultSet> {

        // Read variables
        private Statement statementRead;
        private ResultSet resultSetRead;
        private boolean hasNext;
        private boolean consumed;


        public JdbcTupleItr(JdbcDataSplit split) throws SQLException {

            try {
                Connection connection = getConnection();
                SQLQueryBuilder sqlQueryBuilder = new SQLQueryBuilder(context, connection.getMetaData(), getQueryText(), split);

                // Build SELECT query
                if (quoteColumns == null) {
                    sqlQueryBuilder.autoSetQuoteString();
                } else if (quoteColumns) {
                    sqlQueryBuilder.forceSetQuoteString();
                }

                String queryRead = sqlQueryBuilder.buildSelectQuery();
                LOG.trace("Select query: {}", queryRead);

                // Execute queries
                statementRead = connection.createStatement();
                statementRead.setFetchSize(fetchSize);

                if (queryTimeout != null) {
                    LOG.debug("Setting query timeout to {} seconds", queryTimeout);
                    statementRead.setQueryTimeout(queryTimeout);
                }
                resultSetRead = statementRead.executeQuery(queryRead);
                hasNext = resultSetRead.next();
                consumed = false;
            } catch (SQLException ex) {
                cleanup();
                throw ex;
            }
        }

        @SneakyThrows
        @Override
        public boolean hasNext() {
            checkIfNextIsAvailable();
            return hasNext;
        }

        @SneakyThrows
        @Override
        public ResultSet next() {
            checkIfNextIsAvailable();

            if (!hasNext)
                throw new NoSuchElementException();

            consumed = true;
            return resultSetRead;
        }

        /**
         * cleanup() implementation
         */
        @Override
        public void cleanup() throws SQLException {
            if (resultSetRead != null) {
                try {
                    resultSetRead.close();
                } catch (SQLException e) {
                    // ignore ...
                }
            }
            closeStatementAndConnection(statementRead);
        }

        private void checkIfNextIsAvailable() throws SQLException {
            if (consumed) {
                hasNext = resultSetRead.next();
                consumed = false;
            }
        }
    }

    /**
     * Open a new JDBC connection
     *
     * @return {@link Connection}
     * @throws SQLException if a database access or connection error occurs
     */
    public Connection getConnection() throws SQLException {
        LOG.debug("Requesting a new JDBC connection. URL={} table={} txid:seg={}:{}", jdbcUrl, tableName, context.getTransactionId(), context.getSegmentId());

        Connection connection = null;
        try {
            connection = getConnectionInternal();
            LOG.debug("Obtained a JDBC connection {} for URL={} table={} txid:seg={}:{}", connection, jdbcUrl, tableName, context.getTransactionId(), context.getSegmentId());

            prepareConnection(connection);
        } catch (Exception e) {
            closeConnection(connection);
            if (e instanceof SQLException) {
                throw (SQLException) e;
            } else {
                String msg = e.getMessage();
                if (msg == null) {
                    Throwable t = e.getCause();
                    if (t != null) msg = t.getMessage();
                }
                throw new SQLException(msg, e);
            }
        }

        return connection;
    }

    /**
     * Initializes JDBC properties if they have not been initialized
     */
    private void ensureJdbcInitialized() {
        if (!isJdbcInitialized) {
            initializeJdbcProperties();
        }
    }

    /**
     * Initializes JDBC properties from the context and configuration
     */
    private void initializeJdbcProperties() {
        // Required parameter. Can be auto-overwritten by user options
        String jdbcDriver = configuration.get(JDBC_DRIVER_PROPERTY_NAME);
        assertMandatoryParameter(jdbcDriver, JDBC_DRIVER_PROPERTY_NAME, JDBC_DRIVER_OPTION_NAME);
        try {
            LOG.debug("JDBC driver: '{}'", jdbcDriver);
            Class.forName(jdbcDriver);
        } catch (ClassNotFoundException e) {
            throw new RuntimeException(e);
        }

        // Required parameter. Can be auto-overwritten by user options
        jdbcUrl = configuration.get(JDBC_URL_PROPERTY_NAME);
        assertMandatoryParameter(jdbcUrl, JDBC_URL_PROPERTY_NAME, JDBC_URL_OPTION_NAME);

        // Required metadata
        String dataSource = context.getDataSource();
        if (org.apache.commons.lang.StringUtils.isBlank(dataSource)) {
            throw new IllegalArgumentException("Data source must be provided");
        }

        // Determine if the datasource is a table name or a query name
        if (dataSource.startsWith(QUERY_NAME_PREFIX)) {
            queryName = dataSource.substring(QUERY_NAME_PREFIX_LENGTH);
            if (org.apache.commons.lang.StringUtils.isBlank(queryName)) {
                throw new IllegalArgumentException(String.format("Query name is not provided in data source [%s]", dataSource));
            }
            LOG.debug("Query name is {}", queryName);
        } else {
            tableName = dataSource;
            LOG.debug("Table name is {}", tableName);
        }

        // Required metadata
        columns = context.getTupleDescription();

        // Optional parameters
        batchSizeIsSetByUser = configuration.get(JDBC_STATEMENT_BATCH_SIZE_PROPERTY_NAME) != null;
        if (context.getRequestType() == RequestContext.RequestType.WRITE_BRIDGE) {
            batchSize = configuration.getInt(JDBC_STATEMENT_BATCH_SIZE_PROPERTY_NAME, DEFAULT_BATCH_SIZE);

            if (batchSize == 0) {
                batchSize = 1; // if user set to 0, it is the same as batchSize of 1
            } else if (batchSize < 0) {
                throw new IllegalArgumentException(String.format(
                    "Property %s has incorrect value %s : must be a non-negative integer", JDBC_STATEMENT_BATCH_SIZE_PROPERTY_NAME, batchSize));
            }
        }

        fetchSize = configuration.getInt(JDBC_STATEMENT_FETCH_SIZE_PROPERTY_NAME, DEFAULT_FETCH_SIZE);

        poolSize = context.getOption("POOL_SIZE", DEFAULT_POOL_SIZE);

        String queryTimeoutString = configuration.get(JDBC_STATEMENT_QUERY_TIMEOUT_PROPERTY_NAME);
        if (org.apache.commons.lang.StringUtils.isNotBlank(queryTimeoutString)) {
            try {
                queryTimeout = Integer.parseUnsignedInt(queryTimeoutString);
            } catch (NumberFormatException e) {
                throw new IllegalArgumentException(String.format(
                    "Property %s has incorrect value %s : must be a non-negative integer",
                    JDBC_STATEMENT_QUERY_TIMEOUT_PROPERTY_NAME, queryTimeoutString), e);
            }
        }

        // Optional parameter. The default value is null
        String quoteColumnsRaw = context.getOption("QUOTE_COLUMNS");
        if (quoteColumnsRaw != null) {
            quoteColumns = Boolean.parseBoolean(quoteColumnsRaw);
        }

        // Optional parameter. The default value is empty map
        sessionConfiguration.putAll(getPropsWithPrefix(configuration, JDBC_SESSION_PROPERTY_PREFIX));
        // Check forbidden symbols
        // Note: PreparedStatement enables us to skip this check: its values are distinct from its SQL code
        // However, SET queries cannot be executed this way. This is why we do this check
        if (sessionConfiguration.entrySet().stream()
            .anyMatch(
                entry ->
                    org.apache.commons.lang.StringUtils.containsAny(
                        entry.getKey(), FORBIDDEN_SESSION_PROPERTY_CHARACTERS
                    ) ||
                        org.apache.commons.lang.StringUtils.containsAny(
                            entry.getValue(), FORBIDDEN_SESSION_PROPERTY_CHARACTERS
                        )
            )
        ) {
            throw new IllegalArgumentException("Some session configuration parameter contains forbidden characters");
        }
        if (LOG.isDebugEnabled()) {
            LOG.debug("Session configuration: {}",
                sessionConfiguration.entrySet().stream()
                    .map(entry -> "'" + entry.getKey() + "'='" + entry.getValue() + "'")
                    .collect(Collectors.joining(", "))
            );
        }

        // Optional parameter. The default value is empty map
        connectionConfiguration.putAll(getPropsWithPrefix(configuration, JDBC_CONNECTION_PROPERTY_PREFIX));

        // Optional parameter. The default value depends on the database
        String transactionIsolationString = configuration.get(JDBC_CONNECTION_TRANSACTION_ISOLATION, "NOT_PROVIDED");
        transactionIsolation = TransactionIsolation.typeOf(transactionIsolationString);

        // Set optional user parameter, taking into account impersonation setting for the server.
        String jdbcUser = configuration.get(JDBC_USER_PROPERTY_NAME);
        boolean impersonationEnabledForServer = configuration.getBoolean(CONFIG_KEY_SERVICE_USER_IMPERSONATION, false);
        LOG.debug("JDBC impersonation is {}enabled for server {}", impersonationEnabledForServer ? "" : "not ", context.getServerName());
        if (impersonationEnabledForServer) {
            if (Utilities.isSecurityEnabled(configuration) && org.apache.commons.lang.StringUtils.startsWith(jdbcUrl, HIVE_URL_PREFIX)) {
                // secure impersonation for Hive JDBC driver requires setting URL fragment that cannot be overwritten by properties
                String updatedJdbcUrl = HiveJdbcUtils.updateImpersonationPropertyInHiveJdbcUrl(jdbcUrl, context.getUser());
                LOG.debug("Replaced JDBC URL {} with {}", jdbcUrl, updatedJdbcUrl);
                jdbcUrl = updatedJdbcUrl;
            } else {
                // the jdbcUser is the GPDB user
                jdbcUser = context.getUser();
            }
        }
        if (jdbcUser != null) {
            LOG.debug("Effective JDBC user {}", jdbcUser);
            connectionConfiguration.setProperty("user", jdbcUser);
        } else {
            LOG.debug("JDBC user has not been set");
        }

        if (LOG.isDebugEnabled()) {
            LOG.debug("Connection configuration: {}",
                connectionConfiguration.entrySet().stream()
                    .map(entry -> "'" + entry.getKey() + "'='" + entry.getValue() + "'")
                    .collect(Collectors.joining(", "))
            );
        }

        // This must be the last parameter parsed, as we output connectionConfiguration earlier
        // Optional parameter. By default, corresponding connectionConfiguration property is not set
        if (jdbcUser != null) {
            String jdbcPassword = configuration.get(JDBC_PASSWORD_PROPERTY_NAME);
            if (jdbcPassword != null) {
                LOG.debug("Connection password: {}", ConnectionManager.maskPassword(jdbcPassword));
                connectionConfiguration.setProperty("password", jdbcPassword);
            }
        }

        // connection pool is optional, enabled by default
        isConnectionPoolUsed = configuration.getBoolean(JDBC_CONNECTION_POOL_ENABLED_PROPERTY_NAME, true);
        LOG.debug("Connection pool is {}enabled", isConnectionPoolUsed ? "" : "not ");
        if (isConnectionPoolUsed) {
            poolConfiguration = new Properties();
            // for PXF upgrades where jdbc-site template has not been updated, make sure there're sensible defaults
            poolConfiguration.setProperty("maximumPoolSize", "5");
            poolConfiguration.setProperty("connectionTimeout", "30000");
            poolConfiguration.setProperty("idleTimeout", "30000");
            poolConfiguration.setProperty("minimumIdle", "0");
            // apply values read from the template
            poolConfiguration.putAll(getPropsWithPrefix(configuration, JDBC_CONNECTION_POOL_PROPERTY_PREFIX));

            // packaged Hive JDBC Driver does not support connection.isValid() method, so we need to force set
            // connectionTestQuery parameter in this case, unless already set by the user
            if (jdbcUrl.startsWith(HIVE_URL_PREFIX) && HIVE_DEFAULT_DRIVER_CLASS.equals(jdbcDriver) && poolConfiguration.getProperty("connectionTestQuery") == null) {
                poolConfiguration.setProperty("connectionTestQuery", "SELECT 1");
            }

            // get the qualifier for connection pool, if configured. Might be used when connection session authorization is employed
            // to switch effective user once connection is established
            poolQualifier = configuration.get(JDBC_POOL_QUALIFIER_PROPERTY_NAME);
        }
        isJdbcInitialized = true;
    }

    /**
     * For a Kerberized Hive JDBC connection, it creates a connection as the loginUser.
     * Otherwise, it returns a new connection.
     *
     * @return for a Kerberized Hive JDBC connection, returns a new connection as the loginUser.
     * Otherwise, it returns a new connection.
     * @throws Exception when an error occurs
     */
    private Connection getConnectionInternal() throws Exception {
        if (Utilities.isSecurityEnabled(configuration) && StringUtils.startsWith(jdbcUrl, HIVE_URL_PREFIX)) {
            // TODO: doAs needed here
            throw new UnsupportedOperationException();
//                return SecureLogin.getInstance().getLoginUser(context, configuration).
//                    doAs((PrivilegedExceptionAction<Connection>) () ->
//                        connectionManager.getConnection(context.getServerName(), jdbcUrl, connectionConfiguration, isConnectionPoolUsed, poolConfiguration, poolQualifier));

        } else {
            return connectionManager.getConnection(context.getServerName(), jdbcUrl, connectionConfiguration, isConnectionPoolUsed, poolConfiguration, poolQualifier);
        }
    }

    /**
     * Prepare JDBC connection by setting session-level variables in external database
     *
     * @param connection {@link Connection} to prepare
     */
    private void prepareConnection(Connection connection) throws SQLException {
        if (connection == null) {
            throw new IllegalArgumentException("The provided connection is null");
        }

        DatabaseMetaData metadata = connection.getMetaData();

        // Handle optional connection transaction isolation level
        if (transactionIsolation != TransactionIsolation.NOT_PROVIDED) {
            // user wants to set isolation level explicitly
            if (metadata.supportsTransactionIsolationLevel(transactionIsolation.getLevel())) {
                LOG.debug("Setting transaction isolation level to {} on connection {}", transactionIsolation.toString(), connection);
                connection.setTransactionIsolation(transactionIsolation.getLevel());
            } else {
                throw new RuntimeException(
                    String.format("Transaction isolation level %s is not supported", transactionIsolation.toString())
                );
            }
        }

        // Disable autocommit
        if (metadata.supportsTransactions()) {
            LOG.debug("Setting autoCommit to false on connection {}", connection);
            connection.setAutoCommit(false);
        }

        // Prepare session (process sessionConfiguration)
        if (!sessionConfiguration.isEmpty()) {
            DbProduct dbProduct = DbProduct.getDbProduct(metadata.getDatabaseProductName());

            try (Statement statement = connection.createStatement()) {
                for (Map.Entry<String, String> e : sessionConfiguration.entrySet()) {
                    String sessionQuery = dbProduct.buildSessionQuery(e.getKey(), e.getValue());
                    LOG.debug("Executing statement {} on connection {}", sessionQuery, connection);
                    statement.execute(sessionQuery);
                }
            }
        }
    }

    /**
     * Gets the text of the query by reading the file from the server configuration directory. The name of the file
     * is expected to be the same as the name of the query provided by the user and have extension ".sql"
     *
     * @return text of the query
     */
    private String getQueryText() {
        if (org.apache.commons.lang.StringUtils.isBlank(queryName)) {
            return null;
        }
        // read the contents of the file holding the text of the query with a given name
        String serverDirectory = configuration.get(ConfigurationFactory.PXF_CONFIG_SERVER_DIRECTORY_PROPERTY);
        if (org.apache.commons.lang.StringUtils.isBlank(serverDirectory)) {
            throw new IllegalStateException("No server configuration directory found for server " + context.getServerName());
        }

        String queryText;
        try {
            File queryFile = new File(serverDirectory, queryName + ".sql");
            if (LOG.isDebugEnabled()) {
                LOG.debug("Reading text of query={} from {}", queryName, queryFile.getCanonicalPath());
            }
            queryText = FileUtils.readFileToString(queryFile);
        } catch (IOException e) {
            throw new RuntimeException(String.format("Failed to read text of query %s : %s", queryName, e.getMessage()), e);
        }
        if (org.apache.commons.lang.StringUtils.isBlank(queryText)) {
            throw new RuntimeException(String.format("Query text file is empty for query %s", queryName));
        }

        // Remove one or more semicolons followed by optional blank space
        // happening at the end of the query
        queryText = queryText.replaceFirst("(;+\\s*)+$", "");

        return queryText;
    }

    /**
     * Constructs a mapping of configuration and includes all properties that start with the specified
     * configuration prefix.  Property names in the mapping are trimmed to remove the configuration prefix.
     * This is a method from Hadoop's Configuration class ported here to make older and custom versions of Hadoop
     * work with JDBC profile.
     *
     * @param configuration configuration map
     * @param confPrefix    configuration prefix
     * @return mapping of configuration properties with prefix stripped
     */
    private Map<String, String> getPropsWithPrefix(Configuration configuration, String confPrefix) {
        Map<String, String> configMap = new HashMap<>();
        for (Map.Entry<String, String> stringStringEntry : configuration) {
            String propertyName = stringStringEntry.getKey();
            if (propertyName.startsWith(confPrefix)) {
                // do not use value from the iterator as it might not come with variable substitution
                String value = configuration.get(propertyName);
                String keyName = propertyName.substring(confPrefix.length());
                configMap.put(keyName, value);
            }
        }
        return configMap;
    }

    /**
     * Asserts whether a given parameter has non-empty value, throws IllegalArgumentException otherwise
     *
     * @param value      value to check
     * @param paramName  parameter name
     * @param optionName name of the option for a given parameter
     */
    private void assertMandatoryParameter(String value, String paramName, String optionName) {
        if (StringUtils.isBlank(value)) {
            throw new IllegalArgumentException(String.format(
                "Required parameter %s is missing or empty in jdbc-site.xml and option %s is not specified in table definition.", paramName, optionName)
            );
        }
    }

    /**
     * Close a JDBC statement and underlying {@link Connection}
     *
     * @param statement statement to close
     * @throws SQLException when a SQL error occurs
     */
    public static void closeStatementAndConnection(Statement statement) throws SQLException {
        if (statement == null) {
            LOG.warn("Call to close statement and connection is ignored as statement provided was null");
            return;
        }

        SQLException exception = null;
        Connection connection = null;

        try {
            connection = statement.getConnection();
        } catch (SQLException e) {
            LOG.error("Exception when retrieving Connection from Statement", e);
            exception = e;
        }

        try {
            LOG.debug("Closing statement for connection {}", connection);
            statement.close();
        } catch (SQLException e) {
            LOG.error("Exception when closing Statement", e);
            exception = e;
        }

        try {
            closeConnection(connection);
        } catch (SQLException e) {
            LOG.error(String.format("Exception when closing connection %s", connection), e);
            exception = e;
        }

        if (exception != null) {
            throw exception;
        }
    }

    /**
     * Close a JDBC connection
     *
     * @param connection connection to close
     * @throws SQLException when a SQL error occurs
     */
    private static void closeConnection(Connection connection) throws SQLException {
        if (connection == null) {
            LOG.warn("Call to close connection is ignored as connection provided was null");
            return;
        }
        try {
            if (!connection.isClosed() &&
                connection.getMetaData().supportsTransactions() &&
                !connection.getAutoCommit()) {

                LOG.debug("Committing transaction (as part of connection.close()) on connection {}", connection);
                connection.commit();
            }
        } finally {
            try {
                LOG.debug("Closing connection {}", connection);
                connection.close();
            } catch (Exception e) {
                // ignore
                LOG.warn(String.format("Failed to close JDBC connection %s, ignoring the error.", connection), e);
            }
        }
    }

    private class ResultSetItr implements Iterator<Object> {
        private int currentColumn;
        private final int totalColumns;
        private final ResultSet result;

        public ResultSetItr(ResultSet result) {
            this.result = result;
            this.totalColumns = context.getTupleDescription().size();
        }

        @Override
        public boolean hasNext() {
            return currentColumn < totalColumns;
        }

        @SneakyThrows
        @Override
        public Object next() {
            if (currentColumn >= totalColumns)
                throw new NoSuchElementException();

            ColumnDescriptor column = context.getTupleDescription().get(currentColumn++);
            String colName = column.columnName();
            Object value;

            /*
             * Non-projected columns get null values
             */
            if (!column.isProjected()) return null;

            switch (DataType.get(column.columnTypeCode())) {
                case INTEGER:
                    value = result.getInt(colName);
                    break;
                case FLOAT8:
                    value = result.getDouble(colName);
                    break;
                case REAL:
                    value = result.getFloat(colName);
                    break;
                case BIGINT:
                    value = result.getLong(colName);
                    break;
                case SMALLINT:
                    value = result.getShort(colName);
                    break;
                case BOOLEAN:
                    value = result.getBoolean(colName);
                    break;
                case BYTEA:
                    value = result.getBytes(colName);
                    break;
                case VARCHAR:
                case BPCHAR:
                case TEXT:
                case NUMERIC:
                    value = result.getString(colName);
                    break;
                case DATE:
                    value = result.getDate(colName);
                    break;
                case TIMESTAMP:
                    value = result.getTimestamp(colName);
                    break;
                default:
                    throw new UnsupportedOperationException(
                        String.format("Field type '%s' (column '%s') is not supported",
                            DataType.get(column.columnTypeCode()), column));
            }
            return result.wasNull() ? null : value;
        }
    }
}
