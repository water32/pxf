package org.greenplum.pxf.plugins.jdbc.utils;

import com.google.common.util.concurrent.Uninterruptibles;
import com.zaxxer.hikari.HikariDataSource;
import com.zaxxer.hikari.HikariPoolMXBean;
import org.greenplum.pxf.plugins.jdbc.JdbcTestConfig;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.core.env.Environment;
import org.springframework.test.context.TestPropertySource;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.atLeast;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
@SpringBootTest(
    classes = JdbcTestConfig.class,
    webEnvironment = SpringBootTest.WebEnvironment.NONE
)
@TestPropertySource("classpath:jdbc-config-test-2.properties")
class ConnectionManagerWithQuickerIntervalTest {

    @Autowired
    private Environment environment;

    @Autowired
    @MockBean
    private ConnectionManager.DriverManagerWrapper mockDriverManagerWrapper;

    private Connection mockConnection;

    private Properties connProps, poolProps;

    @BeforeEach
    public void before() {
        connProps = new Properties();
        poolProps = new Properties();
        mockConnection = mock(Connection.class);
    }

    @Test
    public void testPoolExpirationWithActiveConnections() throws SQLException {
        ConnectionManagerTest.FakeTicker ticker = new ConnectionManagerTest.FakeTicker();
        ConnectionManager.DataSourceFactory mockFactory = mock(ConnectionManager.DataSourceFactory.class);
        HikariDataSource mockDataSource = mock(HikariDataSource.class);
        when(mockFactory.createDataSource(any())).thenReturn(mockDataSource);
        when(mockDataSource.getConnection()).thenReturn(mockConnection);

        HikariPoolMXBean mockMBean = mock(HikariPoolMXBean.class);
        when(mockDataSource.getHikariPoolMXBean()).thenReturn(mockMBean);
        when(mockMBean.getActiveConnections()).thenReturn(2, 1, 0);
        ConnectionManager manager = new ConnectionManager(mockFactory, ticker, mockDriverManagerWrapper, environment);

        manager.getConnection("test-server", "test-url", connProps, true, poolProps, null);

        ticker.advanceTime(ConnectionManager.POOL_EXPIRATION_TIMEOUT_HOURS + 1, TimeUnit.HOURS);
        manager.cleanCache();

        // wait for at least 3 iteration of sleeping
        Uninterruptibles.sleepUninterruptibly(2500, TimeUnit.MILLISECONDS);

        verify(mockMBean, times(3)).getActiveConnections();
        verify(mockDataSource, times(1)).close(); // verify datasource is closed when evicted
    }

    @Test
    public void testPoolExpirationWithActiveConnectionsOver24Hours() throws SQLException {
        ConnectionManagerTest.FakeTicker ticker = new ConnectionManagerTest.FakeTicker();
        ConnectionManager.DataSourceFactory mockFactory = mock(ConnectionManager.DataSourceFactory.class);
        HikariDataSource mockDataSource = mock(HikariDataSource.class);
        when(mockFactory.createDataSource(any())).thenReturn(mockDataSource);
        when(mockDataSource.getConnection()).thenReturn(mockConnection);

        HikariPoolMXBean mockMBean = mock(HikariPoolMXBean.class);
        when(mockDataSource.getHikariPoolMXBean()).thenReturn(mockMBean);
        when(mockMBean.getActiveConnections()).thenReturn(1); //always report pool has an active connection
        ConnectionManager manager = new ConnectionManager(mockFactory, ticker, mockDriverManagerWrapper, environment);

        manager.getConnection("test-server", "test-url", connProps, true, poolProps, null);

        ticker.advanceTime(ConnectionManager.POOL_EXPIRATION_TIMEOUT_HOURS + 1, TimeUnit.HOURS);
        manager.cleanCache();

        // wait for at least 3 iteration of sleeping (3 * 50ms = 150ms)
        Uninterruptibles.sleepUninterruptibly(150, TimeUnit.MILLISECONDS);

        ticker.advanceTime(ConnectionManager.CLEANUP_TIMEOUT_NANOS + 100000, TimeUnit.NANOSECONDS);

        // wait again as cleaner needs to pick new ticker value
        Uninterruptibles.sleepUninterruptibly(150, TimeUnit.MILLISECONDS);

        verify(mockMBean, atLeast(3)).getActiveConnections();
        verify(mockDataSource, times(1)).close(); // verify datasource is closed when evicted
    }

}
