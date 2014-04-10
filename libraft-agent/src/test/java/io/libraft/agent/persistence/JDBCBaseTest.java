/*
 * Copyright (c) 2013 - 2014, Allen A. George <allen dot george at gmail dot com>
 *
 * All rights reserved.
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions are met:
 *     * Redistributions of source code must retain the above copyright
 *       notice, this list of conditions and the following disclaimer.
 *     * Redistributions in binary form must reproduce the above copyright
 *       notice, this list of conditions and the following disclaimer in the
 *       documentation and/or other materials provided with the distribution.
 *     * Neither the name of libraft nor the
 *       names of its contributors may be used to endorse or promote products
 *       derived from this software without specific prior written permission.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS" AND
 * ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED
 * WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE
 * DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE LIABLE FOR
 * ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES
 * (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES;
 * LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND
 * ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
 * (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS
 * SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 */

package io.libraft.agent.persistence;

import com.google.common.collect.Lists;
import io.libraft.agent.TestLoggingRule;
import io.libraft.algorithm.StorageException;
import io.libraft.mockjdbc.MockDriver;
import org.hamcrest.Matchers;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.mockito.InOrder;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;
import java.util.Properties;
import java.util.Random;
import java.util.concurrent.Callable;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.instanceOf;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

// TODO (AG): consider simply setting initialized so that I can make the tests more focused
public final class JDBCBaseTest {

    private static final Logger LOGGER = LoggerFactory.getLogger(JDBCBaseTest.class);

    @Rule
    public final TestLoggingRule testLoggingRule = new TestLoggingRule(LOGGER);

    @Rule
    public final ExpectedException expectedException = ExpectedException.none();

    // IMPORTANT: this is the SUT
    private class JDBCTestSubclass extends JDBCBase {

        private JDBCTestSubclass(String url, String username, @Nullable String password) {
            super(url, username, password);
        }

        @Override
        protected void addDatabaseCreateStatementsToBatch(Statement statement) throws Exception {
            // noop
        }
    }

    private Random random = new Random();
    private String driverId;
    private MockDriver mockDriver;
    private Connection mockConnection;
    private Connection mockReplacedConnection;
    private Statement mockStatement;
    private PreparedStatement mockPreparedStatement;
    private ResultSet mockResultSet;
    private JDBCTestSubclass jdbcTestSubclass;

    @Before
    public void setup() throws SQLException {
        driverId = getDriverId();
        mockDriver = spy(new MockDriver(driverId));
        DriverManager.registerDriver(mockDriver);

        mockConnection = mock(Connection.class);
        mockReplacedConnection = mock(Connection.class);
        mockStatement = mock(Statement.class);
        mockPreparedStatement = mock(PreparedStatement.class);
        mockResultSet = mock(ResultSet.class);

        jdbcTestSubclass = spy(new JDBCTestSubclass(getJdbcUrl("test"), "test", "test"));
    }

    private String getDriverId() {
        int driverId = Math.abs(random.nextInt());
        return "mockjdbc-" + driverId;
    }

    @After
    public void teardown() throws SQLException {
        DriverManager.deregisterDriver(mockDriver);
    }

    @Test
    public void shouldThrowStorageExceptionIfStoreCannotBeInitializedBecauseNoDriverIsFound() throws SQLException, StorageException {
        // according to the DriverManager source code, if no one could connect
        // (because no one returns early from DriverManager.getConnection driver-search loop)
        // we'll get an error message of the form:
        // SQLException("No suitable driver found for "+ url, "08001")
        when(mockDriver.connect(anyString(), any(Properties.class))).thenReturn(null);

        expectedException.expect(StorageException.class);
        expectedException.expectCause(Matchers.<Throwable>instanceOf(SQLException.class));
        jdbcTestSubclass.initialize();
    }

    @Test
    public void shouldThrowStorageExceptionIfStoreCannotBeInitializedBecauseCreateStatementReturnsNull() throws SQLException, StorageException {
        when(mockDriver.connect(anyString(), any(Properties.class))).thenReturn(mockConnection);
        when(mockConnection.createStatement()).thenReturn(null);

        assertThatInitializeThrows(NullPointerException.class);

        InOrder inOrder = inOrder(mockConnection);
        inOrder.verify(mockConnection).close();
    }

    @Test
    public void shouldThrowStorageExceptionIfStoreCannotBeInitializedBecauseCreateStatementThrows() throws SQLException, StorageException {
        when(mockDriver.connect(anyString(), any(Properties.class))).thenReturn(mockConnection);
        when(mockConnection.createStatement()).thenThrow(new IllegalStateException("cannot create statement"));

        assertThatInitializeThrows(IllegalStateException.class);

        InOrder inOrder = inOrder(mockConnection);
        inOrder.verify(mockConnection).close();
    }

    // NOTE: use IllegalArgumentException to avoid tripping over Preconditions.checkState -thrown exceptions by mistake

    @Test
    public void shouldThrowStorageExceptionIfStoreCannotBeInitializedBecauseAddDatabaseCreateStatementsToBatchThrows() throws Exception {
        when(mockDriver.connect(anyString(), any(Properties.class))).thenReturn(mockConnection);
        when(mockConnection.createStatement()).thenReturn(mockStatement);
        doThrow(new IllegalArgumentException("database broken")).when(jdbcTestSubclass).addDatabaseCreateStatementsToBatch(mockStatement);

        assertThatInitializeThrows(IllegalArgumentException.class);

        InOrder inOrder = inOrder(mockStatement, mockConnection);
        inOrder.verify(mockStatement).close();
        inOrder.verify(mockConnection).rollback();
        inOrder.verify(mockConnection).close();
    }

    @Test
    public void shouldCommitAfterAddDatabaseCreateStatementsToBatchCompletesSuccessfully() throws Exception {
        initializeJDBCTestSubclass();

        InOrder inOrder = inOrder(mockStatement, mockConnection);
        inOrder.verify(mockStatement).close();
        inOrder.verify(mockConnection).commit();
        inOrder.verifyNoMoreInteractions(); // aren't closing out the connection or doing anything else with either object
    }

    private void assertThatInitializeThrows(Class<? extends Throwable> storageExceptionCauseKlass) {
        boolean caughtException = false;
        try {
            jdbcTestSubclass.initialize();
        } catch (StorageException e) {
            caughtException = true;
            assertThat(e.getCause(), instanceOf(storageExceptionCauseKlass));
        }

        assertThat(caughtException, equalTo(true));
    }

    @Test
    public void shouldCloseResultSetIfExceptionIsThrown() throws Exception {
        when(mockPreparedStatement.executeQuery()).thenReturn(mockResultSet);

        final JDBCBase.ResultSetBlock<?> mockResultSetBlock = mock(JDBCBase.ResultSetBlock.class);
        doThrow(IllegalArgumentException.class).when(mockResultSetBlock).use(mockResultSet);

        assertThatExceptionIsThrown(new Callable<Void>() {

            @Override
            public Void call() throws Exception {
                jdbcTestSubclass.withResultSet(mockPreparedStatement, mockResultSetBlock);
                return null;
            }
        }, IllegalArgumentException.class);

        verify(mockResultSet).close();
    }

    @Test
    public void shouldClosePreparedStatementIfExceptionIsThrownInStatementBlock() throws Exception {
        when(mockConnection.prepareStatement(anyString())).thenReturn(mockPreparedStatement);

        final JDBCBase.StatementBlock mockStatementBlock = mock(JDBCBase.StatementBlock.class);
        doThrow(IllegalArgumentException.class).when(mockStatementBlock).use(mockPreparedStatement);

        assertThatExceptionIsThrown(new Callable<Void>() {

            @Override
            public Void call() throws Exception {
                jdbcTestSubclass.withStatement(mockConnection, "SOME SQL", mockStatementBlock);
                return null;
            }
        }, IllegalArgumentException.class);

        verify(mockPreparedStatement).close();
    }

    @Test
    public void shouldClosePreparedStatementIfExceptionIsThrownInStatementWithReturnBlock() throws Exception {
        when(mockConnection.prepareStatement(anyString())).thenReturn(mockPreparedStatement);

        final JDBCBase.StatementWithReturnBlock<?> mockStatementWithReturnBlock = mock(JDBCBase.StatementWithReturnBlock.class);
        doThrow(IllegalArgumentException.class).when(mockStatementWithReturnBlock).use(mockPreparedStatement);

        assertThatExceptionIsThrown(new Callable<Void>() {

            @Override
            public Void call() throws Exception {
                jdbcTestSubclass.withStatement(mockConnection, "SOME SQL", mockStatementWithReturnBlock);
                return null;
            }
        }, IllegalArgumentException.class);

        verify(mockPreparedStatement).close();
    }

    @Test
    public void shouldRollbackCommitAndCloseConnectionIfExceptionIsThrownInExecute() throws Exception {
        final JDBCBase.ConnectionBlock mockConnectionBlock = mock(JDBCBase.ConnectionBlock.class);
        doThrow(IllegalArgumentException.class).when(mockConnectionBlock).use(mockConnection);

        initializeJDBCTestSubclass();

        assertThatExceptionIsThrown(new Callable<Void>() {

            @Override
            public Void call() throws Exception {
                jdbcTestSubclass.execute(mockConnectionBlock);
                return null;
            }
        }, IllegalArgumentException.class);

        InOrder inOrder = inOrder(mockConnection);
        inOrder.verify(mockConnection).rollback();
        inOrder.verify(mockConnection).close();
    }

    @Test
    public void shouldCreateSecondConnectionAfterFirstThrowsExceptionInsideExecute() throws Exception {
        final JDBCBase.ConnectionBlock mockConnectionBlock = mock(JDBCBase.ConnectionBlock.class);
        final List<Connection> connectionsUsed = Lists.newArrayList();

        // store the connections used
        doAnswer(new Answer() {
            @Override
            public Object answer(InvocationOnMock invocation) throws Throwable {
                connectionsUsed.add((Connection) invocation.getArguments()[0]);

                // the first call will throw
                if (connectionsUsed.size() == 1) {
                    throw new IllegalArgumentException("first invocation");
                }

                return null;
            }
        }).when(mockConnectionBlock).use(any(Connection.class));

        initializeJDBCTestSubclass();

        // check that the first call throws
        assertThatExceptionIsThrown(new Callable<Void>() {
            @Override
            public Void call() throws Exception {
                jdbcTestSubclass.execute(mockConnectionBlock);
                return null;
            }
        }, IllegalArgumentException.class);

        // and that we clean up properly
        InOrder inOrder = inOrder(mockConnection);
        inOrder.verify(mockConnection).rollback();
        inOrder.verify(mockConnection).close();

        // call again, and verify that the connection was replaced
        jdbcTestSubclass.execute(mockConnectionBlock);

        // check that we were handed two different connections in order!
        assertThat(connectionsUsed, hasSize(2));
        assertThat(connectionsUsed, contains(mockConnection, mockReplacedConnection));
    }

    @Test
    public void shouldCommitIfExecuteSucceeds() throws Exception {
        final JDBCBase.ConnectionBlock mockConnectionBlock = mock(JDBCBase.ConnectionBlock.class);

        initializeJDBCTestSubclass();

        jdbcTestSubclass.execute(mockConnectionBlock);

        // and that we commit
        verify(mockConnection, times(2)).commit(); // the first time was because of the initialize() call
        verify(mockConnection, times(0)).close();
    }

    @Test
    public void shouldRollbackCommitAndCloseConnectionIfExceptionIsThrownInExecuteQuery() throws Exception {
        when(mockConnection.prepareStatement(anyString())).thenReturn(mockPreparedStatement);

        final JDBCBase.StatementWithReturnBlock<?> mockStatementWithReturnBlock = mock(JDBCBase.StatementWithReturnBlock.class);
        doThrow(IllegalArgumentException.class).when(mockStatementWithReturnBlock).use(mockPreparedStatement);

        initializeJDBCTestSubclass();

        assertThatExceptionIsThrown(new Callable<Void>() {

            @Override
            public Void call() throws Exception {
                jdbcTestSubclass.executeQuery("NO SQL", mockStatementWithReturnBlock);
                return null;
            }
        }, IllegalArgumentException.class);

        InOrder inOrder = inOrder(mockPreparedStatement, mockConnection);
        inOrder.verify(mockPreparedStatement).close();
        inOrder.verify(mockConnection).rollback();
        inOrder.verify(mockConnection).close();
    }

    @Test
    public void shouldCreateSecondConnectionAfterFirstThrowsExceptionInsideExecuteQuery() throws Exception {
        PreparedStatement mockReplacedPreparedStatement = mock(PreparedStatement.class);

        when(mockConnection.prepareStatement(anyString())).thenReturn(mockPreparedStatement);
        when(mockReplacedConnection.prepareStatement(anyString())).thenReturn(mockReplacedPreparedStatement);

        final JDBCBase.StatementWithReturnBlock<?> mockStatementWithReturnBlock = mock(JDBCBase.StatementWithReturnBlock.class);
        final List<PreparedStatement> statementsUsed = Lists.newArrayList();

        // store the connections used
        doAnswer(new Answer() {
            @Override
            public Object answer(InvocationOnMock invocation) throws Throwable {
                statementsUsed.add((PreparedStatement) invocation.getArguments()[0]);

                // the first call will throw
                if (statementsUsed.size() == 1) {
                    throw new IllegalArgumentException("first invocation");
                }

                return null;
            }
        }).when(mockStatementWithReturnBlock).use(any(PreparedStatement.class));

        initializeJDBCTestSubclass();

        // check that the first call throws
        assertThatExceptionIsThrown(new Callable<Void>() {
            @Override
            public Void call() throws Exception {
                jdbcTestSubclass.executeQuery("SOME SQL", mockStatementWithReturnBlock);
                return null;
            }
        }, IllegalArgumentException.class);

        // and that we clean up properly
        InOrder inOrder = inOrder(mockPreparedStatement, mockConnection);
        inOrder.verify(mockPreparedStatement).close();
        inOrder.verify(mockConnection).rollback();
        inOrder.verify(mockConnection).close();

        // call again, and verify that the connection was replaced
        jdbcTestSubclass.executeQuery("ANOTHER SQL QUERY", mockStatementWithReturnBlock);

        // check that we were handed two different prepared statements (which could only be created by two different mock connections)
        assertThat(statementsUsed, hasSize(2));
        assertThat(statementsUsed, contains(mockPreparedStatement, mockReplacedPreparedStatement));
    }

    @Test
    public void shouldCommitIfExecuteQuerySucceeds() throws Exception {
        when(mockConnection.prepareStatement(anyString())).thenReturn(mockPreparedStatement);

        final JDBCBase.StatementWithReturnBlock<?> mockStatementWithReturnBlock = mock(JDBCBase.StatementWithReturnBlock.class);

        initializeJDBCTestSubclass();

        jdbcTestSubclass.executeQuery("SOME SQL", mockStatementWithReturnBlock);

        // and that we commit
        InOrder inOrder = inOrder(mockPreparedStatement, mockConnection);
        inOrder.verify(mockConnection).commit(); // the first was for the initialize() call
        inOrder.verify(mockPreparedStatement).close();
        inOrder.verify(mockConnection).commit(); // the second is for the actual query
        inOrder.verify(mockConnection, times(0)).close();
    }

    @Test
    public void shouldTeardownSuccessfully() throws Exception {
        initializeJDBCTestSubclass();

        jdbcTestSubclass.teardown();

        verify(mockConnection).close();
    }

    @Test
    public void shouldNoopOnSubsequentCallsIfTeardownCalledMultipleTimes() throws Exception {
        initializeJDBCTestSubclass();

        jdbcTestSubclass.teardown();
        jdbcTestSubclass.teardown();
        jdbcTestSubclass.teardown();
        jdbcTestSubclass.teardown();

        verify(mockConnection, times(1)).close();
    }

    // FIXME (AG): this test is brittle because it relies on the exact sequence of calls within initialize and teardown
    @Test
    public void shouldAllowInitializeToBeCalledAfterTeardown() throws Exception {
        initializeJDBCTestSubclass();

        jdbcTestSubclass.teardown();

        Statement mockReplacedStatement = mock(Statement.class);
        when(mockReplacedConnection.createStatement()).thenReturn(mockReplacedStatement); // used in the second initialize() call

        jdbcTestSubclass.initialize();

        InOrder inOrder = inOrder(mockConnection, mockStatement, mockReplacedConnection, mockReplacedStatement);
        inOrder.verify(mockConnection).createStatement();
        inOrder.verify(mockStatement).executeBatch();
        inOrder.verify(mockStatement).close();
        inOrder.verify(mockConnection).commit();
        inOrder.verify(mockConnection).close();
        inOrder.verify(mockReplacedConnection).createStatement();
        inOrder.verify(mockReplacedStatement).executeBatch();
        inOrder.verify(mockReplacedStatement).close();
        inOrder.verify(mockReplacedConnection).commit();
        inOrder.verifyNoMoreInteractions();
    }

    private void assertThatExceptionIsThrown(Callable<Void> callable, Class<? extends Throwable> expectedThrowableKlass) {
        boolean caughtException = false;
        try {
            callable.call();
        } catch (Exception e) {
            caughtException = true;
            assertThat(e, instanceOf(expectedThrowableKlass));
        }

        assertThat(caughtException, equalTo(true));
    }

    private void initializeJDBCTestSubclass() throws SQLException, StorageException {
        when(mockDriver.connect(anyString(), any(Properties.class)))
                .thenReturn(mockConnection) // first invocation, return this
                .thenReturn(mockReplacedConnection); // second, return this

        when(mockConnection.createStatement()).thenReturn(mockStatement);

        jdbcTestSubclass.initialize();
    }

    private String getJdbcUrl(String specificDatabase) {
        return String.format("jdbc:%s:%s", driverId, specificDatabase);
    }
}
