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

import io.libraft.algorithm.StorageException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Preconditions.checkState;

/**
 * Base class for implementations that persist Raft data and
 * metadata to a JDBC backend.
 * <p/>
 * {@code JDBCBase} initializes and stores a <strong>single</strong>
 * {@link Connection} instance that it uses for all database operations.
 * This means that <strong>all</strong> operations are <strong>serialized</strong>.
 * Subclasses should <strong>never</strong> hold on to the reference
 * to {@code Connection} since it may be released at any time by {@code JDBCBase}.
 * <p/>
 * {@code JDBCBase} also defines helper methods to safely perform queries,
 * updates and transactions. These helper methods correctly clean up and release
 * acquired database resources, even in error conditions. Subclasses should
 * use these helper methods whenever possible.
 * <p/>
 * Subclasses <strong>must</strong> synchronize all calls to {@code JDBCBase}.
 */
abstract class JDBCBase {

    private final String url;
    private final String username;
    private final @Nullable String password;

    private boolean initialized;
    private Connection connection;

    protected final Logger LOGGER = LoggerFactory.getLogger(getClass());

    protected interface ConnectionBlock {

        void use(Connection connection) throws Exception;
    }

    protected interface ResultSetBlock<T> {

        T use(ResultSet resultSet) throws Exception;
    }

    protected interface StatementBlock {

        void use(PreparedStatement statement) throws Exception;
    }

    protected interface StatementWithReturnBlock<T> {

        @Nullable T use(PreparedStatement statement) throws Exception;
    }

    protected JDBCBase(String url, String username, @Nullable String password) {
        LOGGER.trace("jdbc parameters: url:{} user:{} pass:{}", url, username, (password == null || password.isEmpty() ? "" : "****"));
        this.url = url;
        this.username = username;
        this.password = password;
    }

    /**
     * Initialize the database backend.
     * <p/>
     * This method creates any tables and/or indexes needed to
     * persist data to the database. Table and index-creation happen
     * <strong>only once</strong>. Subsequent calls to this method do not
     * modify or delete existing data. An instance of {@link Connection}
     * is also created.
     * <p/>
     * This method <strong>does not</strong> validate any data already stored in database tables.
     *
     * @throws StorageException if the database backend cannot be accessed or initialized. The component
     *                          is in an inconsistent state and <strong>should not</strong> be used if this
     *                          exception is thrown
     * @throws IllegalStateException if this method is called multiple times
     */
    public synchronized final void initialize() throws StorageException {
        checkState(!initialized);

        try {
            setupConnection();
            Statement statement = connection.createStatement();
            checkNotNull(statement);
            try {
                try {
                    addDatabaseCreateStatementsToBatch(statement);
                    statement.executeBatch();
                } finally {
                    closeSilently(statement);
                }
                connection.commit();
                initialized = true;
            } finally {
                if (!initialized) {
                    connection.rollback();
                }
            }
        } catch (Exception e) {
            throw new StorageException(e);
        } finally {
            if (!initialized) {
                cleanupConnection();
            }
        }
    }

    /**
     * Clean up and release the resources used by the database backend.
     * <p/>
     * This method closes the internally-cached connection.
     * Following a successful call to {@code cleanup()} subsequent calls are noops.
     */
    public synchronized final void teardown() {
        cleanupConnection();
        initialized = false;
    }

    protected final synchronized boolean isInitialized() {
        return initialized;
    }

    protected abstract void addDatabaseCreateStatementsToBatch(Statement statement) throws Exception;

    private void setupConnection() throws SQLException {
        if (connection == null) {
            Connection newConnection = DriverManager.getConnection(url, username, password);
            newConnection.setAutoCommit(false);
            connection = newConnection;
        }
    }

    private void cleanupConnection() {
        try {
            if (connection != null) {
                connection.close();
                connection = null;
            }
        } catch (Exception closeException) {
            LOGGER.warn("fail close connection", closeException);
        }
    }

    private void closeSilently(Statement statement) {
        try {
            statement.close();
        } catch (Exception closeException) {
            LOGGER.warn("fail close statement", closeException);
        }
    }

    private void closeSilently(ResultSet resultSet) {
        try {
            resultSet.close();
        } catch (Exception closeException) {
            LOGGER.warn("fail close result set", closeException);
        }
    }

    protected final void withStatement(Connection connection, String sql, StatementBlock statementBlock) throws Exception {
        PreparedStatement statement = connection.prepareStatement(sql);
        checkNotNull(statement);
        try {
            statementBlock.use(statement);
        } finally {
            closeSilently(statement);
        }
    }

    protected final <T> T withStatement(Connection connection, String sql, StatementWithReturnBlock<T> statementBlock) throws Exception {
        PreparedStatement statement = connection.prepareStatement(sql);
        checkNotNull(statement);
        try {
            return statementBlock.use(statement);
        } finally {
            closeSilently(statement);
        }
    }

    protected final <T> T withResultSet(PreparedStatement statement, ResultSetBlock<T> resultSetBlock) throws Exception {
        ResultSet resultSet = statement.executeQuery();
        checkNotNull(resultSet);
        try {
            return resultSetBlock.use(resultSet);
        } finally {
            closeSilently(resultSet);
        }
    }

    // FIXME (AG): both of these methods are extremely similar and should be consolidated

    protected final <T> T executeQuery(final String sql, final StatementWithReturnBlock<T> queryBlock) throws Exception {
        checkState(initialized);

        setupConnection();
        checkNotNull(connection);
        boolean committed = false;
        try {
            T returnValue = withStatement(connection, sql, queryBlock);
            connection.commit();
            committed = true;
            return returnValue;
        } finally {
            if (!committed) {
                connection.rollback();
                cleanupConnection();
            }
        }
    }

    /**
     * Executes the nested SQL statements in a single transaction. If any exception
     * is thrown, the commit is rolled back. Otherwise, when {@code connectionBlock}
     * completes successfully the entire transaction is committed.
     * @param connectionBlock block containing sql statements to be executed in this transaction
     * @throws Exception on any error (from either {@code connectionBlock}, or while
     *         executing a SQL statement
     */
    protected final void execute(ConnectionBlock connectionBlock) throws Exception {
        checkState(initialized);

        setupConnection();
        checkNotNull(connection);
        boolean committed = false;
        try {
            connectionBlock.use(connection);
            connection.commit();
            committed = true;
        } finally {
            if (!committed) {
                connection.rollback();
                cleanupConnection();
            }
        }
    }
}
