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
import io.libraft.algorithm.Store;

import javax.annotation.Nullable;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import static com.google.common.base.Preconditions.checkState;

// TODO (AG): there has got to be a cleaner way to do the insert-or-update idiom in an implementation-independent way, 'cause this is horrible
// TODO (AG): change table definitions so that both current_term and commit_index can _only_ contain a single row
/**
 * Implementation of {@code Store} that uses a JDBC backend.
 * <p/>
 * This implementation creates and uses three tables with the following structures:
 * <p/>
 * {@code current_term} (holds the last-known current term for the local Raft server):
 * <pre>
 * +--------------+
 * | current_term |
 * +--------------+
 * |              | <---- <strong>must</strong> contain only 1 row
 * +--------------+
 * </pre>
 * {@code commit_index} (holds the last-known commit index for the local Raft server):
 * <pre>
 * +--------------+
 * | commit_index |
 * +--------------+
 * |              | <---- <strong>must</strong> contain only 1 row
 * +--------------+
 * </pre>
 * {@code voted_for} (holds the voting record for the local Raft server):
 * <pre>
 * +-----------+-----------+
 * |    term   |  server   |
 * +-----------+-----------+
 * |           |           |
 * +-----------+-----------+
 * </pre>
 */
public final class JDBCStore extends JDBCBase implements Store {

    /**
     * Constructor.
     * <p/>
     * The account used to connect to the database <strong>must</strong>
     * have permission to:
     * <ul>
     *     <li>Create and/or delete tables.</li>
     *     <li>Create and/or delete rows within these tables.</li>
     *     <li>Query these tables.</li>
     *     <li>Create and/or delete indexes.</li>
     * </ul>
     *
     * @param url JDBC connection string used to connect to the database
     * @param username user id of the account used to connect to the database
     * @param password password of the account used to connect to the database or {@code null} if no password is required
     */
    public JDBCStore(String url, String username, @Nullable String password) {
        super(url, username, password);
    }

    @Override
    protected void addDatabaseCreateStatementsToBatch(Statement statement) throws SQLException {
        LOGGER.info("setup raft store");

        statement.addBatch("CREATE TABLE IF NOT EXISTS current_term(term BIGINT NOT NULL)");
        statement.addBatch("CREATE TABLE IF NOT EXISTS commit_index(commit_index BIGINT NOT NULL)");
        statement.addBatch("CREATE TABLE IF NOT EXISTS voted_for(term BIGINT NOT NULL, server VARCHAR(128) DEFAULT NULL)");
    }

    private Long queryAndCheckConsistency(PreparedStatement statement, final String tableName) throws Exception {
        return withResultSet(statement, new ResultSetBlock<Long>() {
            @Override
            public Long use(ResultSet resultSet) throws Exception {
                checkState(resultSet.next(), "%s: no rows", tableName); // need one row
                long value = resultSet.getLong(1);
                checkState(!resultSet.next(), "%s: too many rows", tableName); // not two
                return value;
            }
        });
    }

    @Override
    public synchronized long getCurrentTerm() throws StorageException {
        try {
            return executeQuery("SELECT term FROM current_term", new StatementWithReturnBlock<Long>() {
                @Override
                public @Nullable Long use(PreparedStatement statement) throws Exception {
                    return queryAndCheckConsistency(statement, "current_term");
                }
            });
        } catch (Exception e) {
            throw new StorageException("fail get currentTerm", e);
        }
    }

    @Override
    public synchronized void setCurrentTerm(final long term) throws StorageException {
        try {
            execute(new ConnectionBlock() {
                @Override
                public void use(Connection connection) throws Exception {
                    boolean doUpdate = withStatement(connection, "SELECT COUNT(*) FROM current_term", new StatementWithReturnBlock<Boolean>() {
                        @Override
                        public Boolean use(PreparedStatement statement) throws Exception {
                            return withResultSet(statement, new ResultSetBlock<Boolean>() {
                                @Override
                                public Boolean use(ResultSet resultSet) throws Exception {
                                    resultSet.next(); // COUNT(*) should always return a value

                                    int count = resultSet.getInt(1);
                                    checkState(count == 0 || count == 1, "current_term: too many rows:%s", count);

                                    return count == 1;
                                }
                            });
                        }
                    });
                    if (doUpdate) {
                        withStatement(connection, "UPDATE current_term SET term=?", new StatementBlock() {
                            @Override
                            public void use(PreparedStatement statement) throws Exception {
                                statement.setLong(1, term);
                                int rowsUpdated = statement.executeUpdate();
                                checkState(rowsUpdated == 1, "commit_index: too many rows:%s)", rowsUpdated);
                            }
                        });
                    } else {
                        withStatement(connection, "INSERT INTO current_term VALUES(?)", new StatementBlock() {
                            @Override
                            public void use(PreparedStatement statement) throws Exception {
                                statement.setLong(1, term);
                                int rowsUpdated = statement.executeUpdate();
                                checkState(rowsUpdated == 1, "commit_index: too many rows:%s)", rowsUpdated);
                            }
                        });
                    }
                }
            });
        } catch (Exception e) {
            throw new StorageException(String.format("fail set currentTerm to %d", term), e);
        }
    }

    @Override
    public synchronized long getCommitIndex() throws StorageException {
        try {
            return executeQuery("SELECT commit_index FROM commit_index", new StatementWithReturnBlock<Long>() {
                @Override
                public @Nullable Long use(PreparedStatement statement) throws Exception {
                    return queryAndCheckConsistency(statement, "commit_index");
                }
            });
        } catch (Exception e) {
            throw new StorageException("fail get commitIndex", e);
        }
    }

    @Override
    public synchronized void setCommitIndex(final long logIndex) throws StorageException {
        try {
            execute(new ConnectionBlock() {
                @Override
                public void use(Connection connection) throws Exception {
                    boolean doUpdate = withStatement(connection, "SELECT COUNT(*) FROM commit_index", new StatementWithReturnBlock<Boolean>() {
                        @Override
                        public Boolean use(PreparedStatement statement) throws Exception {
                            return withResultSet(statement, new ResultSetBlock<Boolean>() {
                                @Override
                                public Boolean use(ResultSet resultSet) throws Exception {
                                    resultSet.next(); // COUNT(*) should always return a value

                                    int count = resultSet.getInt(1);
                                    checkState(count == 0 || count == 1, "commit_index: too many rows:%s", count);

                                    return count == 1;
                                }
                            });
                        }
                    });
                    if (doUpdate) {
                        withStatement(connection, "UPDATE commit_index SET commit_index=?", new StatementBlock() {
                            @Override
                            public void use(PreparedStatement statement) throws Exception {
                                statement.setLong(1, logIndex);
                                int rowsUpdated = statement.executeUpdate();
                                checkState(rowsUpdated == 1, "commit_index: too many rows:%s)", rowsUpdated);
                            }
                        });
                    } else {
                        withStatement(connection, "INSERT INTO commit_index VALUES(?)", new StatementBlock() {
                            @Override
                            public void use(PreparedStatement statement) throws Exception {
                                statement.setLong(1, logIndex);
                                int rowsUpdated = statement.executeUpdate();
                                checkState(rowsUpdated == 1, "commit_index: too many rows:%s)", rowsUpdated);
                            }
                        });
                    }
                }
            });
        } catch (Exception e) {
            throw new StorageException(String.format("fail set commitIndex to %d", logIndex), e);
        }
    }

    @Override
    public synchronized @Nullable String getVotedFor(final long term) throws StorageException {
        try {
            return executeQuery("SELECT server FROM voted_for WHERE term=?", new StatementWithReturnBlock<String>() {
                @Override
                public @Nullable String use(PreparedStatement statement) throws Exception {
                    statement.setLong(1, term);

                    return withResultSet(statement, new ResultSetBlock<String>() {
                        @Override
                        public String use(ResultSet resultSet) throws Exception {
                            String server = null;

                            if (resultSet.next()) { // either 1 value or none
                                server = resultSet.getString(1);
                            }

                            checkState(!resultSet.next(), "too many rows for voted_for in term %s", term);

                            return server;
                        }
                    });
                }
            });
        } catch (Exception e) {
            throw new StorageException(String.format("fail get votedFor in term %d", term), e);
        }
    }

    @Override
    public synchronized void setVotedFor(final long term, final String server) throws StorageException {
        try {
            execute(new ConnectionBlock() {
                @Override
                public void use(Connection connection) throws Exception {
                    boolean doUpdate = withStatement(connection, "SELECT COUNT(*) FROM voted_for WHERE term=?", new StatementWithReturnBlock<Boolean>() {
                        @Override
                        public Boolean use(PreparedStatement statement) throws Exception {
                            statement.setLong(1, term);
                            return withResultSet(statement, new ResultSetBlock<Boolean>() {
                                @Override
                                public Boolean use(ResultSet resultSet) throws Exception {
                                    resultSet.next(); // COUNT(*) should always return a value

                                    int count = resultSet.getInt(1);
                                    checkState(count == 0 || count == 1, "voted_for: term:%s too many rows:%s)", term, count);

                                    return count == 1;
                                }
                            });
                        }
                    });
                    if (doUpdate) {
                        withStatement(connection, "UPDATE voted_for SET server=? WHERE term=?", new StatementBlock() {
                            @Override
                            public void use(PreparedStatement statement) throws Exception {
                                statement.setString(1, server);
                                statement.setLong(2, term);
                                int rowsUpdated = statement.executeUpdate();
                                checkState(rowsUpdated == 1, "voted_for: term:%s too many rows:%s)", term, rowsUpdated);
                            }
                        });
                    } else {
                        withStatement(connection, "INSERT INTO voted_for VALUES(?, ?)", new StatementBlock() {
                            @Override
                            public void use(PreparedStatement statement) throws Exception {
                                statement.setLong(1, term);
                                statement.setString(2, server);
                                int rowsUpdated = statement.executeUpdate();
                                checkState(rowsUpdated == 1, "voted_for: term:%s too many rows:%s", term, rowsUpdated);
                            }
                        });
                    }
                }
            });
        } catch (Exception e) {
            throw new StorageException(String.format("fail set votedFor in term %d to %s", term, server), e);
        }
    }

    @Override
    public void clearVotedFor() throws StorageException {
        try {
            execute(new ConnectionBlock() {
                @Override
                public void use(Connection connection) throws Exception {
                    withStatement(connection, "DELETE FROM voted_for", new StatementBlock() {
                        @Override
                        public void use(PreparedStatement statement) throws Exception {
                            statement.execute();
                        }
                    });
                }
            });
        } catch (Exception e) {
            throw new StorageException("fail clear votedFor", e);
        }
    }
}
