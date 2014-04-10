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

import com.google.common.collect.Sets;
import io.libraft.Command;
import io.libraft.agent.CommandDeserializer;
import io.libraft.agent.CommandSerializer;
import io.libraft.algorithm.Log;
import io.libraft.algorithm.LogEntry;
import io.libraft.algorithm.StorageException;

import javax.annotation.Nullable;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Types;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;

/**
 * Implementation of {@code Log} that uses a JDBC backend.
 * <p/>
 * This implementation creates and uses a single table called {@code log_index} with the following structure:
 * <pre>
 * +-----------+-----------+----------+----------+
 * | log_index |   term    |   type   |   data   |
 * +-----------+-----------+----------+----------+
 * |           |           |          |          |
 * +-----------+-----------+----------+----------+
 * </pre>
 * This table contains <strong>all</strong> the {@link LogEntry}
 * instances communicated to the local Raft server. It uses the
 * supplied {@link CommandSerializer} to serialize a {@code Command} for
 * safe storage in the database. It uses a supplied,
 * matching {@link CommandDeserializer} to deserialize a previously-serialized
 * {@code Command} into its corresponding Java representation.
 *
 * @see io.libraft.Command
 */
public final class JDBCLog extends JDBCBase implements Log {

    private CommandSerializer commandSerializer;
    private CommandDeserializer commandDeserializer;

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
     * @param commandSerializer instance of {@code CommandSerializer} that
     *                          will serialize {@code Command} instances to the database
     * @param commandDeserializer instance of {@code CommandDeserializer}
     *                            that will deserialize database rows that contain {@code Command}
     *                            instances into their corresponding Java representation
     *
     * @see io.libraft.Command
     */
    public JDBCLog(String url, String username, @Nullable String password, CommandSerializer commandSerializer, CommandDeserializer commandDeserializer) {
        super(url, username, password);
        this.commandSerializer = commandSerializer;
        this.commandDeserializer = commandDeserializer;
    }

    /**
     * Set the {@code CommandSerializer} and {@code CommandDeserializer} used by this {@code JDBCLog} instance.
     * This <strong>overrides</strong> the serializer/deserializer pair specified in the instance constructor.
     *
     * @param commandSerializer instance of {@code CommandSerializer} that will
     *                          will serialize {@code Command} instances to the database
     * @param commandDeserializer instance of {@code CommandDeserializer}
     *                            that will deserialize database rows that contain {@code Command}
     *                            instances into their corresponding Java representation
     *
     * @throws IllegalStateException if this method is called <strong>after</strong>
     *                               a successful call to {@link JDBCLog#initialize()}
     *
     * @see io.libraft.Command
     */
    public synchronized void setupCustomCommandSerializerAndDeserializer(CommandSerializer commandSerializer, CommandDeserializer commandDeserializer) {
        checkState(!isInitialized());

        this.commandSerializer = commandSerializer;
        this.commandDeserializer = commandDeserializer;
    }

    @Override
    protected void addDatabaseCreateStatementsToBatch(Statement statement) throws SQLException {
        LOGGER.info("setup raft log");

        statement.addBatch("CREATE TABLE IF NOT EXISTS entries(log_index BIGINT PRIMARY KEY, term BIGINT NOT NULL, type TINYINT NOT NULL, data BLOB DEFAULT NULL)");
        statement.addBatch("CREATE INDEX IF NOT EXISTS entries_index ON entries(log_index DESC)");
    }

    @Override
    public @Nullable LogEntry get(final long logIndex) throws StorageException {
        try {
            return executeQuery("SELECT * FROM entries WHERE log_index=?", new StatementWithReturnBlock<LogEntry>() {
                @Override
                public @Nullable LogEntry use(PreparedStatement statement) throws Exception {
                    statement.setLong(1, logIndex);
                    return withResultSet(statement, new ResultSetBlock<LogEntry>() {
                        @Override
                        public LogEntry use(ResultSet resultSet) throws Exception {
                            if (!resultSet.next()) {
                                return null;
                            }

                            long logIndex = resultSet.getLong("log_index");
                            long term = resultSet.getLong("term");
                            LogEntry.Type type = mapDatabaseTypeToLogEntryType(resultSet.getInt("type"));
                            byte[] serializedData = resultSet.getBytes("data");

                            checkState(!resultSet.next(), "entries: incorrect rows for logIndex:%s", logIndex);

                            return newLogEntryFromDatabaseRow(type, logIndex, term, serializedData);
                        }
                    });
                }
            });
        } catch (Exception e) {
            throw new StorageException(String.format("fail get log entry at %d", logIndex), e);
        }
    }

    @Override
    public synchronized @Nullable LogEntry getFirst() throws StorageException {
        try {
            return executeQuery("SELECT * FROM entries WHERE log_index=(SELECT MIN(log_index) FROM ENTRIES)", new StatementWithReturnBlock<LogEntry>() {
                @Override
                public @Nullable LogEntry use(PreparedStatement statement) throws Exception {
                    return withResultSet(statement, new ResultSetBlock<LogEntry>() {
                        @Override
                        public LogEntry use(ResultSet resultSet) throws Exception {
                            if (!resultSet.next()) {
                                return null;
                            }

                            return newLogEntryFromDatabaseRow(
                                    mapDatabaseTypeToLogEntryType(resultSet.getInt("type")),
                                    resultSet.getLong("log_index"),
                                    resultSet.getLong("term"),
                                    resultSet.getBytes("data"));
                        }
                    });
                }
            });
        } catch (Exception e) {
            throw new StorageException("fail get last log entry", e);
        }
    }

    @Override
    public synchronized @Nullable LogEntry getLast() throws StorageException {
        try {
            return executeQuery("SELECT * FROM entries WHERE log_index=(SELECT MAX(log_index) FROM ENTRIES)", new StatementWithReturnBlock<LogEntry>() {
                @Override
                public @Nullable LogEntry use(PreparedStatement statement) throws Exception {
                    return withResultSet(statement, new ResultSetBlock<LogEntry>() {
                        @Override
                        public LogEntry use(ResultSet resultSet) throws Exception {
                            if (!resultSet.next()) {
                                return null;
                            }

                            return newLogEntryFromDatabaseRow(
                                    mapDatabaseTypeToLogEntryType(resultSet.getInt("type")),
                                    resultSet.getLong("log_index"),
                                    resultSet.getLong("term"),
                                    resultSet.getBytes("data"));
                        }
                    });
                }
            });
        } catch (Exception e) {
            throw new StorageException("fail get last log entry", e);
        }
    }

    @Override
    public synchronized void put(final LogEntry logEntry) throws StorageException {
        try {
            execute(new ConnectionBlock() {
                @Override
                public void use(Connection connection) throws Exception {
                    boolean doUpdate = withStatement(connection, "SELECT COUNT(*) FROM entries WHERE log_index=?", new StatementWithReturnBlock<Boolean>() {
                        @Override
                        public @Nullable Boolean use(PreparedStatement statement) throws Exception {
                            statement.setLong(1, logEntry.getIndex());
                            return withResultSet(statement, new ResultSetBlock<Boolean>() {
                                @Override
                                public Boolean use(ResultSet resultSet) throws Exception {
                                    resultSet.next();
                                    long count = resultSet.getInt(1);
                                    checkState(count == 0 || count == 1, "entries: logIndex:%s incorrect row count:", logEntry.getIndex(), count);
                                    return count == 1;
                                }
                            });
                        }
                    });
                    if (doUpdate) {
                        withStatement(connection, "UPDATE entries SET term=?, type=?, data=? WHERE log_index=?", new StatementBlock() {
                            @Override
                            public void use(PreparedStatement statement) throws Exception {
                                statement.setLong(1, logEntry.getTerm());
                                statement.setInt(2, mapLogEntryTypeToDatabaseType(logEntry.getType()));
                                setSerializedData(statement, 3, logEntry);
                                statement.setLong(4, logEntry.getIndex());

                                int rowsUpdated = statement.executeUpdate();
                                checkState(rowsUpdated == 1, "entries: incorrect rows updated:", rowsUpdated);
                            }
                        });
                    } else {
                        withStatement(connection, "INSERT INTO entries VALUES(?, ?, ?, ?)", new StatementBlock() {
                            @Override
                            public void use(PreparedStatement statement) throws Exception {
                                statement.setLong(1, logEntry.getIndex());
                                statement.setLong(2, logEntry.getTerm());
                                statement.setInt(3, mapLogEntryTypeToDatabaseType(logEntry.getType()));
                                setSerializedData(statement, 4, logEntry);

                                int rowsUpdated = statement.executeUpdate();
                                checkState(rowsUpdated == 1, "entries: incorrect rows updated:", rowsUpdated);
                            }
                        });
                    }
                }
            });
        } catch (Exception e) {
            throw new StorageException(String.format("fail put %s into log", logEntry), e);
        }
    }

    @Override
    public synchronized void truncate(final long index) throws StorageException {
        try {
            execute(new ConnectionBlock() {
                @Override
                public void use(Connection connection) throws Exception {
                    withStatement(connection, "DELETE FROM entries WHERE log_index>=?", new StatementBlock() {
                        @Override
                        public void use(PreparedStatement statement) throws Exception {
                            statement.setLong(1, index);
                            statement.execute();
                        }
                    });
                }
            });
        } catch (Exception e) {
            throw new StorageException(String.format("fail truncate log from logIndex %d", index), e);
        }
    }

    private int mapLogEntryTypeToDatabaseType(LogEntry.Type type) {
        switch (type) {
            case SENTINEL:
                return 0;
            case NOOP:
                return 1;
            case CONFIGURATION:
                return 2;
            case CLIENT:
                return 3;
            default:
                throw new IllegalArgumentException("unrecognized type:" + type.name());
        }
    }

    private LogEntry.Type mapDatabaseTypeToLogEntryType(int type) {
        switch (type) {
            case 0:
                return LogEntry.Type.SENTINEL;
            case 1:
                return LogEntry.Type.NOOP;
            case 2:
                return LogEntry.Type.CONFIGURATION;
            case 3:
                return LogEntry.Type.CLIENT;
            default:
                throw new IllegalArgumentException("unrecognized type:" + type);
        }
    }

    private byte[] getSerializedData(Command command) throws IOException {
        ByteArrayOutputStream bos = new ByteArrayOutputStream();
        commandSerializer.serialize(command, bos);
        return bos.toByteArray();
    }

    private void setSerializedData(PreparedStatement statement, int dataPosition, LogEntry logEntry) throws IOException, SQLException {
        if (logEntry.getType() == LogEntry.Type.CLIENT) {
            LogEntry.ClientEntry clientEntry = (LogEntry.ClientEntry) logEntry;
            statement.setBytes(dataPosition, getSerializedData(clientEntry.getCommand()));
        } else {
            statement.setNull(dataPosition, Types.BLOB);
        }
    }

    private LogEntry newLogEntryFromDatabaseRow(LogEntry.Type type, long logIndex, long term, byte[] serializedData) throws IOException {
        switch (type) {
            case SENTINEL:
                checkArgument(logIndex == LogEntry.SENTINEL.getIndex(), "mismatched sentinel logIndex:%s", logIndex);
                checkArgument(term == LogEntry.SENTINEL.getTerm(), "mismatched sentinel term:%s", term);
                return LogEntry.SENTINEL;
            case NOOP:
                return new LogEntry.NoopEntry(logIndex, term);
            case CONFIGURATION:
                return new LogEntry.ConfigurationEntry(logIndex, term, Sets.<String>newHashSet(), Sets.<String>newHashSet());
            case CLIENT:
                InputStream in = new ByteArrayInputStream(serializedData);
                Command command = commandDeserializer.deserialize(in);
                return new LogEntry.ClientEntry(logIndex, term, command);
            default:
                throw new IllegalArgumentException("unrecognized type:" + type.name());
        }
    }
}
