/*
 * Copyright (c) 2013, Allen A. George <allen dot george at gmail dot com>
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

package io.libraft.kayvee.store;

import io.libraft.kayvee.api.KeyValue;
import org.skife.jdbi.v2.DBI;
import org.skife.jdbi.v2.Handle;
import org.skife.jdbi.v2.TransactionCallback;
import org.skife.jdbi.v2.TransactionStatus;
import org.skife.jdbi.v2.tweak.HandleCallback;

import javax.annotation.Nullable;
import java.util.Collection;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;

/**
 * Represents the local server's view of the replicated key-value store.
 * Methods in the class are called to transform the local server's key-value
 * state whenever a {@link KayVeeCommand} is committed to the cluster.
 */
public class LocalStore {

    private final class ExceptionReference {

        private KayVeeException exception = null;

        public KayVeeException getException() {
            return exception;
        }

        public void setException(KayVeeException exception) {
            this.exception = exception;
        }

        public boolean hasException() {
            return exception != null;
        }
    }

    private final DBI dbi;

    public LocalStore(DBI dbi) {
        this.dbi = dbi;
    }

    // NOTE: avoid early returns, because it may prevent updateLastAppliedCommandIndex() from being called

    public void initialize() {
        dbi.inTransaction(new TransactionCallback<Void>() {
            @Override
            public Void inTransaction(Handle conn, TransactionStatus status) throws Exception {
                KeyValueDAO keyValueDAO = conn.attach(KeyValueDAO.class);
                CommandIndexDAO commandIndexDAO = conn.attach(CommandIndexDAO.class);

                // create the tables
                keyValueDAO.createTable();
                commandIndexDAO.createTable();

                // populate the kv_last_applied_command_index table with '0' if it has no entries
                Long lastAppliedCommandIndex = commandIndexDAO.getLastAppliedCommandIndex();
                if (lastAppliedCommandIndex == null) {
                    commandIndexDAO.addLastAppliedCommandIndex(0);
                }

                return null;
            }
        });
    }

    long getLastAppliedCommandIndex() {
        return dbi.inTransaction(new TransactionCallback<Long>() {
            @Override
            public Long inTransaction(Handle conn, TransactionStatus status) throws Exception {
                CommandIndexDAO commandIndexDAO = conn.attach(CommandIndexDAO.class);
                return commandIndexDAO.getLastAppliedCommandIndex();
            }
        });
    }

    void nop(final long commandIndex) {
        dbi.withHandle(new HandleCallback<Void>() {
            @Override
            public Void withHandle(Handle handle) throws Exception {
                CommandIndexDAO commandIndexDAO = handle.attach(CommandIndexDAO.class);
                commandIndexDAO.updateLastAppliedCommandIndex(commandIndex);
                return null;
            }
        });
    }

    KeyValue get(final long commandIndex, final String key) throws KayVeeException {
        checkCommandIndex(commandIndex);

        final ExceptionReference exceptionReference = new ExceptionReference();

        KeyValue keyValue = dbi.inTransaction(new TransactionCallback<KeyValue>() {
            @Override
            public KeyValue inTransaction(Handle conn, TransactionStatus status) throws Exception {
                KeyValueDAO keyValueDAO = conn.attach(KeyValueDAO.class);

                KeyValue keyValue = keyValueDAO.get(key);
                updateAppliedCommandIndexInTransaction(conn, commandIndex);

                if (keyValue == null) {
                    exceptionReference.setException(new KeyNotFoundException(key));
                    return null;
                } else {
                    return keyValue;
                }
            }
        });

        if (exceptionReference.hasException()) {
            throw exceptionReference.getException();
        }

        checkState(keyValue != null, "failed to throw KeyNotFoundException for %s (commandIndex:%s)", key, commandIndex);
        return keyValue;
    }

    Collection<KeyValue> getAll(final long commandIndex) {
        checkCommandIndex(commandIndex);

        return dbi.inTransaction(new TransactionCallback<Collection<KeyValue>>() {
            @Override
            public Collection<KeyValue> inTransaction(Handle conn, TransactionStatus status) throws Exception {
                KeyValueDAO keyValueDAO = conn.attach(KeyValueDAO.class);
                Collection<KeyValue> all = keyValueDAO.getAll();
                updateAppliedCommandIndexInTransaction(conn, commandIndex);
                return all;
            }
        });
    }

    KeyValue set(final long commandIndex, final String key, final String value) {
        checkCommandIndex(commandIndex);

        return dbi.inTransaction(new TransactionCallback<KeyValue>() {
            @Override
            public KeyValue inTransaction(Handle conn, TransactionStatus status) throws Exception {
                KeyValueDAO keyValueDAO = conn.attach(KeyValueDAO.class);

                KeyValue keyValue = keyValueDAO.get(key);

                if (keyValue == null) {
                    keyValueDAO.add(key, value);
                } else {
                    keyValueDAO.update(key, value);
                }

                updateAppliedCommandIndexInTransaction(conn, commandIndex);
                return keyValueDAO.get(key);
            }
        });
    }

    @Nullable KeyValue compareAndSet(final long commandIndex, final String key, @Nullable final String expectedValue, @Nullable final String newValue) throws KayVeeException {
        checkCommandIndex(commandIndex);

        if (expectedValue == null && newValue == null) {
            // TODO (AG): while I _could_ update the lastAppliedCommandIndex here, calling code should never call us with these arguments
            throw new IllegalArgumentException(
                    String.format("both expectedValue and newValue null for %s (commandIndex:%s)", key, commandIndex));
        }

        final ExceptionReference exceptionReference = new ExceptionReference();

        KeyValue keyValue = dbi.inTransaction(new TransactionCallback<KeyValue>() {
            @Override
            public KeyValue inTransaction(Handle conn, TransactionStatus status) throws Exception {
                KeyValueDAO keyValueDAO = conn.attach(KeyValueDAO.class);

                KeyValue existingKeyValue = keyValueDAO.get(key);
                String existingValue = existingKeyValue == null ? null : existingKeyValue.getValue();

                // all possibilities
                // -------------------------------------------------------------------
                // | existingValue | expectedValue |    result
                // |     null      |     null      |    assert newValue != null; create key=>newValue
                // |     null      |     !null     |    KeyNotFoundException
                // |     !null     |     null      |    KeyAlreadyExistsException
                // |     !null     |     !null     |    expectedValue != existingValue ? ValueMismatchException : ( newValue != null ? update key=>newValue : delete key)
                //
                // existingValue != null && expectedValue != null (from last row above)
                // --------------------------------------------------------------------
                // |     match     | result
                // |       Y       | newValue != null ? update key=>newValue : delete key)
                // |       N       | ValueMismatchException

                if (existingValue == null) {
                    if (expectedValue == null) {
                        keyValueDAO.add(key, newValue);
                    } else {
                        exceptionReference.setException(new KeyNotFoundException(key));
                    }
                } else {
                    if (expectedValue != null) {
                        if (existingValue.equals(expectedValue)) {
                            if (newValue != null) {
                                keyValueDAO.update(key, newValue);
                            } else {
                                keyValueDAO.delete(key);
                            }
                        } else {
                            exceptionReference.setException(new ValueMismatchException(key, expectedValue, existingValue));
                        }
                    } else {
                        exceptionReference.setException(new KeyAlreadyExistsException(key));
                    }
                }

                updateAppliedCommandIndexInTransaction(conn, commandIndex);

                if (exceptionReference.hasException()) {
                    return null;
                } else {
                    return keyValueDAO.get(key);
                }
            }
        });

        if (exceptionReference.hasException()) {
            throw exceptionReference.getException();
        }

        return keyValue;
    }

    void delete(final long commandIndex, final String key) {
        checkCommandIndex(commandIndex);

        dbi.inTransaction(new TransactionCallback<Void>() {
            @Override
            public Void inTransaction(Handle conn, TransactionStatus status) throws Exception {
                KeyValueDAO keyValueDAO = conn.attach(KeyValueDAO.class);
                updateAppliedCommandIndexInTransaction(conn, commandIndex);
                keyValueDAO.delete(key);
                return null;
            }
        });
    }

    private static void checkCommandIndex(long commandIndex) {
        checkArgument(commandIndex > 0, "commandIndex must be positive: given:%s", commandIndex);
    }

    private void updateAppliedCommandIndexInTransaction(Handle conn, long commandIndex) {
        checkArgument(commandIndex > 0, "commandIndex must be positive given:%s", commandIndex); // TODO (AG): should I check this here, or at every public entry point?
        CommandIndexDAO commandIndexDAO = conn.attach(CommandIndexDAO.class);
        commandIndexDAO.updateLastAppliedCommandIndex(commandIndex);
    }
}
