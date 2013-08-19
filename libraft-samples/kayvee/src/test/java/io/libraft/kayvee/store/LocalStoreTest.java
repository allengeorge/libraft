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

import com.google.common.collect.Lists;
import io.libraft.kayvee.api.KeyValue;
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.skife.jdbi.v2.DBI;
import org.skife.jdbi.v2.Handle;
import org.skife.jdbi.v2.tweak.HandleCallback;

import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.Collection;

import static com.google.common.base.Preconditions.checkNotNull;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;

public final class LocalStoreTest {
    private static final String KEY = "leslie";
    private static final String EXPECTED_VALUE = "lamport@microsoft";
    private static final String NEW_VALUE = "lamport";

    private DBI dbi;
    private Handle handle; // DO NOT USE THIS IN TESTS!
    private LocalStore localStore;

    @BeforeClass
    public static void loadJDBCDriverClass() throws ClassNotFoundException {
        Class.forName("org.h2.Driver");
    }

    @Before
    public void setup() {
        dbi = new DBI("jdbc:h2:mem:local_store");
        handle = dbi.open(); // keep a handle sitting around so that we don't lose the in-memory db
        localStore = new LocalStore(dbi);
        localStore.initialize();
    }

    @After
    public void teardown() throws Exception {
        // don't need to, but drop the tables
        dbi.withHandle(new HandleCallback<Void>() {
            @Override
            public Void withHandle(Handle handle) throws Exception {
                KeyValueDAO keyValueDAO = handle.attach(KeyValueDAO.class);
                keyValueDAO.dropTable();

                CommandIndexDAO commandIndexDAO = handle.attach(CommandIndexDAO.class);
                commandIndexDAO.dropTable();

                return null;
            }
        });

        // close the last handle, which will delete the db - we'll have a fresh new one for the next test
        handle.close();
    }

    @Test
    public void shouldReturn0AsLastAppliedCommandIndexAfterInitialization() throws Exception {
        long lastAppliedCommandIndex = localStore.getLastAppliedCommandIndex();
        assertThat(lastAppliedCommandIndex, equalTo(0L));
    }

    @Test
    public void shouldReturnCorrectLastAppliedCommandIndex() throws Exception {
        final long commandIndex = 31;
        dbi.withHandle(new HandleCallback<Void>() {
            @Override
            public Void withHandle(Handle handle) throws Exception {
                CommandIndexDAO commandIndexDAO = handle.attach(CommandIndexDAO.class);
                commandIndexDAO.updateLastAppliedCommandIndex(commandIndex);
                return null;
            }
        });

        long lastAppliedCommandIndex = localStore.getLastAppliedCommandIndex();
        assertThat(lastAppliedCommandIndex, equalTo(commandIndex));
    }

    @Test
    public void shouldUpdateCommandIndexForANopCommand() throws Exception {
        final long originalCommandIndex = 123;
        dbi.withHandle(new HandleCallback<Void>() {
            @Override
            public Void withHandle(Handle handle) throws Exception {
                CommandIndexDAO commandIndexDAO = handle.attach(CommandIndexDAO.class);
                commandIndexDAO.updateLastAppliedCommandIndex(originalCommandIndex);
                return null;
            }
        });

        localStore.nop(originalCommandIndex + 1);

        long updatedCommandIndex = dbi.withHandle(new HandleCallback<Long>() {
            @Override
            public Long withHandle(Handle handle) throws Exception {
                CommandIndexDAO commandIndexDAO = handle.attach(CommandIndexDAO.class);
                return commandIndexDAO.getLastAppliedCommandIndex();
            }
        });

        assertThat(updatedCommandIndex, equalTo(originalCommandIndex + 1));
    }

    @Test
    public void shouldThrowKeyNotFoundExceptionIfKeyDoesNotExist()  {
        final int newCommandIndex = 1;
        KayVeeException thrownException = null;
        try {
            localStore.get(newCommandIndex, "FAKE_KEY");
        } catch (KayVeeException e) {
            thrownException = e;
        }
        thrownException = checkNotNull(thrownException);

        assertThat(thrownException, instanceOf(KeyNotFoundException.class));
        assertThat(((KeyNotFoundException) thrownException).getKey(), equalTo("FAKE_KEY"));

        assertThatLastAppliedCommandIndexHasValue(newCommandIndex);
    }

    @Test
    public void shouldReturnCorrectValueIfKeyExists() throws Exception {
        // set the key
        dbi.withHandle(new HandleCallback<Void>() {
            @Override
            public Void withHandle(Handle handle) throws Exception {
                KeyValueDAO keyValueDAO = handle.attach(KeyValueDAO.class);
                keyValueDAO.add(KEY, NEW_VALUE);
                return null;
            }
        });

        // get the key/value
        final int newCommandIndex = 27;
        KeyValue keyValue = localStore.get(newCommandIndex, KEY);

        assertThat(keyValue, notNullValue());
        assertThat(keyValue.getKey(), equalTo(KEY));
        assertThat(keyValue.getValue(), equalTo(NEW_VALUE));

        assertThatLastAppliedCommandIndexHasValue(newCommandIndex);
    }

    @Test
    public void shouldReturnEmptyCollectionIfGetAllIsCalledAndThereAreNoEntriesInDB() throws Exception {
        final long commandIndex = 81;
        Collection<KeyValue> all = localStore.getAll(commandIndex);

        all = checkNotNull(all);
        assertThat(all, hasSize(0));

        assertThatLastAppliedCommandIndexHasValue(commandIndex);
    }

    @Test
    public void shouldReturnCollectionWithAllKeyValuesInItWhenGetAllIsCalled() throws Exception {
        final ArrayList<KeyValue> keyValues = Lists.newArrayList(
                new KeyValue("barbara", "liskov"),
                new KeyValue("fred", "schneider"),
                new KeyValue("ken", "birman"),
                new KeyValue("leslie", "lamport"),
                new KeyValue("nancy", "lynch")
        );

        dbi.withHandle(new HandleCallback<Void>() {
            @Override
            public Void withHandle(Handle handle) throws Exception {
                KeyValueDAO keyValueDAO = handle.attach(KeyValueDAO.class);

                for (KeyValue keyValue : keyValues) {
                    keyValueDAO.add(keyValue.getKey(), keyValue.getValue());
                }

                return null;
            }
        });

        final long commandIndex = 77;
        Collection<KeyValue> all = localStore.getAll(commandIndex);

        all = checkNotNull(all);
        assertThat(all, containsInAnyOrder(keyValues.toArray()));

        assertThatLastAppliedCommandIndexHasValue(commandIndex);
    }

    @Test
    public void shouldCreateKeyIfItDoesNotExistWhenSetIsCalled() {
        final long commandIndex = 172;
        localStore.set(commandIndex, KEY, EXPECTED_VALUE);

        assertThatBackingStoreHasKeyValue(KEY, EXPECTED_VALUE);
        assertThatLastAppliedCommandIndexHasValue(commandIndex);
    }

    @Test
    public void shouldUpdateKeyIfItExistsWhenSetIsCalled() {
        dbi.withHandle(new HandleCallback<Void>() {
            @Override
            public Void withHandle(Handle handle) throws Exception {
                KeyValueDAO keyValueDAO = handle.attach(KeyValueDAO.class);
                keyValueDAO.add(KEY, EXPECTED_VALUE);
                return null;
            }
        });

        assertThatBackingStoreHasKeyValue(KEY, EXPECTED_VALUE);

        final long commandIndex = 99;
        localStore.set(commandIndex, KEY, NEW_VALUE);

        assertThatBackingStoreHasKeyValue(KEY, NEW_VALUE);
        assertThatLastAppliedCommandIndexHasValue(commandIndex);
    }

    @Test
    public void shouldThrowIfGivenCommandIndexIsZero() {
        final long originalCommandIndex = 17;
        dbi.withHandle(new HandleCallback<Object>() {
            @Override
            public Object withHandle(Handle handle) throws Exception {
                CommandIndexDAO commandIndexDAO = handle.attach(CommandIndexDAO.class);
                commandIndexDAO.updateLastAppliedCommandIndex(originalCommandIndex);
                return null;
            }
        });

        IllegalArgumentException setException = null;
        try {
            localStore.set(0, KEY, EXPECTED_VALUE);
        } catch (IllegalArgumentException e) {
            setException = e;
        }
        assertThat(setException, notNullValue());

        assertThatBackingStoreHasKeyValue(KEY, null);
        assertThatLastAppliedCommandIndexHasValue(originalCommandIndex);
    }

    @Test
    public void shouldThrowIfGivenCommandIndexIsNegative() {
        final long originalCommandIndex = 27;
        dbi.withHandle(new HandleCallback<Object>() {
            @Override
            public Object withHandle(Handle handle) throws Exception {
                CommandIndexDAO commandIndexDAO = handle.attach(CommandIndexDAO.class);
                commandIndexDAO.updateLastAppliedCommandIndex(originalCommandIndex);
                return null;
            }
        });

        IllegalArgumentException setException = null;
        try {
            localStore.set(-17, KEY, EXPECTED_VALUE);
        } catch (IllegalArgumentException e) {
            setException = e;
        }
        assertThat(setException, notNullValue());

        assertThatBackingStoreHasKeyValue(KEY, null);
        assertThatLastAppliedCommandIndexHasValue(originalCommandIndex);
    }

    @Test
    public void shouldCreateKeyIfCASIsAttemptedForKeyThatDoesNotExist() throws Exception {
        // do the cas
        final long newCommandIndex = 66;
        KeyValue keyValue = localStore.compareAndSet(newCommandIndex, KEY, null, NEW_VALUE);
        keyValue = checkNotNull(keyValue);

        assertThat(keyValue.getKey(), equalTo(KEY));
        assertThat(keyValue.getValue(), equalTo(NEW_VALUE));

        // check that the backing db and command index were updated
        assertThatBackingStoreHasKeyValue(KEY, NEW_VALUE);
        assertThatLastAppliedCommandIndexHasValue(newCommandIndex);
    }

    @Test
    public void shouldUpdateKeyIfCASIsAttemptedForKeyWhoseNonNullExpectedValueMatchesCurrentValue() throws Exception {
        // set the expected value
        dbi.withHandle(new HandleCallback<Void>() {
            @Override
            public Void withHandle(Handle handle) throws Exception {
                KeyValueDAO keyValueDAO = handle.attach(KeyValueDAO.class);
                keyValueDAO.add(KEY, EXPECTED_VALUE);
                return null;
            }
        });

        final long newCommandIndex = 71;
        KeyValue keyValue = localStore.compareAndSet(newCommandIndex, KEY, EXPECTED_VALUE, NEW_VALUE);
        keyValue = checkNotNull(keyValue);

        assertThat(keyValue.getKey(), equalTo(KEY));
        assertThat(keyValue.getValue(), equalTo(NEW_VALUE));

        assertThatBackingStoreHasKeyValue(KEY, NEW_VALUE);
        assertThatLastAppliedCommandIndexHasValue(newCommandIndex);
    }

    @Test
    public void shouldDeleteKeyIfCASIsAttemptedForKeyWhoseNonNullExpectedValueMatchesCurrentValueAndNewValueIsNull() throws Exception {
        // set the expected value
        dbi.withHandle(new HandleCallback<Void>() {
            @Override
            public Void withHandle(Handle handle) throws Exception {
                KeyValueDAO keyValueDAO = handle.attach(KeyValueDAO.class);
                keyValueDAO.add(KEY, EXPECTED_VALUE);
                return null;
            }
        });

        final long newCommandIndex = 23;
        KeyValue keyValue = localStore.compareAndSet(newCommandIndex, KEY, EXPECTED_VALUE, null);
        assertThat(keyValue, nullValue());

        assertThatBackingStoreHasKeyValue(KEY, null);
        assertThatLastAppliedCommandIndexHasValue(newCommandIndex);
    }

    @Test
    public void shouldThrowIllegalArgumentExceptionIfBothExpectedValueAndNewValueAreNull() throws Exception {
        // set the lastAppliedCommandIndex to some value
        final long preCASLastAppliedCommandIndex = 276;
        dbi.withHandle(new HandleCallback<Object>() {
            @Override
            public Object withHandle(Handle handle) throws Exception {
                CommandIndexDAO commandIndexDAO = handle.attach(CommandIndexDAO.class);
                commandIndexDAO.updateLastAppliedCommandIndex(preCASLastAppliedCommandIndex);
                return null;
            }
        });

        final long casCommandCommandIndex = preCASLastAppliedCommandIndex + 1;
        Exception casException = null;
        try {
            localStore.compareAndSet(casCommandCommandIndex, KEY, null, null);
        } catch (IllegalArgumentException e) {
            casException = e;
        }

        assertThat(casException, notNullValue());
        assertThatLastAppliedCommandIndexHasValue(preCASLastAppliedCommandIndex);
    }

    @Test
    public void shouldThrowKeyAlreadyExistsExceptionIfExpectedValueIsNullButCurrentValueIsNotNull() throws Exception {
        // set the expected value
        dbi.withHandle(new HandleCallback<Void>() {
            @Override
            public Void withHandle(Handle handle) throws Exception {
                KeyValueDAO keyValueDAO = handle.attach(KeyValueDAO.class);
                keyValueDAO.add(KEY, EXPECTED_VALUE);
                return null;
            }
        });

        final long newCommandIndex = 77;
        KayVeeException casException = null;
        try {
            localStore.compareAndSet(newCommandIndex, KEY, null, NEW_VALUE);
        } catch (KayVeeException e) {
            casException = e;
        }

        casException = checkNotNull(casException);
        assertThat(casException, instanceOf(KeyAlreadyExistsException.class));
        assertThat(((KeyAlreadyExistsException) casException).getKey(), equalTo(KEY));

        assertThatBackingStoreHasKeyValue(KEY, EXPECTED_VALUE);
        assertThatLastAppliedCommandIndexHasValue(newCommandIndex);
    }

    @Test
    public void shouldThrowValueMismatchExceptionIfExpectedValueIsNotNullButDoesNotMatchCurrentValue() throws Exception {
        // set the expected value
        dbi.withHandle(new HandleCallback<Void>() {
            @Override
            public Void withHandle(Handle handle) throws Exception {
                KeyValueDAO keyValueDAO = handle.attach(KeyValueDAO.class);
                keyValueDAO.add(KEY, EXPECTED_VALUE);
                return null;
            }
        });

        final long newCommandIndex = 71129837;
        final String mismatchedExpectedValue = "lamport@ibm";
        KayVeeException casException = null;
        try {
            localStore.compareAndSet(newCommandIndex, KEY, mismatchedExpectedValue, NEW_VALUE);
        } catch (KayVeeException e) {
            casException = e;
        }

        casException = checkNotNull(casException);
        ValueMismatchException valueMismatchException = (ValueMismatchException) casException;
        assertThat(valueMismatchException.getKey(), equalTo(KEY));
        assertThat(valueMismatchException.getExpectedValue(), equalTo(mismatchedExpectedValue));
        assertThat(valueMismatchException.getExistingValue(), equalTo(EXPECTED_VALUE));

        assertThatBackingStoreHasKeyValue(KEY, EXPECTED_VALUE);
        assertThatLastAppliedCommandIndexHasValue(newCommandIndex);
    }

    @Test
    public void shouldThrowKeyNotFoundExceptionIfExpectedValueIsNotNullButKeyDoesNotExist() throws Exception {
        final long newCommandIndex = 372;
        KayVeeException casException = null;
        try {
            localStore.compareAndSet(newCommandIndex, KEY, EXPECTED_VALUE, NEW_VALUE);
        } catch (KayVeeException e) {
            casException = e;
        }

        casException = checkNotNull(casException);
        KeyNotFoundException keyNotFoundException = (KeyNotFoundException) casException;
        assertThat(keyNotFoundException.getKey(), equalTo(KEY));

        assertThatBackingStoreHasKeyValue(KEY, null);
        assertThatLastAppliedCommandIndexHasValue(newCommandIndex);
    }

    @Test
    public void shouldDeleteKeyIfItExists() throws Exception {
        dbi.withHandle(new HandleCallback<Void>() {
            @Override
            public Void withHandle(Handle handle) throws Exception {
                KeyValueDAO keyValueDAO = handle.attach(KeyValueDAO.class);
                keyValueDAO.add(KEY, EXPECTED_VALUE);
                return null;
            }
        });

        final long newCommandIndex = 37;
        localStore.delete(newCommandIndex, KEY);

        assertThatBackingStoreHasKeyValue(KEY, null);
        assertThatLastAppliedCommandIndexHasValue(newCommandIndex);
    }

    @Test
    public void shouldNoopIfDeleteCalledForKeyAndItDoesNotExist() throws Exception {
        final long newCommandIndex = 37;
        localStore.delete(newCommandIndex, KEY);

        assertThatBackingStoreHasKeyValue(KEY, null);
        assertThatLastAppliedCommandIndexHasValue(newCommandIndex);
    }

    private void assertThatLastAppliedCommandIndexHasValue(final long expectedLastAppliedCommandIndex) {
        dbi.withHandle(new HandleCallback<Void>() {
            @Override
            public Void withHandle(Handle handle) throws Exception {
                CommandIndexDAO commandIndexDAO = handle.attach(CommandIndexDAO.class);
                Long lastAppliedCommandIndex = commandIndexDAO.getLastAppliedCommandIndex();

                assertThat(lastAppliedCommandIndex, notNullValue());
                assertThat(lastAppliedCommandIndex, equalTo(expectedLastAppliedCommandIndex));

                return null;
            }
        });
    }

    private void assertThatBackingStoreHasKeyValue(final String key, @Nullable final String value) {
        dbi.withHandle(new HandleCallback<Void>() {
            @Override
            public Void withHandle(Handle handle) throws Exception {
                KeyValueDAO keyValueDAO = handle.attach(KeyValueDAO.class);

                KeyValue backingKeyValue = keyValueDAO.get(key);
                if (value != null) {
                    backingKeyValue = checkNotNull(backingKeyValue);
                    assertThat(backingKeyValue.getKey(), equalTo(key));
                    assertThat(backingKeyValue.getValue(), equalTo(value));
                } else {
                    assertThat(backingKeyValue, nullValue());
                }

                return null;
            }
        });
    }
}
