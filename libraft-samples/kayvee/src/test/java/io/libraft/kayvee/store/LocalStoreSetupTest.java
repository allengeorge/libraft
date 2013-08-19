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
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.skife.jdbi.v2.DBI;
import org.skife.jdbi.v2.Handle;
import org.skife.jdbi.v2.exceptions.CallbackFailedException;
import org.skife.jdbi.v2.tweak.HandleCallback;

import java.util.Collection;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.notNullValue;

public final class LocalStoreSetupTest {

    private DBI dbi;
    private Handle handle; // DO NOT USE THIS IN TESTS! only used to keep the database open!

    @BeforeClass
    public static void loadJDBCDriverClass() throws ClassNotFoundException {
        Class.forName("org.h2.Driver");
    }

    @Before
    public void setup() throws Exception {
        dbi = new DBI("jdbc:h2:mem:local_store_setup");
        handle = dbi.open();
    }

    @After
    public void teardown() throws Exception {
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

        handle.close();
    }

    @Test
    public void shouldCreateKayVeeTablesIfTheyDoNotExist() {
        assertThatLocalStoreTablesDoNotExist();

        // initialize the system
        LocalStore localStore = new LocalStore(dbi);
        localStore.initialize();

        // check that the same get calls used in checking that the tables didn't exist now work
        long lastAppliedCommandIndex = localStore.getLastAppliedCommandIndex();
        assertThat(lastAppliedCommandIndex, equalTo(0L));

        final long commandIndex = 18;
        Collection<KeyValue> all = localStore.getAll(commandIndex);
        assertThat(all, hasSize(0));

        // check that our commandIndex updates properly
        long newLastAppliedCommandIndex = localStore.getLastAppliedCommandIndex();
        assertThat(newLastAppliedCommandIndex, equalTo(commandIndex));
    }

    @Test
    public void shouldNotOverwriteKayVeeTablesIfTheyDoExist() throws KayVeeException {
        assertThatLocalStoreTablesDoNotExist();

        final long setCommandIndex = 11209;
        final String key = "leslie";
        final String value = "lamport";

        // initialize LocalStore instance 0
        // put in block to scope localStore0
        {
            LocalStore localStore0 = new LocalStore(dbi);
            localStore0.initialize();

            KeyValue setReturned = localStore0.set(setCommandIndex, key, value);
            assertThat(setReturned.getKey(), equalTo(key));
            assertThat(setReturned.getValue(), equalTo(value));
            assertThat(setCommandIndex, equalTo(localStore0.getLastAppliedCommandIndex()));
        }

        // now, initialize a second instance and check that we haven't actually wiped out the tables
        LocalStore localStore1 = new LocalStore(dbi);
        localStore1.initialize();

        // the commandIndex is the same as the one from the SET
        assertThat(localStore1.getLastAppliedCommandIndex(), equalTo(setCommandIndex));

        // the key=>value we put in is still there
        final long getCommandIndex = setCommandIndex + 1;
        KeyValue getReturned = localStore1.get(getCommandIndex, key);

        // and everything has the correct values
        assertThat(getReturned.getKey(), equalTo(key));
        assertThat(getReturned.getValue(), equalTo(value));
        assertThat(getCommandIndex, equalTo(localStore1.getLastAppliedCommandIndex()));
    }

    private void assertThatLocalStoreTablesDoNotExist() {
        // check that our 'get' calls fail
        Exception callFailedException = null;

        try {
            dbi.withHandle(new HandleCallback<Long>() {
                @Override
                public Long withHandle(Handle handle) throws Exception {
                    CommandIndexDAO commandIndexDAO = handle.attach(CommandIndexDAO.class);
                    return commandIndexDAO.getLastAppliedCommandIndex();
                }
            });
        } catch (CallbackFailedException e) {
            callFailedException = e;
        }

        assertThat(callFailedException, notNullValue());
        callFailedException = null;

        try {
            dbi.withHandle(new HandleCallback<Collection<KeyValue>>() {
                @Override
                public Collection<KeyValue> withHandle(Handle handle) throws Exception {
                    KeyValueDAO keyValueDAO = handle.attach(KeyValueDAO.class);
                    return keyValueDAO.getAll();
                }
            });
        } catch (CallbackFailedException e) {
            callFailedException = e;
        }

        assertThat(callFailedException, notNullValue());
    }
}
