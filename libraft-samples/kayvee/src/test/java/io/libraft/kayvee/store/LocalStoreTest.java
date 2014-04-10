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

package io.libraft.kayvee.store;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.google.common.collect.Lists;
import io.libraft.kayvee.api.KeyValue;
import org.hamcrest.Matchers;
import org.junit.Before;
import org.junit.Test;

import javax.annotation.Nullable;
import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Collection;
import java.util.List;

import static com.google.common.base.Preconditions.checkNotNull;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyInt;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public final class LocalStoreTest {
    private static final String KEY = "leslie";
    private static final String EXPECTED_VALUE = "lamport@microsoft";
    private static final String NEW_VALUE = "lamport";

    private LocalStore localStore;

    @Before
    public void setup() {
        localStore = new LocalStore();
    }

    @Test
    public void shouldReturnZeroAsLastAppliedIndexAfterInitialization() throws Exception {
        long lastAppliedIndex = localStore.getLastAppliedIndex();
        assertThat(lastAppliedIndex, equalTo(0L));
    }

    @Test
    public void shouldReturnCorrectLastAppliedIndex() throws Exception {
        final long index = 31;

        localStore.setLastAppliedIndexForUnitTestsOnly(index);

        long lastAppliedIndex = localStore.getLastAppliedIndex();
        assertThat(lastAppliedIndex, equalTo(index));
    }

    @Test
    public void shouldUpdateIndexForSkip() throws Exception {
        final long originalIndex = 123;

        localStore.setLastAppliedIndexForUnitTestsOnly(originalIndex);

        localStore.skip(originalIndex + 1);

        long updatedIndex = localStore.getLastAppliedIndex();
        assertThat(updatedIndex, equalTo(originalIndex + 1));
    }

    @Test
    public void shouldThrowIfIndexForSkipStaysTheSame() throws Exception {
        final long originalIndex = 123;

        localStore.setLastAppliedIndexForUnitTestsOnly(originalIndex);

        IllegalArgumentException thrownException = null;
        try {
            localStore.skip(originalIndex);
        } catch (IllegalArgumentException e) {
            thrownException = e;
        }
        thrownException = checkNotNull(thrownException);

        assertThat(thrownException, instanceOf(IllegalArgumentException.class));

        long updatedIndex = localStore.getLastAppliedIndex();
        assertThat(updatedIndex, equalTo(originalIndex));
    }

    @Test
    public void shouldThrowIfIndexForSkipIsNegative() throws Exception {
        final long originalIndex = 123;

        localStore.setLastAppliedIndexForUnitTestsOnly(originalIndex);

        IllegalArgumentException thrownException = null;
        try {
            localStore.skip(-3);
        } catch (IllegalArgumentException e) {
            thrownException = e;
        }
        thrownException = checkNotNull(thrownException);

        assertThat(thrownException, instanceOf(IllegalArgumentException.class));

        long updatedIndex = localStore.getLastAppliedIndex();
        assertThat(updatedIndex, equalTo(originalIndex));
    }

    @Test
    public void shouldThrowIfIndexForSkipIsNotMonotonicallyIncreasing() throws Exception {
        final long originalIndex = 123;

        localStore.setLastAppliedIndexForUnitTestsOnly(originalIndex);

        IllegalArgumentException thrownException = null;
        try {
            localStore.skip(originalIndex - 3);
        } catch (IllegalArgumentException e) {
            thrownException = e;
        }
        thrownException = checkNotNull(thrownException);

        assertThat(thrownException, instanceOf(IllegalArgumentException.class));

        long updatedIndex = localStore.getLastAppliedIndex();
        assertThat(updatedIndex, equalTo(originalIndex));
    }

    @Test
    public void shouldThrowIfIndexForSkipIsNotMonotonicallyIncreasingByOne() throws Exception {
        final long originalIndex = 123;

        localStore.setLastAppliedIndexForUnitTestsOnly(originalIndex);

        IllegalArgumentException thrownException = null;
        try {
            localStore.skip(originalIndex + 3);
        } catch (IllegalArgumentException e) {
            thrownException = e;
        }
        thrownException = checkNotNull(thrownException);

        assertThat(thrownException, instanceOf(IllegalArgumentException.class));

        long updatedIndex = localStore.getLastAppliedIndex();
        assertThat(updatedIndex, equalTo(originalIndex));
    }

    @Test
    public void shouldUpdateIndexForANopCommand() throws Exception {
        final long originalIndex = 123;

        localStore.setLastAppliedIndexForUnitTestsOnly(originalIndex);

        localStore.nop(originalIndex + 1);

        long updatedIndex = localStore.getLastAppliedIndex();
        assertThat(updatedIndex, equalTo(originalIndex + 1));
    }

    @Test
    public void shouldThrowKeyNotFoundExceptionIfKeyDoesNotExist()  {
        final int newIndex = 1;
        KayVeeException thrownException = null;
        try {
            localStore.get(newIndex, "FAKE_KEY");
        } catch (KayVeeException e) {
            thrownException = e;
        }
        thrownException = checkNotNull(thrownException);

        assertThat(thrownException, instanceOf(KeyNotFoundException.class));
        assertThat(((KeyNotFoundException) thrownException).getKey(), equalTo("FAKE_KEY"));

        assertThatLocalStoreHasKeyValue("FAKE_KEY", null);
        assertThatLastAppliedIndexHasValue(newIndex);
    }

    @Test
    public void shouldReturnCorrectValueIfKeyExists() throws Exception {
        // set the key
        localStore.setKeyValueForUnitTestsOnly(KEY, NEW_VALUE);

        // set the last applied index
        final long originalIndex = 26;
        localStore.setLastAppliedIndexForUnitTestsOnly(originalIndex);

        // get the key/value
        final long newIndex = originalIndex + 1;
        KeyValue keyValue = localStore.get(newIndex, KEY);

        assertThat(keyValue, notNullValue());
        assertThat(keyValue.getKey(), equalTo(KEY));
        assertThat(keyValue.getValue(), equalTo(NEW_VALUE));

        assertThatLocalStoreHasKeyValue(KEY, NEW_VALUE);
        assertThatLastAppliedIndexHasValue(newIndex);
    }

    @Test
    public void shouldReturnEmptyCollectionIfGetAllIsCalledAndThereAreNoEntriesInDB() throws Exception {
        // set the last applied index
        final long originalIndex = 80;
        localStore.setLastAppliedIndexForUnitTestsOnly(originalIndex);

        final long newIndex = originalIndex + 1;
        Collection<KeyValue> all = localStore.getAll(newIndex);

        all = checkNotNull(all);
        assertThat(all, hasSize(0));

        assertThatLastAppliedIndexHasValue(newIndex);
    }

    @Test
    public void shouldReturnCollectionWithAllKeyValuesInItWhenGetAllIsCalled() throws Exception {
        final KeyValue[] keyValues = {
                new KeyValue("barbara", "liskov"),
                new KeyValue("fred", "schneider"),
                new KeyValue("ken", "birman"),
                new KeyValue("leslie", "lamport"),
                new KeyValue("nancy", "lynch")
        };

        for (KeyValue keyValue : keyValues) {
            localStore.setKeyValueForUnitTestsOnly(keyValue.getKey(), keyValue.getValue());
        }

        // set the last applied index
        final long originalIndex = 76;
        localStore.setLastAppliedIndexForUnitTestsOnly(originalIndex);

        final long newIndex = originalIndex + 1;
        Collection<KeyValue> all = localStore.getAll(newIndex);

        all = checkNotNull(all);
        assertThat(all, containsInAnyOrder(keyValues));

        assertThatLastAppliedIndexHasValue(newIndex);
    }

    @Test
    public void shouldCreateKeyIfItDoesNotExistWhenSetIsCalled() {
        // set the original last applied index
        final long originalIndex = 171;
        localStore.setLastAppliedIndexForUnitTestsOnly(originalIndex);

        final long newIndex = originalIndex + 1;
        localStore.set(newIndex, KEY, EXPECTED_VALUE);

        assertThatLocalStoreHasKeyValue(KEY, EXPECTED_VALUE);
        assertThatLastAppliedIndexHasValue(newIndex);
    }

    @Test
    public void shouldUpdateKeyIfItExistsWhenSetIsCalled() {
        // set the original last applied index
        final long originalIndex = 98;
        localStore.setLastAppliedIndexForUnitTestsOnly(originalIndex);

        // set the original value of the key
        localStore.setKeyValueForUnitTestsOnly(KEY, EXPECTED_VALUE);
        assertThatLocalStoreHasKeyValue(KEY, EXPECTED_VALUE);

        // now, update to the new value
        final long newIndex = originalIndex + 1;
        localStore.set(newIndex, KEY, NEW_VALUE);

        assertThatLocalStoreHasKeyValue(KEY, NEW_VALUE);
        assertThatLastAppliedIndexHasValue(newIndex);
    }

    @Test
    public void shouldThrowIfGivenIndexIsZero() {
        // set the original index
        final long originalIndex = 17;
        localStore.setLastAppliedIndexForUnitTestsOnly(originalIndex);

        // now, try to set a index with '0'
        IllegalArgumentException setException = null;
        try {
            localStore.set(0, KEY, EXPECTED_VALUE);
        } catch (IllegalArgumentException e) {
            setException = e;
        }
        assertThat(setException, notNullValue());

        assertThatLocalStoreHasKeyValue(KEY, null);
        assertThatLastAppliedIndexHasValue(originalIndex);
    }

    @Test
    public void shouldThrowIfGivenIndexIsNegative() {
        // set the original index
        final long originalIndex = 27;
        localStore.setLastAppliedIndexForUnitTestsOnly(originalIndex);

        // now, try to set a negative index
        IllegalArgumentException setException = null;
        try {
            localStore.set(-17, KEY, EXPECTED_VALUE);
        } catch (IllegalArgumentException e) {
            setException = e;
        }
        assertThat(setException, notNullValue());

        assertThatLocalStoreHasKeyValue(KEY, null);
        assertThatLastAppliedIndexHasValue(originalIndex);
    }

    @Test
    public void shouldThrowIfGivenIndexIsNotMonotonicallyIncreasing() {
        // set the original index
        final long originalIndex = 37;
        localStore.setLastAppliedIndexForUnitTestsOnly(originalIndex);

        // now, try to set a index with one less than the original command index
        IllegalArgumentException setException = null;
        try {
            localStore.set(36, KEY, EXPECTED_VALUE);
        } catch (IllegalArgumentException e) {
            setException = e;
        }
        assertThat(setException, notNullValue());

        assertThatLocalStoreHasKeyValue(KEY, null);
        assertThatLastAppliedIndexHasValue(originalIndex);
    }

    @Test
    public void shouldThrowIfGivenIndexIsNotMonotonicallyIncreasingByOne() {
        // set the original index
        final long originalIndex = 34;
        localStore.setLastAppliedIndexForUnitTestsOnly(originalIndex);

        // now, try to set a index with one less than the original command index
        IllegalArgumentException setException = null;
        try {
            localStore.set(36, KEY, EXPECTED_VALUE);
        } catch (IllegalArgumentException e) {
            setException = e;
        }
        assertThat(setException, notNullValue());

        assertThatLocalStoreHasKeyValue(KEY, null);
        assertThatLastAppliedIndexHasValue(originalIndex);
    }

    @Test
    public void shouldCreateKeyIfCASIsAttemptedForKeyThatDoesNotExist() throws Exception {
        // set the original index
        final long originalIndex = 65;
        localStore.setLastAppliedIndexForUnitTestsOnly(originalIndex);

        // do the CAS
        final long newIndex = originalIndex + 1;
        KeyValue keyValue = localStore.compareAndSet(newIndex, KEY, null, NEW_VALUE);
        keyValue = checkNotNull(keyValue);

        assertThat(keyValue.getKey(), equalTo(KEY));
        assertThat(keyValue.getValue(), equalTo(NEW_VALUE));

        // check that the store and command index were updated
        assertThatLocalStoreHasKeyValue(KEY, NEW_VALUE);
        assertThatLastAppliedIndexHasValue(newIndex);
    }

    @Test
    public void shouldUpdateKeyIfCASIsAttemptedForKeyWhoseNonNullExpectedValueMatchesCurrentValue() throws Exception {
        // set the original index
        final long originalIndex = 70;
        localStore.setLastAppliedIndexForUnitTestsOnly(originalIndex);

        // set the expected value
        localStore.setKeyValueForUnitTestsOnly(KEY, EXPECTED_VALUE);

        // do the CAS
        final long newIndex = originalIndex + 1;
        KeyValue keyValue = localStore.compareAndSet(newIndex, KEY, EXPECTED_VALUE, NEW_VALUE);
        keyValue = checkNotNull(keyValue);

        assertThat(keyValue.getKey(), equalTo(KEY));
        assertThat(keyValue.getValue(), equalTo(NEW_VALUE));

        assertThatLocalStoreHasKeyValue(KEY, NEW_VALUE);
        assertThatLastAppliedIndexHasValue(newIndex);
    }

    @Test
    public void shouldDeleteKeyIfCASIsAttemptedForKeyWhoseNonNullExpectedValueMatchesCurrentValueAndNewValueIsNull() throws Exception {
        // set the original index
        final long originalIndex = 22;
        localStore.setLastAppliedIndexForUnitTestsOnly(originalIndex);

        // set the expected value
        localStore.setKeyValueForUnitTestsOnly(KEY, EXPECTED_VALUE);

        // do the CAS
        final long newIndex = originalIndex + 1;
        KeyValue keyValue = localStore.compareAndSet(newIndex, KEY, EXPECTED_VALUE, null);
        assertThat(keyValue, nullValue());

        assertThatLocalStoreHasKeyValue(KEY, null);
        assertThatLastAppliedIndexHasValue(newIndex);
    }

    @Test
    public void shouldThrowIllegalArgumentExceptionIfBothExpectedValueAndNewValueAreNull() throws Exception {
        // set the lastAppliedIndex to some value
        final long preCASLastAppliedIndex = 276;
        localStore.setLastAppliedIndexForUnitTestsOnly(preCASLastAppliedIndex);

        // do the cas
        final long casCommandIndex = preCASLastAppliedIndex + 1;
        Exception casException = null;
        try {
            localStore.compareAndSet(casCommandIndex, KEY, null, null);
        } catch (IllegalArgumentException e) {
            casException = e;
        }

        // neither the value nor the command index should have been updated
        assertThat(casException, notNullValue());
        assertThatLastAppliedIndexHasValue(preCASLastAppliedIndex);
    }

    @Test
    public void shouldThrowKeyAlreadyExistsExceptionIfExpectedValueIsNullButCurrentValueIsNotNull() throws Exception {
        // set the original index
        final long originalIndex = 76;
        localStore.setLastAppliedIndexForUnitTestsOnly(originalIndex);

        // set the expected value
        localStore.setKeyValueForUnitTestsOnly(KEY, EXPECTED_VALUE);

        // do the CAS
        final long newIndex = originalIndex + 1;
        KayVeeException casException = null;
        try {
            localStore.compareAndSet(newIndex, KEY, null, NEW_VALUE);
        } catch (KayVeeException e) {
            casException = e;
        }

        casException = checkNotNull(casException);
        assertThat(casException, instanceOf(KeyAlreadyExistsException.class));
        assertThat(((KeyAlreadyExistsException) casException).getKey(), equalTo(KEY));

        assertThatLocalStoreHasKeyValue(KEY, EXPECTED_VALUE);
        assertThatLastAppliedIndexHasValue(newIndex); // we throw, but the last applied index is updated (because this is a valid operation)
    }

    @Test
    public void shouldThrowValueMismatchExceptionIfExpectedValueIsNotNullButDoesNotMatchCurrentValue() throws Exception {
        // set the original index
        final long originalIndex = 71129836;
        localStore.setLastAppliedIndexForUnitTestsOnly(originalIndex);

        // set the expected value
        localStore.setKeyValueForUnitTestsOnly(KEY, EXPECTED_VALUE);

        // do the CAS
        final long newIndex = originalIndex + 1;
        final String mismatchedExpectedValue = "lamport@ibm";
        KayVeeException casException = null;
        try {
            localStore.compareAndSet(newIndex, KEY, mismatchedExpectedValue, NEW_VALUE);
        } catch (KayVeeException e) {
            casException = e;
        }

        casException = checkNotNull(casException);
        ValueMismatchException valueMismatchException = (ValueMismatchException) casException;
        assertThat(valueMismatchException.getKey(), equalTo(KEY));
        assertThat(valueMismatchException.getExpectedValue(), equalTo(mismatchedExpectedValue));
        assertThat(valueMismatchException.getExistingValue(), equalTo(EXPECTED_VALUE));

        assertThatLocalStoreHasKeyValue(KEY, EXPECTED_VALUE);
        assertThatLastAppliedIndexHasValue(newIndex); // we throw, but the last applied index is updated (because this is a valid operation)
    }

    @Test
    public void shouldThrowKeyNotFoundExceptionIfExpectedValueIsNotNullButKeyDoesNotExist() throws Exception {
        // set the original index
        final long originalIndex = 371;
        localStore.setLastAppliedIndexForUnitTestsOnly(originalIndex);

        final long newIndex = originalIndex + 1;
        KayVeeException casException = null;
        try {
            localStore.compareAndSet(newIndex, KEY, EXPECTED_VALUE, NEW_VALUE);
        } catch (KayVeeException e) {
            casException = e;
        }

        casException = checkNotNull(casException);
        KeyNotFoundException keyNotFoundException = (KeyNotFoundException) casException;
        assertThat(keyNotFoundException.getKey(), equalTo(KEY));

        assertThatLocalStoreHasKeyValue(KEY, null);
        assertThatLastAppliedIndexHasValue(newIndex); // again, we throw, but the last applied index is updated (because this is a valid operation)
    }

    @Test
    public void shouldDeleteKeyIfItExists() throws Exception {
        // set the original index
        final long originalIndex = 36;
        localStore.setLastAppliedIndexForUnitTestsOnly(originalIndex);

        // set the initial value for the key
        localStore.setKeyValueForUnitTestsOnly(KEY, EXPECTED_VALUE);

        // delete it
        final long newIndex = originalIndex + 1;
        localStore.delete(newIndex, KEY);

        assertThatLocalStoreHasKeyValue(KEY, null);
        assertThatLastAppliedIndexHasValue(newIndex);
    }

    @Test
    public void shouldNoopIfDeleteCalledForKeyAndItDoesNotExist() throws Exception {
        // set the original index
        final long originalIndex = 36;
        localStore.setLastAppliedIndexForUnitTestsOnly(originalIndex);

        final long newIndex = originalIndex + 1;
        localStore.delete(newIndex, KEY);

        assertThatLocalStoreHasKeyValue(KEY, null);
        assertThatLastAppliedIndexHasValue(newIndex);
    }

    @Test
    public void shouldSerializeKeyValuesToAndDeserializeKeyValuesFromStream() throws IOException {
        // set the original command index
        long originalIndex = 17;
        localStore.setLastAppliedIndexForUnitTestsOnly(originalIndex);

        // store all the initial key=>value pairs
        int numKeyValues = 1000;
        List<KeyValue> keyValues = Lists.newArrayListWithCapacity(numKeyValues);
        for (int i = 0; i < numKeyValues; i++) {
            keyValues.add(new KeyValue("key_" + i, "value_" + i));
        }

        for (KeyValue keyValue : keyValues) {
            localStore.setKeyValueForUnitTestsOnly(keyValue.getKey(), keyValue.getValue());
        }

        // serialize
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        long index = localStore.dumpState(out);
        assertThat(index, equalTo(originalIndex));

        // now, change (under the hood) the command index and add another key=>value pair (i.e. change localStore state)
        localStore.setLastAppliedIndexForUnitTestsOnly(10); // has to be lower because of the check when we deserialize the snapshot
        assertThat(localStore.getLastAppliedIndex(), equalTo(10L));
        localStore.setKeyValueForUnitTestsOnly("5", "FIVE");

        // let's read the serialized data
        localStore.loadState(index, new ByteArrayInputStream(out.toByteArray()));

        // check that the final state is equal to the initial state
        assertThat(localStore.getLastAppliedIndex(), equalTo(originalIndex));
        assertThat(localStore.getAllForUnitTestsOnly(), containsInAnyOrder(keyValues.toArray()));
    }

    // this test is here because, apparently, closing a ByteArrayOutputStream is a noop
    // it turns out the default setting for a Jackson ObjectMapper is to _close_ a stream after it writes a value to it
    // to catch this I include a test using a stream where close() actually does have an effect
    @Test
    public void shouldSerializeKeyValuesToAndDeserializeKeyValuesToAndFromFile() throws IOException {
        // set the original command index
        long originalIndex = 17;
        localStore.setLastAppliedIndexForUnitTestsOnly(originalIndex);

        // store all the initial key=>value pairs
        int numKeyValues = 1000;
        List<KeyValue> keyValues = Lists.newArrayListWithCapacity(numKeyValues);
        for (int i = 0; i < numKeyValues; i++) {
            keyValues.add(new KeyValue("key_" + i, "value_" + i));
        }

        for (KeyValue keyValue : keyValues) {
            localStore.setKeyValueForUnitTestsOnly(keyValue.getKey(), keyValue.getValue());
        }

        // output file
        Path snapshotFile = Files.createTempFile("snap-", ".snap");
        OutputStream out = new BufferedOutputStream(new FileOutputStream(snapshotFile.toFile()));

        // serialize
        long index = localStore.dumpState(out);
        assertThat(index, equalTo(originalIndex));

        // now, change (under the hood) the command index and add another key=>value pair (i.e. change localStore state)
        localStore.setLastAppliedIndexForUnitTestsOnly(10); // has to be lower because of the check when we deserialize the snapshot
        assertThat(localStore.getLastAppliedIndex(), equalTo(10L));
        localStore.setKeyValueForUnitTestsOnly("5", "FIVE");

        // input file
        InputStream in = new BufferedInputStream(new FileInputStream(snapshotFile.toFile()));

        // let's read the serialized data
        localStore.loadState(index, in);

        // check that the final state is equal to the initial state
        assertThat(localStore.getLastAppliedIndex(), equalTo(originalIndex));
        assertThat(localStore.getAllForUnitTestsOnly(), containsInAnyOrder(keyValues.toArray()));
    }

    @Test
    public void shouldSerializeEmptyStateToAndFromStream() throws IOException {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        long index = localStore.dumpState(out);
        assertThat(index, equalTo(0L));

        localStore.loadState(index, new ByteArrayInputStream(out.toByteArray()));
        assertThat(localStore.getAllForUnitTestsOnly(), Matchers.<KeyValue>iterableWithSize(0));
    }

    @Test
    public void shouldNotChangeInternalStateIfOutputStreamThrowsIOExceptionDuringSerialization() throws Exception {
        // set the lastAppliedIndex to some value
        final long originalIndex = 276;
        localStore.setLastAppliedIndexForUnitTestsOnly(originalIndex);

        // store all the initial key=>value pairs
        KeyValue[] keyValues = new KeyValue[] {
                new KeyValue("1", "ONE"),
                new KeyValue("2", "TWO"),
                new KeyValue("3", "THREE"),
                new KeyValue("4", "FOUR")
        };

        for (KeyValue keyValue : keyValues) {
            localStore.setKeyValueForUnitTestsOnly(keyValue.getKey(), keyValue.getValue());
        }

        // create a broken stream
        OutputStream out = mock(OutputStream.class);
        doThrow(IOException.class).when(out).write(anyInt());
        doThrow(IOException.class).when(out).write(any(byte[].class));
        doThrow(IOException.class).when(out).write(any(byte[].class), anyInt(), anyInt());

        // serialize
        Exception serializeException = null;
        try {
            localStore.dumpState(out);
        } catch (IOException e) {
            serializeException = e;
        }

        // should have thrown an exception
        assertThat(serializeException, notNullValue());

        // check that the final state is equal to the initial state
        assertThat(localStore.getLastAppliedIndex(), equalTo(originalIndex));
        assertThat(localStore.getAllForUnitTestsOnly(), containsInAnyOrder(keyValues));
    }

    @Test
    public void shouldNotChangeInternalStateIfInputStreamThrowsIOExceptionDuringDeserialization() throws Exception {
        // set the lastAppliedIndex to some value
        final long originalIndex = 276;
        localStore.setLastAppliedIndexForUnitTestsOnly(originalIndex);

        // store all the initial key=>value pairs
        KeyValue[] keyValues = new KeyValue[] {
                new KeyValue("1", "ONE"),
                new KeyValue("2", "TWO"),
                new KeyValue("3", "THREE"),
                new KeyValue("4", "FOUR")
        };

        for (KeyValue keyValue : keyValues) {
            localStore.setKeyValueForUnitTestsOnly(keyValue.getKey(), keyValue.getValue());
        }

        // create a broken stream
        InputStream in = mock(InputStream.class);
        when(in.read()).thenThrow(new IOException());
        when(in.read(any(byte[].class))).thenThrow(new IOException());
        when(in.read(any(byte[].class), anyInt(), anyInt())).thenThrow(new IOException());

        // deserialize
        Exception deserializeException = null;
        try {
            localStore.loadState(originalIndex + 1, in);
        } catch (IOException e) {
            deserializeException = e;
        }

        // should have thrown an exception
        assertThat(deserializeException, notNullValue());

        // check that the final state is equal to the initial state
        assertThat(localStore.getLastAppliedIndex(), equalTo(originalIndex));
        assertThat(localStore.getAllForUnitTestsOnly(), containsInAnyOrder(keyValues));
    }

    @Test
    public void shouldNotChangeInternalStateIfInputStreamThrowsJSONSerializationExceptionDuringDeserialization() throws Exception {
        // set the lastAppliedIndex to some value
        final long originalIndex = 276;
        localStore.setLastAppliedIndexForUnitTestsOnly(originalIndex);

        // store all the initial key=>value pairs
        KeyValue[] keyValues = new KeyValue[] {
                new KeyValue("1", "ONE"),
                new KeyValue("2", "TWO"),
                new KeyValue("3", "THREE"),
                new KeyValue("4", "FOUR")
        };

        for (KeyValue keyValue : keyValues) {
            localStore.setKeyValueForUnitTestsOnly(keyValue.getKey(), keyValue.getValue());
        }

        // serialize
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        long index = localStore.dumpState(out);
        assertThat(index, equalTo(originalIndex));

        // create a broken stream
        byte[] serializedBytes = out.toByteArray();
        InputStream in = new ByteArrayInputStream(serializedBytes, 0, serializedBytes.length / 2); // let's cut the data half-way

        // deserialize
        Throwable deserializeThrowable = null;
        try {
            localStore.loadState(originalIndex + 1, in);
        } catch (RuntimeException e) {
            deserializeThrowable = e.getCause();
        }

        // should have thrown an exception
        assertThat(deserializeThrowable, notNullValue());
        assertThat(deserializeThrowable, instanceOf(JsonProcessingException.class));

        // check that the final state is equal to the initial state
        assertThat(localStore.getLastAppliedIndex(), equalTo(originalIndex));
        assertThat(localStore.getAllForUnitTestsOnly(), containsInAnyOrder(keyValues));
    }

    private void assertThatLastAppliedIndexHasValue(final long expectedLastAppliedIndex) {
        assertThat(localStore.getLastAppliedIndex(), equalTo(expectedLastAppliedIndex));
    }

    private void assertThatLocalStoreHasKeyValue(final String key, @Nullable final String value) {
        assertThat(localStore.getKeyValueForUnitTestsOnly(key), equalTo(value));
    }
}
