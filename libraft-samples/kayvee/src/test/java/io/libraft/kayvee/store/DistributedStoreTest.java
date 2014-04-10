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

import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;
import io.libraft.Command;
import io.libraft.NotLeaderException;
import io.libraft.agent.RaftAgent;
import io.libraft.kayvee.TestLoggingRule;
import io.libraft.kayvee.api.KeyValue;
import io.libraft.kayvee.api.SetValue;
import org.hamcrest.Matchers;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.mockito.ArgumentCaptor;
import org.mockito.InOrder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyLong;
import static org.mockito.Matchers.anyString;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public final class DistributedStoreTest {

    private static final Logger LOGGER = LoggerFactory.getLogger(DistributedStoreTest.class);

    private final LocalStore localStore = mock(LocalStore.class);
    private final RaftAgent raftAgent = mock(RaftAgent.class);

    @Rule
    public final TestLoggingRule testLoggingRule = new TestLoggingRule(LOGGER);

    @Rule
    public final ExpectedException expectedException = ExpectedException.none();

    private DistributedStore distributedStore;

    @Before
    public void setup() {
        distributedStore = new DistributedStore(localStore);
        distributedStore.setRaftAgent(raftAgent);
    }

    @Test
    public void shouldSubmitSetCommandWhenSetIsCalled() throws Exception {
        String key = "KEY";
        String value = "VALUE";

        SetValue setValue = new SetValue();
        setValue.setNewValue(value);

        when(raftAgent.submitCommand(any(KayVeeCommand.class))).thenReturn(SettableFuture.<Void>create());

        initializeAndStartDistributedStore();

        ListenableFuture<KeyValue> setFuture = distributedStore.set(key, setValue);
        assertThat(setFuture.isDone(), equalTo(false));

        ArgumentCaptor<Command> captor = ArgumentCaptor.forClass(Command.class);
        verify(raftAgent).submitCommand(captor.capture());

        KayVeeCommand.SETCommand setCommand = (KayVeeCommand.SETCommand) captor.getValue();
        assertThat(setCommand.getKey(), equalTo(key));
        assertThat(setCommand.getNewValue(), equalTo(value));

        stopDistributedStore();
    }

    @Test
    public void shouldFailSetCommandImmediatelyIfNodeIsNotLeader() throws Exception {
        String leader = "OTHER";

        when(raftAgent.submitCommand(any(KayVeeCommand.class))).thenThrow(new NotLeaderException("SELF", leader));

        initializeAndStartDistributedStore();

        ListenableFuture<KeyValue> setFuture = distributedStore.set("IGNORE", new SetValue());
        assertThat(setFuture.isDone(), equalTo(true));

        expectedException.expectCause(Matchers.<NotLeaderException>instanceOf(NotLeaderException.class));
        setFuture.get();
    }

    @Test
    public void shouldApplySetCommandAndTriggerFutureOnRaftCallback() throws Exception {
        String key = "KEY";
        String value = "VALUE";

        SetValue setValue = new SetValue();
        setValue.setNewValue(value);

        when(raftAgent.submitCommand(any(KayVeeCommand.class))).thenReturn(SettableFuture.<Void>create());
        when(localStore.set(anyLong(), eq(key), eq(value))).thenReturn(new KeyValue(key, value));

        initializeAndStartDistributedStore();

        // issue the command
        ListenableFuture<KeyValue> setFuture = distributedStore.set(key, setValue);
        assertThat(setFuture.isDone(), equalTo(false));

        // get the command so that we can trigger it
        ArgumentCaptor<Command> captor = ArgumentCaptor.forClass(Command.class);
        verify(raftAgent).submitCommand(captor.capture());

        // trigger the raft callback
        distributedStore.applyCommitted(new TestCommittedCommand(1, captor.getValue()));

        // check that we did everything we had to
        verify(localStore).set(1, key, value);
        assertThat(setFuture.isDone(), equalTo(true));
        assertThat(setFuture.get().getKey(), equalTo(key));
        assertThat(setFuture.get().getValue(), equalTo(value));

        distributedStore.stop();
    }

    @Test
    public void shouldAttemptToApplyCASCommandOnRaftCallbackButFailFutureIfItFails() throws Exception {
        String key = "KEY";
        SetValue setValue = new SetValue();
        setValue.setExpectedValue("EXPECTED");
        setValue.setNewValue("NEW");

        when(raftAgent.submitCommand(any(KayVeeCommand.class))).thenReturn(SettableFuture.<Void>create());

        initializeAndStartDistributedStore();

        // issue the command
        ListenableFuture<KeyValue> casFuture = distributedStore.compareAndSet(key, setValue);
        assertThat(casFuture.isDone(), equalTo(false));

        // get the command so that we can trigger it
        ArgumentCaptor<Command> captor = ArgumentCaptor.forClass(Command.class);
        verify(raftAgent).submitCommand(captor.capture());

        // throw an exception when we attempt to apply the CAS
        doThrow(new KeyAlreadyExistsException(key)).when(localStore).compareAndSet(anyLong(), anyString(), anyString(), anyString());

        // trigger the raft callback
        distributedStore.applyCommitted(new TestCommittedCommand(1, captor.getValue()));

        // verify that we get the exception
        assertThat(casFuture.isDone(), equalTo(true));
        expectedException.expectCause(Matchers.<KeyAlreadyExistsException>instanceOf(KeyAlreadyExistsException.class));
        casFuture.get();
    }

    @Test
    public void shouldApplyCommittedUnappliedCommandsDuringAgentInitialization() throws Exception {
        KayVeeCommand.SETCommand commandAtIndex1 = new KayVeeCommand.SETCommand(1, "ONE", "ONE");
        KayVeeCommand.SETCommand commandAtIndex2 = new KayVeeCommand.SETCommand(2, "TWO", "TWO");
        KayVeeCommand.SETCommand commandAtIndex5 = new KayVeeCommand.SETCommand(5, "FIV", "FIV");
        KayVeeCommand.SETCommand commandAtIndex6 = new KayVeeCommand.SETCommand(6, "SIX", "SIX");

        when(localStore.getLastAppliedIndex())
                .thenReturn(0L)
                .thenReturn(1L)
                .thenReturn(2L)
                .thenReturn(5L)
                .thenReturn(6L);

        when(raftAgent.getNextCommitted(0)).thenReturn(new TestCommittedCommand(1, commandAtIndex1));
        when(raftAgent.getNextCommitted(1)).thenReturn(new TestCommittedCommand(2, commandAtIndex2));
        when(raftAgent.getNextCommitted(2)).thenReturn(new TestCommittedCommand(5, commandAtIndex5));
        when(raftAgent.getNextCommitted(5)).thenReturn(new TestCommittedCommand(6, commandAtIndex6));
        when(raftAgent.getNextCommitted(6)).thenReturn(null);

        distributedStore.initialize();

        InOrder inOrder = inOrder(raftAgent, localStore);
        inOrder.verify(raftAgent).initialize();
        inOrder.verify(raftAgent).getNextCommitted(0);
        inOrder.verify(localStore).set(1, "ONE", "ONE");
        inOrder.verify(raftAgent).getNextCommitted(1);
        inOrder.verify(localStore).set(2, "TWO", "TWO");
        inOrder.verify(raftAgent).getNextCommitted(2);
        inOrder.verify(localStore).set(5, "FIV", "FIV");
        inOrder.verify(raftAgent).getNextCommitted(5);
        inOrder.verify(localStore).set(6, "SIX", "SIX");
        inOrder.verify(raftAgent).getNextCommitted(6);
        inOrder.verifyNoMoreInteractions();
    }

    private void initializeAndStartDistributedStore() throws Exception {
        distributedStore.initialize();
        distributedStore.start();
    }

    private void stopDistributedStore() throws Exception {
        distributedStore.stop();
        verify(raftAgent).stop();
    }
}
