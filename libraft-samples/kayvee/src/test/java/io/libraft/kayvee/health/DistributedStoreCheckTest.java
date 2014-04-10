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

package io.libraft.kayvee.health;

import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.SettableFuture;
import io.libraft.NotLeaderException;
import io.libraft.kayvee.store.DistributedStore;
import org.hamcrest.Matchers;
import org.junit.Test;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.nullValue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public final class DistributedStoreCheckTest {

    private final DistributedStore distributedStore = mock(DistributedStore.class);
    private final DistributedStoreCheck check = new DistributedStoreCheck(distributedStore);

    @Test
    public void shouldReturnOKResultIfServerIsLeader() {
         // we pretend the NOP command succeeds (only happens if the server is the leader)
        when(distributedStore.nop()).thenReturn(Futures.<Void>immediateFuture(null));

        assertThat(check.check().isHealthy(), equalTo(true));
    }

    @Test
    public void shouldReturnOKResultIfServerIsPartOfClusterAndLeaderIsKnown() {
        // we pretend the NOP command failed, but the leader field is set
        when(distributedStore.nop()).thenReturn(Futures.<Void, NotLeaderException>immediateFailedCheckedFuture(new NotLeaderException("SELF", "LEADER")));

        assertThat(check.check().isHealthy(), equalTo(true));
    }

    @Test
    public void shouldReturnUnhealthyResultIfServerIsPartOfClusterAndLeaderIsUnknown() {
         // we pretend the NOP command failed and the leader field is not set, which means that the cluster is interregna
        NotLeaderException notLeaderException = new NotLeaderException("SELF", null);
        when(distributedStore.nop()).thenReturn(Futures.<Void>immediateFailedFuture(notLeaderException));

        assertThat(check.check().isHealthy(), equalTo(false));
        assertThat(check.check().getError(), nullValue());
    }

    // the actual method call threw an exception
    @Test
    public void shouldReturnUnhealthyResultIfCallToDistributedStoreThrowsException0() {
        // when the NOP command is made the call throws an exception immediately
        IllegalStateException failureCause = new IllegalStateException("failed");
        when(distributedStore.nop()).thenThrow(failureCause);

        assertThat(check.check().isHealthy(), equalTo(false));
        assertThat(check.check().getError(), Matchers.<Throwable>sameInstance(failureCause)); // the error returned is the actual one thrown
    }

    // somehow, internally a failure occurred, so we return a future with that exception set
    @Test
    public void shouldReturnUnhealthyResultIfCallToDistributedStoreThrowsException1() {
        // when the NOP command is made the call throws an exception immediately
        IllegalStateException failureCause = new IllegalStateException("failed");
        when(distributedStore.nop()).thenReturn(Futures.<Void>immediateFailedFuture(failureCause));

        assertThat(check.check().isHealthy(), equalTo(false));
        assertThat(check.check().getError(), Matchers.<Throwable>sameInstance(failureCause)); // the error returned is the actual one thrown, not an ExecutionException
    }

    @Test
    public void shouldReturnUnhealthyResultIfCallToDistributedStoreTimesOut() {
        // just return a future and let the call sit there and wait
        when(distributedStore.nop()).thenReturn(SettableFuture.<Void>create());

        assertThat(check.check().isHealthy(), equalTo(false));
    }
}
