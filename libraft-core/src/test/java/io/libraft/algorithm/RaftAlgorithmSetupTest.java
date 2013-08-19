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

package io.libraft.algorithm;

import com.google.common.collect.Sets;
import io.libraft.RaftListener;
import io.libraft.testlib.TestLoggingRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.mockito.Mockito;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Random;
import java.util.Set;

import static org.mockito.Mockito.when;

public final class RaftAlgorithmSetupTest {

    private static final Logger LOGGER = LoggerFactory.getLogger(RaftAlgorithmSetupTest.class);

    private static final String SELF = "0";

    private final Random random = new Random();
    private final Timer timer = Mockito.mock(Timer.class);
    private final RPCSender sender = Mockito.mock(RPCSender.class);
    private final Store store = Mockito.mock(Store.class);
    private final Log log = Mockito.mock(Log.class);
    private final RaftListener raftListener = Mockito.mock(RaftListener.class);

    @Rule
    public final ExpectedException expectedException = ExpectedException.none();

    @Rule
    public final TestLoggingRule testLoggingRule = new TestLoggingRule(LOGGER);

    @Test(expected = IllegalArgumentException.class)
    public void shouldNotAcceptClustersSmallerThanThree() {
        Set<String> cluster = Sets.newHashSet("1");
        new RaftAlgorithm(random, timer, sender, store, log, raftListener, SELF, cluster);
    }

    @Test(expected = IllegalArgumentException.class)
    public void shouldNotAcceptClustersGreaterThanSeven() {
        Set<String> cluster = Sets.newHashSet(SELF, "1", "2", "3", "4", "5", "6", "7");
        new RaftAlgorithm(random, timer, sender, store, log, raftListener, SELF, cluster);
    }

    @Test(expected = IllegalArgumentException.class)
    public void shouldNotAllowServersWithDuplicateIds() {
        Set<String> cluster = Sets.newHashSet(SELF, SELF, "2");
        new RaftAlgorithm(random, timer, sender, store, log, raftListener, SELF, cluster);
    }

    @Test(expected = IllegalArgumentException.class)
    public void shouldNotAllowAClusterWithoutTheLocalServer() {
        Set<String> cluster = Sets.newHashSet("1", "2", "3");
        new RaftAlgorithm(random, timer, sender, store, log, raftListener, SELF, cluster);
    }

    @Test
    public void shouldSucceedConstructionWithValidArguments() {
        createValidRaftAlgorithmInstance();
    }

    private RaftAlgorithm createValidRaftAlgorithmInstance() {
        Set<String> cluster = Sets.newHashSet(SELF, "1", "2");
        return new RaftAlgorithm(random, timer, sender, store, log, raftListener, SELF, cluster);
    }

    @Test
    public void shouldFailToStartIfCurrentTermLessThanZero() throws StorageException {
        when(log.getLast()).thenReturn(LogEntry.SENTINEL);
        when(store.getCurrentTerm()).thenReturn(-1L);
        when(store.getCommitIndex()).thenReturn(0L);

        RaftAlgorithm algorithm = createValidRaftAlgorithmInstance();

        expectedException.expect(IllegalStateException.class);
        algorithm.start();
    }

    @Test
    public void shouldFailToStartIfLastLogTermGreaterThanCurrentTerm() throws StorageException {
        when(log.getLast()).thenReturn(new LogEntry.NoopEntry(2, 3));
        when(store.getCurrentTerm()).thenReturn(1L);
        when(store.getCommitIndex()).thenReturn(0L);

        RaftAlgorithm algorithm = createValidRaftAlgorithmInstance();

        expectedException.expect(IllegalStateException.class);
        algorithm.start();
    }

    @Test
    public void shouldFailToStartIfCommitIndexLessThanZero() throws StorageException {
        when(log.getLast()).thenReturn(LogEntry.SENTINEL);
        when(store.getCurrentTerm()).thenReturn(0L);
        when(store.getCommitIndex()).thenReturn(-1L);

        RaftAlgorithm algorithm = createValidRaftAlgorithmInstance();

        expectedException.expect(IllegalStateException.class);
        algorithm.start();
    }

    @Test
    public void shouldFailToStartIfCommitIndexGreaterThanLastLogIndex() throws StorageException {
        when(log.getLast()).thenReturn(new LogEntry.NoopEntry(2, 1));
        when(store.getCurrentTerm()).thenReturn(1L);
        when(store.getCommitIndex()).thenReturn(3L);

        RaftAlgorithm algorithm = createValidRaftAlgorithmInstance();

        expectedException.expect(IllegalStateException.class);
        algorithm.start();
    }
}