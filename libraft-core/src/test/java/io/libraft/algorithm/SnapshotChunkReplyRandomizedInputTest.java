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

package io.libraft.algorithm;

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import io.libraft.RaftListener;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestRule;
import org.junit.rules.TestWatcher;
import org.junit.runner.Description;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Random;

import static io.libraft.algorithm.UnitTestLogEntries.NOOP;
import static io.libraft.algorithm.UnitTestLogEntries.SENTINEL;
import static org.mockito.Mockito.mock;

@RunWith(Parameterized.class)
public final class SnapshotChunkReplyRandomizedInputTest {

    private static final Logger LOGGER = LoggerFactory.getLogger(SnapshotChunkReplyRandomizedInputTest.class);

    private static final String SELF = "S_00"; // we are the leader
    private static final String[] CLUSTER = new String[] {SELF, "S_01", "S_02", "S_03", "S_04", "S_05", "S_06"};

    private static final int CURRENT_TERM = 6;
    private static final int INITIAL_COMMIT_INDEX = 5;
    private static final int MATCHING_PREFIX_INDEX = 5;
    private static final int MIN_APPEND_ENTRIES_REPLIES_TO_SEND = 200;
    private static final int QUORUM_SIZE = (int) Math.ceil(((double) CLUSTER.length) / 2);

    private final LogEntry[] BASE_LOG = {
            SENTINEL(),
            NOOP(1, 1),
            NOOP(1, 2),
            NOOP(1, 3),
            NOOP(2, 4),
            NOOP(2, 5), // <--- MATCHING PREFIX
            NOOP(2, 6),
            NOOP(2, 7),
            NOOP(3, 8),
            NOOP(5, 9),
            NOOP(5, 10),
            NOOP(5, 11),
            NOOP(5, 12),
            NOOP(5, 13)
    };

    private final LogEntry[] FULL_LOG = {
            SENTINEL(),
            NOOP(1, 1),
            NOOP(1, 2),
            NOOP(1, 3),
            NOOP(2, 4),
            NOOP(2, 5), // <--- MATCHING PREFIX
            NOOP(2, 6),
            NOOP(2, 7),
            NOOP(3, 8),
            NOOP(5, 9),
            NOOP(5, 10),
            NOOP(5, 11),
            NOOP(5, 12),
            NOOP(5, 13),
            NOOP(6, 14), // <--- FIRST COMMAND OF CURRENT_TERM
            NOOP(6, 15),
            NOOP(6, 16),
            NOOP(6, 17),
            NOOP(6, 18),
            NOOP(6, 19),
            NOOP(6, 20),
            NOOP(6, 21),
            NOOP(6, 22),
            NOOP(6, 23),
            NOOP(6, 24),
            NOOP(6, 25),
            NOOP(6, 26),
    };

    private final int LAST_LOG_INDEX = FULL_LOG.length - 1;

    @Parameterized.Parameters
    public static java.util.Collection<Object[]> generateRandomSeeds() {
        int testIterations = 50;

        Random random = new Random(System.nanoTime());
        List<Object[]> randomSeeds = Lists.newArrayListWithCapacity(testIterations);
        for (int i = 0; i < testIterations; i++) {
            randomSeeds.add(new Object[]{random.nextInt()});
        }

        return randomSeeds;
    }

    private final Random random;

    private final int randomSeed;
    private final Random raftAlgorithmRandom;
    private final Timer timer;
    private final StoringSender sender;
    private final Store store;
    private final Log log;
    private final SnapshotsStore snapshotsStore;
    private final RaftListener listener;

    private RaftAlgorithm algorithm;

    @Rule
    public final TestRule randomizedTestRule = new TestWatcher() {

        @Override
        protected void starting(Description description) {
            LOGGER.info("random seed:{}", randomSeed);
            super.starting(description);
        }
    };

    public SnapshotChunkReplyRandomizedInputTest(int randomSeed) {
        this.randomSeed = randomSeed;

        // create the Random that's used to randomize the input messages
        random = new Random(randomSeed);

        // create all objects required by algorithm
        raftAlgorithmRandom = new Random(random.nextLong());
        timer = new UnitTestTimer();
        sender = new StoringSender();
        store = new InMemoryStore();
        log = new InMemoryLog();
        snapshotsStore = mock(SnapshotsStore.class);
        listener = mock(RaftListener.class);
    }

    //
    // setup
    //

    @Before
    public void setup() throws Exception {
        // setup starting state for SELF
        // done here instead of constructor because I don't believe in throwing inside a constructor
        store.setCommitIndex(INITIAL_COMMIT_INDEX);
        store.setCurrentTerm(CURRENT_TERM - 1);

        for (LogEntry entry : BASE_LOG) {
            log.put(entry);
        }

        // create and start the algorithm instance
        algorithm = new RaftAlgorithm(raftAlgorithmRandom, timer, sender, store, log, snapshotsStore, listener, SELF, ImmutableSet.copyOf(CLUSTER));
        algorithm.initialize();
        algorithm.start();

        // become the leader
        // we have to first become a candidate, vote for ourself, and then become a leader
        algorithm.becomeCandidate(CURRENT_TERM);
        store.setVotedFor(CURRENT_TERM, SELF);
        algorithm.becomeLeader(CURRENT_TERM);

        // NOTE: I have to insert log entries in two pieces
        // because of the logic in becomeLeader
        // that method, when called, asserts that there is no
        // entry for the current term, and then appends a new
        // log entry for the current term
        //
        // I believe (although this remains to be proven)
        // that it's better to make the test code slightly
        // more complicated as opposed to relaxing the
        // assertions in the mainline code

        // now, add the remaining entries to get to the final log state
        // BASE_LOG.length is the first entry added by "becomeLeader"
        // BASE_LOG.length + 1 is the first entry that we have to add...
        for (int i = BASE_LOG.length + 1; i < FULL_LOG.length; i++) {
            algorithm.addOrUpdateLogEntryWhileLeaderForUnitTestsOnly(FULL_LOG[i]);
        }
    }

    @Ignore
    @Test
    public void shouldConsumeRandomSnapshotChunkReplyMessagesUntilTheCompleteSnapshotIsSent() throws Exception {
    }
}
