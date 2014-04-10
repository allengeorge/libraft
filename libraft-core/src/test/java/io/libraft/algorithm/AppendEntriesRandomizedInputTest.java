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
import org.hamcrest.Matchers;
import org.junit.Before;
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
import java.util.Set;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;
import static io.libraft.algorithm.StoringSender.AppendEntries;
import static io.libraft.algorithm.UnitTestLogEntries.NOOP;
import static io.libraft.algorithm.UnitTestLogEntries.SENTINEL;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.mockito.Mockito.mock;

@RunWith(Parameterized.class)
public final class AppendEntriesRandomizedInputTest {

    private static final Logger LOGGER = LoggerFactory.getLogger(AppendEntriesRandomizedInputTest.class);

    private static final int MIN_APPEND_ENTRIES_TO_GENERATE = 25;
    private static final int MIN_EXTRA_APPEND_ENTRIES_TO_GENERATE = 10;
    private static final int RANGE_EXTRA_APPEND_ENTRIES_TO_GENERATE = 10;

    private static final String SELF = "S_00";
    private static final String LEADER = "S_03";
    private static final Set<String> CLUSTER = ImmutableSet.of(SELF, "S_01", "S_02", LEADER, "S_04");

    private static final int CURRENT_TERM = 9;
    private static final int STARTING_COMMIT_INDEX = 5;
    private static final int MATCHING_PREFIX_INDEX = 6;

    private final LogEntry[] LEADER_LOG = {
            SENTINEL(),
            NOOP(1, 1),
            NOOP(2, 1),
            NOOP(3, 2),
            NOOP(4, 2),
            NOOP(5, 2),
            NOOP(6, 2), // <--- MATCHING PREFIX
            NOOP(7, 5),
            NOOP(8, 6),
            NOOP(9, 6),
            NOOP(10, 6),
            NOOP(11, 6),
            NOOP(12, 6),
            NOOP(13, 6),
            NOOP(14, 6),
            NOOP(15, 6),
            NOOP(16, 6),
            NOOP(17, 6),
            NOOP(18, 6),
            NOOP(19, 8),
            NOOP(20, 8),
            NOOP(21, 8)
    };

    private static final LogEntry[] SELF_LOG = {
            SENTINEL(),
            NOOP(1, 1),
            NOOP(2, 1),
            NOOP(3, 2),
            NOOP(4, 2),
            NOOP(5, 2),
            NOOP(6, 2), // <--- MATCHING PREFIX
            NOOP(7, 3),
            NOOP(8, 3),
            NOOP(9, 3),
            NOOP(10, 4),
            NOOP(11, 4)
    };

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
    private final RPCSender sender;
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

    public AppendEntriesRandomizedInputTest(int randomSeed) {
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

    @Before
    public void setup() throws Exception {
        // setup starting state for SELF
        // done here instead of constructor because I don't believe in throwing inside a constructor
        store.setCommitIndex(STARTING_COMMIT_INDEX);
        store.setCurrentTerm(CURRENT_TERM);
        store.setVotedFor(CURRENT_TERM, LEADER);
        insertIntoLog(SELF_LOG);

        // create the algorithm instance
        algorithm = new RaftAlgorithm(raftAlgorithmRandom, timer, sender, store, log, snapshotsStore, listener, SELF, CLUSTER);
        algorithm.initialize();
        algorithm.start();
    }

    private void insertIntoLog(LogEntry... entries) throws StorageException {
        entries = checkNotNull(entries);
        checkArgument(entries.length > 0);

        for (LogEntry entry : entries) {
            log.put(entry);
        }
    }

    @Test
    public void shouldApplyLeaderLogEntries() throws Exception {
        List<AppendEntries> appendEntriesRequests = generateAppendEntriesRequests();
        for (AppendEntries appendEntries : appendEntriesRequests) {
            algorithm.onAppendEntries(appendEntries.server, appendEntries.term, appendEntries.commitIndex, appendEntries.prevLogIndex, appendEntries.prevLogTerm, appendEntries.entries);
        }

        LogEntry lastLog = checkNotNull(log.getLast());
        assertThat(lastLog.getIndex(), equalTo((long) (LEADER_LOG.length - 1)));

        for (int index = 0; index < LEADER_LOG.length; index++) {
            assertThat(log.get(index), Matchers.<LogEntry>equalTo(LEADER_LOG[index]));
        }
    }

    private List<AppendEntries> generateAppendEntriesRequests() {
        List<AppendEntries> appendEntriesRequests = Lists.newArrayListWithCapacity(MIN_APPEND_ENTRIES_TO_GENERATE);

        // generate all the messages necessary to cover the entire log
        long prefixIndex = 6;
        while (generateMoreMessages(appendEntriesRequests, prefixIndex)) {
            AppendEntries appendEntries = newAppendEntries();

            appendEntriesRequests.add(appendEntries);

            int shouldDuplicate = random.nextInt(29);
            if (shouldDuplicate >= 20 && appendEntriesRequests.size() > 1) {
                int duplicatePosition = random.nextInt(appendEntriesRequests.size() - 1);
                appendEntriesRequests.add(duplicatePosition, appendEntries);
            }

            if (appendEntries.prevLogIndex <= prefixIndex) {
                int entryCount = appendEntries.entries == null ? 0 : appendEntries.entries.size();
                prefixIndex = Math.max(prefixIndex, appendEntries.prevLogIndex + entryCount);
            }
        }

        // now, generate a couple extra
        int extraAppendEntriesToGenerate = MIN_EXTRA_APPEND_ENTRIES_TO_GENERATE + raftAlgorithmRandom.nextInt(RANGE_EXTRA_APPEND_ENTRIES_TO_GENERATE);
        for (int i = 0; i < extraAppendEntriesToGenerate; i++) {
            appendEntriesRequests.add(newAppendEntries());
        }

        LOGGER.info("generated {} AppendEntries messages", appendEntriesRequests.size());

        return appendEntriesRequests;
    }

    private boolean generateMoreMessages(List<AppendEntries> appendEntriesRequests, long prefixIndex) {
        return (appendEntriesRequests.size() < MIN_APPEND_ENTRIES_TO_GENERATE) || (prefixIndex < LEADER_LOG[LEADER_LOG.length - 1].getIndex());
    }

    private AppendEntries newAppendEntries() {
        int prevLogIndex = MATCHING_PREFIX_INDEX + random.nextInt(LEADER_LOG.length - 1 - MATCHING_PREFIX_INDEX);
        int prevLogTerm = (int) LEADER_LOG[prevLogIndex].getTerm();
        int entryCount = random.nextInt(LEADER_LOG.length - 1 - prevLogIndex);

        List<LogEntry> entries;
        if (entryCount > 0) {
            entries = Lists.newArrayListWithCapacity(entryCount);
            for (int count = 0; count <= entryCount; count++) {
                entries.add(LEADER_LOG[prevLogIndex + 1 + count]);
            }
        } else {
            entries = null;
        }

        return new AppendEntries(LEADER, CURRENT_TERM, STARTING_COMMIT_INDEX, prevLogIndex, prevLogTerm, entries);
    }
}
