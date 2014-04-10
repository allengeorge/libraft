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
import com.google.common.collect.Maps;
import io.libraft.RaftListener;
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
import java.util.Map;
import java.util.Random;

import static com.google.common.base.Preconditions.checkNotNull;
import static io.libraft.algorithm.StoringSender.AppendEntriesReply;
import static io.libraft.algorithm.UnitTestLogEntries.NOOP;
import static io.libraft.algorithm.UnitTestLogEntries.SENTINEL;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.mockito.Mockito.mock;

@RunWith(Parameterized.class)
public final class AppendEntriesReplyRandomizedInputTest {

    private static final Logger LOGGER = LoggerFactory.getLogger(AppendEntriesReplyRandomizedInputTest.class);

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
            NOOP(2, 1),
            NOOP(3, 1),
            NOOP(4, 2),
            NOOP(5, 2), // <--- MATCHING PREFIX
            NOOP(6, 2),
            NOOP(7, 2),
            NOOP(8, 3),
            NOOP(9, 5),
            NOOP(10, 5),
            NOOP(11, 5),
            NOOP(12, 5),
            NOOP(13, 5)
    };

    private final LogEntry[] FULL_LOG = {
            SENTINEL(),
            NOOP(1, 1),
            NOOP(2, 1),
            NOOP(3, 1),
            NOOP(4, 2),
            NOOP(5, 2), // <--- MATCHING PREFIX
            NOOP(6, 2),
            NOOP(7, 2),
            NOOP(8, 3),
            NOOP(9, 5),
            NOOP(10, 5),
            NOOP(11, 5),
            NOOP(12, 5),
            NOOP(13, 5),
            NOOP(14, 6), // <--- FIRST COMMAND OF CURRENT_TERM
            NOOP(15, 6),
            NOOP(16, 6),
            NOOP(17, 6),
            NOOP(18, 6),
            NOOP(19, 6),
            NOOP(20, 6),
            NOOP(21, 6),
            NOOP(22, 6),
            NOOP(23, 6),
            NOOP(24, 6),
            NOOP(25, 6),
            NOOP(26, 6),
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

    public AppendEntriesReplyRandomizedInputTest(int randomSeed) {
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

    //
    // APPLYING
    //

    @Test
    public void shouldConsumeRandomAppendEntriesReplyMessagesInApplyingPhase() throws Exception {
        // update nextIndexes for all the servers
        for (String server : CLUSTER) {
            if (!server.equals(SELF)) {
                algorithm.setServerNextIndexWhileLeaderForUnitTestsOnly(server, MATCHING_PREFIX_INDEX + 1);
            }
        }

        // dump the contents of the sender
        sender.drainSentRPCs();

        List<AppendEntriesReply> previouslyProcessed = Lists.newArrayListWithCapacity(MIN_APPEND_ENTRIES_REPLIES_TO_SEND);
        Map<String, Long> maxAppliedIndices = Maps.newHashMap();

        long selfCommitIndex = store.getCommitIndex();
        long expectedCommitIndex = INITIAL_COMMIT_INDEX;
        for (int round = 0; round < MIN_APPEND_ENTRIES_REPLIES_TO_SEND; round++) {
            // pick a message we sent in the past and resend it
            boolean shouldDuplicate = random.nextBoolean();
            if (shouldDuplicate && previouslyProcessed.size() > 0) {
                AppendEntriesReply reply = checkNotNull(previouslyProcessed.get(random.nextInt(previouslyProcessed.size())));
                processMessage(reply); // don't have to update state, since it's a duplicate
            }

            // create a new message
            // note that all we're varying is the number of entries we've applied, starting at MATCHING_PREFIX_INDEX
            String server = chooseSender(); // we don't want 0, because it's us
            int entryCount = random.nextInt(LAST_LOG_INDEX - MATCHING_PREFIX_INDEX);
            AppendEntriesReply reply = new AppendEntriesReply(server, CURRENT_TERM, MATCHING_PREFIX_INDEX, entryCount, true);

            // process this message
            // - updates the list of messages previously sent
            // - updates the max index applied by the sending server
            // - returns the commit index we expect locally, having sent this message
            expectedCommitIndex = processMessageAndUpdateApplyingState(reply, previouslyProcessed, maxAppliedIndices);

            // now, check if the commit index should have been updated
            assertThat(store.getCommitIndex(), equalTo(expectedCommitIndex));

            // check that our (the leader's) commitIndex is monotonically increasing
            assertThat(store.getCommitIndex(), greaterThanOrEqualTo(selfCommitIndex));
            selfCommitIndex = store.getCommitIndex();
        }

        // process some cleanup messages
        // we have to do this because the randomly sent messages above may not
        // result in _all_ the entries being applied. so, we add the last few entries
        // required
        for (Map.Entry<String, Long> maxAppliedEntry: maxAppliedIndices.entrySet()) {
            String server = maxAppliedEntry.getKey();
            long maxAppliedIndex = maxAppliedEntry.getValue();

            if (maxAppliedIndex < LAST_LOG_INDEX) {
                AppendEntriesReply reply = new AppendEntriesReply(server, CURRENT_TERM, maxAppliedIndex, LAST_LOG_INDEX - maxAppliedIndex, true);
                expectedCommitIndex = processMessageAndUpdateApplyingState(reply, previouslyProcessed, maxAppliedIndices);
            }
        }

        // we should have committed all the entries
        assertThat(expectedCommitIndex, equalTo((long) LAST_LOG_INDEX));
        assertThat(store.getCommitIndex(), equalTo(expectedCommitIndex));
    }

    private long processMessageAndUpdateApplyingState(AppendEntriesReply reply, List<AppendEntriesReply> previouslyProcessed, Map<String, Long> maxAppliedIndices) throws StorageException {
        // process it, and add it to the list of messages we've previously sent
        processMessage(reply);
        previouslyProcessed.add(reply);

        // update the latest applied entry for this server
        long maxAppliedEntry = reply.prevLogIndex + reply.entryCount;

        Long previousMax = maxAppliedIndices.get(reply.server);
        if (previousMax == null) {
            maxAppliedIndices.put(reply.server, maxAppliedEntry);
        } else {
            if (previousMax < maxAppliedEntry) {
                maxAppliedIndices.put(reply.server, maxAppliedEntry);
            }
        }

        // return the commit index we expect locally
        return findNewExpectedCommitIndex(maxAppliedIndices);
    }

    // this is the _absolute, most naive_ check possible
    //
    // this is incredibly wasteful, but, simple to understand.
    // for each log index in our (the leader's) log, check how
    // many other servers have applied a log entry with index >= it.
    // since we're going from the end of the log down, the first
    // index for which the number of servers (including us) who've
    // applied the entry is >= quorum size, _and_ whose term is the
    // current term is the commit index
    private long findNewExpectedCommitIndex(Map<String, Long> maxAppliedIndices) throws StorageException {
        long newExpectedCommitIndex = INITIAL_COMMIT_INDEX;

        boolean foundHighestCommittedIndex = false;
        for (int logIndex = LAST_LOG_INDEX; logIndex >= MATCHING_PREFIX_INDEX; logIndex--) {
            int numAppliedServers = 1; // the leader has already applied the entry

            for (long maxAppliedIndex : maxAppliedIndices.values()) { // loop through the indices of the applied entries
                if (maxAppliedIndex >= logIndex) {
                    numAppliedServers++;
                }

                if (numAppliedServers >= QUORUM_SIZE && FULL_LOG[logIndex].getTerm() == CURRENT_TERM) {
                    newExpectedCommitIndex = Math.max(newExpectedCommitIndex, logIndex);
                    foundHighestCommittedIndex = true;
                    break;
                }
            }

            if (foundHighestCommittedIndex) {
                break;
            }
        }

        return newExpectedCommitIndex;
    }

    //
    // PREFIX_SEARCH
    //

    @Test
    public void shouldConsumeAppendEntriesRepliesInPrefixSearchPhase() {
        // setup initial state
        List<AppendEntriesReply> previouslyProcessed = Lists.newArrayListWithCapacity(MIN_APPEND_ENTRIES_REPLIES_TO_SEND);
        Map<String, Integer> nextIndices = Maps.newHashMap();
        for (String server : CLUSTER) {
            if (!server.equals(SELF)) {
                nextIndices.put(server, BASE_LOG.length);
            }
        }

        // send messages
        for (int i = 0; i < MIN_APPEND_ENTRIES_REPLIES_TO_SEND; i++) {
            AppendEntriesReply reply;

            boolean isDuplicate = random.nextBoolean() && random.nextBoolean();
            if (isDuplicate) {
                if (previouslyProcessed.size() == 0) { // bail early if there are no messages sent yet
                    continue;
                }

                reply = previouslyProcessed.get(random.nextInt(previouslyProcessed.size()));
            } else {
                boolean isRandomMessage = random.nextBoolean(); // is this a message that starts at a random prevLogIndex?
                String server = chooseSender();
                int storedNextIndex = nextIndices.get(server);

                // construct the message
                if (isRandomMessage) {
                    int prevLogIndex = random.nextInt(BASE_LOG.length - MATCHING_PREFIX_INDEX) + MATCHING_PREFIX_INDEX;
                    int randomEntryCount = random.nextInt(BASE_LOG.length - prevLogIndex);
                    reply = new AppendEntriesReply(server, CURRENT_TERM, prevLogIndex, randomEntryCount, false);
                } else {
                    int prevLogIndex = storedNextIndex - 1;
                    int actualEntryCount = BASE_LOG.length - prevLogIndex;
                    reply = new AppendEntriesReply(server, CURRENT_TERM, prevLogIndex, actualEntryCount, false);
                }
            }

            processMessageAndUpdatePrefixSearchState(reply, isDuplicate, nextIndices, previouslyProcessed);
        }

        // I iterate twice below for clarity

        // since the messages are random we may not always send
        // a message that updates nextIndex. To make the check easier
        // simply send any "cleanup" messages
        for (String server : CLUSTER) {
            if (!SELF.equals(server)) {
                int startingNextIndex = nextIndices.get(server);
                for (int nextIndex = startingNextIndex; nextIndex > MATCHING_PREFIX_INDEX + 1; nextIndex--) {
                    AppendEntriesReply reply = new AppendEntriesReply(server, CURRENT_TERM, nextIndex - 1, BASE_LOG.length - (nextIndex - 1), false);
                    processMessage(reply);
                }
            }
        }

        // verify that at the end, we're at the matching prefix
        for (String server : CLUSTER) {
            if (!SELF.equals(server)) {
                assertThat(algorithm.getNextIndex(server), equalTo(MATCHING_PREFIX_INDEX + 1L));
            }
        }
    }

    private void processMessageAndUpdatePrefixSearchState(AppendEntriesReply reply, boolean isDuplicate, Map<String, Integer> nextIndices, List<AppendEntriesReply> previouslyProcessed) {
        int nextIndex = nextIndices.get(reply.server);

        // bail early if we've reached the matching prefix
        if (nextIndices.get(reply.server) == MATCHING_PREFIX_INDEX + 1) {
            return;
        }

        // always have RaftAlgorithm process the message
        processMessage(reply);

        if (!isDuplicate) { // we've already added this message to the list, so don't add it again
            previouslyProcessed.add(reply);
        }

        // update nextIndex for the server
        if (reply.prevLogIndex + 1 == nextIndex) {
            nextIndices.put(reply.server, nextIndex - 1);
        }

        // verify that RaftAlgorithm's state is correct
        assertThat(algorithm.getNextIndex(reply.server), equalTo((long) nextIndices.get(reply.server)));
    }

    //
    // common methods
    //

    private void processMessage(AppendEntriesReply reply) {
        algorithm.onAppendEntriesReply(reply.server, reply.term, reply.prevLogIndex, reply.entryCount, reply.applied);
    }

    private String chooseSender() {
        return CLUSTER[random.nextInt(CLUSTER.length - 1) + 1];
    }
}
