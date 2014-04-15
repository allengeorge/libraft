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
import com.google.common.io.Files;
import io.libraft.RaftListener;
import io.libraft.SnapshotWriter;
import org.hamcrest.Description;
import org.hamcrest.Matcher;
import org.hamcrest.TypeSafeMatcher;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Random;
import java.util.Set;

import static io.libraft.algorithm.SnapshotsStore.ExtendedSnapshotWriter;
import static io.libraft.algorithm.UnitTestLogEntries.CLIENT;
import static io.libraft.algorithm.UnitTestLogEntries.NOOP;
import static io.libraft.algorithm.UnitTestLogEntries.SENTINEL;
import static io.libraft.algorithm.UnitTestLogEntries.assertThatLogContains;
import static io.libraft.algorithm.UnitTestLogEntries.assertThatLogContainsOnlySentinel;
import static io.libraft.algorithm.UnitTestLogEntries.assertThatLogIsEmpty;
import static io.libraft.algorithm.UnitTestLogEntries.clearLog;
import static io.libraft.algorithm.UnitTestLogEntries.insertIntoLog;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.argThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

public final class RaftAlgorithmTriggerAndStoreSnapshotTest {

    private static final Logger LOGGER = LoggerFactory.getLogger(RaftAlgorithmTest.class);

    private static final String SELF = "0";
    private static final String S_01 = "1";
    private static final String S_02 = "2";
    private static final String S_03 = "3";
    private static final String S_04 = "4";

    private static final Set<String> CLUSTER = ImmutableSet.of(SELF, S_01, S_02, S_03, S_04);
    private static final int SNAPSHOT_CHECK_INTERVAL = 30 * 1000; // 30 seconds

    private final Random randomSeeder = new Random();
    private final long seed = randomSeeder.nextLong();
    private final Random random = new Random(seed);
    private final UnitTestTimer timer = new UnitTestTimer();
    private final StoringSender sender = new StoringSender();
    private final InMemoryStore store = new InMemoryStore();
    private final InMemoryLog log = new InMemoryLog();
    private final RaftListener listener = mock(RaftListener.class);
    private final TempFileSnapshotsStore snapshotsStore = spy(new TempFileSnapshotsStore(Files.createTempDir()));

    private RaftAlgorithm algorithm;

    @Rule
    public LoggingRule loggingRule = new LoggingRule(LOGGER);

    private Matcher<SnapshotWriter> isValidInitialSnapshotWriter() {
        return new TypeSafeMatcher<SnapshotWriter>() {
            @Override
            protected boolean matchesSafely(SnapshotWriter item) {
                return item.getIndex() == RaftConstants.INITIAL_SNAPSHOT_WRITER_LOG_INDEX;
            }

            @Override
            public void describeTo(Description description) {
                description.appendText("getIndex should return ").appendValue(RaftConstants.INITIAL_SNAPSHOT_WRITER_LOG_INDEX);
            }

            @Override
            protected void describeMismatchSafely(SnapshotWriter item, Description mismatchDescription) {
                mismatchDescription.appendText(" getIndex was ").appendValue(item.getIndex());
            }
        };
    }

    @Before
    public void setup() throws StorageException {
        LOGGER.info("test seed:{}", seed);

        algorithm = new RaftAlgorithm(
                random,
                timer,
                sender,
                store,
                log,
                snapshotsStore,
                listener,
                SELF,
                CLUSTER,
                5, // snapshot every 5 log entries
                2, // two entries should overlap
                SNAPSHOT_CHECK_INTERVAL,
                10, // send 10 log entries out in every append entries
                RaftConstants.RPC_TIMEOUT,
                SNAPSHOT_CHECK_INTERVAL * 10, // since I'm not interested in checking the Raft stuff, set the min election timeout really high
                0, // don't want any additional time to be added to the min timeout (makes reasoning about tests easier)
                RaftConstants.HEARTBEAT_INTERVAL,
                RaftConstants.TIME_UNIT);

        algorithm.initialize();
        algorithm.start();
    }

    @After
    public void teardown() {
        algorithm.stop();
    }

    //================================================================================================================//
    //
    // Create Snapshot Writer Tests
    //
    //================================================================================================================//

    // FIXME (AG): how do I check what happens when calling the listener throws an exception?

    @Test
    public void shouldCallWriteSnapshotOnListenerWhenSnapshotTimeoutOccursAndMinimumNumberOfLogEntriesHaveBeenGeneratedAndCommitted() throws StorageException {
        final LogEntry[] entries = new LogEntry[] {
                SENTINEL(),
                NOOP(1, 1),
                NOOP(1, 2),
                CLIENT(1, 3, new UnitTestCommand()),
                NOOP(1, 4),
                NOOP(1, 5)  // <------ we've committed up to here (we've committed exactly the minimum number required)
        };
        insertIntoLog(log, entries);

        // set the current term
        long currentTerm = 1;
        store.setCurrentTerm(currentTerm);

        // set the committed index
        long commitIndex = 5;
        store.setCommitIndex(commitIndex);

        //
        // situation is as follows:
        //
        //                            +----------- COMMITTED
        //                            V
        //  ------------------------------
        // | SN | 01 | 02 | 03 | 04 | 05 | (LOG)
        // ------------------------------
        //

        // check that we've scheduled a timeout
        Timer.TimeoutHandle snapshotTimeoutHandle0 = algorithm.getSnapshotTimeoutHandleForUnitTestsOnly();
        assertThat(snapshotTimeoutHandle0, notNullValue());

        // alright - let's move up to the snapshot check time
        timer.fastForwardTillTaskExecutes(snapshotTimeoutHandle0);

        // check that we asked for a snapshot to be created
        verify(listener).writeSnapshot(argThat(isValidInitialSnapshotWriter()));

        // check that we rescheduled the snapshot timeout
        assertThatSnapshotRescheduled(snapshotTimeoutHandle0);

        // check that we didn't change our internal state
        assertThatLogCurrentTermAndCommitIndexHaveValues(entries, currentTerm, commitIndex);
    }

    @Test
    public void shouldCallWriteSnapshotOnListenerWhenSnapshotTimeoutOccursAndEnoughLogEntriesHaveBeenGeneratedAndCommitted() throws StorageException {
        // we have a log that contains entries from index 0 onwards
        final LogEntry[] entries = new LogEntry[] {
                SENTINEL(),
                NOOP(1, 1),
                NOOP(1, 2),
                CLIENT(1, 3, new UnitTestCommand()),
                NOOP(1, 4),
                NOOP(1, 5),
                CLIENT(3, 6, new UnitTestCommand()),
                CLIENT(3, 7, new UnitTestCommand()),
                NOOP(3, 8),
                NOOP(3, 9),
                NOOP(3, 10),
                CLIENT(4, 11, new UnitTestCommand()),  // <------ we've committed up to here (well more than the minimum)
                CLIENT(4, 12, new UnitTestCommand()),
                CLIENT(4, 13, new UnitTestCommand()),
                CLIENT(4, 14, new UnitTestCommand())
        };
        insertIntoLog(log, entries);

        // set the current term
        long currentTerm = 5;
        store.setCurrentTerm(currentTerm);

        // set the committed index
        long commitIndex = 11;
        store.setCommitIndex(commitIndex);

        //
        // situation is as follows:
        //
        //                                                           +----------- COMMITTED
        //                                                           V
        //  ---------------------------------------------------------------------------
        // | SN | 01 | 02 | 03 | 04 | 05 | 06 | 07 | 08 | 09 | 10 | 11 | 12 | 13 | 14 | (LOG)
        // ---------------------------------------------------------------------------
        //

        // check that we've scheduled a timeout
        Timer.TimeoutHandle snapshotTimeoutHandle0 = algorithm.getSnapshotTimeoutHandleForUnitTestsOnly();
        assertThat(snapshotTimeoutHandle0, notNullValue());

        // alright - let's move up to the snapshot check time
        timer.fastForwardTillTaskExecutes(snapshotTimeoutHandle0);

        // check that we asked for a snapshot to be created
        verify(listener).writeSnapshot(argThat(isValidInitialSnapshotWriter()));

        // check that we rescheduled the snapshot timeout
        assertThatSnapshotRescheduled(snapshotTimeoutHandle0);

        // check that we didn't change our internal state
        assertThatLogCurrentTermAndCommitIndexHaveValues(entries, currentTerm, commitIndex);
    }

    @Test
    public void shouldCallWriteSnapshotOnListenerWhenSnapshotTimeoutOccursAndSnapshotExistsAndEnoughLogEntriesHaveBeenGeneratedAndCommitted() throws StorageException {
        // we have a snapshot that contains data to index 6 (inclusive)
        storeSnapshot(1, 6);

        // we have a log that contains entries from index 3 onwards
        final LogEntry[] entries = new LogEntry[] {
                CLIENT(1, 3, new UnitTestCommand()),
                NOOP(1, 4),
                NOOP(1, 5),
                CLIENT(3, 6, new UnitTestCommand()),
                CLIENT(3, 7, new UnitTestCommand()),
                NOOP(3, 8),
                NOOP(3, 9),
                NOOP(3, 10),
                CLIENT(4, 11, new UnitTestCommand()),  // <------ we've committed up to here (we've committed exactly the minimum number required)
                CLIENT(4, 12, new UnitTestCommand()),
                CLIENT(4, 13, new UnitTestCommand()),
                CLIENT(4, 14, new UnitTestCommand())
        };
        clearLog(log); // clear out the log completely - we don't even want the sentinel
        insertIntoLog(log, entries);

        // set the current term
        long currentTerm = 4;
        store.setCurrentTerm(currentTerm);

        // set the committed index
        long commitIndex = 11;
        store.setCommitIndex(commitIndex);

        //
        // situation is as follows:
        //
        //                                                         +----------- COMMITTED
        //                                                         V
        //               ------------------------------------------------------------
        //  .. EMPTY .. | 03 | 04 | 05 | 06 | 07 | 08 | 09 | 10 | 11 | 12 | 13 | 14 | (LOG)
        //              ------------------------------------------------------------
        //  --------------------------------
        // |       LAST APPLIED = 6        | (SNAPSHOT)
        // --------------------------------
        //

        // check that we've scheduled a timeout
        Timer.TimeoutHandle snapshotTimeoutHandle0 = algorithm.getSnapshotTimeoutHandleForUnitTestsOnly();
        assertThat(snapshotTimeoutHandle0, notNullValue());

        // alright - let's move up to the snapshot check time
        timer.fastForwardTillTaskExecutes(snapshotTimeoutHandle0);

        // check that we asked for a snapshot to be created
        verify(listener).writeSnapshot(argThat(isValidInitialSnapshotWriter()));

        // check that we rescheduled the snapshot timeout
        assertThatSnapshotRescheduled(snapshotTimeoutHandle0);

        // check that we didn't change our internal state
        assertThatLogCurrentTermAndCommitIndexHaveValues(entries, currentTerm, commitIndex);
    }

    @Test
    public void shouldCallWriteSnapshotRepeatedlyOnListenerWhenSnapshotTimeoutOccursAndSnapshotExistsAndEnoughLogEntriesHaveBeenGeneratedAndCommitted() throws StorageException {
        // we have a snapshot that contains data to index 6 (inclusive)
        storeSnapshot(1, 6);

        // we have a log that contains entries from index 3 onwards
        final LogEntry[] entries = new LogEntry[] {
                CLIENT(1, 3, new UnitTestCommand()),
                NOOP(1, 4),
                NOOP(1, 5),
                CLIENT(3, 6, new UnitTestCommand()),
                CLIENT(3, 7, new UnitTestCommand()),
                NOOP(3, 8),
                NOOP(3, 9),
                NOOP(3, 10),
                CLIENT(4, 11, new UnitTestCommand()),  // <------ we've committed up to here (we've committed exactly the minimum number required)
                CLIENT(4, 12, new UnitTestCommand()),
                CLIENT(4, 13, new UnitTestCommand()),
                CLIENT(4, 14, new UnitTestCommand())
        };
        clearLog(log); // clear out the log completely - we don't even want the sentinel
        insertIntoLog(log, entries);

        // set the current term
        long currentTerm = 4;
        store.setCurrentTerm(currentTerm);

        // set the committed index
        long commitIndex = 11;
        store.setCommitIndex(commitIndex);

        //
        // situation is as follows:
        //
        //                                                         +----------- COMMITTED
        //                                                         V
        //               ------------------------------------------------------------
        //  .. EMPTY .. | 03 | 04 | 05 | 06 | 07 | 08 | 09 | 10 | 11 | 12 | 13 | 14 | (LOG)
        //              ------------------------------------------------------------
        //  --------------------------------
        // |       LAST APPLIED = 6        | (SNAPSHOT)
        // --------------------------------
        //

        // --- STEP 1

        // check that we've scheduled a timeout
        Timer.TimeoutHandle snapshotTimeoutHandle0 = algorithm.getSnapshotTimeoutHandleForUnitTestsOnly();
        assertThat(snapshotTimeoutHandle0, notNullValue());

        // alright - let's move up to the snapshot check time
        timer.fastForwardTillTaskExecutes(snapshotTimeoutHandle0);

        // check that we rescheduled the snapshot timeout
        assertThatSnapshotRescheduled(snapshotTimeoutHandle0);

        // check that we didn't change our internal state
        assertThatLogCurrentTermAndCommitIndexHaveValues(entries, currentTerm, commitIndex);

        // --- STEP 2
        //     the listener did not snapshot, so we'll try again

        // get the second snapshot timeout handle
        Timer.TimeoutHandle snapshotTimeoutHandle1 = algorithm.getSnapshotTimeoutHandleForUnitTestsOnly();
        assertThat(snapshotTimeoutHandle1, notNullValue());

        // alright - let's move up to the snapshot check time
        timer.fastForwardTillTaskExecutes(snapshotTimeoutHandle1);

        // check that we rescheduled the snapshot timeout
        assertThatSnapshotRescheduled(snapshotTimeoutHandle1);

        // check that we didn't change our internal state
        assertThatLogCurrentTermAndCommitIndexHaveValues(entries, currentTerm, commitIndex);

        // check that we asked for a snapshot to be created twice
        verify(listener, times(2)).writeSnapshot(argThat(isValidInitialSnapshotWriter()));
    }

    @Test
    public void shouldMakeCorrectSequenceOfWriteSnapshotCallsFromFreshBootOnListenerWhenSnapshotTimeoutOccurs() throws StorageException {
        // we start off with the following log
        final LogEntry[] entries = new LogEntry[] {
                SENTINEL(),
                NOOP(1, 1),
                NOOP(1, 2),
                CLIENT(1, 3, new UnitTestCommand()),
                NOOP(1, 4),
                NOOP(1, 5),
                CLIENT(3, 6, new UnitTestCommand()),
                CLIENT(3, 7, new UnitTestCommand()),
                NOOP(3, 8),
                NOOP(3, 9),
                NOOP(3, 10),
                CLIENT(4, 11, new UnitTestCommand()),  // <------ we've committed up to here
                CLIENT(4, 12, new UnitTestCommand()),
                CLIENT(4, 13, new UnitTestCommand()),
                CLIENT(4, 14, new UnitTestCommand())
        };
        insertIntoLog(log, entries);

        // set the current term
        long currentTerm = 5;
        store.setCurrentTerm(currentTerm);

        // set the committed index
        long commitIndex = 11;
        store.setCommitIndex(commitIndex);

        //
        // situation is as follows:
        //
        //                                                           +----------- COMMITTED
        //                                                           V
        //  ---------------------------------------------------------------------------
        // | SN | 01 | 02 | 03 | 04 | 05 | 06 | 07 | 08 | 09 | 10 | 11 | 12 | 13 | 14 | (LOG)
        // ---------------------------------------------------------------------------
        //

        // --- STEP 1
        //     we have no snapshots
        //     committed index = 11
        //     we have enough entries to request a snapshot

        // check that we've scheduled a timeout
        Timer.TimeoutHandle snapshotTimeoutHandle0 = algorithm.getSnapshotTimeoutHandleForUnitTestsOnly();
        assertThat(snapshotTimeoutHandle0, notNullValue());

        // alright - let's move up to the snapshot check time
        timer.fastForwardTillTaskExecutes(snapshotTimeoutHandle0);

        // check that we rescheduled the snapshot timeout
        assertThatSnapshotRescheduled(snapshotTimeoutHandle0);

        // check that we didn't change our internal state
        assertThatLogCurrentTermAndCommitIndexHaveValues(entries, currentTerm, commitIndex);

        // pretend that we wrote a snapshot up till index 5
        storeSnapshot(1, 5);

        // --- STEP 2
        //     for some reason the listener only created a snapshot up to 5
        //     committed index = 11
        //     we have enough entries to request a snapshot, so we'll try again

        // get the second snapshot timeout handle
        Timer.TimeoutHandle snapshotTimeoutHandle1 = algorithm.getSnapshotTimeoutHandleForUnitTestsOnly();
        assertThat(snapshotTimeoutHandle1, notNullValue());

        // alright - let's move up to the snapshot check time
        timer.fastForwardTillTaskExecutes(snapshotTimeoutHandle1);

        // check that we rescheduled the snapshot timeout
        assertThatSnapshotRescheduled(snapshotTimeoutHandle1);

        // check that we didn't change our internal state
        assertThatLogCurrentTermAndCommitIndexHaveValues(entries, currentTerm, commitIndex);

        // pretend that we wrote a snapshot up till index 11 now
        storeSnapshot(1, 11);

        // --- STEP 3
        //     our last snapshot contains up to index 11
        //     committed index = 11
        //     we don't have enough entries to create a snapshot, so we're not going to bother

        // get the third snapshot timeout handle
        Timer.TimeoutHandle snapshotTimeoutHandle2 = algorithm.getSnapshotTimeoutHandleForUnitTestsOnly();
        assertThat(snapshotTimeoutHandle2, notNullValue());

        // alright - let's move up to the snapshot check time
        timer.fastForwardTillTaskExecutes(snapshotTimeoutHandle2);

        // check that we rescheduled the snapshot timeout
        assertThatSnapshotRescheduled(snapshotTimeoutHandle2);

        // check that we didn't change our internal state
        assertThatLogCurrentTermAndCommitIndexHaveValues(entries, currentTerm, commitIndex);

        // check that we only asked for a snapshot to be created twice
        verify(listener, times(2)).writeSnapshot(argThat(isValidInitialSnapshotWriter()));
    }

    @Test
    public void shouldMakeCorrectSequenceOfWriteSnapshotCallsOnListenerWhenSnapshotTimeoutOccurs() throws StorageException {
        // first we start off with a snapshot at index 6
        storeSnapshot(1, 6);

        // we have a log that contains entries from index 3 onwards
        final LogEntry[] entries = new LogEntry[] {
                CLIENT(1, 3, new UnitTestCommand()),
                NOOP(1, 4),
                NOOP(1, 5),
                CLIENT(3, 6, new UnitTestCommand()),
                CLIENT(3, 7, new UnitTestCommand()),
                NOOP(3, 8),
                NOOP(3, 9),
                NOOP(3, 10),
                CLIENT(4, 11, new UnitTestCommand()),
                CLIENT(4, 12, new UnitTestCommand()), // <------ we've committed up to here (we've committed just one more than the minimum number we'll snapshot after)
                CLIENT(4, 13, new UnitTestCommand()),
                CLIENT(4, 14, new UnitTestCommand())
        };
        clearLog(log); // clear out the log completely - we don't even want the sentinel
        insertIntoLog(log, entries);

        // set the current term
        long currentTerm = 5;
        store.setCurrentTerm(currentTerm);

        // set the committed index
        long commitIndex = 12;
        store.setCommitIndex(commitIndex);

        //
        // situation is as follows:
        //
        //                                                              +----------- COMMITTED
        //                                                              V
        //               ------------------------------------------------------------
        //  .. EMPTY .. | 03 | 04 | 05 | 06 | 07 | 08 | 09 | 10 | 11 | 12 | 13 | 14 | (LOG)
        //              ------------------------------------------------------------
        //  --------------------------------
        // |       LAST APPLIED = 6        | (SNAPSHOT)
        // --------------------------------
        //

        // --- STEP 1
        //     our last snapshot contains up to index 6
        //     committed index = 12
        //     we have enough entries to request a snapshot

        // check that we've scheduled a timeout
        Timer.TimeoutHandle snapshotTimeoutHandle0 = algorithm.getSnapshotTimeoutHandleForUnitTestsOnly();
        assertThat(snapshotTimeoutHandle0, notNullValue());

        // alright - let's move up to the snapshot check time
        timer.fastForwardTillTaskExecutes(snapshotTimeoutHandle0);

        // check that we rescheduled the snapshot timeout
        assertThatSnapshotRescheduled(snapshotTimeoutHandle0);

        // check that we didn't change our internal state
        assertThatLogCurrentTermAndCommitIndexHaveValues(entries, currentTerm, commitIndex);

        // --- STEP 2
        //     our last snapshot still contains up to index 6 (maybe the listener ignored our request)
        //     committed index = 12
        //     we have enough entries to request a snapshot, so we'll try again

        // get the second snapshot timeout handle
        Timer.TimeoutHandle snapshotTimeoutHandle1 = algorithm.getSnapshotTimeoutHandleForUnitTestsOnly();
        assertThat(snapshotTimeoutHandle1, notNullValue());

        // alright - let's move up to the snapshot check time
        timer.fastForwardTillTaskExecutes(snapshotTimeoutHandle1);

        // check that we rescheduled the snapshot timeout
        assertThatSnapshotRescheduled(snapshotTimeoutHandle1);

        // check that we didn't change our internal state
        assertThatLogCurrentTermAndCommitIndexHaveValues(entries, currentTerm, commitIndex);

        // OK ... now we add a snapshot till index 11
        storeSnapshot(1, 11);

        // --- STEP 3
        //     our last snapshot contains up to index 11 (for whatever reason)
        //     committed index = 12
        //     we don't have enough entries to create a snapshot, so we're not going to bother

        // get the third snapshot timeout handle
        Timer.TimeoutHandle snapshotTimeoutHandle2 = algorithm.getSnapshotTimeoutHandleForUnitTestsOnly();
        assertThat(snapshotTimeoutHandle2, notNullValue());

        // alright - let's move up to the snapshot check time
        timer.fastForwardTillTaskExecutes(snapshotTimeoutHandle2);

        // check that we rescheduled the snapshot timeout
        assertThatSnapshotRescheduled(snapshotTimeoutHandle2);

        // check that we didn't change our internal state
        assertThatLogCurrentTermAndCommitIndexHaveValues(entries, currentTerm, commitIndex);

        // check that we only asked for a snapshot to be created twice
        verify(listener, times(2)).writeSnapshot(argThat(isValidInitialSnapshotWriter()));
    }

    @Test
    public void shouldCallWriteSnapshotOnListenerWhenSnapshotTimeoutOccursAndSnapshotExistsAndEnoughLogEntriesHaveBeenGeneratedAndCommittedNoOverlap() throws StorageException {
        // we have a snapshot that contains data to index 6 (inclusive)
        storeSnapshot(1, 6);

        // we have a log that contains entries from index 3 onwards
        final LogEntry[] entries = new LogEntry[] {
                CLIENT(3, 7, new UnitTestCommand()),
                NOOP(3, 8),
                NOOP(3, 9),
                NOOP(3, 10),
                CLIENT(4, 11, new UnitTestCommand()),  // <------ we've committed up to here (we've committed exactly the minimum number required)
                CLIENT(4, 12, new UnitTestCommand()),
                CLIENT(4, 13, new UnitTestCommand()),
                CLIENT(4, 14, new UnitTestCommand())
        };
        clearLog(log); // clear out the log completely - we don't even want the sentinel
        insertIntoLog(log, entries);

        // set the current term
        long currentTerm = 4;
        store.setCurrentTerm(currentTerm);

        // set the committed index
        long commitIndex = 11;
        store.setCommitIndex(commitIndex);

        //
        // situation is as follows:
        //
        //                                                         +----------- COMMITTED
        //                                                         V
        //                                   ----------------------------------------
        //       ...... EMPTY ......        | 07 | 08 | 09 | 10 | 11 | 12 | 13 | 14 | (LOG)
        //                                  ----------------------------------------
        //  --------------------------------
        // |       LAST APPLIED = 6        | (SNAPSHOT)
        // --------------------------------
        //

        // check that we've scheduled a timeout
        Timer.TimeoutHandle snapshotTimeoutHandle0 = algorithm.getSnapshotTimeoutHandleForUnitTestsOnly();
        assertThat(snapshotTimeoutHandle0, notNullValue());

        // alright - let's move up to the snapshot check time
        timer.fastForwardTillTaskExecutes(snapshotTimeoutHandle0);

        // check that we asked for a snapshot to be created
        verify(listener).writeSnapshot(argThat(isValidInitialSnapshotWriter()));

        // check that we rescheduled the snapshot timeout
        assertThatSnapshotRescheduled(snapshotTimeoutHandle0);

        // check that we didn't change our internal state
        assertThatLogCurrentTermAndCommitIndexHaveValues(entries, currentTerm, commitIndex);
    }

    @Test
    public void shouldNotCallWriteSnapshotOnListenerIfLogIsEmpty() throws StorageException {
        // we have no snapshots
        // the starting log (with a sentinel) is enough

        // check that we've scheduled a timeout
        Timer.TimeoutHandle snapshotTimeoutHandle0 = algorithm.getSnapshotTimeoutHandleForUnitTestsOnly();
        assertThat(snapshotTimeoutHandle0, notNullValue());

        // alright - let's move up to the snapshot check time
        timer.fastForwardTillTaskExecutes(snapshotTimeoutHandle0);

        // check that we _did not ask_ for a snapshot to be created
        verify(listener, times(0)).writeSnapshot(any(SnapshotWriter.class));

        // check that we still reschedule the snapshot timeout
        assertThatSnapshotRescheduled(snapshotTimeoutHandle0);

        // check that we didn't change our internal state
        assertThatLogContainsOnlySentinel(log);
        assertThat(store.getCommitIndex(), equalTo(0L));
    }

    @Test
    public void shouldNotCallWriteSnapshotOnListenerIfOnlySnapshotExists() throws StorageException {
        // we have a snapshot that contains data to index 8 (inclusive)
        storeSnapshot(1, 8);

        // set the current term
        long currentTerm = 1;
        store.setCurrentTerm(currentTerm);

        // set the commit index to 8 (because a snapshot can only be done to commit index)
        long commitIndex = 8;
        store.setCommitIndex(commitIndex);

        // we have an empty log
        clearLog(log);

        //
        // situation is as follows:
        //
        //  ------------------------------------------
        // |            LAST APPLIED = 8             | (SNAPSHOT)
        // ------------------------------------------
        //

        // check that we've scheduled a timeout
        Timer.TimeoutHandle snapshotTimeoutHandle0 = algorithm.getSnapshotTimeoutHandleForUnitTestsOnly();
        assertThat(snapshotTimeoutHandle0, notNullValue());

        // alright - let's move up to the snapshot check time
        timer.fastForwardTillTaskExecutes(snapshotTimeoutHandle0);

        // check that we _did not ask_ for a snapshot to be created
        verify(listener, times(0)).writeSnapshot(any(SnapshotWriter.class));

        // check that we still reschedule the snapshot timeout
        assertThatSnapshotRescheduled(snapshotTimeoutHandle0);

        // check that we didn't change our internal state
        assertThatLogIsEmpty(log);
        assertThat(store.getCommitIndex(), equalTo(commitIndex));
    }

    @Test
    public void shouldNotCallWriteSnapshotOnListenerWhenSnapshotTimeoutOccursAndNotEnoughLogEntriesHaveBeenGenerated() throws StorageException {
        // have the snapshot store return nothing
        // we only have the following log
        final LogEntry[] entries = new LogEntry[] {
                SENTINEL(),
                CLIENT(1, 1, new UnitTestCommand()),
                NOOP(1, 2),
                CLIENT(1, 3, new UnitTestCommand()),
                NOOP(1, 4), // <------ we've committed up to here
        };
        insertIntoLog(log, entries);

        // set the current term
        long currentTerm = 2;
        store.setCurrentTerm(currentTerm);

        // set the committed index
        long commitIndex = 4;
        store.setCommitIndex(commitIndex);

        //
        // situation is as follows:
        //
        //                        +----------- COMMITTED
        //                        V
        //  --------------------------
        //  | SN | 01 | 02 | 03 | 04 | (LOG)
        //  --------------------------
        //

        // check that we've scheduled a timeout
        Timer.TimeoutHandle snapshotTimeoutHandle0 = algorithm.getSnapshotTimeoutHandleForUnitTestsOnly();
        assertThat(snapshotTimeoutHandle0, notNullValue());

        // alright - let's move up to the snapshot check time
        timer.fastForwardTillTaskExecutes(snapshotTimeoutHandle0);

        // check that we _did not ask_ for a snapshot to be created
        // because we don't count the SENTINEL as an entry than can be committed, we only have 4, not 5 entries)
        verify(listener, times(0)).writeSnapshot(any(SnapshotWriter.class));

        // check that we still reschedule the snapshot timeout
        assertThatSnapshotRescheduled(snapshotTimeoutHandle0);

        // check that we didn't change our internal state
        assertThatLogCurrentTermAndCommitIndexHaveValues(entries, currentTerm, commitIndex);
    }

    @Test
    public void shouldNotCallWriteSnapshotOnListenerWhenSnapshotTimeoutOccursAndSnapshotExistsAndNotEnoughLogEntriesHaveBeenGenerated() throws StorageException {
        // we have a snapshot that contains data to index 8 (inclusive)
        storeSnapshot(1, 8);

        // we have a log that contains entries from index 3 onwards
        final LogEntry[] entries = new LogEntry[] {
                CLIENT(1, 3, new UnitTestCommand()),
                NOOP(1, 4),
                NOOP(1, 5),
                CLIENT(3, 6, new UnitTestCommand()),
                CLIENT(3, 7, new UnitTestCommand()),
                NOOP(3, 8),
                NOOP(3, 9),
                NOOP(3, 10),
                CLIENT(4, 11, new UnitTestCommand()),  // <------ we've committed up to here
        };
        clearLog(log); // clear out the log completely - we don't even want the sentinel
        insertIntoLog(log, entries);

        // set the current term
        long currentTerm = 4;
        store.setCurrentTerm(currentTerm);

        // set the committed index
        long commitIndex = 11;
        store.setCommitIndex(commitIndex);

        //
        // situation is as follows:
        //
        //                                                         +----------- COMMITTED
        //                                                         V
        //               ---------------------------------------------
        //  .. EMPTY .. | 03 | 04 | 05 | 06 | 07 | 08 | 09 | 10 | 11 | (LOG)
        //              ---------------------------------------------
        //  ------------------------------------------
        // |            LAST APPLIED = 8             | (SNAPSHOT)
        // ------------------------------------------
        //

        // check that we've scheduled a timeout
        Timer.TimeoutHandle snapshotTimeoutHandle0 = algorithm.getSnapshotTimeoutHandleForUnitTestsOnly();
        assertThat(snapshotTimeoutHandle0, notNullValue());

        // alright - let's move up to the snapshot check time
        timer.fastForwardTillTaskExecutes(snapshotTimeoutHandle0);

        // check that we _did not ask_ for a snapshot to be created
        // although the log has enough entries in total what we're checking for are how many entries
        // have been generated after the last applied entry in the snapshot
        verify(listener, times(0)).writeSnapshot(any(SnapshotWriter.class));

        // check that we still reschedule the snapshot timeout
        assertThatSnapshotRescheduled(snapshotTimeoutHandle0);

        // check that we didn't change our internal state
        assertThatLogCurrentTermAndCommitIndexHaveValues(entries, currentTerm, commitIndex);
    }

    @Test
    public void shouldNotCallWriteSnapshotOnListenerWhenSnapshotTimeoutOccursAndNotEnoughLogEntriesHaveBeenGeneratedNoOverlap() throws StorageException {
        // we have a snapshot that contains data to index 8 (inclusive)
        storeSnapshot(1, 8);

        // we have a log that contains entries from index 3 onwards
        final LogEntry[] entries = new LogEntry[] {
                NOOP(3, 9),
                NOOP(3, 10),
                CLIENT(4, 11, new UnitTestCommand()),  // <------ we've committed up to here
        };
        clearLog(log); // clear out the log completely - we don't even want the sentinel
        insertIntoLog(log, entries);

        // set the current term
        long currentTerm = 4;
        store.setCurrentTerm(currentTerm);

        // set the committed index
        long commitIndex = 11;
        store.setCommitIndex(commitIndex);

        //
        // situation is as follows:
        //
        //                                                         +----------- COMMITTED
        //                                                         V
        //                                             ---------------
        //             ...... EMPTY ......            | 09 | 10 | 11 | (LOG)
        //                                            ---------------
        //  ------------------------------------------
        // |            LAST APPLIED = 8             | (SNAPSHOT)
        // ------------------------------------------
        //

        // check that we've scheduled a timeout
        Timer.TimeoutHandle snapshotTimeoutHandle0 = algorithm.getSnapshotTimeoutHandleForUnitTestsOnly();
        assertThat(snapshotTimeoutHandle0, notNullValue());

        // alright - let's move up to the snapshot check time
        timer.fastForwardTillTaskExecutes(snapshotTimeoutHandle0);

        // check that we _did not ask_ for a snapshot to be created
        verify(listener, times(0)).writeSnapshot(any(SnapshotWriter.class));

        // check that we still reschedule the snapshot timeout
        assertThatSnapshotRescheduled(snapshotTimeoutHandle0);

        // check that we didn't change our internal state
        assertThatLogCurrentTermAndCommitIndexHaveValues(entries, currentTerm, commitIndex);
    }

    @Test
    public void shouldNotCallWriteSnapshotOnListenerWhenSnapshotTimeoutOccursAndNotEnoughLogEntriesHaveBeenCommitted() throws StorageException {
        // have the snapshot store return nothing
        // we only have the following log
        // we've generated a lot of entries, but simply not committed enough
        final LogEntry[] entries = new LogEntry[] {
                SENTINEL(),
                CLIENT(1, 1, new UnitTestCommand()),
                NOOP(1, 2),
                CLIENT(1, 3, new UnitTestCommand()),
                NOOP(1, 4), // <------ we've committed up to here
                CLIENT(1, 5, new UnitTestCommand()),
                CLIENT(1, 6, new UnitTestCommand()),
                NOOP(1, 7),
                NOOP(1, 8),
        };
        insertIntoLog(log, entries);

        // set the current term
        long currentTerm = 1;
        store.setCurrentTerm(currentTerm);

        // set the committed index
        long commitIndex = 4;
        store.setCommitIndex(commitIndex);

        //
        // situation is as follows:
        //
        //                        +----------- COMMITTED
        //                        V
        //  ----------------------------------------------
        //  | SN | 01 | 02 | 03 | 04 | 05 | 06 | 07 | 08 | (LOG)
        //  ---------------------------------------------
        //

        // check that we've scheduled a timeout
        Timer.TimeoutHandle snapshotTimeoutHandle0 = algorithm.getSnapshotTimeoutHandleForUnitTestsOnly();
        assertThat(snapshotTimeoutHandle0, notNullValue());

        // alright - let's move up to the snapshot check time
        timer.fastForwardTillTaskExecutes(snapshotTimeoutHandle0);

        // check that we _did not ask_ for a snapshot to be created
        // because we don't count the SENTINEL, we simply don't have enough entries that have been committed
        verify(listener, times(0)).writeSnapshot(any(SnapshotWriter.class));

        // check that we still reschedule the snapshot timeout though
        assertThatSnapshotRescheduled(snapshotTimeoutHandle0);

        // check that we didn't change our internal state
        assertThatLogCurrentTermAndCommitIndexHaveValues(entries, currentTerm, commitIndex);
    }

    @Test
    public void shouldNotCallWriteSnapshotOnListenerWhenSnapshotTimeoutOccursAndSnapshotExistsAndNotEnoughLogEntriesHaveBeenCommitted() throws StorageException {
        // we have a snapshot that contains data to index 6 (inclusive)
        storeSnapshot(1, 6);

        // we have a log that contains entries from index 3 onwards
        final LogEntry[] entries = new LogEntry[] {
                CLIENT(1, 3, new UnitTestCommand()),
                NOOP(1, 4),
                NOOP(1, 5),
                CLIENT(3, 6, new UnitTestCommand()),
                CLIENT(3, 7, new UnitTestCommand()), // <------ we've committed up to here
                NOOP(3, 8),
                NOOP(3, 9),
                NOOP(3, 10),
                CLIENT(4, 11, new UnitTestCommand()),
                CLIENT(4, 12, new UnitTestCommand()),
                CLIENT(4, 13, new UnitTestCommand()),
                CLIENT(4, 14, new UnitTestCommand())
        };
        clearLog(log); // clear out the log completely - we don't even want the sentinel
        insertIntoLog(log, entries);

        // set the current term
        long currentTerm = 5;
        store.setCurrentTerm(currentTerm);

        // set the committed index
        long commitIndex = 7;
        store.setCommitIndex(commitIndex);

        //
        // situation is as follows:
        //
        //                                     +----------- COMMITTED
        //                                     V
        //               ------------------------------------------------------------
        //  .. EMPTY .. | 03 | 04 | 05 | 06 | 07 | 08 | 09 | 10 | 11 | 12 | 13 | 14 | (LOG)
        //              ------------------------------------------------------------
        //  --------------------------------
        // |       LAST APPLIED = 6        | (SNAPSHOT)
        // --------------------------------
        //

        // check that we've scheduled a timeout
        Timer.TimeoutHandle snapshotTimeoutHandle0 = algorithm.getSnapshotTimeoutHandleForUnitTestsOnly();
        assertThat(snapshotTimeoutHandle0, notNullValue());

        // alright - let's move up to the snapshot check time
        timer.fastForwardTillTaskExecutes(snapshotTimeoutHandle0);

        // check that we _did not ask_ for a snapshot to be created
        // although the log has enough entries in total what we're checking for are how many entries
        // have been _generated and committed_ after the last applied entry in the snapshot
        verify(listener, times(0)).writeSnapshot(any(SnapshotWriter.class));

        // check that we rescheduled the snapshot timeout
        assertThatSnapshotRescheduled(snapshotTimeoutHandle0);

        // check that we didn't change our internal state
        assertThatLogCurrentTermAndCommitIndexHaveValues(entries, currentTerm, commitIndex);
    }

        @Test
    public void shouldNotCallWriteSnapshotOnListenerWhenSnapshotTimeoutOccursAndSnapshotExistsAndNotEnoughLogEntriesHaveBeenCommittedNoOverlap() throws StorageException {
        // we have a snapshot that contains data to index 6 (inclusive)
        storeSnapshot(1, 6);

        // we have a log that contains entries from index 3 onwards
        final LogEntry[] entries = new LogEntry[] {
                CLIENT(3, 7, new UnitTestCommand()), // <------ we've committed up to here
                NOOP(3, 8),
                NOOP(3, 9),
                NOOP(3, 10),
                CLIENT(4, 11, new UnitTestCommand()),
                CLIENT(4, 12, new UnitTestCommand()),
                CLIENT(4, 13, new UnitTestCommand()),
                CLIENT(4, 14, new UnitTestCommand())
        };
        clearLog(log); // clear out the log completely - we don't even want the sentinel
        insertIntoLog(log, entries);

        // set the current term
        long currentTerm = 5;
        store.setCurrentTerm(currentTerm);

        // set the committed index
        long commitIndex = 7;
        store.setCommitIndex(commitIndex);

        //
        // situation is as follows:
        //
        //                                     +----------- COMMITTED
        //                                     V
        //                                   ----------------------------------------
        //       ...... EMPTY ......        | 07 | 08 | 09 | 10 | 11 | 12 | 13 | 14 | (LOG)
        //                                  ----------------------------------------
        //  --------------------------------
        // |       LAST APPLIED = 6        | (SNAPSHOT)
        // --------------------------------
        //

        // check that we've scheduled a timeout
        Timer.TimeoutHandle snapshotTimeoutHandle0 = algorithm.getSnapshotTimeoutHandleForUnitTestsOnly();
        assertThat(snapshotTimeoutHandle0, notNullValue());

        // alright - let's move up to the snapshot check time
        timer.fastForwardTillTaskExecutes(snapshotTimeoutHandle0);

        // check that we _did not ask_ for a snapshot to be created
        verify(listener, times(0)).writeSnapshot(any(SnapshotWriter.class));

        // check that we rescheduled the snapshot timeout
        assertThatSnapshotRescheduled(snapshotTimeoutHandle0);

        // check that we didn't change our internal state
        assertThatLogCurrentTermAndCommitIndexHaveValues(entries, currentTerm, commitIndex);
    }

    @Test
    public void shouldNotScheduleSnapshotTimeoutAfterAlgorithmStopped() throws StorageException {
        // have the snapshot store return nothing
        // we start off with a log that has enough generated and committed entries
        final LogEntry[] entries = new LogEntry[] {
                SENTINEL(),
                NOOP(1, 1),
                NOOP(1, 2),
                CLIENT(1, 3, new UnitTestCommand()),
                NOOP(1, 4),
                NOOP(1, 5),
                CLIENT(3, 6, new UnitTestCommand()),
                CLIENT(3, 7, new UnitTestCommand()),
                NOOP(3, 8),
                NOOP(3, 9),
                NOOP(3, 10),
                CLIENT(4, 11, new UnitTestCommand()),  // <------ we've committed up to here (well more than the minimum)
                CLIENT(4, 12, new UnitTestCommand()),
                CLIENT(4, 13, new UnitTestCommand()),
                CLIENT(4, 14, new UnitTestCommand())
        };
        insertIntoLog(log, entries);

        // set the current term
        long currentTerm = 5;
        store.setCurrentTerm(currentTerm);

        // set the committed index
        long commitIndex = 11;
        store.setCommitIndex(commitIndex);

        //
        // situation is as follows:
        //
        //                                                           +----------- COMMITTED
        //                                                           V
        //  ---------------------------------------------------------------------------
        // | SN | 01 | 02 | 03 | 04 | 05 | 06 | 07 | 08 | 09 | 10 | 11 | 12 | 13 | 14 | (LOG)
        // ---------------------------------------------------------------------------
        //

        // --- STEP 1
        //     ask for a snapshot to be created

        // check that we've scheduled a timeout
        Timer.TimeoutHandle snapshotTimeoutHandle0 = algorithm.getSnapshotTimeoutHandleForUnitTestsOnly();
        assertThat(snapshotTimeoutHandle0, notNullValue());

        // alright - let's move up to the snapshot check time
        timer.fastForwardTillTaskExecutes(snapshotTimeoutHandle0);

        // check that we rescheduled the snapshot timeout
        assertThatSnapshotRescheduled(snapshotTimeoutHandle0);

        // check that we didn't change our internal state
        assertThatLogCurrentTermAndCommitIndexHaveValues(entries, currentTerm, commitIndex);

        // --- STEP 2
        //     turn off the algorithm

        // grab the snapshot timeout handle
        Timer.TimeoutHandle snapshotTimeoutHandle1 = algorithm.getSnapshotTimeoutHandleForUnitTestsOnly();
        assertThat(snapshotTimeoutHandle1, notNullValue());

        // now, stop the algorithm
        algorithm.stop();

        // --- STEP 3
        //     execute the snapshot timeout and check if it runs!

        // attempt to execute the task
        timer.fastForwardTillTaskExecutes(snapshotTimeoutHandle1);

        // check that we _did not_ reschedule the timeout
        assertThat(algorithm.getSnapshotTimeoutHandleForUnitTestsOnly(), nullValue());

        // check that we asked we only asked for the snapshot to be created once (first time the snapshot timeout ran)
        verify(listener, times(1)).writeSnapshot(argThat(isValidInitialSnapshotWriter()));

        // check that we didn't change our internal state
        assertThatLogCurrentTermAndCommitIndexHaveValues(entries, currentTerm, commitIndex);
    }

    private void assertThatSnapshotRescheduled(Timer.TimeoutHandle previousSnapshotTimeoutHandle) {
        Timer.TimeoutHandle snapshotTimeoutHandle1 = algorithm.getSnapshotTimeoutHandleForUnitTestsOnly();
        assertThat(timer.getTickForHandle(snapshotTimeoutHandle1), equalTo(timer.getTickForHandle(previousSnapshotTimeoutHandle) + SNAPSHOT_CHECK_INTERVAL));
    }

    //================================================================================================================//
    //
    // Snapshot Written Tests
    //
    //================================================================================================================//

    // TODO (AG): check that we attempt to truncate as many log entries as exist, not just the minimum

    @Test
    public void shouldThrowIllegalArgumentExceptionIfSnapshotWriterContainsInvalidIndex() throws StorageException {
        // set the log
        final LogEntry[] entries = new LogEntry[] {
                SENTINEL(),
                NOOP(1, 1), // <----- we've committed up to here
        };
        insertIntoLog(log, entries);

        // set the current term
        long currentTerm = 3;
        store.setCurrentTerm(currentTerm);

        // and the commit index
        long commitIndex = 1;
        store.setCommitIndex(commitIndex);

        // pretend that the caller handled the request, but set something invalid
        UnitTestTempFileSnapshotWriter snapshotWriter = snapshotsStore.newSnapshotWriter();
        snapshotWriter.setIndex(-1);

        boolean exceptionThrown = false;
        try {
            // submit the handled snapshot request
            algorithm.snapshotWritten(snapshotWriter);
        } catch (Exception e) {
            assertThat(e, instanceOf(IllegalArgumentException.class));
            exceptionThrown = true;
        }

        // we threw an exception
        assertThat(exceptionThrown, equalTo(true));

        // we should have nooped
        verify(snapshotsStore, times(0)).storeSnapshot(any(SnapshotsStore.ExtendedSnapshotWriter.class));

        // and not changed internal state
        assertThatLogCurrentTermAndCommitIndexHaveValues(entries, currentTerm, commitIndex);
    }

    @Test
    public void shouldThrowIllegalArgumentExceptionIfSnapshotWriterHasIndexGreaterThanCommitIndex() throws StorageException {
        // set the log
        final LogEntry[] entries = new LogEntry[] {
                SENTINEL(),
                NOOP(1, 1),
                NOOP(1, 2),
                NOOP(1, 3),  // <----- we've committed up to here
                NOOP(1, 4),
                NOOP(1, 5),
                NOOP(1, 6),
                NOOP(1, 7),
        };
        insertIntoLog(log, entries);

        // set the current term
        long currentTerm = 3;
        store.setCurrentTerm(currentTerm);

        // and the commit index
        long commitIndex = 3;
        store.setCommitIndex(commitIndex);

        // pretend that the caller handled the request, but said that they committed more than the commit index but less than the last log index
        UnitTestTempFileSnapshotWriter snapshotWriter = snapshotsStore.newSnapshotWriter();
        snapshotWriter.setIndex(6);

        boolean exceptionThrown = false;
        try {
            // submit the handled snapshot request
            algorithm.snapshotWritten(snapshotWriter);
        } catch (Exception e) {
            assertThat(e, instanceOf(IllegalArgumentException.class));
            exceptionThrown = true;
        }

        // we threw an exception
        assertThat(exceptionThrown, equalTo(true));

        // we should have nooped
        verify(snapshotsStore, times(0)).storeSnapshot(any(SnapshotsStore.ExtendedSnapshotWriter.class));

        // and not changed internal state
        assertThatLogCurrentTermAndCommitIndexHaveValues(entries, currentTerm, commitIndex);
    }

    @Test
    public void shouldThrowIllegalArgumentExceptionIfSnapshotWriterHasIndexGreaterThanCommitIndexAndSnapshotExists() throws StorageException {
        // we have a snapshot that contains data to index 6 (inclusive)
        storeSnapshot(1, 6);

        // we have a log that contains entries from index 7 onwards
        final LogEntry[] entries = new LogEntry[] {
                CLIENT(3, 7, new UnitTestCommand()),
                NOOP(3, 8),
                NOOP(3, 9),
                NOOP(3, 10),
                CLIENT(4, 11, new UnitTestCommand()),  // <------ we've committed up to here (we've committed exactly the minimum number required)
                CLIENT(4, 12, new UnitTestCommand()),
                CLIENT(4, 13, new UnitTestCommand()),
                CLIENT(4, 14, new UnitTestCommand())
        };
        clearLog(log); // clear out the log completely - we don't even want the sentinel
        insertIntoLog(log, entries);

        // set the current term
        long currentTerm = 4;
        store.setCurrentTerm(currentTerm);

        // set the committed index
        long commitIndex = 11;
        store.setCommitIndex(commitIndex);

        //
        // situation is as follows:
        //
        //                                                         +----------- COMMITTED
        //                                                         V
        //                                   ----------------------------------------
        //           .. EMPTY ..            | 07 | 08 | 09 | 10 | 11 | 12 | 13 | 14 | (LOG)
        //                                  ----------------------------------------
        //  --------------------------------
        // |       LAST APPLIED = 6        | (SNAPSHOT)
        // --------------------------------
        //

        // pretend that the caller handled the request, but said that they committed more than the commit index but less than the last log index
        UnitTestTempFileSnapshotWriter snapshotWriter = snapshotsStore.newSnapshotWriter();
        snapshotWriter.setIndex(12);

        boolean exceptionThrown = false;
        try {
            // submit the handled snapshot request
            algorithm.snapshotWritten(snapshotWriter);
        } catch (Exception e) {
            assertThat(e, instanceOf(IllegalArgumentException.class));
            exceptionThrown = true;
        }

        // we threw an exception
        assertThat(exceptionThrown, equalTo(true));

        // we should have nooped
        verify(snapshotsStore, times(0)).storeSnapshot(any(SnapshotsStore.ExtendedSnapshotWriter.class));

        // and not changed internal state
        assertThatLogCurrentTermAndCommitIndexHaveValues(entries, currentTerm, commitIndex);
    }

    @Test
    public void shouldThrowIllegalArgumentExceptionIfSnapshotWriterHasIndexGreaterThanLastLogIndex() throws StorageException {
        // set the log
        final LogEntry[] entries = new LogEntry[] {
                SENTINEL(),
                NOOP(1, 1),
                NOOP(1, 2),
                NOOP(1, 3),  // <----- we've committed up to here
        };
        insertIntoLog(log, entries);

        // set the current term
        long currentTerm = 1;
        store.setCurrentTerm(currentTerm);

        // and the commit index
        long commitIndex = 3;
        store.setCommitIndex(commitIndex);

        // pretend that the caller handled the request, but claimed that they've snapshotted more entries than the log contains!
        UnitTestTempFileSnapshotWriter snapshotWriter = snapshotsStore.newSnapshotWriter();
        snapshotWriter.setIndex(7);

        boolean exceptionThrown = false;
        try {
            // submit the handled snapshot request
            algorithm.snapshotWritten(snapshotWriter);
        } catch (Exception e) {
            assertThat(e, instanceOf(IllegalArgumentException.class));
            exceptionThrown = true;
        }

        // we threw an exception
        assertThat(exceptionThrown, equalTo(true));

        // we should have nooped
        verify(snapshotsStore, times(0)).storeSnapshot(any(SnapshotsStore.ExtendedSnapshotWriter.class));

        // and not changed internal state
        assertThatLogCurrentTermAndCommitIndexHaveValues(entries, currentTerm, commitIndex);
    }

    @Test
    public void shouldNoopIfSnapshotWriterContainsZeroIndex() throws StorageException {
        // set the log
        final LogEntry[] entries = new LogEntry[] {
                SENTINEL(),
                NOOP(1, 1), // <----- we've committed up to here
        };
        insertIntoLog(log, entries);

        // set the current term
        long currentTerm = 5;
        store.setCurrentTerm(currentTerm);

        // and the commit index
        long commitIndex = 1;
        store.setCommitIndex(commitIndex);

        // pretend that the caller handled the request, but had processed nothing
        UnitTestTempFileSnapshotWriter snapshotWriter = snapshotsStore.newSnapshotWriter();
        snapshotWriter.setIndex(0);

        // submit the handled snapshot request
        algorithm.snapshotWritten(snapshotWriter);

        // we should have nooped
        verify(snapshotsStore, times(0)).storeSnapshot(any(SnapshotsStore.ExtendedSnapshotWriter.class));

        // and not changed internal state
        assertThatLogCurrentTermAndCommitIndexHaveValues(entries, currentTerm, commitIndex);
    }

    @Test
    public void shouldNoopIfSnapshotWriterContainsValidIndexButLogIsNullAndSnapshotExists() throws StorageException {
        // we have a snapshot until index 7
        storeSnapshot(3, 7);

        // empty out the log
        clearLog(log);

        // set the current term
        long currentTerm = 5;
        store.setCurrentTerm(currentTerm);

        // and the commit index
        long commitIndex = 7;
        store.setCommitIndex(commitIndex);

        //
        // situation is as follows:
        //
        //                               +----------- COMMITTED
        //           .. EMPTY ..         V
        //  --------------------------------
        // |       LAST APPLIED = 7        | (SNAPSHOT)
        // --------------------------------
        //

        // pretend that the caller handled the request, but they were trying to create the same snapshot again
        UnitTestTempFileSnapshotWriter snapshotWriter = snapshotsStore.newSnapshotWriter();
        snapshotWriter.setIndex(7);

        // submit the handled snapshot request
        algorithm.snapshotWritten(snapshotWriter);

        // we should have nooped
        verify(snapshotsStore, times(0)).storeSnapshot(any(SnapshotsStore.ExtendedSnapshotWriter.class));

        // and not changed internal state
        assertThatLogIsEmpty(log);
        assertThat(store.getCurrentTerm(), equalTo(currentTerm));
        assertThat(store.getCommitIndex(), equalTo(commitIndex));
    }

    @Test
    public void shouldNotAddSnapshotIfSnapshotWriterSubmittedWithoutEnoughLogEntriesForLogOnly() throws StorageException {
        // have the snapshot store return nothing
        // we only have the following log
        final LogEntry[] entries = new LogEntry[]{
                SENTINEL(),
                CLIENT(1, 1, new UnitTestCommand()),
                NOOP(1, 2),
                CLIENT(1, 3, new UnitTestCommand()),
                NOOP(1, 4),
                CLIENT(1, 5, new UnitTestCommand()),
                CLIENT(1, 6, new UnitTestCommand()),
                NOOP(1, 7),  // <------ we've committed up to here
                NOOP(1, 8),
        };
        insertIntoLog(log, entries);

        // set the current term
        long currentTerm = 1;
        store.setCurrentTerm(currentTerm);

        // set the committed index
        long commitIndex = 7;
        store.setCommitIndex(commitIndex);

        //
        // situation is as follows:
        //
        //                                        +----------- COMMITTED
        //                                        V
        //  ----------------------------------------------
        //  | SN | 01 | 02 | 03 | 04 | 05 | 06 | 07 | 08 | (LOG)
        //  ---------------------------------------------
        //                        ^
        //  SNAPSHOT CREATED TO --+

        // pretend that the caller handled the request, but said that they committed less than the minimum number we need to create a snapshot
        UnitTestTempFileSnapshotWriter snapshotWriter = snapshotsStore.newSnapshotWriter();
        snapshotWriter.setIndex(4);

        // submit that snapshot request
        algorithm.snapshotWritten(snapshotWriter);

        // we should have nooped
        verify(snapshotsStore, times(0)).storeSnapshot(any(SnapshotsStore.ExtendedSnapshotWriter.class));

        // and not changed internal state
        assertThatLogCurrentTermAndCommitIndexHaveValues(entries, currentTerm, commitIndex);
    }

    @Test
    public void shouldNotAddSnapshotIfSnapshotWriterSubmittedWithoutEnoughLogEntriesForLogAndSnapshotNoOverlap() throws StorageException {
        // we have a snapshot that contains data to index 6 (inclusive)
        storeSnapshot(1, 6);

        // we have a log that contains entries from index 7 onwards
        final LogEntry[] entries = new LogEntry[] {
                CLIENT(3, 7, new UnitTestCommand()),
                NOOP(3, 8),
                NOOP(3, 9),
                NOOP(3, 10),
                CLIENT(4, 11, new UnitTestCommand()),  // <------ we've committed up to here (we've committed exactly the minimum number required)
                CLIENT(4, 12, new UnitTestCommand()),
                CLIENT(4, 13, new UnitTestCommand()),
                CLIENT(4, 14, new UnitTestCommand())
        };
        clearLog(log); // clear out the log completely - we don't even want the sentinel
        insertIntoLog(log, entries);

        // set the current term
        long currentTerm = 4;
        store.setCurrentTerm(currentTerm);

        // set the committed index
        long commitIndex = 11;
        store.setCommitIndex(commitIndex);

        //
        // situation is as follows:
        //
        //                                                         +----------- COMMITTED
        //                                                         V
        //                                   ----------------------------------------
        //           .. EMPTY ..            | 07 | 08 | 09 | 10 | 11 | 12 | 13 | 14 | (LOG)
        //                                  ----------------------------------------
        //  --------------------------------                 ^
        // |       LAST APPLIED = 6        | (SNAPSHOT)      |
        // --------------------------------                  |
        //                                                   |
        //                  SNAPSHOT CREATED TO -------------+
        //

        // pretend that the caller handled the request, but said that they committed less than the minimum number we need to create a snapshot
        UnitTestTempFileSnapshotWriter snapshotWriter = snapshotsStore.newSnapshotWriter();
        snapshotWriter.setIndex(10);

        // submit that snapshot request
        algorithm.snapshotWritten(snapshotWriter);

        // we should have nooped
        verify(snapshotsStore, times(0)).storeSnapshot(any(SnapshotsStore.ExtendedSnapshotWriter.class));

        // and not changed internal state
        assertThatLogCurrentTermAndCommitIndexHaveValues(entries, currentTerm, commitIndex);
    }

    @Test
    public void shouldNotAddSnapshotIfSnapshotWriterSubmittedWithoutEnoughLogEntriesAndLastAppliedIndexInSnapshotForLogAndSnapshotNoOverlap() throws StorageException {
        // we have a snapshot that contains data to index 6 (inclusive)
        storeSnapshot(1, 6);

        // we have a log that contains entries from index 7 onwards
        final LogEntry[] entries = new LogEntry[] {
                CLIENT(3, 7, new UnitTestCommand()),
                NOOP(3, 8),
                NOOP(3, 9),
                NOOP(3, 10),
                CLIENT(4, 11, new UnitTestCommand()),  // <------ we've committed up to here (we've committed exactly the minimum number required)
                CLIENT(4, 12, new UnitTestCommand()),
                CLIENT(4, 13, new UnitTestCommand()),
                CLIENT(4, 14, new UnitTestCommand())
        };
        clearLog(log); // clear out the log completely - we don't even want the sentinel
        insertIntoLog(log, entries);

        // set the current term
        long currentTerm = 4;
        store.setCurrentTerm(currentTerm);

        // set the committed index
        long commitIndex = 11;
        store.setCommitIndex(commitIndex);

        //
        // situation is as follows:
        //
        //                                                         +----------- COMMITTED
        //                                                         V
        //                                   ----------------------------------------
        //           .. EMPTY ..            | 07 | 08 | 09 | 10 | 11 | 12 | 13 | 14 | (LOG)
        //                                  ----------------------------------------
        //  --------------------------------
        // |       LAST APPLIED = 6        | (SNAPSHOT)
        // --------------------------------
        //                              ^
        //     SNAPSHOT CREATED TO -----+
        //

        // pretend that the caller handled the request
        // they indicate the same index as what created the initial snapshot
        UnitTestTempFileSnapshotWriter snapshotWriter = snapshotsStore.newSnapshotWriter();
        snapshotWriter.setIndex(6);

        // submit that snapshot request
        algorithm.snapshotWritten(snapshotWriter);

        // we should have nooped
        verify(snapshotsStore, times(0)).storeSnapshot(any(SnapshotsStore.ExtendedSnapshotWriter.class));

        // and not changed internal state
        assertThatLogCurrentTermAndCommitIndexHaveValues(entries, currentTerm, commitIndex);
    }

    // we only count entries _after_ the end of the previous snapshot
    // that means that even if there are enough entries from the _beginning of the log_ to the _last committed index_
    // we don't care unless there are enough entries from the _end of the snapshot_ to the _last committed index_
    @Test
    public void shouldNotAddSnapshotIfSnapshotWriterSubmittedWithoutEnoughLogEntriesForLogAndSnapshotWithOverlap() throws StorageException {
        // we have a snapshot that contains data to index 6 (inclusive)
        storeSnapshot(1, 6);

        // we have a log that contains entries from index 3 onwards
        final LogEntry[] entries = new LogEntry[] {
                CLIENT(1, 3, new UnitTestCommand()),
                NOOP(1, 4),
                NOOP(1, 5),
                CLIENT(3, 6, new UnitTestCommand()),
                CLIENT(3, 7, new UnitTestCommand()),
                NOOP(3, 8),
                NOOP(3, 9),
                NOOP(3, 10),
                CLIENT(4, 11, new UnitTestCommand()),  // <------ we've committed up to here (we've committed exactly the minimum number required)
                CLIENT(4, 12, new UnitTestCommand()),
                CLIENT(4, 13, new UnitTestCommand()),
                CLIENT(4, 14, new UnitTestCommand())
        };
        clearLog(log); // clear out the log completely - we don't even want the sentinel
        insertIntoLog(log, entries);

        // set the current term
        long currentTerm = 4;
        store.setCurrentTerm(currentTerm);

        // set the committed index
        long commitIndex = 11;
        store.setCommitIndex(commitIndex);

        //
        // situation is as follows:
        //
        //                                                         +----------- COMMITTED
        //                                                         V
        //               ------------------------------------------------------------
        //  .. EMPTY .. | 03 | 04 | 05 | 06 | 07 | 08 | 09 | 10 | 11 | 12 | 13 | 14 | (LOG)
        //              ------------------------------------------------------------
        //  --------------------------------                 ^
        // |       LAST APPLIED = 6        | (SNAPSHOT)      |
        // --------------------------------                  |
        //                                                   |
        //                  SNAPSHOT CREATED TO -------------+
        //

        // pretend that the caller handled the request
        // if you count from the beginning of the log they have more than enough entries
        // but, what we care about is the number of entries after the end of the last snapshot,
        // which is fewer than the minimum
        UnitTestTempFileSnapshotWriter snapshotWriter = snapshotsStore.newSnapshotWriter();
        snapshotWriter.setIndex(10);

        // submit that snapshot request
        algorithm.snapshotWritten(snapshotWriter);

        // we should have nooped
        verify(snapshotsStore, times(0)).storeSnapshot(any(SnapshotsStore.ExtendedSnapshotWriter.class));

        // and not changed internal state
        assertThatLogCurrentTermAndCommitIndexHaveValues(entries, currentTerm, commitIndex);
    }

    @Test
    public void shouldNotAddSnapshotIfSnapshotWriterSubmittedWithoutEnoughLogEntriesAndLastAppliedIndexInSnapshotForLogAndSnapshotWithOverlap() throws StorageException {
        // we have a snapshot that contains data to index 6 (inclusive)
        storeSnapshot(1, 6);

        // we have a log that contains entries from index 3 onwards
        final LogEntry[] entries = new LogEntry[] {
                CLIENT(1, 3, new UnitTestCommand()),
                NOOP(1, 4),
                NOOP(1, 5),
                CLIENT(3, 6, new UnitTestCommand()),
                CLIENT(3, 7, new UnitTestCommand()),
                NOOP(3, 8),
                NOOP(3, 9),
                NOOP(3, 10),
                CLIENT(4, 11, new UnitTestCommand()),  // <------ we've committed up to here (we've committed exactly the minimum number required)
                CLIENT(4, 12, new UnitTestCommand()),
                CLIENT(4, 13, new UnitTestCommand()),
                CLIENT(4, 14, new UnitTestCommand())
        };
        clearLog(log); // clear out the log completely - we don't even want the sentinel
        insertIntoLog(log, entries);

        // set the current term
        long currentTerm = 5;
        store.setCurrentTerm(currentTerm);

        // set the committed index
        long commitIndex = 11;
        store.setCommitIndex(commitIndex);

        //
        // situation is as follows:
        //
        //                                                         +----------- COMMITTED
        //                                                         V
        //               ------------------------------------------------------------
        //  .. EMPTY .. | 03 | 04 | 05 | 06 | 07 | 08 | 09 | 10 | 11 | 12 | 13 | 14 | (LOG)
        //              ------------------------------------------------------------
        //  --------------------------------
        // |       LAST APPLIED = 6        | (SNAPSHOT)
        // --------------------------------
        //                              ^
        //       SNAPSHOT CREATED TO ---+
        //

        // pretend that the caller handled the request
        UnitTestTempFileSnapshotWriter snapshotWriter = snapshotsStore.newSnapshotWriter();
        snapshotWriter.setIndex(6);

        // submit that snapshot request
        algorithm.snapshotWritten(snapshotWriter);

        // we should have nooped
        verify(snapshotsStore, times(0)).storeSnapshot(any(ExtendedSnapshotWriter.class));

        // and not changed internal state
        assertThatLogCurrentTermAndCommitIndexHaveValues(entries, currentTerm, commitIndex);
    }

    @Test
    public void shouldAddSnapshotIfSnapshotWriterSubmittedWithEnoughLogEntriesForLogOnly() throws StorageException {
        // there are no snapshots
        // we only have the following log
        final LogEntry[] entries = new LogEntry[] {
                SENTINEL(),
                CLIENT(1, 1, new UnitTestCommand()),
                NOOP(1, 2),
                CLIENT(1, 3, new UnitTestCommand()),
                NOOP(1, 4),
                CLIENT(1, 5, new UnitTestCommand()),
                CLIENT(1, 6, new UnitTestCommand()),
                NOOP(1, 7),  // <------ we've committed up to here
                NOOP(1, 8),
        };
        insertIntoLog(log, entries);

        // set the current term
        // ensure that this is _greater_ than the term in the entry that the snapshot is created to!
        // we do this to check that the correct term is used in the setTerm() call in the code
        long currentTerm = 3;
        store.setCurrentTerm(currentTerm);

        // set the committed index
        long commitIndex = 7;
        store.setCommitIndex(commitIndex);

        //
        // situation is as follows:
        //
        //                                        +----------- COMMITTED
        //                                        V
        //  ----------------------------------------------
        //  | SN | 01 | 02 | 03 | 04 | 05 | 06 | 07 | 08 | (LOG)
        //  ---------------------------------------------
        //                                  ^
        //        SNAPSHOT CREATED TO  -----+

        // pretend that the caller handled the request, but said that they committed less than the minimum number we need to create a snapshot
        UnitTestTempFileSnapshotWriter snapshotWriter = snapshotsStore.newSnapshotWriter();
        snapshotWriter.setIndex(6);

        // submit that snapshot request
        algorithm.snapshotWritten(snapshotWriter);

        // we should have have submitted the snapshot to the store
        verify(snapshotsStore, times(1)).storeSnapshot(snapshotWriter);

        // check that the term is set properly
        assertThat(snapshotWriter.getTerm(), equalTo(1L));

        // we've stored a new snapshot
        // and have truncated the log prefix, so now the log should
        // look as follows
        final LogEntry[] truncatedEntries = new LogEntry[] {
                entries[5],
                entries[6],
                entries[7],
                entries[8]
        };
        assertThatLogCurrentTermAndCommitIndexHaveValues(truncatedEntries, currentTerm, commitIndex);
    }

    @Test
    public void shouldAddSnapshotIfSnapshotWriterSubmittedWithEnoughLogEntriesForLogAndSnapshotNoOverlap() throws StorageException {
        // we have a snapshot that contains data to index 6 (inclusive)
        storeSnapshot(1, 6);

        // we have a log that contains entries from index 7 onwards
        final LogEntry[] entries = new LogEntry[] {
                CLIENT(3, 7, new UnitTestCommand()),
                NOOP(3, 8),
                NOOP(3, 9),
                NOOP(3, 10),
                CLIENT(4, 11, new UnitTestCommand()),  // <------ we've committed up to here (we've committed exactly the minimum number required)
                CLIENT(4, 12, new UnitTestCommand()),
                CLIENT(4, 13, new UnitTestCommand()),
                CLIENT(4, 14, new UnitTestCommand())
        };
        clearLog(log); // clear out the log completely - we don't even want the sentinel
        insertIntoLog(log, entries);

        // set the current term
        // ensure that this is _greater_ than the term in the entry that the snapshot is created to!
        // we do this to check that the correct term is used in the setTerm() call in the code
        long currentTerm = 5;
        store.setCurrentTerm(currentTerm);

        // set the committed index
        long commitIndex = 11;
        store.setCommitIndex(commitIndex);

        //
        // situation is as follows:
        //
        //                                                         +----------- COMMITTED
        //                                                         V
        //                                   ----------------------------------------
        //           .. EMPTY ..            | 07 | 08 | 09 | 10 | 11 | 12 | 13 | 14 | (LOG)
        //                                  ----------------------------------------
        //  --------------------------------                      ^
        // |       LAST APPLIED = 6        | (SNAPSHOT)           |
        // --------------------------------                       |
        //                                                        |
        //                    SNAPSHOT CREATED TO ----------------+
        //

        // pretend that the caller handled the request, but said that they committed less than the minimum number we need to create a snapshot
        UnitTestTempFileSnapshotWriter snapshotWriter = snapshotsStore.newSnapshotWriter();
        snapshotWriter.setIndex(11);

        // submit that snapshot request
        algorithm.snapshotWritten(snapshotWriter);

        // we should have submitted that snapshot request
        verify(snapshotsStore, times(1)).storeSnapshot(snapshotWriter);

        // check that the term is set properly
        assertThat(snapshotWriter.getTerm(), equalTo(4L));

        // now that a new snapshot has been added and prefix truncation occurred
        // we have new entries
        final LogEntry[] truncatedEntries = new LogEntry[] {
                entries[3],
                entries[4],
                entries[5],
                entries[6],
                entries[7],
        };
        assertThatLogCurrentTermAndCommitIndexHaveValues(truncatedEntries, currentTerm, commitIndex);
    }

    @Test
    public void shouldAddSnapshotIfSnapshotWriterSubmittedWithEnoughLogEntriesForLogAndSnapshotWithOverlap() throws StorageException {
        // we have a snapshot that contains data to index 6 (inclusive)
        storeSnapshot(1, 6);

        // we have a log that contains entries from index 3 onwards
        final LogEntry[] entries = new LogEntry[] {
                CLIENT(1, 3, new UnitTestCommand()),
                NOOP(1, 4),
                NOOP(1, 5),
                CLIENT(3, 6, new UnitTestCommand()),
                CLIENT(3, 7, new UnitTestCommand()),
                NOOP(3, 8),
                NOOP(3, 9),
                NOOP(3, 10),
                CLIENT(4, 11, new UnitTestCommand()),  // <------ we've committed up to here (we've committed exactly the minimum number required)
                CLIENT(4, 12, new UnitTestCommand()),
                CLIENT(4, 13, new UnitTestCommand()),
                CLIENT(4, 14, new UnitTestCommand())
        };
        clearLog(log); // clear out the log completely - we don't even want the sentinel
        insertIntoLog(log, entries);

        // set the current term
        // ensure that this is _greater_ than the term in the entry that the snapshot is created to!
        // we do this to check that the correct term is used in the setTerm() call in the code
        long currentTerm = 7;
        store.setCurrentTerm(currentTerm);

        // set the committed index
        long commitIndex = 11;
        store.setCommitIndex(commitIndex);

        //
        // situation is as follows:
        //
        //                                                         +----------- COMMITTED
        //                                                         V
        //               ------------------------------------------------------------
        //  .. EMPTY .. | 03 | 04 | 05 | 06 | 07 | 08 | 09 | 10 | 11 | 12 | 13 | 14 | (LOG)
        //              ------------------------------------------------------------
        //  --------------------------------                      ^
        // |       LAST APPLIED = 6        | (SNAPSHOT)           |
        // --------------------------------                       |
        //                                                        |
        //                     SNAPSHOT CREATED TO ---------------+
        //

        // pretend that the caller handled the request
        UnitTestTempFileSnapshotWriter snapshotWriter = snapshotsStore.newSnapshotWriter();
        snapshotWriter.setIndex(11);

        // submit that snapshot request
        algorithm.snapshotWritten(snapshotWriter);

        // we should have submitted that request to the store
        verify(snapshotsStore, times(1)).storeSnapshot(snapshotWriter);

        // and set the term for that snapshot properly
        assertThat(snapshotWriter.getTerm(), equalTo(4L));

        // now that a new snapshot has been added and prefix truncation occurred
        // we have new entries
        final LogEntry[] truncatedEntries = new LogEntry[] {
                entries[7],
                entries[8],
                entries[9],
                entries[10],
                entries[11],
        };
        assertThatLogCurrentTermAndCommitIndexHaveValues(truncatedEntries, currentTerm, commitIndex);
    }

    @Test
    public void shouldHandleRepeatedCallsToSnapshotWrittenWithTheSameLastAppliedIndexCorrectly() throws StorageException {
        // we have a single snapshot
        storeSnapshot(1, 6);

        // we have a log that contains entries from index 3 onwards
        final LogEntry[] entries = new LogEntry[] {
                CLIENT(1, 3, new UnitTestCommand()),
                NOOP(1, 4),
                NOOP(1, 5),
                CLIENT(3, 6, new UnitTestCommand()),
                CLIENT(3, 7, new UnitTestCommand()),
                NOOP(3, 8),
                NOOP(3, 9),
                NOOP(3, 10),
                CLIENT(4, 11, new UnitTestCommand()),  // <------ we've committed up to here (we've committed exactly the minimum number required)
                CLIENT(4, 12, new UnitTestCommand()),
                CLIENT(4, 13, new UnitTestCommand()),
                CLIENT(4, 14, new UnitTestCommand())
        };
        clearLog(log); // clear out the log completely - we don't even want the sentinel
        insertIntoLog(log, entries);

        // set the current term
        // ensure that this is _greater_ than the term in the entry that the snapshot is created to!
        // we do this to check that the correct term is used in the setTerm() call in the code
        long currentTerm = 6;
        store.setCurrentTerm(currentTerm);

        // set the committed index
        long commitIndex = 11;
        store.setCommitIndex(commitIndex);

        //
        // starting situation is as follows:
        //
        //                                                         +----------- COMMITTED
        //                                                         V
        //               ------------------------------------------------------------
        //  .. EMPTY .. | 03 | 04 | 05 | 06 | 07 | 08 | 09 | 10 | 11 | 12 | 13 | 14 | (LOG)
        //              ------------------------------------------------------------
        //  --------------------------------                      ^
        // |       LAST APPLIED = 6        | (SNAPSHOT)           |
        // --------------------------------                       |
        //                                                        |
        //                     SNAPSHOT CREATED TO ---------------+
        //

        // -- CALL 1
        //    the caller should claim that they've created the snapshot

        // pretend that the caller handled the request
        UnitTestTempFileSnapshotWriter snapshotWriter0 = snapshotsStore.newSnapshotWriter();
        snapshotWriter0.setIndex(11);
        algorithm.snapshotWritten(snapshotWriter0);

        // we've stored a new snapshot
        // and have truncated the log prefix, so now the log should
        // look as follows (because we've written up to index 11)
        final LogEntry[] truncatedEntries = new LogEntry[] {
                entries[7],
                entries[8],
                entries[9],
                entries[10],
                entries[11],
        };
        assertThatLogContains(log, truncatedEntries);
        assertThat(store.getCommitIndex(), equalTo(commitIndex));

        //
        // new situation is as follows:
        //
        //                                                         +----------- COMMITTED
        //                                                         V
        //               ------------------------------------------------------------
        //  .. EMPTY .. | 03 | 04 | 05 | 06 | 07 | 08 | 09 | 10 | 11 | 12 | 13 | 14 | (LOG)
        //              ------------------------------------------------------------
        //  ---------------------------------------------------------
        // |                   LAST APPLIED = 11                    | (SNAPSHOT)
        // ---------------------------------------------------------
        //                                                        ^
        //                     SNAPSHOT CREATED TO ---------------+
        //

        // -- CALL 2
        //    act as if the caller created the same snapshot again

        // pretend that the caller handled the second snapshot request
        // and created exactly the same snapshot
        UnitTestTempFileSnapshotWriter snapshotWriter1 = snapshotsStore.newSnapshotWriter();
        snapshotWriter1.setIndex(11);
        algorithm.snapshotWritten(snapshotWriter1);

        //
        // verify
        //

        // we should have submitted only _one_ request to be added to the snapshot store
        ArgumentCaptor<ExtendedSnapshotWriter> snapshotCaptor = ArgumentCaptor.forClass(SnapshotsStore.ExtendedSnapshotWriter.class);
        verify(snapshotsStore, times(1)).storeSnapshot(snapshotCaptor.capture());

        // and it should have the right values
        UnitTestTempFileSnapshotWriter capturedSnapshot = (UnitTestTempFileSnapshotWriter) snapshotCaptor.getValue();
        assertThat(capturedSnapshot.getTerm(), equalTo(4L));
        assertThat(capturedSnapshot.getIndex(), equalTo(11L));

        // and not changed internal state
        assertThatLogCurrentTermAndCommitIndexHaveValues(truncatedEntries, currentTerm, commitIndex);
    }

    @Test
    public void shouldHandleOutOfOrderCallsToSnapshotWrittenCorrectly() throws StorageException {
        // we start off with a single snapshot up to index 6
        storeSnapshot(1, 6);

        // we have a log that contains entries from index 3 onwards
        final LogEntry[] entries = new LogEntry[] {
                CLIENT(1, 3, new UnitTestCommand()),
                NOOP(1, 4),
                NOOP(1, 5),
                CLIENT(3, 6, new UnitTestCommand()),
                CLIENT(3, 7, new UnitTestCommand()),
                NOOP(3, 8),
                NOOP(3, 9),
                NOOP(3, 10),
                CLIENT(4, 11, new UnitTestCommand()),  // <------ we've committed up to here (we've committed exactly the minimum number required)
                CLIENT(4, 12, new UnitTestCommand()),
                CLIENT(4, 13, new UnitTestCommand()),
                CLIENT(4, 14, new UnitTestCommand())
        };
        clearLog(log); // clear out the log completely - we don't even want the sentinel
        insertIntoLog(log, entries);

        // set the current term
        // ensure that this is _greater_ than the term in the entry that the snapshot is created to!
        // we do this to check that the correct term is used in the setTerm() call in the code
        long currentTerm = 6;
        store.setCurrentTerm(currentTerm);

        // set the committed index
        long commitIndex = 11;
        store.setCommitIndex(commitIndex);

        //
        // starting situation is as follows:
        //
        //                                                         +----------- COMMITTED
        //                                                         V
        //               ------------------------------------------------------------
        //  .. EMPTY .. | 03 | 04 | 05 | 06 | 07 | 08 | 09 | 10 | 11 | 12 | 13 | 14 | (LOG)
        //              ------------------------------------------------------------
        //  --------------------------------                      ^
        // |       LAST APPLIED = 6        | (SNAPSHOT)           |
        // --------------------------------                       |
        //                                                        |
        //                     SNAPSHOT CREATED TO ---------------+
        //

        // -- CALL 1
        //    the caller should claim that they've created the snapshot

        // pretend that the caller handled the request
        UnitTestTempFileSnapshotWriter snapshotWriter0 = snapshotsStore.newSnapshotWriter();
        snapshotWriter0.setIndex(11);
        algorithm.snapshotWritten(snapshotWriter0);

        // we've stored a new snapshot
        // and have truncated the log prefix, so now the log should
        // look as follows (because we've written up to index 11)
        final LogEntry[] truncatedEntries = new LogEntry[] {
                entries[7],
                entries[8],
                entries[9],
                entries[10],
                entries[11],
        };
        assertThatLogContains(log, truncatedEntries);
        assertThat(store.getCommitIndex(), equalTo(commitIndex));

        //
        // new situation is as follows:
        //
        //                                                         +----------- COMMITTED
        //                                                         V
        //               ------------------------------------------------------------
        //  .. EMPTY .. | 03 | 04 | 05 | 06 | 07 | 08 | 09 | 10 | 11 | 12 | 13 | 14 | (LOG)
        //              ------------------------------------------------------------
        //  ---------------------------------------------------------
        // |                   LAST APPLIED = 11                    | (SNAPSHOT)
        // ---------------------------------------------------------
        //                                                        ^
        //                     SNAPSHOT CREATED TO ---------------+
        //

        // -- CALL 2
        //    act as if the caller created the same snapshot again

        // pretend that we got a repeat of the snapshot that created the initial state
        UnitTestTempFileSnapshotWriter snapshotWriter1 = snapshotsStore.newSnapshotWriter();
        snapshotWriter1.setIndex(6);
        algorithm.snapshotWritten(snapshotWriter1);

        //
        // verify
        //

        // we should have submitted only _one_ request to be added to the snapshot store
        ArgumentCaptor<SnapshotsStore.ExtendedSnapshotWriter> snapshotCaptor = ArgumentCaptor.forClass(SnapshotsStore.ExtendedSnapshotWriter.class);
        verify(snapshotsStore, times(1)).storeSnapshot(snapshotCaptor.capture());

        // and it should have the right values
        UnitTestTempFileSnapshotWriter capturedSnapshot = (UnitTestTempFileSnapshotWriter) snapshotCaptor.getValue();
        assertThat(capturedSnapshot.getTerm(), equalTo(4L));
        assertThat(capturedSnapshot.getIndex(), equalTo(11L));

        // and not changed internal state
        assertThatLogCurrentTermAndCommitIndexHaveValues(truncatedEntries, currentTerm, commitIndex);
    }

    private void assertThatLogCurrentTermAndCommitIndexHaveValues(LogEntry[] entries, long currentTerm, long commitIndex) throws StorageException {
        assertThatLogContains(log, entries);
        assertThat(store.getCurrentTerm(), equalTo(currentTerm));
        assertThat(store.getCommitIndex(), equalTo(commitIndex));
    }

    private void storeSnapshot(long snapshotTerm, long snapshotIndex) throws StorageException {
        // create the snapshot
        ExtendedSnapshotWriter writer = snapshotsStore.newSnapshotWriter();
        writer.setTerm(snapshotTerm);
        writer.setIndex(snapshotIndex);

        // write it and reset the interactions we've had with this object so that the tests don't have to account for it
        snapshotsStore.storeSnapshot(writer);
        reset(snapshotsStore);
    }
}
