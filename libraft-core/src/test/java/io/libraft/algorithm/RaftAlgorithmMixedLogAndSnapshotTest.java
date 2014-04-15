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

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import com.google.common.io.ByteStreams;
import com.google.common.io.Closeables;
import com.google.common.io.Files;
import io.libraft.RaftListener;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Random;
import java.util.Set;

import static io.libraft.algorithm.RPCCalls.assertNoMoreMessagesSent;
import static io.libraft.algorithm.RPCCalls.assertThatAppendEntriesHasValues;
import static io.libraft.algorithm.RPCCalls.assertThatSnapshotChunkHasValues;
import static io.libraft.algorithm.RPCCalls.getCallsOfType;
import static io.libraft.algorithm.RPCCalls.getDestinations;
import static io.libraft.algorithm.RaftAlgorithm.Role.LEADER;
import static io.libraft.algorithm.SnapshotsStore.ExtendedSnapshot;
import static io.libraft.algorithm.StoringSender.AppendEntries;
import static io.libraft.algorithm.StoringSender.RPCCall;
import static io.libraft.algorithm.StoringSender.RequestVote;
import static io.libraft.algorithm.StoringSender.SnapshotChunk;
import static io.libraft.algorithm.UnitTestLogEntries.NOOP;
import static io.libraft.algorithm.UnitTestLogEntries.SENTINEL;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;

@SuppressWarnings({"unchecked", "ConstantConditions"})
public final class RaftAlgorithmMixedLogAndSnapshotTest {

    private static final Logger LOGGER = LoggerFactory.getLogger(RaftAlgorithmTest.class);

    private static final String SELF = "0";
    private static final String S_01 = "1";
    private static final String S_02 = "2";
    private static final String S_03 = "3";
    private static final String S_04 = "4";

    private static final Set<String> CLUSTER = ImmutableSet.of(SELF, S_01, S_02, S_03, S_04);
    private static final Set<String> CLUSTER_WITHOUT_SELF = ImmutableSet.of(S_01, S_02, S_03, S_04);
    private static final String SNAPSHOT_CONTENT = "SNAPSHOT_CONTENT";

    private final Random randomSeeder = new Random();
    private final long seed = randomSeeder.nextLong();
    private final Random random = new Random(seed);
    private final UnitTestTimer timer = new UnitTestTimer();
    private final StoringSender sender = spy(new StoringSender());
    private final InMemoryStore store = spy(new InMemoryStore());
    private final InMemoryLog log = spy(new InMemoryLog());
    private final RaftListener listener = mock(RaftListener.class);
    private final TempFileSnapshotsStore snapshotsStore = spy(new TempFileSnapshotsStore(Files.createTempDir()));

    private RaftAlgorithm algorithm;

    @Rule
    public final ExpectedException expectedException = ExpectedException.none();

    @Rule
    public LoggingRule loggingRule = new LoggingRule(LOGGER);

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
                5, // at least 5 entries per snapshot
                3, // 3 entry overlap
                RaftConstants.SNAPSHOT_CHECK_INTERVAL,
                15, // send out 15 entries per AppendEntries
                RaftConstants.RPC_TIMEOUT,
                RaftConstants.MIN_ELECTION_TIMEOUT,
                0,
                RaftConstants.HEARTBEAT_INTERVAL,
                RaftConstants.TIME_UNIT);
        algorithm.initialize();
        algorithm.start();
    }

    @After
    public void teardown() {
        algorithm.stop();
    }

    //
    // utilities
    //

    void assertThatRequestVotesSentToCluster(long term, long lastLogTerm, long lastLogIndex) {
        int expectedCallCount = CLUSTER_WITHOUT_SELF.size();
        List<RPCCall> calls = Lists.newArrayListWithExpectedSize(expectedCallCount);

        while (sender.hasNext() && calls.size() != expectedCallCount) {
            calls.add(sender.nextAndRemove(RPCCall.class));
        }

        assertThat(calls, hasSize(CLUSTER.size() - 1)); // excluding yourself

        for (RPCCall call : calls) {
            assertThat(call, instanceOf(RequestVote.class));

            RequestVote requestVote = (RequestVote) call;

            assertThat(requestVote.term, equalTo(term));
            assertThat(requestVote.lastLogTerm, equalTo(lastLogTerm));
            assertThat(requestVote.lastLogIndex, equalTo(lastLogIndex));
        }

        assertThat(getDestinations(calls), containsInAnyOrder(CLUSTER_WITHOUT_SELF.toArray()));
    }

    private void writeSnapshot(long snapshotTerm, long snapshotIndex, long expectedFirstLogIndexAfterTruncate) throws StorageException, IOException {
        writeSnapshotWithContent(snapshotTerm, snapshotIndex, SNAPSHOT_CONTENT.getBytes(), expectedFirstLogIndexAfterTruncate);
    }

    private void writeSnapshotWithContent(long snapshotTerm, long snapshotIndex, byte[] snapshotContent, long expectedFirstLogIndexAfterTruncate) throws StorageException, IOException {
        UnitTestTempFileSnapshotWriter snapshotWriter = snapshotsStore.newSnapshotWriter();
        snapshotWriter.getSnapshotOutputStream().write(snapshotContent);
        snapshotWriter.setTerm(snapshotTerm);
        snapshotWriter.setIndex(snapshotIndex);
        Closeables.close(snapshotWriter.getSnapshotOutputStream(), false);
        algorithm.snapshotWritten(snapshotWriter);

        // check that we actually stored the snapshot
        assertThat(snapshotsStore.getLatestSnapshot(), notNullValue());

        ExtendedSnapshot snapshot = snapshotsStore.getLatestSnapshot();
        assertThat(snapshot.getTerm(), is(snapshotTerm));
        assertThat(snapshot.getIndex(),is(snapshotIndex));

        assertThat(log.getFirst().getIndex(), is(expectedFirstLogIndexAfterTruncate));
    }

    private byte[] generateRandomContent(int contentLength) {
        byte[] content = new byte[contentLength];
        random.nextBytes(content);
        return content;
    }

    private void replaceLogAndSetNextIndexToEnd(LogEntry[] entries) throws StorageException {
        Preconditions.checkArgument(entries.length > 0);

        UnitTestLogEntries.clearLog(log);

        // add all the entries
        for (LogEntry entry : entries) {
            algorithm.addOrUpdateLogEntryWhileLeaderForUnitTestsOnly(entry);
        }

        // by default set the nextIndex to just after the log end
        for (String server : CLUSTER_WITHOUT_SELF) {
            algorithm.setServerNextIndexWhileLeaderForUnitTestsOnly(server, entries.length);
        }
    }

    private <T extends RPCCall> List<T> getCallsOfTypeAndVerifyNoFurtherCallsSent(int callCount, Class<T> klass) {
        List<T> appendEntriesRequests = getCallsOfType(sender, callCount, klass);

        assertNoMoreMessagesSent(sender);
        assertThat(sender.hasNext(), is(false));

        return appendEntriesRequests;
    }

    private void becomeLeaderInTerm1() {
        fastForwardToElection();

        // verify that we're sending out the correct RequestVote messages
        assertThatRequestVotesSentToCluster(1, 0, 0);
        assertNoMoreMessagesSent(sender);

        // send in a bunch of responses so that we can become the leader
        for (String server : CLUSTER_WITHOUT_SELF) {
            algorithm.onRequestVoteReply(server, 1, true);
        }

        // we should be the leader right now
        assertThat(algorithm.getRole(), is(LEADER));

        // drain all the first NOOPs because they're going to be invalid
        sender.drainSentRPCs();
    }

    private void fastForwardToHeartbeat() {
        Timer.TimeoutHandle heartbeatTimeoutHandle = algorithm.getHeartbeatTimeoutHandleForUnitTestsOnly();
        timer.fastForwardTillTaskExecutes(heartbeatTimeoutHandle);
    }

    private void fastForwardToElection() {
        Timer.TimeoutHandle electionTimeoutHandle = algorithm.getElectionTimeoutHandleForUnitTestsOnly();
        timer.fastForwardTillTaskExecutes(electionTimeoutHandle);
    }

    @Test
    public void shouldSendCompleteSnapshotToLaggingPeer() throws Exception {
        long currentTerm = 1;
        long commitIndex = 10;

        becomeLeaderInTerm1();

        // set the log
        LogEntry[] entries = new LogEntry[] {
                SENTINEL(),
                NOOP(1,  1),
                NOOP(1,  2),
                NOOP(1,  3),
                NOOP(1,  4),
                NOOP(1,  5), // <--- S_01 nextIndex
                NOOP(1,  6),
                NOOP(1,  7),
                NOOP(1,  8),
                NOOP(1,  9), // <--- snapshot created till here
                NOOP(1, 10), // <--- committed
                NOOP(1, 11),
                NOOP(1, 12),
        };
        replaceLogAndSetNextIndexToEnd(entries);

        // and our commit index
        store.setCommitIndex(commitIndex);

        // this server is lagging
        algorithm.setServerNextIndexWhileLeaderForUnitTestsOnly(S_01, 5);

        // write a snapshot
        long snapshotTerm = 1;
        long snapshotIndex = 9;
        byte[] snapshotContent = generateRandomContent((int) (RaftConstants.MAX_CHUNK_SIZE * 2.5));
        writeSnapshotWithContent(snapshotTerm, snapshotIndex, snapshotContent, 7);

        List<RPCCall> calls;
        SnapshotChunk snapshotChunk;
        ByteArrayOutputStream receivedSnapshot = new ByteArrayOutputStream(snapshotContent.length);

        //-------------------
        // 1st chunk
        //

        // move to heartbeat
        fastForwardToHeartbeat();

        // check that we started sending out a snapshot
        calls = getCallsOfTypeAndVerifyNoFurtherCallsSent(CLUSTER_WITHOUT_SELF.size(), RPCCall.class);
        for (RPCCall call : calls) {
            if (call.server.equals(S_01)) {
                assertThat(call, instanceOf(SnapshotChunk.class));
                snapshotChunk = (SnapshotChunk) call;
                assertThatSnapshotChunkHasValues(snapshotChunk, currentTerm, snapshotTerm, snapshotIndex, 0, true);
                ByteStreams.copy(snapshotChunk.chunkInputStream, receivedSnapshot);
            } else {
                assertThat(call, instanceOf(AppendEntries.class));
            }
        }

        // S_01 received the first chunk
        algorithm.onSnapshotChunkReply(S_01, currentTerm, snapshotTerm, snapshotIndex, 1);

        // check that we send a snapshot chunk out immediately
        snapshotChunk = getCallsOfTypeAndVerifyNoFurtherCallsSent(1, SnapshotChunk.class).get(0);
        assertThatSnapshotChunkHasValues(snapshotChunk, currentTerm, snapshotTerm, snapshotIndex, 1, true);

        //-------------------
        // 2nd chunk
        //

        // move to the next heartbeat
        fastForwardToHeartbeat();

        // we should continue sending out the snapshot
        calls = getCallsOfTypeAndVerifyNoFurtherCallsSent(CLUSTER_WITHOUT_SELF.size(), RPCCall.class);
        for (RPCCall call : calls) {
            if (call.server.equals(S_01)) {
                assertThat(call, instanceOf(SnapshotChunk.class));
                snapshotChunk = (SnapshotChunk) call;
                assertThatSnapshotChunkHasValues(snapshotChunk, currentTerm, snapshotTerm, snapshotIndex, 1, true);
                ByteStreams.copy(snapshotChunk.chunkInputStream, receivedSnapshot);
            } else {
                assertThat(call, instanceOf(AppendEntries.class));
            }
        }

        // S_01 received the second chunk
        algorithm.onSnapshotChunkReply(S_01, currentTerm, snapshotTerm, snapshotIndex, 2);

        // check that we send out another snapshot chunk out immediately
        snapshotChunk = getCallsOfTypeAndVerifyNoFurtherCallsSent(1, SnapshotChunk.class).get(0);
        assertThatSnapshotChunkHasValues(snapshotChunk, currentTerm, snapshotTerm, snapshotIndex, 2, true);

        //-------------------
        // 3rd chunk
        //

        // move to the next heartbeat
        fastForwardToHeartbeat();

        // we should continue sending out the snapshot
        calls = getCallsOfTypeAndVerifyNoFurtherCallsSent(CLUSTER_WITHOUT_SELF.size(), RPCCall.class);
        for (RPCCall call : calls) {
            if (call.server.equals(S_01)) {
                assertThat(call, instanceOf(SnapshotChunk.class));
                snapshotChunk = (SnapshotChunk) call;
                assertThatSnapshotChunkHasValues(snapshotChunk, currentTerm, snapshotTerm, snapshotIndex, 2, true);
                ByteStreams.copy(snapshotChunk.chunkInputStream, receivedSnapshot);
            } else {
                assertThat(call, instanceOf(AppendEntries.class));
            }
        }

        // S_01 received the third chunk
        algorithm.onSnapshotChunkReply(S_01, currentTerm, snapshotTerm, snapshotIndex, 3);

        // and another
        snapshotChunk = getCallsOfTypeAndVerifyNoFurtherCallsSent(1, SnapshotChunk.class).get(0);
        assertThatSnapshotChunkHasValues(snapshotChunk, currentTerm, snapshotTerm, snapshotIndex, 3, false);

        //-------------------
        // Final chunk
        //

        // move to the next heartbeat
        fastForwardToHeartbeat();

        // at this point there are no more chunks left to send
        calls = getCallsOfTypeAndVerifyNoFurtherCallsSent(CLUSTER_WITHOUT_SELF.size(), RPCCall.class);
        for (RPCCall call : calls) {
            if (call.server.equals(S_01)) {
                assertThat(call, instanceOf(SnapshotChunk.class));
                snapshotChunk = (SnapshotChunk) call;
                assertThatSnapshotChunkHasValues(snapshotChunk, currentTerm, snapshotTerm, snapshotIndex, 3, false);
            } else {
                assertThat(call, instanceOf(AppendEntries.class));
            }
        }

        // check that we received the complete snapshot
        byte[] receivedSnapshotContent = receivedSnapshot.toByteArray();
        assertThat(Arrays.equals(receivedSnapshotContent, snapshotContent), is(true));

        // S_01 received the final chunk
        // FIXME (AG): ummm...yeah...I didn't think about the end case
        // transition out is a problem
        // also, if this message is lost...yeah...
        algorithm.onSnapshotChunkReply(S_01, currentTerm, snapshotTerm, snapshotIndex, 4);

        // and...we shouldn't be sending out anything
        assertThat(sender.hasNext(), is(false));

        //-------------------
        // Everyone synced up
        //

        // move to the next heartbeat
        fastForwardToHeartbeat();

        // at this point all servers should be completely synced up
        calls = getCallsOfTypeAndVerifyNoFurtherCallsSent(CLUSTER_WITHOUT_SELF.size(), RPCCall.class);
        for (RPCCall call : calls) {
            assertThat(call, instanceOf(AppendEntries.class));
            AppendEntries appendEntries = (AppendEntries) call;
            assertThatAppendEntriesHasValues(appendEntries, currentTerm, commitIndex, currentTerm, boo); // this is broken
        }
    }

    // TODO (AG):
    // should still keep sending out the same chunk if no response received
    // should send old chunk if receiver missed a message

    @Test
    public void shouldBeginSendingSnapshotIfSnapshotCreatedAndAppliedAppendEntriesReplyIsReceivedAndLastAppliedIndexLessThanSnapshotIndex() throws Exception {
        becomeLeaderInTerm1();

        // set the log
        LogEntry[] entries = new LogEntry[] {
                SENTINEL(),
                NOOP(1, 1),
                NOOP(1, 2),
                NOOP(1, 3),
                NOOP(1, 4), // <--- S_01 nextIndex
                NOOP(1, 5),
                NOOP(1, 6),
                NOOP(1, 7),
                NOOP(1, 8), // <--- committed; snapshot created till here
        };
        replaceLogAndSetNextIndexToEnd(entries);

        // and our commit index
        store.setCommitIndex(8);

        // this server is lagging
        algorithm.setServerNextIndexWhileLeaderForUnitTestsOnly(S_01, 4);

        // trigger the heartbeat to send AppendEntries
        fastForwardToHeartbeat();

        // for server 1 we're sending out entries 4, 5, 6, 7, 8
        // for the other server's we're simply sending out nulls
        List<AppendEntries> appendEntriesRequests = getCallsOfTypeAndVerifyNoFurtherCallsSent(CLUSTER_WITHOUT_SELF.size(), AppendEntries.class);
        for (AppendEntries appendEntries : appendEntriesRequests) {
            if (appendEntries.server.equals(S_01)) {
                assertThatAppendEntriesHasValues(appendEntries,
                        1,
                        8,
                        1,
                        3,
                        NOOP(1, 4),
                        NOOP(1, 5),
                        NOOP(1, 6),
                        NOOP(1, 7),
                        NOOP(1, 8));
            } else {
                assertThatAppendEntriesHasValues(appendEntries, 1, 8, 1, 8);
            }
        }

        // pretend that a snapshot was triggered and that we've written it
        writeSnapshot(1, 8, 6);

        // a late message is coming back from S_01
        // notice that it has fewer entries than exist in the snapshot
        algorithm.onAppendEntriesReply(S_01, 1, 3, 2, true);

        // move forward to the next heartbeat
        // and get the messages sent out
        fastForwardToHeartbeat();

        List<RPCCall> calls = getCallsOfTypeAndVerifyNoFurtherCallsSent(CLUSTER_WITHOUT_SELF.size(), RPCCall.class);
        for (RPCCall call : calls) {
            if (call.server.equals(S_01)) {
                // for the server lagging behind we should send a snapshot chunk
                assertThat(call, instanceOf(SnapshotChunk.class));
                SnapshotChunk snapshotChunk = (SnapshotChunk) call;
                assertThatSnapshotChunkHasValues(snapshotChunk, 1, 1, 8, 0, true);
            } else {
                // for all the rest we're sending out AppendEntries
                assertThat(call, instanceOf(AppendEntries.class));
                AppendEntries appendEntries = (AppendEntries) call;
                assertThatAppendEntriesHasValues(appendEntries, 1, 8, 1, 8);
            }
        }
    }

    @Test
    public void shouldSendLogEntriesIfSnapshotCreatedAndAppliedAppendEntriesReplyIsReceivedAndLastAppliedIndexGreaterThanEqualToSnapshotIndex() throws Exception {
        becomeLeaderInTerm1();

        // replace the log
        LogEntry[] entries = new LogEntry[] {
                SENTINEL(),
                NOOP(1, 1),
                NOOP(1, 2),
                NOOP(1, 3),
                NOOP(1, 4),  // <--- S_01 nextIndex
                NOOP(1, 5),
                NOOP(1, 6),  // <--- committed; snapshot created till here
                NOOP(1, 7),
                NOOP(1, 8),
        };
        replaceLogAndSetNextIndexToEnd(entries);

        // set the commit index
        store.setCommitIndex(6);

        // S_01 is lagging
        algorithm.setServerNextIndexWhileLeaderForUnitTestsOnly(S_01, 4);

        // trigger the heartbeat
        // we should be sending AppendEntries out
        fastForwardToHeartbeat();

        // for server 1 we're sending out entries 4, 5, 6, 7, 8
        // for the other server's we're simply sending out nulls
        List<AppendEntries> appendEntriesRequests = getCallsOfTypeAndVerifyNoFurtherCallsSent(CLUSTER_WITHOUT_SELF.size(), AppendEntries.class);
        for (AppendEntries appendEntries : appendEntriesRequests) {
            if (appendEntries.server.equals(S_01)) {
                assertThatAppendEntriesHasValues(appendEntries,
                        1,
                        6,
                        1,
                        3,
                        NOOP(1, 4),
                        NOOP(1, 5),
                        NOOP(1, 6),
                        NOOP(1, 7),
                        NOOP(1, 8));
            } else {
                assertThatAppendEntriesHasValues(appendEntries, 1, 6, 1, 8);
            }
        }

        // pretend that a snapshot was written
        writeSnapshot(1, 6, 4);

        // message comes in from S_01 saying that it has applied up to index 7
        // this is _after_ the end of the snapshot
        algorithm.onAppendEntriesReply(S_01, 1, 3, 4, true);

        // move forward to the next heartbeat
        // and get a list of messages sent out
        fastForwardToHeartbeat();

        // they should all be AppendEntries
        List<RPCCall> calls = getCallsOfTypeAndVerifyNoFurtherCallsSent(CLUSTER_WITHOUT_SELF.size(), RPCCall.class);
        for (RPCCall call : calls) {
            assertThat(call, instanceOf(AppendEntries.class));
            AppendEntries appendEntries = (AppendEntries) call;

            if (call.server.equals(S_01)) {
                assertThatAppendEntriesHasValues(appendEntries, 1, 6, 1, 7, NOOP(1, 8));
            } else {
                assertThatAppendEntriesHasValues(appendEntries, 1, 6, 1, 8);
            }
        }
    }

    @Test
    public void shouldBeginSendingSnapshotIfSnapshotCreatedAndUnappliedAppendEntriesReplyIsReceivedAndNextIndexLessThanSnapshotIndex() throws Exception {
        becomeLeaderInTerm1();

        // create a log
        LogEntry[] entries = new LogEntry[] {
                SENTINEL(),
                NOOP(1, 1),
                NOOP(1, 2),  // <--- S_01 nextIndex
                NOOP(1, 3),
                NOOP(1, 4),
                NOOP(1, 5),
                NOOP(1, 6), // <--- snapshot created to here
                NOOP(1, 7),
                NOOP(1, 8), // <--- committed
        };
        replaceLogAndSetNextIndexToEnd(entries);

        // and setup the commit index
        store.setCommitIndex(8);

        // S_01 is lagging
        algorithm.setServerNextIndexWhileLeaderForUnitTestsOnly(S_01, 2);

        // move forward to the heartbeat
        fastForwardToHeartbeat();

        // verify that AppendEntries are being sent out
        // for server 1 we're sending out entries 2, 3, 4, 5, 6, 7, 8
        // for the other server's we're simply sending out nulls
        List<AppendEntries> appendEntriesRequests = getCallsOfTypeAndVerifyNoFurtherCallsSent(CLUSTER_WITHOUT_SELF.size(), AppendEntries.class);
        for (AppendEntries appendEntries : appendEntriesRequests) {
            if (appendEntries.server.equals(S_01)) {
                assertThatAppendEntriesHasValues(appendEntries,
                        1,
                        8,
                        1,
                        1,
                        NOOP(1, 2),
                        NOOP(1, 3),
                        NOOP(1, 4),
                        NOOP(1, 5),
                        NOOP(1, 6),
                        NOOP(1, 7),
                        NOOP(1, 8));
            } else {
                assertThatAppendEntriesHasValues(appendEntries, 1, 8, 1, 8);
            }
        }

        // write a snapshot
        writeSnapshot(1, 6, 4);

        // reply coming back in from S_01
        // nothing was applied
        // at this point we've no option but to send a snapshot
        algorithm.onAppendEntriesReply(S_01, 1, 1, 7, false);

        // move forward to the next heartbeat
        fastForwardToHeartbeat();

        // check the messages going out
        List<RPCCall> calls = getCallsOfTypeAndVerifyNoFurtherCallsSent(CLUSTER_WITHOUT_SELF.size(), RPCCall.class);
        for (RPCCall call : calls) {
            if (call.server.equals(S_01)) {
                // for the server lagging behind we should send a snapshot chunk
                assertThat(call, instanceOf(SnapshotChunk.class));
                SnapshotChunk snapshotChunk = (SnapshotChunk) call;
                assertThatSnapshotChunkHasValues(snapshotChunk, 1, 1, 6, 0, true);
            } else {
                // for all other servers verify that we're only sending AppendEntries
                assertThat(call, instanceOf(AppendEntries.class));
                AppendEntries appendEntries = (AppendEntries) call;
                assertThatAppendEntriesHasValues(appendEntries, 1, 8, 1, 8);
            }
        }
    }

    @Test
    public void shouldContinueSendingLogEntriesIfSnapshotCreatedAndUnappliedAppendEntriesReplyIsReceivedAndNextIndexGreaterThanSnapshotIndex() throws Exception {
        becomeLeaderInTerm1();

        // set the log
        LogEntry[] entries = new LogEntry[] {
                SENTINEL(),
                NOOP(1, 1),
                NOOP(1, 2),
                NOOP(1, 3),
                NOOP(1, 4),
                NOOP(1, 5), // <--- commit index
                NOOP(1, 6),
                NOOP(1, 7),
                NOOP(1, 8), // <--- S_01 nextIndex
        };
        replaceLogAndSetNextIndexToEnd(entries);

        // set the commit index
        store.setCommitIndex(5);

        // unlike everyone else S_01 is lagging
        algorithm.setServerNextIndexWhileLeaderForUnitTestsOnly(S_01, 8);

        // we send append entries out
        fastForwardToHeartbeat();

        // they should all be AppendEntries
        List<AppendEntries> appendEntriesRequests = getCallsOfTypeAndVerifyNoFurtherCallsSent(CLUSTER_WITHOUT_SELF.size(), AppendEntries.class);
        for (AppendEntries appendEntries : appendEntriesRequests) {
            if (appendEntries.server.equals(S_01)) {
                assertThatAppendEntriesHasValues(appendEntries, 1, 5, 1, 7, NOOP(1, 8));
            } else {
                assertThatAppendEntriesHasValues(appendEntries, 1, 5, 1, 8);
            }
        }

        // write a snapshot
        writeSnapshot(1, 5, 3);

        // S_01 responds and notifies us that its prefix does not match
        algorithm.onAppendEntriesReply(S_01, 1, 7, 1, false);

        // move forward to the next heartbeat
        fastForwardToHeartbeat();

        // get the list of AppendEntries we're sending out
        List<RPCCall> calls = getCallsOfTypeAndVerifyNoFurtherCallsSent(CLUSTER_WITHOUT_SELF.size(), RPCCall.class);
        for (RPCCall call : calls) {
            assertThat(call, instanceOf(AppendEntries.class));
            AppendEntries appendEntries = (AppendEntries) call;

            if (call.server.equals(S_01)) {
                // since we can still send out log entries for S_01 we should do so
                assertThatAppendEntriesHasValues(appendEntries, 1, 5, 1, 6, NOOP(1, 7), NOOP(1, 8));
            } else {
                // for all the rest a standard heartbeat is enough
                assertThatAppendEntriesHasValues(appendEntries, 1, 5, 1, 8);
            }
        }
    }

    @Test
    public void shouldBeginSendingSnapshotIfSnapshotCreatedAndHeartbeatSentImmediatelyAfterwards() throws Exception {
        becomeLeaderInTerm1();

        // set the log
        LogEntry[] entries = new LogEntry[] {
                SENTINEL(),
                NOOP(1, 1),
                NOOP(1, 2),
                NOOP(1, 3),
                NOOP(1, 4), // <--- S_01 nextIndex
                NOOP(1, 5),
                NOOP(1, 6),
                NOOP(1, 7),
                NOOP(1, 8), // <--- commit index
        };
        replaceLogAndSetNextIndexToEnd(entries);

        // and our commit index
        store.setCommitIndex(8);

        // S_01 is lagging a lot
        algorithm.setServerNextIndexWhileLeaderForUnitTestsOnly(S_01, 4);

        // we send append entries out
        fastForwardToHeartbeat();

        // for S_01 we're sending out entries 4, 5, 6, 7, 8
        // for the other server's we're simply sending out heartbeats
        List<AppendEntries> appendEntriesRequests = getCallsOfTypeAndVerifyNoFurtherCallsSent(CLUSTER_WITHOUT_SELF.size(), AppendEntries.class);
        for (AppendEntries appendEntries : appendEntriesRequests) {
            if (appendEntries.server.equals(S_01)) {
                assertThatAppendEntriesHasValues(appendEntries,
                        1,
                        8,
                        1,
                        3,
                        NOOP(1, 4),
                        NOOP(1, 5),
                        NOOP(1, 6),
                        NOOP(1, 7),
                        NOOP(1, 8));
            } else {
                assertThatAppendEntriesHasValues(appendEntries, 1, 8, 1, 8);
            }
        }

        // write a snapshot
        writeSnapshot(1, 8, 6);

        // move forward to the next heartbeat
        fastForwardToHeartbeat();

        // get the list of outgoing messages
        List<RPCCall> calls = getCallsOfTypeAndVerifyNoFurtherCallsSent(CLUSTER_WITHOUT_SELF.size(), RPCCall.class);
        for (RPCCall call : calls) {
            if (call.server.equals(S_01)) {
                // since we no longer have entries for the
                // log index at which S_01 is we have to send out a snapshot
                assertThat(call, instanceOf(SnapshotChunk.class));
                SnapshotChunk snapshotChunk = (SnapshotChunk) call;
                assertThatSnapshotChunkHasValues(snapshotChunk, 1, 1, 8, 0, true);
            } else {
                // for the rest, we send out heartbeats
                assertThat(call, instanceOf(AppendEntries.class));
                AppendEntries appendEntries = (AppendEntries) call;
                assertThatAppendEntriesHasValues(appendEntries, 1, 8, 1, 8);
            }
        }
    }

}
