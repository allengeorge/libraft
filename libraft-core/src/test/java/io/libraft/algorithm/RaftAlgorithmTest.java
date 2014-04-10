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
import com.google.common.util.concurrent.ListenableFuture;
import io.libraft.Command;
import io.libraft.Committed;
import io.libraft.CommittedCommand;
import io.libraft.NotLeaderException;
import io.libraft.RaftListener;
import io.libraft.ReplicationException;
import io.libraft.Snapshot;
import io.libraft.testlib.TestLoggingRule;
import org.hamcrest.Description;
import org.hamcrest.Matcher;
import org.hamcrest.TypeSafeMatcher;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.mockito.InOrder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import java.io.IOException;
import java.util.Collection;
import java.util.List;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.ExecutionException;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static io.libraft.algorithm.RaftAlgorithm.Role.CANDIDATE;
import static io.libraft.algorithm.RaftAlgorithm.Role.FOLLOWER;
import static io.libraft.algorithm.RaftAlgorithm.Role.LEADER;
import static io.libraft.algorithm.StoringSender.AppendEntries;
import static io.libraft.algorithm.StoringSender.AppendEntriesReply;
import static io.libraft.algorithm.StoringSender.RPCCall;
import static io.libraft.algorithm.StoringSender.RequestVote;
import static io.libraft.algorithm.StoringSender.RequestVoteReply;
import static io.libraft.algorithm.UnitTestLogEntries.CLIENT;
import static io.libraft.algorithm.UnitTestLogEntries.NOOP;
import static io.libraft.algorithm.UnitTestLogEntries.SENTINEL;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.lessThanOrEqualTo;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;
import static org.hamcrest.Matchers.sameInstance;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyCollection;
import static org.mockito.Matchers.anyLong;
import static org.mockito.Matchers.anyString;
import static org.mockito.Matchers.argThat;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.doCallRealMethod;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

@SuppressWarnings({"unchecked", "ConstantConditions"})
public final class RaftAlgorithmTest {

    private static final Logger LOGGER = LoggerFactory.getLogger(RaftAlgorithmTest.class);

    private static final String SELF = "0";
    private static final String S_01 = "1";
    private static final String S_02 = "2";
    private static final String S_03 = "3";
    private static final String S_04 = "4";

    private static final Set<String> CLUSTER = ImmutableSet.of(SELF, S_01, S_02, S_03, S_04);

    private final Random randomSeeder = new Random();
    private final long seed = randomSeeder.nextLong();
    private final Random random = new Random(seed);
    private final UnitTestTimer timer = new UnitTestTimer();
    private final StoringSender sender = spy(new StoringSender());
    private final InMemoryStore store = spy(new InMemoryStore());
    private final InMemoryLog log = spy(new InMemoryLog());
    private final RaftListener listener = mock(RaftListener.class);
    private final SnapshotsStore snapshotsStore = mock(SnapshotsStore.class);

    private RaftAlgorithm algorithm;

    @Rule
    public final ExpectedException expectedException = ExpectedException.none();

    @Rule
    public TestLoggingRule testLoggingRule = new TestLoggingRule(LOGGER);

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
                30, // want this to be high enough that the standard tests won't trigger the snapshot, but low enough that testing snapshots isn't onerous
                RaftConstants.SNAPSHOT_CHECK_INTERVAL,
                RaftConstants.RPC_TIMEOUT,
                RaftConstants.MIN_ELECTION_TIMEOUT,
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

    // TODO (AG): reduce test overlap
    // TODO (AG): consider writing custom matchers so that I can use the nice "assertThat" syntax
    // TODO (AG): consider creating a DSL and refactoring out even more common code to reduce test length

    private void clearLog() throws StorageException {
        UnitTestLogEntries.clearLog(log);
    }

    private void insertIntoLog(LogEntry... entries) throws StorageException {
        UnitTestLogEntries.insertIntoLog(log, entries);
    }

    //================================================================================================================//
    //
    // Additional Asserts
    //
    //================================================================================================================//

    private void assertThatLogIsEmpty() throws StorageException {
        UnitTestLogEntries.assertThatLogIsEmpty(log);
    }

    private void assertThatLogContainsOnlySentinel() throws StorageException {
        UnitTestLogEntries.assertThatLogContainsOnlySentinel(log);
    }

    private void assertThatLogContains(LogEntry... entries) throws StorageException {
        UnitTestLogEntries.assertThatLogContains(log, entries);
    }

    private void assertThatTermAndCommitIndexHaveValues(long term, long commitIndex) throws StorageException {
        assertThat(store.getCurrentTerm(), equalTo(term));
        assertThat(store.getCommitIndex(), equalTo(commitIndex));
    }

    private void assertThatSelfTransitionedToCandidate(long term, long commitIndex) throws StorageException {
        assertThatTermAndCommitIndexHaveValues(term, commitIndex);
        assertThat(algorithm.getRole(), equalTo(CANDIDATE));
        assertThat(algorithm.getLeader(), nullValue());
        assertThat(store.getVotedFor(term), equalTo(SELF));
    }

    private void assertThatSelfTransitionedToFollower(long term, long commitIndex, @Nullable String leader, boolean checkLeadershipChangeNotification) throws StorageException {
        assertThatTermAndCommitIndexHaveValues(term, commitIndex);
        assertThat(algorithm.getRole(), equalTo(FOLLOWER));
        assertThat(algorithm.getLeader(), equalTo(leader));

        if (checkLeadershipChangeNotification) {
            verify(listener).onLeadershipChange(leader);
        }
    }

    private void assertThatSelfTransitionedToLeader(long term, long commitIndex) throws StorageException {
        assertThatTermAndCommitIndexHaveValues(term, commitIndex);
        assertThat(algorithm.getRole(), equalTo(LEADER));
        assertThat(algorithm.getLeader(), equalTo(SELF));
        assertThat(store.getVotedFor(term), equalTo(SELF));
        verify(listener).onLeadershipChange(SELF);
    }

    private void assertThatStateAfterRequestVoteIs(long term, @Nullable String votedFor, RaftAlgorithm.Role expectedRole) throws StorageException {
        assertThat(store.getCurrentTerm(), equalTo(term));
        assertThat(store.getVotedFor(term), equalTo(votedFor));
        assertThat(algorithm.getRole(), equalTo(expectedRole));
        assertThat(algorithm.getLeader(), nullValue());
    }

    private <T extends StoringSender.RPCCall> Collection<T> getRPCs(int callCount, Class<T> klass) {
        List<T> calls = Lists.newArrayListWithCapacity(callCount);
        for (int i = 0; i < callCount; i++) {
            calls.add(i, sender.nextAndRemove(klass));
        }
        return calls;
    }

    private Collection<String> getRPCDestinations(Collection<? extends RPCCall> calls) {
        List<String> destinations = Lists.newArrayListWithCapacity(calls.size());
        for (RPCCall call : calls) {
            destinations.add(call.server);
        }
        return destinations;
    }

    private void assertThatRPCsSentTo(Collection<? extends RPCCall> rpcs, String... destinationServers) {
        assertThat(getRPCDestinations(rpcs), containsInAnyOrder(destinationServers));
    }

    private void assertThatNoMoreRPCsWereSent() {
        assertThat(sender.getCalls().toString(), sender.hasNext(), equalTo(false));
    }

    private void assertThatRequestVotesHaveValues(Collection<RequestVote> requestVotes, long term, long lastLogIndex, long lastLogTerm) {
        for(RequestVote requestVote : requestVotes) {
            assertThat(requestVote.term, equalTo(term));
            assertThat(requestVote.lastLogIndex, equalTo(lastLogIndex));
            assertThat(requestVote.lastLogTerm, equalTo(lastLogTerm));
        }
    }

    private void assertThatRequestVoteReplyHasValues(RequestVoteReply requestVoteReply, String server, long term, boolean voteGranted) {
        assertThat(requestVoteReply.server, equalTo(server));
        assertThat(requestVoteReply.term, equalTo(term));
        assertThat(requestVoteReply.voteGranted, equalTo(voteGranted));
    }

    private void assertThatAppendEntriesHaveValues(Collection<AppendEntries> appendEntries, long term, long commitIndex, long prevLogIndex, long prevLogTerm, LogEntry... entries) {
        for(AppendEntries request : appendEntries) {
            assertThatAppendEntriesHasValues(request, term, commitIndex, prevLogIndex, prevLogTerm, entries);
        }
    }

    private void assertThatAppendEntriesHasValues(AppendEntries request, long term, long commitIndex, long prevLogIndex, long prevLogTerm, LogEntry... entries) {
        assertThat(request.term, equalTo(term));
        assertThat(request.commitIndex, equalTo(commitIndex));
        assertThat(request.prevLogIndex, equalTo(prevLogIndex));
        assertThat(request.prevLogTerm, equalTo(prevLogTerm));
        if (entries.length == 0) {
            assertThat(request.entries, nullValue());
        } else {
            for (LogEntry entry : entries) {
                assertThat(entry, notNullValue());
            }
            assertThat(request.entries, hasSize(entries.length));
            assertThat(request.entries, contains(entries));
        }
    }

    private void assertThatAppendEntriesReplyHasValues(AppendEntriesReply appendEntriesReply, String server, long term, long prevLogIndex, long entryCount, boolean applied) {
        assertThat(appendEntriesReply.server, equalTo(server));
        assertThat(appendEntriesReply.term, equalTo(term));
        assertThat(appendEntriesReply.prevLogIndex, equalTo(prevLogIndex));
        assertThat(appendEntriesReply.entryCount, equalTo(entryCount));
        assertThat(appendEntriesReply.applied, equalTo(applied));
    }

    //================================================================================================================//
    //
    // Custom Matcher for CommittedCommand
    //
    //================================================================================================================//

    private Matcher<CommittedCommand> isCommittedCommandAtIndex(final long logIndex, final Command command) {
        return new TypeSafeMatcher<CommittedCommand>() {
            @Override
            protected boolean matchesSafely(CommittedCommand item) {
                return logIndex == item.getIndex() && command.equals(item.getCommand());
            }

            @Override
            public void describeTo(Description description) {
                description.appendText("getIndex should return ").appendValue(logIndex).appendText(" and getCommand should return").appendValue(command);
            }

            @Override
            protected void describeMismatchSafely(CommittedCommand item, Description mismatchDescription) {
                mismatchDescription.appendText(" getIndex was ").appendValue(item.getIndex()).appendText(" and getCommand was ").appendValue(item.getCommand());
            }
        };
    }

    private Matcher<Committed> isSkipAtIndex(final long logIndex) {
        return new TypeSafeMatcher<Committed>() {
            @Override
            protected boolean matchesSafely(Committed item) {
                return item.getType() == Committed.Type.SKIP && item.getIndex() == logIndex;
            }

            @Override
            public void describeTo(Description description) {
                description.appendText("type should be ").appendValue(Committed.Type.SKIP).appendText(" and getIndex should return ").appendValue(logIndex);
            }

            @Override
            protected void describeMismatchSafely(Committed item, Description mismatchDescription) {
                mismatchDescription.appendText(" getType was ").appendValue(item.getType()).appendText(" and getIndex was ").appendValue(item.getIndex());
            }
        };
    }

    //================================================================================================================//
    //
    // Consensus Tests
    //
    //================================================================================================================//

    // generally, in the following tests we'll assume that
    // the server is trying to do work in term '3'

    //================================================================================================================//
    //
    // Election Tests
    //
    //================================================================================================================//

    // FIXME (AG): how do I verify that the election timeouts are correct when receiving vote replies/requests?

    @Test
    public void shouldStartElectionOnElectionTimeout() throws StorageException {
        // when RaftAlgorithm starts, we immediately schedule
        // an election timeout, and wait for an AppendEntries, or...any message
        // that tells us what the environment is like
        long electionTimeoutTick = getElectionTimeoutTick();
        long preElectionTerm = store.getCurrentTerm();
        long preElectionCommitIndex = store.getCommitIndex();

        assertThatSelfTransitionedToFollower(preElectionTerm, preElectionCommitIndex, null, false);

        // assume that no message was received
        timer.fastForward(electionTimeoutTick - timer.getTick());

        // check that we actually started an election
        // this method checks a number of things:
        //   - that the term was increased
        //   - that we're in the candidate role, and that we voted for ourself
        //   - that the commit index didn't arbitrarily increase
        assertThatSelfTransitionedToCandidate(preElectionTerm + 1, preElectionCommitIndex);

        assertThatLogContainsOnlySentinel();
        assertThatTermAndCommitIndexHaveValues(preElectionTerm + 1, preElectionCommitIndex);
    }

    private void triggerElection(long electionTerm) throws StorageException {
        fastForwardToElection();
        assertThatSelfTransitionedToCandidate(electionTerm, store.getCommitIndex());
    }

    private void fastForwardToElection() {
        Timer.TimeoutHandle handle = algorithm.getElectionTimeoutHandleForUnitTestsOnly();
        timer.fastForwardTillTaskExecutes(handle);
    }

    private void fastForwardToHeartbeat() {
        Timer.TimeoutHandle handle = algorithm.getHeartbeatTimeoutHandleForUnitTestsOnly();
        timer.fastForwardTillTaskExecutes(handle);
    }

    private long getElectionTimeoutTick() {
        Timer.TimeoutHandle handle = algorithm.getElectionTimeoutHandleForUnitTestsOnly();
        return timer.getTickForHandle(handle);
    }

    private long getHeartbeatTimeoutTick() {
        Timer.TimeoutHandle handle = algorithm.getHeartbeatTimeoutHandleForUnitTestsOnly();
        return timer.getTickForHandle(handle);
    }

    @Test
    public void shouldIssueRequestVoteWithCorrectLogPrefixOnFirstBoot() throws StorageException {
        triggerElection(1);

        assertThat(timer.getTick(), equalTo((long) RaftConstants.MIN_ELECTION_TIMEOUT));
        assertThatTermAndCommitIndexHaveValues(1, 0);
        assertThat(store.getVotedFor(1), equalTo(SELF));

        Collection<RequestVote> requestVotes = getRPCs(4, RequestVote.class);
        assertThatRPCsSentTo(requestVotes, S_01, S_02, S_03, S_04);
        assertThatRequestVotesHaveValues(requestVotes, 1, 0, 0);
        assertThatNoMoreRPCsWereSent();

        assertThatLogContainsOnlySentinel();
        assertThatTermAndCommitIndexHaveValues(1, 0);
    }

    @Test
    public void shouldIssueRequestVoteWithCorrectLogPrefix() throws StorageException {
        insertIntoLog(
                SENTINEL(),
                NOOP(1, 1),
                NOOP(2, 1),
                NOOP(3, 2)
        );

        algorithm.becomeFollower(2, null);
        assertThatSelfTransitionedToFollower(2, 0, null, false);

        triggerElection(3);

        Collection<RequestVote> requestVotes = getRPCs(4, RequestVote.class);
        assertThatRPCsSentTo(requestVotes, S_01, S_02, S_03, S_04);
        assertThatRequestVotesHaveValues(requestVotes, 3, 3, 2);
        assertThatNoMoreRPCsWereSent();

        assertThatLogContains(
                SENTINEL(),
                NOOP(1, 1),
                NOOP(2, 1),
                NOOP(3, 2)
        );
        assertThatTermAndCommitIndexHaveValues(3, 0);
    }

    @Test
    public void shouldConcludeElectionAndSendNoMoreRequestVoteRPCsAndIssueHeartbeatOnWinningAllTheVotes() throws RPCException, StorageException {
        triggerElection(1);

        Collection<RequestVote> requestVotes = getRPCs(4, RequestVote.class);
        assertThatRPCsSentTo(requestVotes, S_01, S_02, S_03, S_04);
        assertThatRequestVotesHaveValues(requestVotes, 1, 0, 0);
        assertThatNoMoreRPCsWereSent();

        algorithm.onRequestVoteReply(S_01, 1, true);
        algorithm.onRequestVoteReply(S_02, 1, true);
        algorithm.onRequestVoteReply(S_03, 1, true);
        algorithm.onRequestVoteReply(S_04, 1, true);

        // check the we became the leader
        assertThat(algorithm.getRole(), equalTo(LEADER));
        assertThat(algorithm.getLeader(), equalTo(SELF));
        verify(listener).onLeadershipChange(SELF);

        // and that we issued heartbeats to everyone
        Collection<AppendEntries> heartbeats = getRPCs(4, AppendEntries.class);
        assertThatRPCsSentTo(heartbeats, S_01, S_02, S_03, S_04);
        assertThatAppendEntriesHaveValues(heartbeats, 1, 0, 0, 0, NOOP(1, 1));
        assertThatNoMoreRPCsWereSent();

        assertThatLogContains(
                SENTINEL(),
                NOOP(1, 1)
        );
        assertThatTermAndCommitIndexHaveValues(1, 0);
    }

    @Test
    public void shouldConcludeElectionAndSendNoMoreRequestVoteRPCsAndIssueHeartbeatOnWinningAMajorityOfTheVote() throws RPCException, StorageException {
        insertIntoLog(
                SENTINEL(),
                NOOP(1, 1),
                NOOP(2, 1),
                NOOP(3, 2)
        );

        algorithm.becomeFollower(2, null);
        assertThatSelfTransitionedToFollower(2, 0, null, false);

        triggerElection(3);

        Collection<RequestVote> requestVotes = getRPCs(4, RequestVote.class);
        assertThatRPCsSentTo(requestVotes, S_01, S_02, S_03, S_04);
        assertThatRequestVotesHaveValues(requestVotes, 3, 3, 2);
        assertThatNoMoreRPCsWereSent();

        // send back positive replies from 2 nodes
        // given that we automatically vote for ourselves on becoming
        // a candidate, two additional votes are enough
        // for us to achieve election quorum and become the leader
        algorithm.onRequestVoteReply(S_01, 3, true);
        algorithm.onRequestVoteReply(S_04, 3, true);

        // check that we actually became the leader
        assertThat(algorithm.getRole(), equalTo(LEADER));
        assertThat(algorithm.getLeader(), equalTo(SELF));
        verify(listener).onLeadershipChange(SELF);

        // and that we sent out heartbeats confirming this
        Collection<AppendEntries> heartbeats = getRPCs(4, AppendEntries.class);
        assertThatRPCsSentTo(heartbeats, S_01, S_02, S_03, S_04);
        assertThatAppendEntriesHaveValues(heartbeats, 3, 0, 3, 2, NOOP(4, 3));
        assertThatNoMoreRPCsWereSent();

        assertThatLogContains(
                SENTINEL(),
                NOOP(1, 1),
                NOOP(2, 1),
                NOOP(3, 2),
                NOOP(4, 3) // this NOOP is sent out immediately once we become the leader
        );
        assertThatTermAndCommitIndexHaveValues(3, 0);
    }

    @Test
    public void shouldIssueRequestVoteRPCsToNonVotingServersUntilElectionConcludes() throws StorageException {
        Collection<RequestVote> requestVotes;

        // trigger an election and issue RequestVote RPCs
        algorithm.becomeFollower(2, S_04);
        assertThatSelfTransitionedToFollower(2, 0, S_04, true);
        triggerElection(3);

        // ensure that the first batch of request votes were sent out to _all_ the servers
        requestVotes = getRPCs(4, RequestVote.class);
        assertThatRPCsSentTo(requestVotes, S_01, S_02, S_03, S_04);
        assertThatRequestVotesHaveValues(requestVotes, 3, 0, 0);
        assertThatNoMoreRPCsWereSent();

        // simulate an RPC timeout, indicating that no server voted for us
        timer.fastForward(RaftConstants.RPC_TIMEOUT);

        // check that RequestVote RPCs were issued to all servers in the cluster
        requestVotes = getRPCs(4, RequestVote.class);
        assertThatRPCsSentTo(requestVotes, S_01, S_02, S_03, S_04);
        assertThatRequestVotesHaveValues(requestVotes, 3, 0, 0);
        assertThatNoMoreRPCsWereSent();

        // get response from a single server
        algorithm.onRequestVoteReply(S_02, 3, true);

        // simulate a second RPC timeout
        timer.fastForward(RaftConstants.RPC_TIMEOUT);

        // ensure that RequestVote RPCs are only sent to _non-voting_ servers
        assertThatRequestVoteRPCsSentToNonVotingServers();
        assertThatNoMoreRPCsWereSent();

        // check that we keep sending RequestVote RPCs to the non-voting servers for term 3
        long ticksUntilElectionConcludes = getElectionTimeoutTick() - timer.getTick();
        long numRequestVoteRPCRounds = (ticksUntilElectionConcludes / RaftConstants.RPC_TIMEOUT) - 1; // subtract - 1 because the last round will occur _exactly_ on the election timeout
        for (int i = 0; i < numRequestVoteRPCRounds; i++) {
            timer.fastForward(RaftConstants.RPC_TIMEOUT);
            assertThatRequestVoteRPCsSentToNonVotingServers();
        }

        // now, move up to the election timeout
        fastForwardToElection();

        // check that the first election concluded unsuccessfully
        // and that we're still vying for election
        assertThatSelfTransitionedToCandidate(4, 0);

        // and that RequestVote RPCs were issued to all servers in the cluster for term _4_
        requestVotes = getRPCs(4, RequestVote.class);
        assertThatRPCsSentTo(requestVotes, S_01, S_02, S_03, S_04);
        assertThatRequestVotesHaveValues(requestVotes, 4, 0, 0);
        assertThatNoMoreRPCsWereSent();

        // check final state
        assertThatLogContainsOnlySentinel();
        assertThatTermAndCommitIndexHaveValues(4, 0);
    }

    private void assertThatRequestVoteRPCsSentToNonVotingServers() throws StorageException {
        Collection<RequestVote> requestVotes = getRPCs(3, RequestVote.class);
        assertThatRPCsSentTo(requestVotes, S_01, S_03, S_04);
        assertThatRequestVotesHaveValues(requestVotes, 3, 0, 0);

        // the election hasn't completed yet, so we're still a candidate for this term
        assertThat(algorithm.getRole(), equalTo(CANDIDATE));
        assertThat(algorithm.getLeader(), nullValue());

        assertThatLogContainsOnlySentinel();
        assertThatTermAndCommitIndexHaveValues(3, 0);
    }

    @Test
    public void shouldConcludeCurrentElectionAndRescheduleANewElectionForNextTermOnASplitVote() throws StorageException {
        Collection<RequestVote> requestVotes;

        algorithm.becomeFollower(2, null);
        assertThatSelfTransitionedToFollower(2, 0, null, false);

        triggerElection(3);

        // check when the election timeout will occur
        long electionTimeoutTick = getElectionTimeoutTick();

        // check that the first batch of Request Vote RPCs were sent out
        requestVotes = getRPCs(4, RequestVote.class);
        assertThatRPCsSentTo(requestVotes, S_01, S_02, S_03, S_04);
        assertThatRequestVotesHaveValues(requestVotes, 3, 0, 0);
        assertThatNoMoreRPCsWereSent();

        // receive a single positive vote before the first RPC timeout
        algorithm.onRequestVoteReply(S_02, 3, true);
        timer.fastForward(RaftConstants.RPC_TIMEOUT);

        // request votes from all non-voting servers
        requestVotes = getRPCs(3, RequestVote.class);
        assertThatRPCsSentTo(requestVotes, S_01, S_03, S_04);
        assertThatRequestVotesHaveValues(requestVotes, 3, 0, 0);
        assertThatNoMoreRPCsWereSent();

        // receive denials from two other servers
        algorithm.onRequestVoteReply(S_01, 3, false);
        algorithm.onRequestVoteReply(S_04, 3, false);

        // at this point the tally looks as follows:
        // Yay: SELF, S_02
        // Nay: S_01, S_04
        // Undecided: S_03

        // check that we continue requesting a vote from S_03
        // we want to stop just before the last round (since that's the round at which the term will switch over)
        long numRoundsTillElectionConcludes = ((electionTimeoutTick - timer.getTick()) / RaftConstants.RPC_TIMEOUT) - 1;
        for (int i = 0; i < numRoundsTillElectionConcludes; i++) {
            timer.fastForward(RaftConstants.RPC_TIMEOUT);

            requestVotes = getRPCs(1, RequestVote.class);
            assertThat(getRPCDestinations(requestVotes), contains(equalTo(S_03)));
            assertThatRequestVotesHaveValues(requestVotes, 3, 0, 0);
            assertThatNoMoreRPCsWereSent();
        }

        // at this point the state is as follows
        // 1. we're still a candidate in term 3
        // 2. no leader was chosen
        assertThat(algorithm.getRole(), equalTo(CANDIDATE));
        assertThat(algorithm.getLeader(), nullValue());
        assertThatTermAndCommitIndexHaveValues(3, 0);

        // now, move to the final election timeout
        fastForwardToElection();

        // at this point, a couple of things should have happened:
        // 1. no leader was chosen in term '3', so a new election for term '4' will be triggered
        // 2. we become a candidate for term '4'
        // 3. we send out RequestVote RPCs again
        assertThatSelfTransitionedToCandidate(4, 0);

        requestVotes = getRPCs(4, RequestVote.class);
        assertThatRPCsSentTo(requestVotes, S_01, S_02, S_03, S_04);
        assertThatRequestVotesHaveValues(requestVotes, 4, 0, 0);
        assertThatNoMoreRPCsWereSent();

        assertThatLogContainsOnlySentinel();
        assertThatTermAndCommitIndexHaveValues(4, 0);
    }

    @Test
    public void shouldIgnoreDuplicateVotes() throws StorageException {
        algorithm.becomeCandidate(3);
        assertThatSelfTransitionedToCandidate(3, 0);

        algorithm.onRequestVoteReply(S_01, 3, true);
        algorithm.onRequestVoteReply(S_01, 3, true);

        assertThatNoMoreRPCsWereSent();

        // at this point, if we double-counted votes we'd have
        // enough to become the leader (SELF + the 2 votes above >= quorum)
        assertThat(algorithm.getRole(), equalTo(CANDIDATE));

        assertThatLogContainsOnlySentinel();
        assertThatTermAndCommitIndexHaveValues(3, 0);
    }

    @Test
    public void shouldRejectRequestVoteWithOlderTerm() throws StorageException {
        algorithm.becomeFollower(3, null);
        assertThatSelfTransitionedToFollower(3, 0, null, false);

        algorithm.onRequestVote(S_01, 2, 1, 1);
        assertThatStateAfterRequestVoteIs(3, null, FOLLOWER);

        RequestVoteReply requestVoteReply = sender.nextAndRemove(RequestVoteReply.class);
        assertThatRequestVoteReplyHasValues(requestVoteReply, S_01, 3, false);
        assertThatNoMoreRPCsWereSent();

        assertThatLogContainsOnlySentinel();
        assertThatTermAndCommitIndexHaveValues(3, 0);
    }

    // for the next 3 tests, the server that requests the votes has a log that dominates ours

    @Test
    public void shouldIncreaseCurrentTermGrantVoteAndResetElectionTimeoutIfReceiveARequestVoteWithHigherTermAndCurrentlyACandidate() throws StorageException {
        algorithm.becomeCandidate(3);
        assertThatSelfTransitionedToCandidate(3, 0);

        // move time forward half-way into the election
        // assume that we're getting no replies
        long preRequestVoteElectionTimeoutTick = getElectionTimeoutTick();
        long ticksBeforeRequestVoteIsReceived = (preRequestVoteElectionTimeoutTick - timer.getTick()) / 2;
        timer.fastForward(ticksBeforeRequestVoteIsReceived);

        sender.drainSentRPCs(); // clear out all the sent RequestVote RPCs

        // get a Request Vote with a greater term
        // check that the vote was granted, and that we've shifted our state
        algorithm.onRequestVote(S_01, 4, 1, 1);
        assertThatStateAfterRequestVoteIs(4, S_01, FOLLOWER);

        RequestVoteReply requestVoteReply = sender.nextAndRemove(RequestVoteReply.class);
        assertThatRequestVoteReplyHasValues(requestVoteReply, S_01, 4, true);
        assertThatNoMoreRPCsWereSent();

        // and that we increased our election timeout
        long postRequestVoteElectionTimeoutTick = getElectionTimeoutTick();
        assertThat(postRequestVoteElectionTimeoutTick, greaterThan(preRequestVoteElectionTimeoutTick));
        assertThat(postRequestVoteElectionTimeoutTick, equalTo(timer.getTick() + RaftConstants.MIN_ELECTION_TIMEOUT));

        // and that our final state is sane
        assertThatLogContainsOnlySentinel();
        assertThatTermAndCommitIndexHaveValues(4, 0);
    }

    @Test
    public void shouldIncreaseCurrentTermGrantVoteAndResetElectionTimeoutIfReceiveARequestVoteWithHigherTermAndCurrentlyAFollower() throws StorageException {
        algorithm.becomeFollower(3, null);
        assertThatSelfTransitionedToFollower(3, 0, null, false);

        // move time forward half-way into the election
        // assume that we're getting no messages
        long preRequestVoteElectionTimeoutTick = getElectionTimeoutTick();
        long ticksBeforeRequestVoteIsReceived = (preRequestVoteElectionTimeoutTick - timer.getTick()) / 2;
        timer.fastForward(ticksBeforeRequestVoteIsReceived);

        // we should be pretty silent
        assertThatNoMoreRPCsWereSent();

        // get a Request Vote with a greater term
        // check that the vote was granted, and that we've shifted our state
        algorithm.onRequestVote(S_01, 4, 1, 1);
        assertThatStateAfterRequestVoteIs(4, S_01, FOLLOWER);

        RequestVoteReply requestVoteReply = sender.nextAndRemove(RequestVoteReply.class);
        assertThatRequestVoteReplyHasValues(requestVoteReply, S_01, 4, true);
        assertThatNoMoreRPCsWereSent();

        // and that we increased our election timeout
        long postRequestVoteElectionTimeoutTick = getElectionTimeoutTick();
        assertThat(postRequestVoteElectionTimeoutTick, greaterThan(preRequestVoteElectionTimeoutTick));
        assertThat(postRequestVoteElectionTimeoutTick, equalTo(timer.getTick() + RaftConstants.MIN_ELECTION_TIMEOUT));

        // and that our final state is sane
        assertThatLogContainsOnlySentinel();
        assertThatTermAndCommitIndexHaveValues(4, 0);
    }

    @Test
    public void shouldIncreaseCurrentTermAndGrantVoteAndResetElectionTimeoutIfReceiveARequestVoteWithHigherTermWhileCurrentlyALeader() throws StorageException {
        becomeLeaderInTerm(3, false);

        // move time forward half-way to the point at which we'd send out a heartbeat
        // assume that we're getting no replies
        long preRequestVoteHeartbeatTimeoutTick = getHeartbeatTimeoutTick();
        long ticksBeforeRequestVoteIsReceived = (preRequestVoteHeartbeatTimeoutTick - timer.getTick()) / 2;
        timer.fastForward(ticksBeforeRequestVoteIsReceived);

        sender.drainSentRPCs(); // clear out any heartbeats we may have already sent

        // get a Request Vote with a greater term and a greater prevLogTerm
        // check that the vote was granted, and that we've shifted our state
        algorithm.onRequestVote(S_01, 6, 1, 5);
        assertThatStateAfterRequestVoteIs(6, S_01, FOLLOWER);

        RequestVoteReply requestVoteReply = sender.nextAndRemove(RequestVoteReply.class);
        assertThatRequestVoteReplyHasValues(requestVoteReply, S_01, 6, true);
        assertThatNoMoreRPCsWereSent();

        // and that we increased our election timeout
        long postRequestVoteElectionTimeoutTick = getElectionTimeoutTick();
        assertThat(postRequestVoteElectionTimeoutTick, greaterThan(preRequestVoteHeartbeatTimeoutTick));
        assertThat(postRequestVoteElectionTimeoutTick, equalTo(timer.getTick() + RaftConstants.MIN_ELECTION_TIMEOUT));

        // and that our final state is sane
        assertThatLogContains(
                SENTINEL(),
                NOOP(1, 3) // the NOOP that we issued on becoming the leader
        );
        assertThatTermAndCommitIndexHaveValues(6, 0);
    }

    @Test
    public void shouldConcludeElectionAndBecomeFollowerIfAnotherServerIssuesAppendEntriesForElectionTerm() throws StorageException {
        algorithm.becomeFollower(2, null);
        assertThatSelfTransitionedToFollower(2, 0, null, false);

        // start the election in term 3
        triggerElection(3);

        long preAppendEntriesElectionTimeoutTick = getElectionTimeoutTick();
        long ticksBeforeAppendEntriesReceived = (preAppendEntriesElectionTimeoutTick - timer.getTick()) / 2;

        // ensure that we've started requesting votes
        Collection<RequestVote> requestVotes = getRPCs(4, RequestVote.class);
        assertThatRPCsSentTo(requestVotes, S_01, S_02, S_03, S_04);
        assertThatRequestVotesHaveValues(requestVotes, 3, 0, 0);
        assertThatNoMoreRPCsWereSent();

        // move time forward a bit (but not enough to actually conclude the election)
        timer.fastForward(ticksBeforeAppendEntriesReceived);

        // clear out all the Request Vote RPCs that we've sent
        sender.drainSentRPCs();

        // receive a heartbeat from a server for this term
        algorithm.onAppendEntries(S_03, 3, 0, 0, 0, null);

        // check that we become a follower for this guy
        // and that we reschedule our election timeout
        // and that we send back a nice reply
        long postAppendEntriesElectionTimeoutTick = getElectionTimeoutTick();

        assertThatSelfTransitionedToFollower(3, 0, S_03, true);
        assertThat(postAppendEntriesElectionTimeoutTick, greaterThan(preAppendEntriesElectionTimeoutTick));
        assertThat(postAppendEntriesElectionTimeoutTick, equalTo(timer.getTick() + RaftConstants.MIN_ELECTION_TIMEOUT));

        AppendEntriesReply appendEntriesReply = sender.nextAndRemove(AppendEntriesReply.class);
        assertThatAppendEntriesReplyHasValues(appendEntriesReply, S_03, 3, 0, 0, true);

        assertThatLogContainsOnlySentinel();
        assertThatTermAndCommitIndexHaveValues(3, 0);
    }

    @Test
    public void shouldConcludeElectionAndBecomeFollowerIfAnotherServerIssuesAppendEntriesForTermGreaterThanElectionTerm() throws StorageException {
        algorithm.becomeFollower(2, null);
        assertThatSelfTransitionedToFollower(2, 0, null, false);

        // start the election in term 3
        triggerElection(3);

        long preAppendEntriesElectionTimeoutTick = getElectionTimeoutTick();
        long ticksBeforeAppendEntriesReceived = (preAppendEntriesElectionTimeoutTick - timer.getTick()) / 2;

        // ensure that we've started requesting votes
        Collection<RequestVote> requestVotes = getRPCs(4, RequestVote.class);
        assertThatRPCsSentTo(requestVotes, S_01, S_02, S_03, S_04);
        assertThatRequestVotesHaveValues(requestVotes, 3, 0, 0);
        assertThatNoMoreRPCsWereSent();

        // move time forward a bit (but not enough to actually conclude the election)
        timer.fastForward(ticksBeforeAppendEntriesReceived);

        // clear out all the Request Vote RPCs that we've sent
        sender.drainSentRPCs();

        // receive a heartbeat from a server for a term greater than this term
        algorithm.onAppendEntries(S_03, 4, 0, 0, 0, null);

        // check that we become a follower for this guy
        // and that we reschedule our election timeout
        // and that we send back a nice reply
        long postAppendEntriesElectionTimeoutTick = getElectionTimeoutTick();

        assertThatSelfTransitionedToFollower(4, 0, S_03, true);
        assertThat(postAppendEntriesElectionTimeoutTick, greaterThan(preAppendEntriesElectionTimeoutTick));
        assertThat(postAppendEntriesElectionTimeoutTick, equalTo(timer.getTick() + RaftConstants.MIN_ELECTION_TIMEOUT));

        AppendEntriesReply appendEntriesReply = sender.nextAndRemove(AppendEntriesReply.class);
        assertThatAppendEntriesReplyHasValues(appendEntriesReply, S_03, 4, 0, 0, true);

        assertThatLogContainsOnlySentinel();
        assertThatTermAndCommitIndexHaveValues(4, 0);
    }

    @Test
    public void shouldConcludeElectionAndBecomeFollowerIfAnotherServerIssuesAppendEntriesForTermGreaterThanElectionTermEvenIfPrefixDoesNotMatch() throws StorageException {
        algorithm.becomeFollower(2, null);
        assertThatSelfTransitionedToFollower(2, 0, null, false);

        // start the election in term 3
        triggerElection(3);

        long preAppendEntriesElectionTimeoutTick = getElectionTimeoutTick();
        long ticksBeforeAppendEntriesReceived = (preAppendEntriesElectionTimeoutTick - timer.getTick()) / 2;

        // ensure that we've started requesting votes
        Collection<RequestVote> requestVotes = getRPCs(4, RequestVote.class);
        assertThatRPCsSentTo(requestVotes, S_01, S_02, S_03, S_04);
        assertThatRequestVotesHaveValues(requestVotes, 3, 0, 0);
        assertThatNoMoreRPCsWereSent();

        // move time forward a bit (but not enough to actually conclude the election)
        timer.fastForward(ticksBeforeAppendEntriesReceived);

        // clear out all the Request Vote RPCs that we've sent
        sender.drainSentRPCs();

        // receive a heartbeat from a server for a term greater than this term
        // notice that the prefix does not match and the commit index has been bumped up
        algorithm.onAppendEntries(S_03, 4, 1, 1, 4, null);

        // regardless of that, check that we become a follower for this guy
        // and that we reschedule our election timeout
        // and that we send back a nice reply
        long postAppendEntriesElectionTimeoutTick = getElectionTimeoutTick();

        assertThatSelfTransitionedToFollower(4, 0, S_03, true); // we can't have changed our commit index
        assertThat(postAppendEntriesElectionTimeoutTick, greaterThan(preAppendEntriesElectionTimeoutTick));
        assertThat(postAppendEntriesElectionTimeoutTick, equalTo(timer.getTick() + RaftConstants.MIN_ELECTION_TIMEOUT));

        AppendEntriesReply appendEntriesReply = sender.nextAndRemove(AppendEntriesReply.class);
        assertThatAppendEntriesReplyHasValues(appendEntriesReply, S_03, 4, 1, 0, false);

        assertThatLogContainsOnlySentinel();
        assertThatTermAndCommitIndexHaveValues(4, 0);
    }

    @Test
    public void shouldIgnoreRequestVoteReplyWithOlderTerm() throws StorageException {
        algorithm.becomeCandidate(3);
        assertThatSelfTransitionedToCandidate(3, 0);

        // notice that the first two replies list a term of '2'
        // since the cluster size is 5, if all the votes were
        // counted, then we would transition to being a
        // leader. if we don't, it means that we didn't count the
        // old vote
        algorithm.onRequestVoteReply(S_01, 2, true);
        algorithm.onRequestVoteReply(S_04, 2, true);
        algorithm.onRequestVoteReply(S_02, 3, true);

        assertThat(algorithm.getRole(), equalTo(CANDIDATE));
        assertThat(algorithm.getLeader(), nullValue());

        assertThatNoMoreRPCsWereSent();

        assertThatLogContainsOnlySentinel();
        assertThatTermAndCommitIndexHaveValues(3, 0);
    }

    @Test
    public void shouldConvertToFollowerIfReceivedARequestVoteReplyWithHigherTerm() throws StorageException {
        algorithm.becomeCandidate(3);
        assertThatSelfTransitionedToCandidate(3, 0);

        algorithm.onRequestVoteReply(S_01, 4, false);

        assertThatNoMoreRPCsWereSent();

        assertThat(store.getVotedFor(3), equalTo(SELF));
        assertThat(store.getVotedFor(4), nullValue());
        assertThat(algorithm.getRole(), equalTo(FOLLOWER));
        assertThat(algorithm.getLeader(), nullValue());

        assertThatLogContainsOnlySentinel();
        assertThatTermAndCommitIndexHaveValues(4, 0);
    }

    @Test
    public void shouldIgnoreRequestVoteReplyIfFollower() throws StorageException {
        algorithm.becomeFollower(3, null);
        assertThatSelfTransitionedToFollower(3, 0, null, false);

        algorithm.onRequestVoteReply(S_01, 3, true);
        algorithm.onRequestVoteReply(S_02, 3, true);
        algorithm.onRequestVoteReply(S_03, 3, true);
        algorithm.onRequestVoteReply(S_04, 3, true);

        assertThat(store.getVotedFor(3), nullValue());
        assertThat(algorithm.getRole(), equalTo(FOLLOWER));
        assertThat(algorithm.getLeader(), nullValue());

        assertThatNoMoreRPCsWereSent();

        assertThatLogContainsOnlySentinel();
        assertThatTermAndCommitIndexHaveValues(3, 0);
    }

    @Test
    public void shouldNotVoteForCandidateIfItsLogIsNotAPrefixOfLocalLogV1() throws StorageException {
        insertIntoLog(
                NOOP(1, 1),
                NOOP(2, 2),
                NOOP(3, 2),
                NOOP(4, 2)
        );

        algorithm.becomeFollower(3, null);
        assertThatSelfTransitionedToFollower(3, 0, null, false);

        algorithm.onRequestVote(S_01, 3, 4, 1); // we have entries from a more recent term than he does
        assertThatStateAfterRequestVoteIs(3, null, FOLLOWER);

        RequestVoteReply requestVoteReply = sender.nextAndRemove(RequestVoteReply.class);
        assertThatRequestVoteReplyHasValues(requestVoteReply, S_01, 3, false);
        assertThatNoMoreRPCsWereSent();

        assertThatLogContains(
                SENTINEL(),
                NOOP(1, 1),
                NOOP(2, 2),
                NOOP(3, 2),
                NOOP(4, 2)
        );
        assertThatTermAndCommitIndexHaveValues(3, 0);
    }

    @Test
    public void shouldNotVoteForCandidateIfItsLogIsNotAPrefixOfLocalLogV2() throws StorageException {
        insertIntoLog(
                NOOP(1, 1),
                NOOP(2, 2),
                NOOP(3, 2),
                NOOP(4, 2),
                NOOP(5, 2) // additional entry
        );

        algorithm.becomeFollower(3, null);
        assertThatSelfTransitionedToFollower(3, 0, null, false);

        algorithm.onRequestVote(S_01, 3, 4, 2); // we have more entries for the last term than he does
        assertThatStateAfterRequestVoteIs(3, null, FOLLOWER);

        RequestVoteReply requestVoteReply = sender.nextAndRemove(RequestVoteReply.class);
        assertThatRequestVoteReplyHasValues(requestVoteReply, S_01, 3, false);
        assertThatNoMoreRPCsWereSent();

        assertThatLogContains(
                SENTINEL(),
                NOOP(1, 1),
                NOOP(2, 2),
                NOOP(3, 2),
                NOOP(4, 2),
                NOOP(5, 2)
        );
        assertThatTermAndCommitIndexHaveValues(3, 0);
    }

    @Test
    public void shouldConvertFromCandidateToFollowerAndVoteForCandidateWithMoreUpToDateLogPrefix() throws StorageException {
        algorithm.becomeCandidate(3);
        assertThatSelfTransitionedToCandidate(3, 0);

        algorithm.onRequestVote(S_01, 3, 8, 2); // they have a more up-to-date log prefix than we do
        assertThatStateAfterRequestVoteIs(3, S_01, FOLLOWER);

        RequestVoteReply requestVoteReply = sender.nextAndRemove(RequestVoteReply.class);
        assertThatRequestVoteReplyHasValues(requestVoteReply, S_01, 3, true);
        assertThatNoMoreRPCsWereSent();

        assertThatLogContainsOnlySentinel();
        assertThatTermAndCommitIndexHaveValues(3, 0);
    }

    @Test
    public void shouldNeverRescindVoteToAnotherCandidate() throws StorageException {
        algorithm.becomeFollower(3, null);
        assertThatSelfTransitionedToFollower(3, 0, null, false);

        RequestVoteReply requestVoteReply;

        algorithm.onRequestVote(S_01, 3, 0, 0);
        assertThatStateAfterRequestVoteIs(3, S_01, FOLLOWER);

        requestVoteReply = sender.nextAndRemove(RequestVoteReply.class);
        assertThatRequestVoteReplyHasValues(requestVoteReply, S_01, 3, true);
        assertThatNoMoreRPCsWereSent();

        // it doesn't matter if they have a more up-to-date
        // log prefix - once we've sent out an RPC response (i.e.
        // it could be used to make decisions) we can't rescind it
        algorithm.onRequestVote(S_02, 3, 1, 1);
        assertThatStateAfterRequestVoteIs(3, S_01, FOLLOWER);

        requestVoteReply = sender.nextAndRemove(RequestVoteReply.class);
        assertThatRequestVoteReplyHasValues(requestVoteReply, S_02, 3, false);
        assertThatNoMoreRPCsWereSent();

        assertThatLogContainsOnlySentinel();
        assertThatTermAndCommitIndexHaveValues(3, 0);
    }

    @Test
    public void shouldNeverRescindVoteToAnotherCandidateEvenAfterStopStart() throws StorageException {
        algorithm.becomeFollower(3, null);
        assertThatSelfTransitionedToFollower(3, 0, null, false);

        RequestVoteReply requestVoteReply;

        algorithm.onRequestVote(S_01, 3, 0, 0);
        assertThatStateAfterRequestVoteIs(3, S_01, FOLLOWER);

        requestVoteReply = sender.nextAndRemove(RequestVoteReply.class);
        assertThatRequestVoteReplyHasValues(requestVoteReply, S_01, 3, true);
        assertThatNoMoreRPCsWereSent();

        assertThatLogContainsOnlySentinel();
        assertThatTermAndCommitIndexHaveValues(3, 0);

        algorithm.stop();
        algorithm.start();

        // it doesn't matter if they have a more up-to-date
        // log prefix - once we've sent out an RPC response (i.e.
        // it could be used to make decisions) we can't rescind it
        algorithm.onRequestVote(S_02, 3, 1, 1);
        assertThatStateAfterRequestVoteIs(3, S_01, FOLLOWER);

        requestVoteReply = sender.nextAndRemove(RequestVoteReply.class);
        assertThatRequestVoteReplyHasValues(requestVoteReply, S_02, 3, false);
        assertThatNoMoreRPCsWereSent();

        assertThatLogContainsOnlySentinel();
        assertThatTermAndCommitIndexHaveValues(3, 0);
    }

    @Test
    public void shouldRejectRequestVoteForCurrentTermIfLeaderAlreadyChosen() throws StorageException {
        algorithm.becomeFollower(3, S_01);
        assertThatSelfTransitionedToFollower(3, 0, S_01, true);

        algorithm.onRequestVote(S_02, 3, 1, 1);
        assertThat(store.getVotedFor(3), nullValue()); // let's pretend that we didn't vote for anyone
        assertThat(algorithm.getRole(), equalTo(FOLLOWER));
        assertThat(algorithm.getLeader(), equalTo(S_01));

        RequestVoteReply requestVoteReply = sender.nextAndRemove(RequestVoteReply.class);
        assertThatRequestVoteReplyHasValues(requestVoteReply, S_02, 3, false);
        assertThatNoMoreRPCsWereSent();

        assertThatLogContainsOnlySentinel();
        assertThatTermAndCommitIndexHaveValues(3, 0);
    }

    @Test
    public void shouldStepDownAsCandidateIfALeaderIsChosen() throws StorageException {
        algorithm.becomeCandidate(3);
        assertThatSelfTransitionedToCandidate(3, 0);

        algorithm.onAppendEntries(S_01, 3, 2, 3, 1, null);

        assertThat(store.getVotedFor(3), equalTo(SELF));
        assertThat(algorithm.getRole(), equalTo(FOLLOWER));
        assertThat(algorithm.getLeader(), equalTo(S_01));
        verify(listener).onLeadershipChange(S_01);
        verifyNoMoreInteractions(listener);

        AppendEntriesReply appendEntriesReply = sender.nextAndRemove(AppendEntriesReply.class);
        assertThatAppendEntriesReplyHasValues(appendEntriesReply, S_01, 3, 3, 0, false); // we're happy that they're the leader, but our prefix doesn't match
        assertThatNoMoreRPCsWereSent();

        assertThatLogContainsOnlySentinel();
        assertThatTermAndCommitIndexHaveValues(3, 0);
    }

    //================================================================================================================//
    //
    // Log Replication Tests
    //
    //================================================================================================================//

    @Test
    public void shouldIgnoreAppendEntriesWithLowerTerm() throws StorageException {
        algorithm.becomeFollower(3, S_02);
        assertThatSelfTransitionedToFollower(3, 0, S_02, true);

        // receive valid AppendEntries from an earlier term (notice that the leader is different)
        algorithm.onAppendEntries(S_01, 2, 0, 0, 0, Lists.<LogEntry>newArrayList(NOOP(1, 1)));

        // haven't added any entries to the log or changed terms
        assertThatLogContainsOnlySentinel();
        assertThatTermAndCommitIndexHaveValues(3, 0);

        // reject the AppendEntries request
        AppendEntriesReply appendEntriesReply = sender.nextAndRemove(AppendEntriesReply.class);
        assertThatAppendEntriesReplyHasValues(appendEntriesReply, S_01, 3, 0, 1, false);
        assertThatNoMoreRPCsWereSent();

        assertThatLogContainsOnlySentinel();
        assertThatTermAndCommitIndexHaveValues(3, 0);
    }

    @Test
    public void shouldIgnoreAppendEntriesWithLowerTermForSameLeader() throws StorageException, RPCException {
        // S_02 is the leader in term 2
        // S_02 issues an AppendEntries for (index, term) (1, 2)
        // S_02 crashes, reboots and gets re-elected as leader (term is now 3)
        // SELF receives knowledge (for example, RequestVote, etc.) about term 3
        // SELF then receives the old AppendEntries request, issued before S_02 crashed
        // S_02 re-issues an AppendEntries for (index, term) (5, 2)
        // SELF should simply ignore this old request,
        // otherwise it will send back 'false', forcing S_02 to decrement nextIndex

        algorithm.becomeFollower(3, S_02);
        assertThatSelfTransitionedToFollower(3, 0, S_02, true);
        verify(listener).onLeadershipChange(S_02);
        verifyNoMoreInteractions(listener);

        // receive valid AppendEntries from an earlier term (notice that the leader is same)
        // but we want to simply ignore the request
        algorithm.onAppendEntries(S_02, 2, 0, 0, 0, Lists.<LogEntry>newArrayList(NOOP(1, 1)));
        assertThatNoMoreRPCsWereSent();

        assertThatLogContainsOnlySentinel();
        assertThatTermAndCommitIndexHaveValues(3, 0);
    }

    @Test
    public void shouldResetElectionTimeoutOnReceivingAnAppendEntriesForCurrentTerm() throws StorageException {
        algorithm.becomeFollower(3, S_01);
        assertThatSelfTransitionedToFollower(3, 0, S_01, true);
        verifyNoMoreInteractions(listener); // we're notified once about a leadership change, and then never again

        long preAppendEntriesElectionTimeoutTick = getElectionTimeoutTick();

        // fast forward half-way into the election timeout period
        // this allows us to verify easily that the election timeout was reset
        long ticksBeforeAppendEntriesReceived = (preAppendEntriesElectionTimeoutTick - timer.getTick()) / 2;
        timer.fastForward(ticksBeforeAppendEntriesReceived);

        // at this point, we receive a heartbeat
        algorithm.onAppendEntries(S_01, 3, 0, 0, 0, null);

        // verify that we've reset the election timeout
        long postAppendEntriesElectionTimeoutTick = getElectionTimeoutTick();
        assertThat(postAppendEntriesElectionTimeoutTick, greaterThan(preAppendEntriesElectionTimeoutTick));
        assertThat(postAppendEntriesElectionTimeoutTick, equalTo(timer.getTick() + RaftConstants.MIN_ELECTION_TIMEOUT));

        assertThatLogContainsOnlySentinel();
        assertThatTermAndCommitIndexHaveValues(3, 0);
    }

    @Test
    public void shouldResetElectionTimeoutAndSetSendingServerAsLeaderOnReceivingAnAppendEntriesForElectionTerm() throws StorageException {
        store.setCurrentTerm(3);

        long preAppendEntriesElectionTimeoutTick = getElectionTimeoutTick();

        assertThatSelfTransitionedToFollower(3, 0, null, false);

        // fast forward half-way into the election timeout period
        // this allows us to verify easily that the election timeout was reset
        long ticksBeforeAppendEntriesReceived = (preAppendEntriesElectionTimeoutTick - timer.getTick()) / 2;
        timer.fastForward(ticksBeforeAppendEntriesReceived);

        // at this point, we receive a heartbeat
        algorithm.onAppendEntries(S_01, 3, 0, 0, 0, null);
        assertThatSelfTransitionedToFollower(3, 0, S_01, true);

        // verify that we've reset the election timeout
        long postAppendEntriesElectionTimeoutTick = getElectionTimeoutTick();
        assertThat(postAppendEntriesElectionTimeoutTick, greaterThan(preAppendEntriesElectionTimeoutTick));
        assertThat(postAppendEntriesElectionTimeoutTick, equalTo(timer.getTick() + RaftConstants.MIN_ELECTION_TIMEOUT));

        assertThatLogContainsOnlySentinel();
        assertThatTermAndCommitIndexHaveValues(3, 0);
    }

    // FIXME (AG): this test is broken
    @Test
    public void shouldIgnoreDuplicateDelayedAppendEntriesReplyForLowerTerm() throws StorageException, RPCException {
        insertIntoLog(NOOP(1, 2));
        insertIntoLog(NOOP(2, 2));

        becomeLeaderInTerm(3, false);

        Collection<AppendEntries> appendEntriesRequests;

        // drain out the heartbeats
        // notice that a NOOP entry was added for this tem when we became a leader
        appendEntriesRequests = getRPCs(4, AppendEntries.class);
        assertThatAppendEntriesHaveValues(appendEntriesRequests, 3, 0, 2, 2, NOOP(3, 3));
        assertThatNoMoreRPCsWereSent();

        // get a rejection from S_04
        // can happen because S_04 never got the entry at prevLogIndex (i.e. LogEntry.NoopEntry(2, 2))
        algorithm.onAppendEntriesReply(S_04, 3, 2, 1, false);

        // now, receive a delayed (from earlier term) duplicate rejection from S_04
        algorithm.onAppendEntriesReply(S_04, 2, 1, 1, false);
        algorithm.onAppendEntriesReply(S_04, 2, 1, 1, false);
        algorithm.onAppendEntriesReply(S_04, 2, 1, 1, false);

        // fast-forward time to simulate a heartbeat timeout
        timer.fastForward();

        // we're expecting another set of heartbeats
        // and, even though we got multiple rejections from S_04, we haven't moved its nextIndex
        appendEntriesRequests = getRPCs(4, AppendEntries.class);
        for (AppendEntries appendEntries : appendEntriesRequests) {
            if (appendEntries.server.equals(S_04)) { // we know for sure that they don't have the entry at 2, 2
                assertThatAppendEntriesHasValues(appendEntries, 3, 0, 1, 2, NOOP(2, 2), NOOP(3, 3));
            } else { // there's still a chance that these guys have the entry at index 1
                assertThatAppendEntriesHasValues(appendEntries, 3, 0, 2, 2, NOOP(3, 3));
            }
        }
        assertThatNoMoreRPCsWereSent();

        assertThatLogContains(
                SENTINEL(),
                NOOP(1, 2),
                NOOP(2, 2),
                NOOP(3, 3)
        );
        assertThatTermAndCommitIndexHaveValues(3, 0);
    }

    @Test
    public void shouldAppendSingleLogEntry() throws StorageException {
        insertIntoLog(
                SENTINEL(),
                NOOP(1, 2),
                NOOP(2, 2)
        );
        store.setCommitIndex(1);

        algorithm.becomeFollower(3, S_03);
        assertThatSelfTransitionedToFollower(3, 1, S_03, true);

        algorithm.onAppendEntries(S_03, 3, 1, 2, 2, Lists.<LogEntry>newArrayList(NOOP(3, 3)));

        AppendEntriesReply appendEntriesReply = sender.nextAndRemove(AppendEntriesReply.class);
        assertThatAppendEntriesReplyHasValues(appendEntriesReply, S_03, 3, 2, 1, true);
        assertThatNoMoreRPCsWereSent();

        assertThatLogContains(
                SENTINEL(),
                NOOP(1, 2),
                NOOP(2, 2),
                NOOP(3, 3)
        );
        assertThatTermAndCommitIndexHaveValues(3, 1);
    }

    @Test
    public void shouldAppendMultipleLogEntriesButNotCommit() throws StorageException {
        insertIntoLog(NOOP(1, 2));

        algorithm.becomeFollower(3, S_03);
        assertThatSelfTransitionedToFollower(3, 0, S_03, true);

        AppendEntriesReply appendEntriesReply;

        algorithm.onAppendEntries(S_02, 3, 0, 1, 2, Lists.<LogEntry>newArrayList(NOOP(2, 3), NOOP(3, 3)));

        appendEntriesReply = sender.nextAndRemove(AppendEntriesReply.class);
        assertThatAppendEntriesReplyHasValues(appendEntriesReply, S_02, 3, 1, 2, true);
        assertThatNoMoreRPCsWereSent();

        // receive even more updates
        // the message must have been sent before they received the response
        algorithm.onAppendEntries(S_02, 3, 0, 1, 2, Lists.<LogEntry>newArrayList(
                NOOP(2, 3),
                NOOP(3, 3),
                NOOP(4, 3)
        ));

        appendEntriesReply = sender.nextAndRemove(AppendEntriesReply.class);
        assertThatAppendEntriesReplyHasValues(appendEntriesReply, S_02, 3, 1, 3, true);
        assertThatNoMoreRPCsWereSent();

        assertThatLogContains(
                SENTINEL(),
                NOOP(1, 2),
                NOOP(2, 3),
                NOOP(3, 3),
                NOOP(4, 3)
        );
        assertThatTermAndCommitIndexHaveValues(3, 0);
    }

    @Test
    public void shouldRejectAppendEntriesIfPrefixDoesNotMatchLeaders() throws StorageException {
        // Log (SELF):
        //
        //   0   1   2
        // +---+---+---+
        // | S | 1 | 1 |
        // +---+---+---+

        // Log (S_01) Leader:
        //
        //   0   1   2
        // +---+---+---+
        // | S | 1 | 2 |
        // +---+---+---+

        insertIntoLog(
                NOOP(1, 1),
                NOOP(2, 1)
        );

        algorithm.becomeFollower(3, S_02);
        assertThatSelfTransitionedToFollower(3, 0, S_02, true);

        algorithm.onAppendEntries(S_02, 3, 0, 2, 2, Lists.<LogEntry>newArrayList(NOOP(3, 3)));

        AppendEntriesReply appendEntriesReply = sender.nextAndRemove(AppendEntriesReply.class);
        assertThatAppendEntriesReplyHasValues(appendEntriesReply, S_02, 3, 2, 1, false);
        assertThatNoMoreRPCsWereSent();

        assertThatLogContains(
                SENTINEL(),
                NOOP(1, 1),
                NOOP(2, 1)
        );
        assertThatTermAndCommitIndexHaveValues(3, 0);
    }

    @Test
    public void shouldOverwritePrefixAndAddLeadersLogEntriesIfPrevLogIndexAndTermMatch() throws StorageException {
        // Log (SELF):
        //
        //   0   1   2
        // +---+---+---+
        // | S | 1 | 1 |
        // +---+---+---+

        // Log (S_01) Leader:
        //
        //   0   1   2   3   4
        // +---+---+---+---+---+
        // | S | 1 | 2 | 3 | 3 |
        // +---+---+---+---+---+

        insertIntoLog(
                NOOP(1, 1),
                NOOP(2, 1)
        );

        algorithm.becomeFollower(3, S_02);
        assertThatSelfTransitionedToFollower(3, 0, S_02, true);

        algorithm.onAppendEntries(S_02, 3, 0, 1, 1, Lists.<LogEntry>newArrayList(
                NOOP(2, 2),
                NOOP(3, 3),
                NOOP(4, 3)
        ));

        AppendEntriesReply appendEntriesReply = sender.nextAndRemove(AppendEntriesReply.class);
        assertThatAppendEntriesReplyHasValues(appendEntriesReply, S_02, 3, 1, 3, true);
        assertThatNoMoreRPCsWereSent();

        assertThatLogContains(
                SENTINEL(),
                NOOP(1, 1),
                NOOP(2, 2),
                NOOP(3, 3),
                NOOP(4, 3)
        );
        assertThatTermAndCommitIndexHaveValues(3, 0);
    }

    @Test
    public void shouldThrowIfLeaderSendsAnAppendEntriesWithAHoleInIt() throws StorageException {
        // Log (SELF):
        //
        //   0   1   2
        // +---+---+---+
        // | S | 1 | 1 |
        // +---+---+---+

        // Log (S_01) Leader:
        //
        //   0   1   2   3   4
        // +---+---+---+---+---+
        // | S | 1 | 2 | 3 | 3 |
        // +---+---+---+---+---+

        insertIntoLog(
                NOOP(1, 1),
                NOOP(2, 1)
        );

        algorithm.becomeFollower(3, S_02);
        assertThatSelfTransitionedToFollower(3, 0, S_02, true);

        expectedException.expect(IllegalArgumentException.class);
        algorithm.onAppendEntries(S_02, 3, 0, 1, 1, Lists.<LogEntry>newArrayList(
                NOOP(3, 3),
                NOOP(4, 3)
        ));

        assertThatLogContains(
                SENTINEL(),
                NOOP(1, 1),
                NOOP(2, 1)
        );
        assertThatTermAndCommitIndexHaveValues(3, 0);
    }

    @Test
    public void shouldRescheduleElectionTimeoutOnReceivingAppendEntriesForSameTerm() throws StorageException {
        algorithm.becomeFollower(3, S_03);
        assertThatSelfTransitionedToFollower(3, 0, S_03, true);

        // get the ticks till the initial election timeout
        long initialElectionTimeout = getElectionTimeoutTick();
        assertThat(initialElectionTimeout, equalTo((long) RaftConstants.MIN_ELECTION_TIMEOUT)); // time is 0

        long electionTimeoutInterval = initialElectionTimeout - timer.getTick();
        timer.fastForward(electionTimeoutInterval / 2);

        // verify that the election timeout has still not changed
        assertThat(getElectionTimeoutTick(), equalTo(initialElectionTimeout));

        // not a heartbeat
        algorithm.onAppendEntries(S_03, 3, 0, 0, 0, Lists.<LogEntry>newArrayList(NOOP(1, 1)));

        AppendEntriesReply appendEntriesReply = sender.nextAndRemove(AppendEntriesReply.class);
        assertThatAppendEntriesReplyHasValues(appendEntriesReply, S_03, 3, 0, 1, true);
        assertThatNoMoreRPCsWereSent();

        // the new election timeout was scheduled
        // scheduled as an offset from the current tick
        assertThat(getElectionTimeoutTick(), greaterThanOrEqualTo(timer.getTick() + RaftConstants.MIN_ELECTION_TIMEOUT));

        assertThatLogContains(
                SENTINEL(),
                NOOP(1, 1)
        );
        assertThatTermAndCommitIndexHaveValues(3, 0);
    }

    @Test
    public void shouldNotRescheduleElectionTimeoutOnReceivingAppendEntriesForLowerTerm() throws StorageException {
        algorithm.becomeFollower(3, S_03);
        assertThatSelfTransitionedToFollower(3, 0, S_03, true);

        // calculate the ticks till initial election timeout
        long initialElectionTimeout = getElectionTimeoutTick();
        long electionTimeoutInterval = initialElectionTimeout - timer.getTick();

        timer.fastForward(electionTimeoutInterval / 2);

        // the election timeout is still active
        assertThat(getElectionTimeoutTick(), equalTo(initialElectionTimeout));

        // receive an AppendEntries for a lower term
        // we're also going to reject it because the sender
        // is not the same as the current leader, and they
        // may not know of the term change
        algorithm.onAppendEntries(S_02, 1, 0, 0, 0, Lists.<LogEntry>newArrayList(NOOP(1, 1)));

        AppendEntriesReply appendEntriesReply = sender.nextAndRemove(AppendEntriesReply.class);
        assertThatAppendEntriesReplyHasValues(appendEntriesReply, S_02, 3, 0, 1, false);
        assertThatNoMoreRPCsWereSent();

        // the election timeout hasn't changed
        assertThat(getElectionTimeoutTick(), equalTo(initialElectionTimeout));

        // check the leader just to make sure
        assertThat(algorithm.getLeader(), equalTo(S_03));
        assertThat(algorithm.getRole(), equalTo(FOLLOWER));

        assertThatLogContainsOnlySentinel();
        assertThatTermAndCommitIndexHaveValues(3, 0);
    }

    @Test
    public void shouldSwitchTermsAndChangeLeaderAndApplyLogChangesIfReceivingAppendEntriesWithNewerTermOnFirstBoot() throws StorageException {
        // before the election timeout trips we get an AppendEntries
        algorithm.onAppendEntries(S_04, 1, 0, 0, 0, Lists.<LogEntry>newArrayList(NOOP(1, 1)));

        AppendEntriesReply appendEntriesReply = sender.nextAndRemove(AppendEntriesReply.class);
        assertThatAppendEntriesReplyHasValues(appendEntriesReply, S_04, 1, 0, 1, true);
        assertThatNoMoreRPCsWereSent();

        assertThat(algorithm.getLeader(), equalTo(S_04));
        assertThat(algorithm.getRole(), equalTo(FOLLOWER));
        verify(listener).onLeadershipChange(S_04);

        assertThatLogContains(
                SENTINEL(),
                NOOP(1, 1)
        );
        assertThatTermAndCommitIndexHaveValues(1, 0);
    }

    @Test
    public void shouldNotNotifyListenersOfNonCommittedCommands() throws StorageException {
        insertIntoLog(NOOP(1, 1));
        store.setCommitIndex(1);

        algorithm.becomeFollower(3, S_02);
        assertThatSelfTransitionedToFollower(3, 1, S_02, true);

        UnitTestCommand command = new UnitTestCommand();
        algorithm.onAppendEntries(S_02, 3, 1, 1, 1, Lists.<LogEntry>newArrayList(CLIENT(2, 3, command)));

        AppendEntriesReply appendEntriesReply = sender.nextAndRemove(AppendEntriesReply.class);
        assertThatAppendEntriesReplyHasValues(appendEntriesReply, S_02, 3, 1, 1, true);
        assertThatNoMoreRPCsWereSent();

        verify(listener, times(0)).applyCommitted(any(Committed.class));

        assertThatLogContains(
                SENTINEL(),
                NOOP(1, 1),
                CLIENT(2, 3, command)
        );
        assertThatTermAndCommitIndexHaveValues(3, 1);
    }

    @Test
    public void shouldNeverCreateLogWithHolesInIt() throws StorageException {
        insertIntoLog(NOOP(1, 1));

        algorithm.becomeFollower(3, S_04);
        assertThatSelfTransitionedToFollower(3, 0, S_04, true);

        algorithm.onAppendEntries(S_04, 3, 0, 2, 2, Lists.<LogEntry>newArrayList(NOOP(3, 3)));

        AppendEntriesReply appendEntriesReply = sender.nextAndRemove(AppendEntriesReply.class);
        assertThatAppendEntriesReplyHasValues(appendEntriesReply, S_04, 3, 2, 1, false);
        assertThatNoMoreRPCsWereSent();

        assertThatLogContains(
                SENTINEL(),
                NOOP(1, 1)
        );
        assertThatTermAndCommitIndexHaveValues(3, 0);
    }

    // in this test I'm going to skip testing the replies
    // because they will be taken care of in other tests
    @Test
    public void shouldNeverCreateLogWithHolesInItLongerTest() throws StorageException {
        // Log (SELF):
        //
        //   0   1   2   3
        // +---+---+---+---+
        // | S | 1 |
        // +---+---+---+---+

        // Log (S_01) Leader:
        //
        //   0   1   2   3   4   5   6   7   8   9   10  11  12
        // +---+---+---+---+---+---+---+---+---+---+---+---+---+
        // | S | 1 | 1 | 1 | 1 | 1 | 2 | 2 | 2 | 2 | 3 | 3 | 3 |
        // +---+---+---+---+---+---+---+---+---+---+---+---+---+

        insertIntoLog(NOOP(1, 1));

        algorithm.becomeFollower(3, S_01);
        assertThatSelfTransitionedToFollower(3, 0, S_01, true);

        // receive a number of messages, after which, our log should look like the leader's
        algorithm.onAppendEntries(
                S_01,
                3, 0, 9, 2,
                Lists.<LogEntry>newArrayList(
                        NOOP(10, 3),
                        NOOP(11, 3),
                        NOOP(12, 3)
        ));
        algorithm.onAppendEntries(
                S_01,
                3, 0, 7, 2,
                Lists.<LogEntry>newArrayList(
                        NOOP(8, 2),
                        NOOP(9, 2),
                        NOOP(10, 3),
                        NOOP(11, 3),
                        NOOP(12, 3)
        ));
        algorithm.onAppendEntries(
                S_01,
                3, 0, 8, 2,
                Lists.<LogEntry>newArrayList(
                        NOOP(9, 2),
                        NOOP(10, 3),
                        NOOP(11, 3),
                        NOOP(12, 3)
        ));
        algorithm.onAppendEntries(
                S_01,
                3, 0, 5, 1,
                Lists.<LogEntry>newArrayList(
                        NOOP(6, 2),
                        NOOP(7, 2)
        ));
        algorithm.onAppendEntries(
                S_01,
                3, 0, 5, 1,
                Lists.<LogEntry>newArrayList(
                        NOOP(6, 2)
        ));
        algorithm.onAppendEntries(
                S_01,
                3, 0, 2, 1,
                Lists.<LogEntry>newArrayList(
                        NOOP(3, 1),
                        NOOP(4, 1)
        ));
        algorithm.onAppendEntries(
                S_01,
                3, 0, 1, 1,
                Lists.<LogEntry>newArrayList(
                        NOOP(2, 1),
                        NOOP(3, 1)
        ));
        algorithm.onAppendEntries(
                S_01,
                3, 0, 1, 1,
                Lists.<LogEntry>newArrayList(
                        NOOP(2, 1),
                        NOOP(3, 1)
        ));
        algorithm.onAppendEntries(
                S_01,
                3, 0, 4, 1,
                Lists.<LogEntry>newArrayList(
                        NOOP(5, 1),
                        NOOP(6, 2),
                        NOOP(7, 2)
        ));
        algorithm.onAppendEntries(
                S_01,
                3, 0, 2, 1,
                Lists.<LogEntry>newArrayList(
                        NOOP(3, 1),
                        NOOP(4, 1)
        ));
        algorithm.onAppendEntries(
                S_01,
                3, 0, 4, 1,
                Lists.<LogEntry>newArrayList(
                        NOOP(5, 1),
                        NOOP(6, 2),
                        NOOP(7, 2),
                        NOOP(8, 2)
        ));
        algorithm.onAppendEntries(
                S_01,
                3, 0, 9, 2,
                Lists.<LogEntry>newArrayList(
                        NOOP(10, 3),
                        NOOP(11, 3),
                        NOOP(12, 3)
        ));
        algorithm.onAppendEntries(
                S_01,
                3, 0, 6, 2,
                Lists.<LogEntry>newArrayList(
                        NOOP(7, 2),
                        NOOP(8, 2),
                        NOOP(9, 2),
                        NOOP(10, 3)
        ));
        algorithm.onAppendEntries(
                S_01,
                3, 0, 6, 2,
                Lists.<LogEntry>newArrayList(
                        NOOP(7, 2),
                        NOOP(8, 2),
                        NOOP(9, 2),
                        NOOP(10, 3)
        ));
        algorithm.onAppendEntries(
                S_01,
                3, 0, 11, 3,
                Lists.<LogEntry>newArrayList(
                        NOOP(12, 3)
        ));
        algorithm.onAppendEntries(
                S_01,
                3, 0, 10, 3,
                Lists.<LogEntry>newArrayList(
                        NOOP(11, 3),
                        NOOP(12, 3)
        ));

        verifyNoMoreInteractions(listener);

        assertThatLogContains(
                SENTINEL(),
                NOOP(1, 1),
                NOOP(2, 1),
                NOOP(3, 1),
                NOOP(4, 1),
                NOOP(5, 1),
                NOOP(6, 2),
                NOOP(7, 2),
                NOOP(8, 2),
                NOOP(9, 2),
                NOOP(10, 3),
                NOOP(11, 3),
                NOOP(12, 3)
        );
        assertThatTermAndCommitIndexHaveValues(3, 0);
    }

    @Test
    public void shouldAppendMultipleLogEntriesAndCommitAllOfThem() throws StorageException {
        insertIntoLog(NOOP(1, 1));
        store.setCommitIndex(1);

        algorithm.becomeFollower(3, S_04);
        assertThatSelfTransitionedToFollower(3, 1, S_04, true);

        // we're sent 3 additional log entries, all of which are committed
        algorithm.onAppendEntries(S_04, 3, 4, 1, 1, Lists.<LogEntry>newArrayList(
                NOOP(2, 2),
                NOOP(3, 2),
                NOOP(4, 3)
        ));

        // check that we ack this
        AppendEntriesReply appendEntriesReply = sender.nextAndRemove(AppendEntriesReply.class);
        assertThatAppendEntriesReplyHasValues(appendEntriesReply, S_04, 3, 1, 3, true);
        assertThatNoMoreRPCsWereSent();

        // verify that we notify the listener that these entries have been committed
        // but, since they're noops, we should simply update our concept of time, and not 'do' anything
        InOrder notificationOrder = inOrder(listener);
        notificationOrder.verify(listener).applyCommitted(argThat(isSkipAtIndex(2)));
        notificationOrder.verify(listener).applyCommitted(argThat(isSkipAtIndex(3)));
        notificationOrder.verify(listener).applyCommitted(argThat(isSkipAtIndex(4)));
        notificationOrder.verifyNoMoreInteractions();

        assertThatLogContains(
                SENTINEL(),
                NOOP(1, 1),
                NOOP(2, 2),
                NOOP(3, 2),
                NOOP(4, 3)
        );
       assertThatTermAndCommitIndexHaveValues(3, 4);
    }

    @Test
    public void shouldCommitNecessaryEntriesIfReceivingAppendEntriesWithAnIncreasedCommitIndex() throws StorageException {
        insertIntoLog(
                NOOP(1, 1),
                NOOP(2, 1),
                NOOP(3, 2),
                NOOP(4, 2),
                NOOP(5, 2),
                NOOP(6, 3)
        );
        store.setCommitIndex(3);

        algorithm.becomeFollower(3, S_04);
        assertThatSelfTransitionedToFollower(3, 3, S_04, true);

        // we're given an additional entry
        // but, it's not committed
        // only entries 4 -> 6 are committed
        algorithm.onAppendEntries(S_04, 3, 6, 6, 3, Lists.<LogEntry>newArrayList(NOOP(7, 3)));

        AppendEntriesReply appendEntriesReply = sender.nextAndRemove(AppendEntriesReply.class);
        assertThatAppendEntriesReplyHasValues(appendEntriesReply, S_04, 3, 6, 1, true);
        assertThatNoMoreRPCsWereSent();

        // verify that we notify the listener that these entries have been committed
        // but, since they're noops they're communicated to the listener as 'skip' entries
        InOrder notificationOrder = inOrder(listener);
        notificationOrder.verify(listener).applyCommitted(argThat(isSkipAtIndex(4)));
        notificationOrder.verify(listener).applyCommitted(argThat(isSkipAtIndex(5)));
        notificationOrder.verify(listener).applyCommitted(argThat(isSkipAtIndex(6)));
        notificationOrder.verifyNoMoreInteractions();

        assertThatLogContains(
                SENTINEL(),
                NOOP(1, 1),
                NOOP(2, 1),
                NOOP(3, 2),
                NOOP(4, 2),
                NOOP(5, 2),
                NOOP(6, 3),
                NOOP(7, 3)
        );
        assertThatTermAndCommitIndexHaveValues(3, 6);
    }

    @Test
    public void shouldCommitNecessaryEntriesIfReceivingHeartbeatWithAnIncreasedCommitIndex() throws StorageException {
        insertIntoLog(
                NOOP(1, 1),
                NOOP(2, 3),
                NOOP(3, 3)
        );

        // we're in term 3, with some entries, but nothing has been committed
        algorithm.becomeFollower(3, S_02);
        assertThatSelfTransitionedToFollower(3, 0, S_02, true);

        // we're notified that all the entries in our log were committed
        algorithm.onAppendEntries(S_02, 3, 3, 3, 3, null);

        AppendEntriesReply appendEntriesReply = sender.nextAndRemove(AppendEntriesReply.class);
        assertThatAppendEntriesReplyHasValues(appendEntriesReply, S_02, 3, 3, 0, true);
        assertThatNoMoreRPCsWereSent();

        // we notify the listener that these have been committed and that they're to be skipped
        InOrder notificationOrder = inOrder(listener);
        notificationOrder.verify(listener).applyCommitted(argThat(isSkipAtIndex(1)));
        notificationOrder.verify(listener).applyCommitted(argThat(isSkipAtIndex(2)));
        notificationOrder.verify(listener).applyCommitted(argThat(isSkipAtIndex(3)));
        notificationOrder.verifyNoMoreInteractions();

        assertThatLogContains(
                SENTINEL(),
                NOOP(1, 1),
                NOOP(2, 3),
                NOOP(3, 3)
        );
        assertThatTermAndCommitIndexHaveValues(3, 3);
    }

    @Test
    public void shouldNotifyListenersOfCommittedCommands() throws StorageException {
        UnitTestCommand commandAtIndex1 = new UnitTestCommand();
        UnitTestCommand commandAtIndex2 = new UnitTestCommand();
        UnitTestCommand commandAtIndex4 = new UnitTestCommand();

        // we start off with the commit index at 1
        insertIntoLog(
                CLIENT(1, 1, commandAtIndex1),
                CLIENT(2, 1, commandAtIndex2),
                NOOP(3, 3)
        );
        store.setCommitIndex(1);

        algorithm.becomeFollower(3, S_03);
        assertThatSelfTransitionedToFollower(3, 1, S_03, true);

        // we're informed of another entry at index 4, and told that everything up to this index is committed
        algorithm.onAppendEntries(S_03, 3, 4, 3, 3, Lists.<LogEntry>newArrayList(CLIENT(4, 3, commandAtIndex4)));

        AppendEntriesReply appendEntriesReply = sender.nextAndRemove(AppendEntriesReply.class);
        assertThatAppendEntriesReplyHasValues(appendEntriesReply, S_03, 3, 3, 1, true);
        assertThatNoMoreRPCsWereSent();

        InOrder notificationOrder = inOrder(listener);
        notificationOrder.verify(listener).applyCommitted(argThat(isCommittedCommandAtIndex(2, commandAtIndex2)));
        notificationOrder.verify(listener).applyCommitted(argThat(isSkipAtIndex(3)));
        notificationOrder.verify(listener).applyCommitted(argThat(isCommittedCommandAtIndex(4, commandAtIndex4)));
        notificationOrder.verifyNoMoreInteractions();

        assertThatLogContains(
                SENTINEL(),
                CLIENT(1, 1, commandAtIndex1),
                CLIENT(2, 1, commandAtIndex2),
                NOOP(3, 3),
                CLIENT(4, 3, commandAtIndex4)
        );
        assertThatTermAndCommitIndexHaveValues(3, 4);
    }

    // This case can actually happen
    // Imagine a server that received all the entries, but missed out on the
    // heartbeat that notified it of the commitIndex. That server could be elected
    // leader. It would still have the correct log prefix, but its commit index would
    // be much lower than those of the quorum
    @Test
    public void shouldApplyEntriesButNotRollBackCommitIndex() throws StorageException {
        insertIntoLog(NOOP(1, 1));
        store.setCommitIndex(1);

        algorithm.becomeFollower(3, S_04);
        assertThatSelfTransitionedToFollower(3, 1, S_04, true);

        algorithm.onAppendEntries(S_04, 3, 0, 1, 1, Lists.<LogEntry>newArrayList(NOOP(2, 3)));
        AppendEntriesReply appendEntriesReply = sender.nextAndRemove(AppendEntriesReply.class);
        assertThatAppendEntriesReplyHasValues(appendEntriesReply, S_04, 3, 1, 1, true);

        assertThatLogContains(
                SENTINEL(),
                NOOP(1, 1),
                NOOP(2, 3)
        );
        assertThatTermAndCommitIndexHaveValues(3, 1);
    }

    private void becomeLeaderInTerm3OnFirstBoot() throws StorageException {
        // only have the SENTINEL to start
        LogEntry lastLog = log.getLast();

        checkState(lastLog.getIndex() == 0);
        checkState(lastLog.getTerm() == 0);
        checkState(lastLog.getType().equals(LogEntry.Type.SENTINEL));

        // don't drain the NOOP messages because I'm going to check them all
        becomeLeaderInTerm(3, false);

        // drain out "I'm leader" messages with the NOOP entry
        Collection<AppendEntries> heartbeats = getRPCs(4, AppendEntries.class);
        assertThatAppendEntriesHaveValues(heartbeats, 3, 0, 0, 0, NOOP(1, 3));
        assertThatNoMoreRPCsWereSent();

        assertThatLogContains(
                SENTINEL(),
                NOOP(1, 3)
        );
        assertThatTermAndCommitIndexHaveValues(3, 0);

        // receive responses from everyone saying that they've added the NOOP entry
        algorithm.onAppendEntriesReply(S_01, 3, 0, 1, true);
        algorithm.onAppendEntriesReply(S_02, 3, 0, 1, true);
        algorithm.onAppendEntriesReply(S_03, 3, 0, 1, true);
        algorithm.onAppendEntriesReply(S_04, 3, 0, 1, true);

        // verify that the listener was notified of this committed noop
        verify(listener).applyCommitted(argThat(isSkipAtIndex(1)));

        // check that we've not done anything funky to the log, but we've bumped our commitIndex
        // hopefully, this means internally we've now moved nextIndex for all the servers
        assertThatLogContains(
                SENTINEL(),
                NOOP(1, 3)
        );
        assertThatTermAndCommitIndexHaveValues(3, 1);

        // verify that we've still got a valid heartbeat scheduled
        // since we haven't moved time at all, the timer's tick is sitting at 0
        // so the scheduled heartbeat is at (currentTick (0) + HEARTBEAT_INTERVAL)
        long heartbeatTimeout = getHeartbeatTimeoutTick();
        assertThat(heartbeatTimeout, equalTo((long) RaftConstants.HEARTBEAT_INTERVAL));
    }

    @Test
    public void shouldMarkEntryAsCommittedAndNotifyListenerIfCommandFromThisTermReceivesAQuorumOfAcks() throws StorageException, RPCException, NotLeaderException {
        becomeLeaderInTerm3OnFirstBoot();

        long heartbeatTimeout = getHeartbeatTimeoutTick();
        assertThat(heartbeatTimeout, equalTo((long) RaftConstants.HEARTBEAT_INTERVAL));

        // move time forward a bit, but not enough to trigger the heartbeat
        long heartbeatInterval = heartbeatTimeout - timer.getTick();
        timer.fastForward(heartbeatInterval / 2);

        // submit a command to the cluster
        UnitTestCommand command = new UnitTestCommand();
        algorithm.submitCommand(command);
        Collection<AppendEntries> appendEntriesRequests = getRPCs(4, AppendEntries.class);
        assertThatAppendEntriesHaveValues(appendEntriesRequests, 3, 1, 1, 3, CLIENT(2, 3, command));

        // even before sending anything out our log should have the entry
        assertThatLogContains(
                SENTINEL(),
                NOOP(1, 3),
                CLIENT(2, 3, command)
        );

        // move time forward a bit more, and start receiving responses
        timer.fastForward(heartbeatInterval / 4);

        // don't have a quorum after the first response
        algorithm.onAppendEntriesReply(S_01, 3, 1, 1, true);
        assertThatTermAndCommitIndexHaveValues(3, 1);

        // now we have a quorum
        algorithm.onAppendEntriesReply(S_03, 3, 1, 1, true);
        assertThatTermAndCommitIndexHaveValues(3, 2);

        // notified the listener
        verify(listener, times(1)).applyCommitted(argThat(isCommittedCommandAtIndex(2, command)));

        // move to the heartbeat timeout
        fastForwardToHeartbeat();

        // verify that our commitIndex was updated
        // and is being sent out properly with subsequent heartbeats
        Collection<AppendEntries> heartbeats = getRPCs(4, AppendEntries.class);
        for (AppendEntries heartbeat : heartbeats) {
            if (heartbeat.server.equals(S_01) || heartbeat.server.equals(S_03)) {
                assertThatAppendEntriesHasValues(heartbeat, 3, 2, 2, 3);
            } else {
                assertThatAppendEntriesHasValues(heartbeat, 3, 2, 1, 3, CLIENT(2, 3, command));
            }
        }
        assertThatNoMoreRPCsWereSent();

        assertThatLogContains(
                SENTINEL(),
                NOOP(1, 3),
                CLIENT(2, 3, command)
        );
        assertThatTermAndCommitIndexHaveValues(3, 2);
    }

    @Test
    public void shouldIgnoreDuplicateAppendEntriesReplyForUncommittedEntry() throws StorageException, RPCException, NotLeaderException {
        becomeLeaderInTerm3OnFirstBoot();

        long heartbeatTimeout = getHeartbeatTimeoutTick();
        long heartbeatInterval = heartbeatTimeout - timer.getTick();

        UnitTestCommand command = new UnitTestCommand();
        algorithm.submitCommand(command);

        // after the command is submitted it should be entered immediately into our logs
        assertThatLogContains(
                SENTINEL(),
                NOOP(1, 3),
                CLIENT(2, 3, command)
        );

        Collection<AppendEntries> appendEntriesRequests = getRPCs(4, AppendEntries.class);
        assertThatAppendEntriesHaveValues(appendEntriesRequests, 3, 1, 1, 3, CLIENT(2, 3, command));
        assertThatNoMoreRPCsWereSent();

        timer.fastForward(heartbeatInterval / 2);

        // receive duplicate AppendEntries responses from the same peer
        algorithm.onAppendEntriesReply(S_01, 3, 1, 1, true);
        algorithm.onAppendEntriesReply(S_01, 3, 1, 1, true);

        verifyNoMoreInteractions(listener);

        // if we weren't ignoring duplicate responses we should
        // commit this entry now since the cluster is only 5 servers
        // and, we shouldn't have touched the log
        assertThatLogContains(
                SENTINEL(),
                NOOP(1, 3),
                CLIENT(2, 3, command)
        );
        assertThatTermAndCommitIndexHaveValues(3, 1);
    }

    // This method sets up (and verifies) that SELF to be the leader
    // with the following parameters:
    //
    // Log:
    //
    //                   +----- NOOP issued when SELF becomes leader
    //                   |
    //                   V
    //   0   1   2   3   4   5   6   7   8   9
    // +---+---+---+---+---+---+---+---+---+---+
    // | S | 1 | 1 | 2 | 3 | 3 | 3 | 3 | 3 | 3 |
    // +---+---+---+---+---+---+---+---+---+---+
    //  ^
    //  |
    //  +----- commitIndex
    //
    // All log entries are NOOP entries
    //
    // currentTerm: 3
    // commitIndex: 0
    // lastLogIndex: 9
    // nextIndex (S_01): 4
    // nextIndex (S_02): 4
    // nextIndex (S_03): 4
    // nextIndex (S_04): 4
    private void setupLeaderForCommitUnitTests() throws StorageException {
        insertIntoLog(
                NOOP(1, 1),
                NOOP(2, 1),
                NOOP(3, 2)
        );

        // become the leader
        // also, drain the initial RPCs that the leader sends out
        // because we're going to invasively modify the log
        becomeLeaderInTerm(3, true);

        long currentTerm = store.getCurrentTerm();
        LogEntry lastLog = log.getLast();
        long lastIndex = lastLog.getIndex();

        // pretend as if we added a bunch of NOOP entries for no
        // reason. I use NOOPs becase there was a bug previously where
        // NOOP entries and CLIENT entries used separate code paths
        // and the NOOP entry path incorrectly initialized a data structure
        // used to calculate whether you'd achieved quorum or not
        // in the end, it shouldn't matter whether the entries to be committed
        // are NOOPs, CONFIGURATION, or CLIENT - they all
        // use the same commit logic
        // NOTE: these entries will go _after_ the leader's NOOP
        algorithm.addOrUpdateLogEntryWhileLeaderForUnitTestsOnly(NOOP(lastIndex + 1, currentTerm));
        algorithm.addOrUpdateLogEntryWhileLeaderForUnitTestsOnly(NOOP(lastIndex + 2, currentTerm));
        algorithm.addOrUpdateLogEntryWhileLeaderForUnitTestsOnly(NOOP(lastIndex + 3, currentTerm));
        algorithm.addOrUpdateLogEntryWhileLeaderForUnitTestsOnly(NOOP(lastIndex + 4, currentTerm));
        algorithm.addOrUpdateLogEntryWhileLeaderForUnitTestsOnly(NOOP(lastIndex + 5, currentTerm));

        // verify that we're setup correctly
        assertThatLogContains(
                SENTINEL(),
                NOOP(1, 1),
                NOOP(2, 1),
                NOOP(3, 2),
                NOOP(4, 3), // NOOP added when server becomes leader
                NOOP(5, 3),
                NOOP(6, 3),
                NOOP(7, 3),
                NOOP(8, 3),
                NOOP(9, 3)
        );

        // move to the heartbeat timeout
        timer.fastForward();

        // verify that the nextIndex value is correct
        Collection<AppendEntries> heartbeats = getRPCs(4, AppendEntries.class);
        assertThatAppendEntriesHaveValues(
                heartbeats,
                3, 0, 3, 2,
                NOOP(4, 3),
                NOOP(5, 3),
                NOOP(6, 3),
                NOOP(7, 3),
                NOOP(8, 3),
                NOOP(9, 3)
        );
        assertThatNoMoreRPCsWereSent();

        assertThatTermAndCommitIndexHaveValues(3, 0); // nothing's been committed yet
    }

    @Test
    public void shouldNotMarkEntryAsCommittedEvenIfReceiveAQuorumOfAcksUnlessEntryFromCurrentTermIsCommitted() throws RPCException, StorageException {
        setupLeaderForCommitUnitTests();

        // now that we know that nextIndex is correct, back it down appropriately
        // move everyone to agree only up to logIndex 2
        algorithm.setServerNextIndexWhileLeaderForUnitTestsOnly(S_01, 3);
        algorithm.setServerNextIndexWhileLeaderForUnitTestsOnly(S_02, 3);
        algorithm.setServerNextIndexWhileLeaderForUnitTestsOnly(S_03, 3);
        algorithm.setServerNextIndexWhileLeaderForUnitTestsOnly(S_04, 3);

        // move to the next heartbeat and verify that we've reset nextIndex properly
        timer.fastForward();

        Collection<AppendEntries> heartbeats = getRPCs(4, AppendEntries.class);
        assertThatAppendEntriesHaveValues(
                heartbeats,
                3, 0, 2, 1,
                NOOP(3, 2),
                NOOP(4, 3),
                NOOP(5, 3),
                NOOP(6, 3),
                NOOP(7, 3),
                NOOP(8, 3),
                NOOP(9, 3)
        );
        assertThatNoMoreRPCsWereSent();

        long heartbeatTimeout = getHeartbeatTimeoutTick();
        long heartbeatInterval = heartbeatTimeout - timer.getTick();

        // now, receive agreement from two people for logIndex 3
        timer.fastForward(heartbeatInterval / 4);
        algorithm.onAppendEntriesReply(S_02, 3, 2, 1, true);
        algorithm.onAppendEntriesReply(S_03, 3, 2, 1, true);

        // check that we didn't update anything!
        assertThatLogContains(
                SENTINEL(),
                NOOP(1, 1),
                NOOP(2, 1),
                NOOP(3, 2),
                NOOP(4, 3),
                NOOP(5, 3),
                NOOP(6, 3),
                NOOP(7, 3),
                NOOP(8, 3),
                NOOP(9, 3)
        );
        assertThatTermAndCommitIndexHaveValues(3, 0);
    }

    @Test
    public void shouldMarkEntryAsCommittedIfReceiveAQuorumOfAcks() throws RPCException, StorageException {
        setupLeaderForCommitUnitTests();

        long heartbeatTimeout = getHeartbeatTimeoutTick();
        long heartbeatInterval = heartbeatTimeout - timer.getTick();

        // move time forward a bit and get our first responses
        timer.fastForward(heartbeatInterval / 2);
        // the real clients won't do this, but I'm testing this for the sake
        // of commit logic
        algorithm.onAppendEntriesReply(S_03, 3, 3, 1, true); // only wants to apply only the initial NOOP
        algorithm.onAppendEntriesReply(S_01, 3, 3, 6, true); // wants to apply everything

        // at this point we've achieved quorum on the NOOP
        assertThatTermAndCommitIndexHaveValues(3, 4);

        // move time forward a bit more and get additional responses
        timer.fastForward(heartbeatInterval / 4);
        algorithm.onAppendEntriesReply(S_02, 3, 3, 2, true); // only wants to apply the initial NOOP and the term after

        // at this point we've achieved quorum on everything up to logIndex 5
        assertThatTermAndCommitIndexHaveValues(3, 5);

        // move to the heartbeat timeout and verify that we've updated
        // nextIndex and the commitIndex appropriately
        fastForwardToHeartbeat();

        Collection<AppendEntries> heartbeats = getRPCs(4, AppendEntries.class);
        for (AppendEntries heartbeat : heartbeats) {
            if (heartbeat.server.equals(S_01)) {
                assertThatAppendEntriesHasValues(heartbeat, 3, 5, 9, 3);
            } else if (heartbeat.server.equals(S_02)) {
                assertThatAppendEntriesHasValues(
                        heartbeat,
                        3, 5, 5, 3,
                        NOOP(6, 3),
                        NOOP(7, 3),
                        NOOP(8, 3),
                        NOOP(9, 3)
                );
            } else if (heartbeat.server.equals(S_03)) {
                assertThatAppendEntriesHasValues(
                        heartbeat,
                        3, 5, 4, 3,
                        NOOP(5, 3),
                        NOOP(6, 3),
                        NOOP(7, 3),
                        NOOP(8, 3),
                        NOOP(9, 3)
                );
            } else {
                assertThatAppendEntriesHasValues(
                        heartbeat,
                        3, 5, 3, 2,
                        NOOP(4, 3),
                        NOOP(5, 3),
                        NOOP(6, 3),
                        NOOP(7, 3),
                        NOOP(8, 3),
                        NOOP(9, 3)
                );
            }
        }
        assertThatNoMoreRPCsWereSent();

        // final verification
        assertThatLogContains(
                SENTINEL(),
                NOOP(1, 1),
                NOOP(2, 1),
                NOOP(3, 2),
                NOOP(4, 3),
                NOOP(5, 3),
                NOOP(6, 3),
                NOOP(7, 3),
                NOOP(8, 3),
                NOOP(9, 3)
        );
        assertThatTermAndCommitIndexHaveValues(3, 5);
    }

    @Test
    public void shouldNotMarkEntryAsCommittedIfNotReceiveAQuorumOfAcks() throws RPCException, StorageException {
        setupLeaderForCommitUnitTests();

        // receive only one ack for the NOOP entry
        algorithm.onAppendEntriesReply(S_03, 3, 3, 1, true);

        // fast forward to the next heartbeat
        timer.fastForward();

        Collection<AppendEntries> heartbeats = getRPCs(4, AppendEntries.class);
        for (AppendEntries heartbeat : heartbeats) {
            if (heartbeat.server.equals(S_03)) {
                assertThatAppendEntriesHasValues(
                        heartbeat,
                        3, 0, 4, 3,
                        NOOP(5, 3),
                        NOOP(6, 3),
                        NOOP(7, 3),
                        NOOP(8, 3),
                        NOOP(9, 3)
                );
            } else {
                assertThatAppendEntriesHasValues(
                        heartbeat,
                        3, 0, 3, 2,
                        NOOP(4, 3),
                        NOOP(5, 3),
                        NOOP(6, 3),
                        NOOP(7, 3),
                        NOOP(8, 3),
                        NOOP(9, 3)
                );
            }
        }
        assertThatNoMoreRPCsWereSent();

        // and that nothing changed in the log, term, or commitIndex
        assertThatLogContains(
                SENTINEL(),
                NOOP(1, 1),
                NOOP(2, 1),
                NOOP(3, 2),
                NOOP(4, 3),
                NOOP(5, 3),
                NOOP(6, 3),
                NOOP(7, 3),
                NOOP(8, 3),
                NOOP(9, 3)
        );
        assertThatTermAndCommitIndexHaveValues(3, 0);
    }

    @Test
    public void shouldNotMarkCommandAsCommittedIfNotReceiveAQuorumOfAcks() throws RPCException, StorageException, NotLeaderException {
        becomeLeaderInTerm3OnFirstBoot();

        long heartbeatTimeout = getHeartbeatTimeoutTick();
        long heartbeatInterval = heartbeatTimeout - timer.getTick();

        // submit the command
        UnitTestCommand command = new UnitTestCommand();
        algorithm.submitCommand(command);

        // after the command is submitted it should be entered immediately into our logs
        assertThatLogContains(
                SENTINEL(),
                NOOP(1, 3),
                CLIENT(2, 3, command)
        );

        // we submit an AppendEntries to the cluster
        Collection<AppendEntries> appendEntriesRequests = getRPCs(4, AppendEntries.class);
        assertThatAppendEntriesHaveValues(appendEntriesRequests, 3, 1, 1, 3, CLIENT(2, 3, command));
        assertThatNoMoreRPCsWereSent();

        // move forward time a bit (but not enough to trigger the heartbeat interval)
        // and get a single response
        timer.fastForward(heartbeatInterval / 2);
        algorithm.onAppendEntriesReply(S_01, 3, 1, 1, true);

        // because the entry wasn't committed the listener shouldn't be notified
        verifyNoMoreInteractions(listener);

        // now, move forward to the heartbeat
        // and check the outgoing heartbeats
        fastForwardToHeartbeat();

        Collection<AppendEntries> heartbeats = getRPCs(4, AppendEntries.class);
        for (AppendEntries heartbeat : heartbeats) {
            if (heartbeat.server.equals(S_01)) {
                assertThatAppendEntriesHasValues(heartbeat, 3, 1, 2, 3); // got a response, so they're up-to-date
            } else {
                assertThatAppendEntriesHasValues(heartbeat, 3, 1, 1, 3, CLIENT(2, 3, command));  // assume that the others aren't
            }
        }

        // nothing should have changed with the log or the commitIndex
        assertThatLogContains(
                SENTINEL(),
                NOOP(1, 3),
                CLIENT(2, 3, command)
        );
        assertThatTermAndCommitIndexHaveValues(3, 1);
    }

    @Test
    public void shouldKeepReissuingAppendEntriesToServersThatDidNotSendBackAnAppendEntriesReply() throws RPCException, StorageException, NotLeaderException {
        becomeLeaderInTerm3OnFirstBoot();

        Collection<AppendEntries> appendEntriesRequests;
        long heartbeatInterval;

        // get the time till the first heartbeat timeout
        long firstHeartbeatTimeout = getHeartbeatTimeoutTick();
        heartbeatInterval = firstHeartbeatTimeout - timer.getTick();

        UnitTestCommand command1 = new UnitTestCommand();
        UnitTestCommand command2 = new UnitTestCommand();

        // --- COMMAND 1
        algorithm.submitCommand(command1);

        appendEntriesRequests = getRPCs(4, AppendEntries.class);
        assertThatAppendEntriesHaveValues(appendEntriesRequests, 3, 1, 1, 3, CLIENT(2, 3, command1));
        assertThatNoMoreRPCsWereSent();

        // move forward a bit, but not enough to trigger the heartbeat timeout
        timer.fastForward(heartbeatInterval / 2);

        // --- S_01 responds
        algorithm.onAppendEntriesReply(S_01, 3, 1, 1, true);
        assertThatNoMoreRPCsWereSent();

        // --- FIRST HEARTBEAT TRIGGERS
        fastForwardToHeartbeat();

        // get the time till the second heartbeat timeout
        long secondHeartbeatTimeout = getHeartbeatTimeoutTick();
        assertThat(secondHeartbeatTimeout, greaterThanOrEqualTo(firstHeartbeatTimeout));
        assertThat(secondHeartbeatTimeout, equalTo(timer.getTick() + RaftConstants.HEARTBEAT_INTERVAL));

        heartbeatInterval = secondHeartbeatTimeout - timer.getTick();

        // verify the heartbeat messages going out
        appendEntriesRequests = getRPCs(4, AppendEntries.class);
        for (AppendEntries heartbeat : appendEntriesRequests) {
            if (heartbeat.server.equals(S_01)) {
                assertThatAppendEntriesHasValues(heartbeat, 3, 1, 2, 3);
            } else {
                assertThatAppendEntriesHasValues(heartbeat, 3, 1, 1, 3, CLIENT(2, 3, command1));
            }
        }
        assertThatNoMoreRPCsWereSent();

        // check that our state hasn't changed after the heartbeat
        assertThatTermAndCommitIndexHaveValues(3, 1);

        // move time forward just a bit, but not enough to trigger another heartbeat
        timer.fastForward(heartbeatInterval / 4);

        // --- COMMAND 2
        algorithm.submitCommand(command2);

        // verify the append entries messages going out
        appendEntriesRequests = getRPCs(4, AppendEntries.class);
        for (AppendEntries appendEntries : appendEntriesRequests) {
            if (appendEntries.server.equals(S_01)) {
                assertThatAppendEntriesHasValues(appendEntries, 3, 1, 2, 3, CLIENT(3, 3, command2));
            } else {
                assertThatAppendEntriesHasValues(
                        appendEntries,
                        3, 1, 1, 3,
                        CLIENT(2, 3, command1),
                        CLIENT(3, 3, command2));
            }
        }
        assertThatNoMoreRPCsWereSent();

        // --- SECOND HEARTBEAT TRIGGERS
        fastForwardToHeartbeat();

        assertThatTermAndCommitIndexHaveValues(3, 1);

        // get the time till the third heartbeat timeout
        long thirdHeartbeatTimeout = getHeartbeatTimeoutTick();
        assertThat(thirdHeartbeatTimeout, greaterThanOrEqualTo(secondHeartbeatTimeout));
        assertThat(thirdHeartbeatTimeout, equalTo(timer.getTick() + RaftConstants.HEARTBEAT_INTERVAL));

        heartbeatInterval = thirdHeartbeatTimeout - timer.getTick();

        // verify the heartbeat messages going out
        appendEntriesRequests = getRPCs(4, AppendEntries.class);
        for (AppendEntries appendEntries : appendEntriesRequests) {
            if (appendEntries.server.equals(S_01)) {
                assertThatAppendEntriesHasValues(appendEntries, 3, 1, 2, 3, CLIENT(3, 3, command2));
            } else {
                assertThatAppendEntriesHasValues(
                        appendEntries,
                        3, 1, 1, 3,
                        CLIENT(2, 3, command1),
                        CLIENT(3, 3, command2));
            }
        }
        assertThatNoMoreRPCsWereSent();

        // check that our state hasn't changed after the heartbeat
        assertThatTermAndCommitIndexHaveValues(3, 1);

        // --- S_03 responds
        timer.fastForward(heartbeatInterval / 4);

        algorithm.onAppendEntriesReply(S_03, 3, 1, 1, true);
        assertThatNoMoreRPCsWereSent();

        // we can commit command1 now because 3 people in the cluster support it
        assertThatTermAndCommitIndexHaveValues(3, 2);
        verify(listener).applyCommitted(argThat(isCommittedCommandAtIndex(2, command1)));

        // --- S_04 responds
        timer.fastForward(heartbeatInterval / 4);

        algorithm.onAppendEntriesReply(S_04, 3, 1, 2, true);

        assertThatTermAndCommitIndexHaveValues(3, 2);

        assertThatNoMoreRPCsWereSent();

        // --- THIRD HEARTBEAT TRIGGERS
        fastForwardToHeartbeat();

        assertThatTermAndCommitIndexHaveValues(3, 2);

        // get the time till the fourth heartbeat timeout
        long fourthHeartbeatTimeout = getHeartbeatTimeoutTick();
        assertThat(fourthHeartbeatTimeout, greaterThanOrEqualTo(thirdHeartbeatTimeout));
        assertThat(fourthHeartbeatTimeout, equalTo(timer.getTick() + RaftConstants.HEARTBEAT_INTERVAL));

        // verify the heartbeat messages going out
        appendEntriesRequests = getRPCs(4, AppendEntries.class);
        for (AppendEntries appendEntries : appendEntriesRequests) {
            if (appendEntries.server.equals(S_01) || appendEntries.server.equals(S_03)) {
                assertThatAppendEntriesHasValues(appendEntries, 3, 2, 2, 3, CLIENT(3, 3, command2)); // missing the last entry
            } else if (appendEntries.server.equals(S_04)) {
                assertThatAppendEntriesHasValues(appendEntries, 3, 2, 3, 3); // has all the entries
            } else {
                assertThatAppendEntriesHasValues(
                        appendEntries,
                        3, 2, 1, 3,
                        CLIENT(2, 3, command1),
                        CLIENT(3, 3, command2));
            }
        }
        assertThatNoMoreRPCsWereSent();

        verifyNoMoreInteractions(listener);

        assertThatLogContains(
                SENTINEL(),
                NOOP(1, 3),
                CLIENT(2, 3, command1),
                CLIENT(3, 3, command2)
        );
        assertThatTermAndCommitIndexHaveValues(3, 2);
    }

    @Test
    public void shouldIgnoreDelayedAppendEntriesReplies() throws RPCException, StorageException, NotLeaderException {
        becomeLeaderInTerm3OnFirstBoot();

        Collection<AppendEntries> appendEntriesRequests;

        UnitTestCommand command1 = new UnitTestCommand();
        UnitTestCommand command2 = new UnitTestCommand();

        // get the heartbeat timeout
        long heartbeatTimeout = getHeartbeatTimeoutTick();
        long heartbeatInterval = heartbeatTimeout - timer.getTick();

        // submit a command
        algorithm.submitCommand(command1);
        appendEntriesRequests = getRPCs(4, AppendEntries.class);
        assertThatAppendEntriesHaveValues(appendEntriesRequests, 3, 1, 1, 3, CLIENT(2, 3, command1));
        assertThatNoMoreRPCsWereSent();

        // move time forward a bit (but not enough to trigger the heartbeat)
        timer.fastForward(heartbeatInterval / 3);

        // get enough responses to get a quorum
        algorithm.onAppendEntriesReply(S_04, 3, 1, 1, true);
        algorithm.onAppendEntriesReply(S_02, 3, 1, 1, true);

        assertThatTermAndCommitIndexHaveValues(3, 2);
        verify(listener, times(1)).applyCommitted(argThat(isCommittedCommandAtIndex(2, command1)));

        // move time forward a bit more
        timer.fastForward(heartbeatInterval / 3);

        // now, submit a second command
        algorithm.submitCommand(command2);
        appendEntriesRequests = getRPCs(4, AppendEntries.class);
        for (AppendEntries appendEntries : appendEntriesRequests) {
            if (appendEntries.server.equals(S_04) || appendEntries.server.equals(S_02)) {
                assertThatAppendEntriesHasValues(appendEntries, 3, 2, 2, 3, CLIENT(3, 3, command2));
            } else {
                assertThatAppendEntriesHasValues(
                        appendEntries,
                        3, 2, 1, 3,
                        CLIENT(2, 3, command1),
                        CLIENT(3, 3, command2));
            }
        }
        assertThatNoMoreRPCsWereSent();

        // move time forward a bit more
        timer.fastForward(heartbeatInterval / 6);

        // ... and ... suddenly the network goes crazy
        // duplicating and replaying messages all over the place
        // I know, more doesn't matter...but...cut'n'paste guys!
        algorithm.onAppendEntriesReply(S_04, 3, 1, 1, true);
        algorithm.onAppendEntriesReply(S_02, 3, 1, 1, true);
        algorithm.onAppendEntriesReply(S_04, 3, 1, 1, true);
        algorithm.onAppendEntriesReply(S_02, 3, 1, 1, true);
        algorithm.onAppendEntriesReply(S_02, 3, 1, 1, true);
        algorithm.onAppendEntriesReply(S_02, 3, 1, 1, true);

        // despite all this, we keep our cool
        verifyNoMoreInteractions(listener);
        assertThatLogContains(
                SENTINEL(),
                NOOP(1, 3),
                CLIENT(2, 3, command1),
                CLIENT(3, 3, command2)
        );
        assertThatTermAndCommitIndexHaveValues(3, 2);
    }

    @Test
    public void shouldIssueNoopToCommitEntriesCreatedInPreviousTermWhenElected() throws StorageException, RPCException {
        insertIntoLog(NOOP(1, 1));

        // become the leader
        // should send an "I am leader" message out immediately
        // with an entry for this term to try and force a commit
        becomeLeaderInTerm(3, false);
        Collection<AppendEntries> appendEntriesRequests = getRPCs(4, AppendEntries.class);
        assertThatAppendEntriesHaveValues(appendEntriesRequests, 3, 0, 1, 1, NOOP(2, 3));
        assertThatNoMoreRPCsWereSent();

        // we haven't notified the listener,
        // but we've added a NOOP entry to the log
        // and haven't touched the commitIndex
        verifyNoMoreInteractions(listener);
        assertThatLogContains(
                SENTINEL(),
                NOOP(1, 1),
                NOOP(2, 3)
        );
        assertThatTermAndCommitIndexHaveValues(3, 0);
    }

    @Test
    // DO NOT DELETE THIS TEST! IMPORTANT! VERIFIES THAT SELF-GENERATED NOOPs ARE PROPERLY INITIALIZED
    public void shouldInitializeAsLeaderCorrectlyAndCommitLeaderNoopIfQuorumOfAcksReceived() throws StorageException, RPCException {
        // only have the SENTINEL to start
        assertThatLogContains(
                SENTINEL()
        );

        becomeLeaderInTerm(3, false);

        // drain out "I'm leader" messages with the NOOP entry
        Collection<AppendEntries> heartbeats = getRPCs(4, AppendEntries.class);
        assertThatAppendEntriesHaveValues(heartbeats, 3, 0, 0, 0, NOOP(1, 3));
        assertThatNoMoreRPCsWereSent();

        assertThatLogContains(
                SENTINEL(),
                NOOP(1, 3)
        );
        assertThatTermAndCommitIndexHaveValues(3, 0);

        // receive responses from a couple of people (just enough to achieve quorum)
        // saying that they've added the NOOP entry
        algorithm.onAppendEntriesReply(S_01, 3, 0, 1, true);
        algorithm.onAppendEntriesReply(S_02, 3, 0, 1, true);

        // check that we've not done anything funky to the log, but we've bumped our commitIndex
        assertThatLogContains(
                SENTINEL(),
                NOOP(1, 3)
        );
        assertThatTermAndCommitIndexHaveValues(3, 1);

        // verify that we've still got a valid heartbeat scheduled
        // since we haven't moved time at all, the timer's tick is sitting at 0
        // so the scheduled heartbeat is at (currentTick (0) + HEARTBEAT_INTERVAL)
        long heartbeatTimeout = getHeartbeatTimeoutTick();
        assertThat(heartbeatTimeout, equalTo((long) RaftConstants.HEARTBEAT_INTERVAL));
    }

    @Test
    public void shouldDecrementNextIndexWhenReceivingAppendEntriesRejectToFindMatchingPrefix() throws StorageException, RPCException {
        insertIntoLog(
                NOOP(1, 1),
                NOOP(2, 1),
                NOOP(3, 2)
        );
        store.setCommitIndex(1);

        // become the leader
        becomeLeaderInTerm(3, false);

        // ... which should trigger a NOOP being added for this term
        assertThatLogContains(
                SENTINEL(),
                NOOP(1, 1),
                NOOP(2, 1),
                NOOP(3, 2),
                NOOP(4, 3)
        );

        Collection<AppendEntries> appendEntriesRequests;

        // check the "I am leader messages"
        appendEntriesRequests = getRPCs(4, AppendEntries.class);
        assertThatAppendEntriesHaveValues(appendEntriesRequests, 3, 1, 3, 2, NOOP(4, 3));
        assertThatNoMoreRPCsWereSent();

        // apparently S_03 does not have the same log you do
        algorithm.onAppendEntriesReply(S_03, 3, 3, 1, false);

        // move to the heartbeat timeout
        timer.fastForward();

        // again, heartbeat, but with more entries (to try overwrite their prefix)
        appendEntriesRequests = getRPCs(4, AppendEntries.class);
        for (AppendEntries appendEntries : appendEntriesRequests) {
            if (appendEntries.server.equals(S_03)) { // this server should get two entries (implying that nextIndex was decremented)
                assertThatAppendEntriesHasValues(
                        appendEntries,
                        3, 1, 2, 1,
                        NOOP(3, 2),
                        NOOP(4, 3));
            } else { // everyone gets the heartbeat with the noop entry only
                assertThatAppendEntriesHasValues(appendEntries, 3, 1, 3, 2, NOOP(4, 3));
            }
        }
        assertThatNoMoreRPCsWereSent();

        // check that no one was notified and that nothing changed
        // with the log or commitIndex
        verifyNoMoreInteractions(listener);
        assertThatLogContains(
                SENTINEL(),
                NOOP(1, 1),
                NOOP(2, 1),
                NOOP(3, 2),
                NOOP(4, 3)
        );
        assertThatTermAndCommitIndexHaveValues(3, 1);
    }

    @Test
    public void shouldIncrementNextIndexAfterReceivingAppliedAppendEntriesReply() throws StorageException, RPCException {
        insertIntoLog(
                NOOP(1, 1),
                NOOP(2, 1),
                NOOP(3, 2),
                NOOP(4, 2)
        );
        store.setCommitIndex(2);

        becomeLeaderInTerm(3, false);

        assertThatLogContains(
                SENTINEL(),
                NOOP(1, 1),
                NOOP(2, 1),
                NOOP(3, 2),
                NOOP(4, 2),
                NOOP(5, 3)
        );
        assertThatTermAndCommitIndexHaveValues(3, 2);

        // drain out the original "I am leader" messages because I'm going to modify nextIndex
        sender.drainSentRPCs();

        algorithm.setServerNextIndexWhileLeaderForUnitTestsOnly(S_01, 2);
        algorithm.setServerNextIndexWhileLeaderForUnitTestsOnly(S_02, 2);
        algorithm.setServerNextIndexWhileLeaderForUnitTestsOnly(S_03, 2);
        algorithm.setServerNextIndexWhileLeaderForUnitTestsOnly(S_04, 2);

        Collection<AppendEntries> heartbeats;

        // move to the heartbeat timeout and check the heartbeats
        fastForwardToHeartbeat();
        heartbeats = getRPCs(4, AppendEntries.class);
        assertThatAppendEntriesHaveValues(heartbeats,
                3, 2, 1, 1,
                NOOP(2, 1),
                NOOP(3, 2),
                NOOP(4, 2),
                NOOP(5, 3)
        );
        assertThatNoMoreRPCsWereSent();

        long heartbeatTimeout = getHeartbeatTimeoutTick();
        long heartbeatInterval = heartbeatTimeout - timer.getTick();

        // move time forward a bit and get a response
        timer.fastForward(heartbeatInterval / 2);
        algorithm.onAppendEntriesReply(S_02, 3, 1, 2, true); // applied a couple of entries

        // move forward to the heartbeat timeout
        fastForwardToHeartbeat();
        heartbeats = getRPCs(4, AppendEntries.class);
        for (AppendEntries heartbeat : heartbeats) {
            if (heartbeat.server.equals(S_02)) { // nextIndex should have moved up
                assertThatAppendEntriesHasValues(
                        heartbeat,
                        3, 2, 3, 2,
                        NOOP(4, 2),
                        NOOP(5, 3)
                );
            } else {
                assertThatAppendEntriesHasValues(
                        heartbeat,
                        3, 2, 1, 1,
                        NOOP(2, 1),
                        NOOP(3, 2),
                        NOOP(4, 2),
                        NOOP(5, 3)
                );
            }
        }

        assertThatLogContains(
                SENTINEL(),
                NOOP(1, 1),
                NOOP(2, 1),
                NOOP(3, 2),
                NOOP(4, 2),
                NOOP(5, 3)
        );
        assertThatTermAndCommitIndexHaveValues(3, 2);
    }

    @Test
    public void shouldNotCommitEntriesUnlessAnEntryFromTheCurrentTermIsCommitted() throws StorageException, RPCException {
        insertIntoLog(
                NOOP(1, 1),
                NOOP(2, 1),
                NOOP(3, 2)
        );
        store.setCommitIndex(1);

        becomeLeaderInTerm(3, false);

        Collection<AppendEntries> appendEntriesRequests;

        // after we become a leader we should automatically
        // generate a NOOP in this term and send it out in our first
        // "I am leader" message
        assertThatLogContains(
                SENTINEL(),
                NOOP(1, 1),
                NOOP(2, 1),
                NOOP(3, 2),
                NOOP(4, 3)
        );

        // check the "I am leader messages)
        appendEntriesRequests = getRPCs(4, AppendEntries.class);
        assertThatAppendEntriesHaveValues(appendEntriesRequests, 3, 1, 3, 2, NOOP(4, 3));
        assertThatNoMoreRPCsWereSent();

        // apparently no one has the same log you do
        algorithm.onAppendEntriesReply(S_03, 3, 3, 1, false);
        algorithm.onAppendEntriesReply(S_01, 3, 3, 1, false);
        algorithm.onAppendEntriesReply(S_04, 3, 3, 1, false);
        algorithm.onAppendEntriesReply(S_02, 3, 3, 1, false);

        // move to the heartbeat timeout
        timer.fastForward();

        // again, heartbeat, but with more entries (to try overwrite their prefix)
        appendEntriesRequests = getRPCs(4, AppendEntries.class);
        assertThatAppendEntriesHaveValues(
                appendEntriesRequests,
                3, 1, 2, 1,
                NOOP(3, 2),
                NOOP(4, 3));
        assertThatNoMoreRPCsWereSent();

        // OK, this is broken (i.e. the real followers won't do this), but I'm
        // doing this just to check the commit logic

        // everyone's happy, but they only want to commit one entry, the one from the previous term
        algorithm.onAppendEntriesReply(S_03, 3, 2, 1, true);
        algorithm.onAppendEntriesReply(S_01, 3, 2, 1, true);
        algorithm.onAppendEntriesReply(S_04, 3, 2, 1, true);
        algorithm.onAppendEntriesReply(S_02, 3, 2, 1, true);


        // it's nice that we have quorum for an entry in the older term,
        // but unless they also accept an entry from this term I can't consider
        // it committed
        // so, the listener shouldn't be notified and the commit index shouldn't be changed
        verifyNoMoreInteractions(listener);
        assertThatLogContains(
                SENTINEL(),
                NOOP(1, 1),
                NOOP(2, 1),
                NOOP(3, 2),
                NOOP(4, 3)
        );
        assertThatTermAndCommitIndexHaveValues(3, 1);
    }

    @Test
    public void shouldStepDownAsLeaderIfReceiveAnAppendEntriesReplyWithAHigherTerm() throws RPCException, StorageException, NotLeaderException {
        becomeLeaderInTerm3OnFirstBoot();

        long heartbeatTimeout = getHeartbeatTimeoutTick();
        long heartbeatInterval = heartbeatTimeout - timer.getTick();

        // submit a new command
        UnitTestCommand command = new UnitTestCommand();
        algorithm.submitCommand(command);

        Collection<AppendEntries> appendEntriesRequests = getRPCs(4, AppendEntries.class);
        assertThatAppendEntriesHaveValues(appendEntriesRequests, 3, 1, 1, 3, CLIENT(2, 3, command));
        assertThatNoMoreRPCsWereSent();

        // move time forward incrementally, and get two responses
        timer.fastForward(heartbeatInterval / 4);

        // applied
        algorithm.onAppendEntriesReply(S_03, 3, 1, 1, true);

        // uhoh, not applied, and, they have a larger term!
        algorithm.onAppendEntriesReply(S_02, 4, 1, 1, false);

        // we should transition to follower
        // but, we don't know who the leader is yet
        assertThatSelfTransitionedToFollower(4, 1, null, true);

        // although we wrote the submitted command to our log,
        // we haven't changed the commitIndex
        // and, we haven't notified the listener
        verifyNoMoreInteractions(listener);
        assertThatLogContains(
                SENTINEL(),
                NOOP(1, 3),
                CLIENT(2, 3, command)
        );
        assertThatTermAndCommitIndexHaveValues(4, 1);
    }

    @Test
    public void shouldIgnoreDelayedAppendEntriesReplyWherePrevLogIndexGreaterThanNextIndexMinusOneAndAppliedIsFalse() throws StorageException, RPCException {
        insertIntoLog(
                NOOP(1, 1),
                NOOP(2, 1),
                NOOP(3, 2)
        );

        becomeLeaderInTerm(3, false);

        assertThatLogContains(
                SENTINEL(),
                NOOP(1, 1),
                NOOP(2, 1),
                NOOP(3, 2),
                NOOP(4, 3)
        );

        Collection<AppendEntries> appendEntriesRequests;

        appendEntriesRequests = getRPCs(4, AppendEntries.class);
        assertThatAppendEntriesHaveValues(appendEntriesRequests, 3, 0, 3, 2, NOOP(4, 3));
        assertThatNoMoreRPCsWereSent();

        // S_02 says it doesn't have entry at index 3
        algorithm.onAppendEntriesReply(S_02, 3, 3, 1, false);

        // move to the next heartbeat
        timer.fastForward();

        // send heartbeats again
        appendEntriesRequests = getRPCs(4, AppendEntries.class);
        for(AppendEntries appendEntries : appendEntriesRequests) {
            if (appendEntries.server.equals(S_02)) { // know for sure they're behind
                assertThatAppendEntriesHasValues(appendEntries,
                        3, 0, 2, 1,
                        NOOP(3, 2),
                        NOOP(4, 3));
            } else { // there's a chance the others are not
                assertThatAppendEntriesHasValues(appendEntries, 3, 0, 3, 2, NOOP(4, 3));
            }
        }
        assertThatNoMoreRPCsWereSent();

        // S_02 says it doesn't have entry at index 2 either
        algorithm.onAppendEntriesReply(S_02, 3, 2, 2, false);

        // move to the next heartbeat
        timer.fastForward();

        // send heartbeats again
        appendEntriesRequests = getRPCs(4, AppendEntries.class);
        for(AppendEntries appendEntries : appendEntriesRequests) {
            if (appendEntries.server.equals(S_02)) { // know for sure they're behind
                assertThatAppendEntriesHasValues(appendEntries,
                        3, 0, 1, 1,
                        NOOP(2, 1),
                        NOOP(3, 2),
                        NOOP(4, 3));
            } else {
                assertThatAppendEntriesHasValues(appendEntries, 3, 0, 3, 2, NOOP(4, 3));
            }
        }
        assertThatNoMoreRPCsWereSent();

        // S_02 _repeats_ that it doesn't have entry at index 2 either
        algorithm.onAppendEntriesReply(S_02, 3, 2, 2, false);

        // move to the next heartbeat
        timer.fastForward();

        // send heartbeats again
        appendEntriesRequests = getRPCs(4, AppendEntries.class);
        for(AppendEntries appendEntries : appendEntriesRequests) {
            if (appendEntries.server.equals(S_02)) { // we shouldn't have changed nextIndex despite the repeat
                assertThatAppendEntriesHasValues(appendEntries,
                        3, 0, 1, 1,
                        NOOP(2, 1),
                        NOOP(3, 2),
                        NOOP(4, 3));
            } else {
                assertThatAppendEntriesHasValues(appendEntries, 3, 0, 3, 2, NOOP(4, 3));
            }
        }
        assertThatNoMoreRPCsWereSent();

        assertThatLogContains(
                SENTINEL(),
                NOOP(1, 1),
                NOOP(2, 1),
                NOOP(3, 2),
                NOOP(4, 3)
        );
        assertThatTermAndCommitIndexHaveValues(3, 0);
    }

    @Test
    public void shouldIgnoreDelayedAppendEntriesReplyWherePrevLogIndexLessThanNextIndexMinusOneAndAppliedIsFalse() throws StorageException, RPCException {
        insertIntoLog(
                SENTINEL(),
                NOOP(1, 1),
                NOOP(2, 2),
                NOOP(3, 2),
                NOOP(4, 2),
                NOOP(5, 2)
        );
        store.setCommitIndex(1);

        becomeLeaderInTerm(3, true);

        // The test subject will be S_02
        // we imagine that S_02 was completely unavailable for term 2, and so missed
        // all the log entries that were replicated during that term
        // it came alive during this term and now the leader's trying to roll back
        // to find out at exactly what point its prefix matches. Apparently the
        // rollback worked, and now, as the nextIndex is moving forward, a delayed
        // message shows up

        algorithm.setServerNextIndexWhileLeaderForUnitTestsOnly(S_02, 5);

        timer.fastForward();

        Collection<AppendEntries> appendEntriesRequests;

        appendEntriesRequests = getRPCs(4, AppendEntries.class);
        for (AppendEntries appendEntries : appendEntriesRequests) {
            if (appendEntries.server.equals(S_02)) {
                assertThatAppendEntriesHasValues(
                        appendEntries,
                        3, 1, 4, 2,
                        NOOP(5, 2),
                        NOOP(6, 3)
                );
            } else {
                assertThatAppendEntriesHasValues(appendEntries, 3, 1, 5, 2, NOOP(6, 3));
            }
        }

        algorithm.onAppendEntriesReply(S_02, 3, 2, 1, false);

        timer.fastForward();

        appendEntriesRequests = getRPCs(4, AppendEntries.class);
        for (AppendEntries appendEntries : appendEntriesRequests) {
            if (appendEntries.server.equals(S_02)) {
                assertThatAppendEntriesHasValues(
                        appendEntries,
                        3, 1, 4, 2,
                        NOOP(5, 2),
                        NOOP(6, 3)
                );
            } else {
                assertThatAppendEntriesHasValues(appendEntries, 3, 1, 5, 2, NOOP(6, 3));
            }
        }

        assertThatLogContains(
                SENTINEL(),
                NOOP(1, 1),
                NOOP(2, 2),
                NOOP(3, 2),
                NOOP(4, 2),
                NOOP(5, 2),
                NOOP(6, 3)
        );
    }

    @Test
    public void shouldIgnoreDelayedAppendEntriesReplyWherePrevLogIndexPlusAppliedEntryCountLessThanNextIndex() throws StorageException, RPCException {
        // start off with a simple log where everything is committed
        insertIntoLog(
                SENTINEL(),
                NOOP(1, 1)
        );
        store.setCommitIndex(1);

        // do all our work in term 2
        becomeLeaderInTerm(2, true);

        // now, imagine that somehow the system added 2 NOOP entries
        algorithm.addOrUpdateLogEntryWhileLeaderForUnitTestsOnly(NOOP(2, 2));
        algorithm.addOrUpdateLogEntryWhileLeaderForUnitTestsOnly(NOOP(3, 2));

        // move to the first heartbeat
        // we use this to verify that nextIndex is properly set for all the servers in the cluster
        timer.fastForward();

        Collection<AppendEntries> appendEntriesRequests;

        appendEntriesRequests = getRPCs(4, AppendEntries.class);
        assertThatAppendEntriesHaveValues(
                appendEntriesRequests,
                2, 1, 1, 1,
                NOOP(2, 2),
                NOOP(3, 2)
        );

        // TEST DESCRIPTION:
        //
        // we will use S_04 as our test subject
        // imagine, that for whatever reason, we sent AppendEntries for the three NoopEntry objects as follows:
        //
        // N(1, 1) + N(2, 2)
        // N(1, 1) + N(2, 2), N(3, 2)
        //
        // and we now receive responses in order
        //
        // [m1] N(1, 1) ec=2 a=T
        // [m2] N(1, 1) ec=1 a=T
        //
        // at the end of this, S_04 should have applied everything, _and not rolled back nextIndex_ on the delayed message!

        // receive [m1]
        algorithm.onAppendEntriesReply(S_04, 2, 1, 2, true);

        // move to the heartbeat timeout to verify that we've updated nextIndex appropriately for S_04
        timer.fastForward();

        appendEntriesRequests = getRPCs(4, AppendEntries.class);
        for (AppendEntries appendEntries : appendEntriesRequests) {
            if (appendEntries.server.equals(S_04)) { // verify that we updated nextIndex appropriately for S_04
                assertThatAppendEntriesHasValues(appendEntries, 2, 1, 3, 2);
            } else {
                assertThatAppendEntriesHasValues(
                        appendEntries,
                        2, 1, 1, 1,
                        NOOP(2, 2),
                        NOOP(3, 2)
                );
            }
        }

        // now, receive [m2] (this is the delayed message)
        algorithm.onAppendEntriesReply(S_04, 2, 1, 1, true);

        // move to the heartbeat timeout to verify that we haven't rolled back nextIndex
        // IOW, we should still be making _forward_ progress
        timer.fastForward();

        appendEntriesRequests = getRPCs(4, AppendEntries.class);
        for (AppendEntries appendEntries : appendEntriesRequests) {
            if (appendEntries.server.equals(S_04)) { // we haven't changed nextIndex for S_04
                assertThatAppendEntriesHasValues(appendEntries, 2, 1, 3, 2);
            } else {
                assertThatAppendEntriesHasValues(
                        appendEntries,
                        2, 1, 1, 1,
                        NOOP(2, 2),
                        NOOP(3, 2)
                );
            }
        }

        // finally, verify that we're in a good state
        assertThatLogContains(
                SENTINEL(),
                NOOP(1, 1),
                NOOP(2, 2),
                NOOP(3, 2)
        );
        assertThatTermAndCommitIndexHaveValues(2, 1);
    }

    private void becomeLeaderInTerm(long term, boolean drainIAmLeaderNOOPs) throws StorageException {
        long commitIndex = store.getCommitIndex();

        // first, transition into being a candidate
        algorithm.becomeCandidate(term);
        assertThatSelfTransitionedToCandidate(term, commitIndex);

        // always drain the Request Vote RPCs
        sender.drainSentRPCs();

        // next, transition to being a leader
        algorithm.becomeLeader(term);
        assertThatSelfTransitionedToLeader(term, commitIndex);

        // drain out any NOOPs sent on being a leader (if necessary)
        if (drainIAmLeaderNOOPs) {
            // TODO (AG): should I check the NOOPs to see if they're correct?
            sender.drainSentRPCs();
        }

        // IMPORTANT: we _do not_ receive AppendEntriesReply messages committing our NOOP!
    }

    // NOTE: This test is a simple extension of the test above, with an additional step for another delayed message
    @Test
    public void shouldApplyDelayedAppendEntriesReplyWherePrevLogIndexPlusAppliedEntryCountGreaterThanNextIndex() throws StorageException, RPCException {
        // start off with a simple log where everything is committed
        insertIntoLog(
                SENTINEL(),
                NOOP(1, 1)
        );
        store.setCommitIndex(1);

        becomeLeaderInTerm(2, true);

        // now, imagine that somehow the system added 3 NOOP entries
        algorithm.addOrUpdateLogEntryWhileLeaderForUnitTestsOnly(NOOP(2, 2));
        algorithm.addOrUpdateLogEntryWhileLeaderForUnitTestsOnly(NOOP(3, 2));
        algorithm.addOrUpdateLogEntryWhileLeaderForUnitTestsOnly(NOOP(4, 2));

        // move to the first heartbeat
        // we use this to verify that nextIndex is properly set for all the servers in the cluster
        timer.fastForward();

        Collection<AppendEntries> appendEntriesRequests;

        appendEntriesRequests = getRPCs(4, AppendEntries.class);
        assertThatAppendEntriesHaveValues(
                appendEntriesRequests,
                2, 1, 1, 1,
                NOOP(2, 2),
                NOOP(3, 2),
                NOOP(4, 2)
        );

        // TEST DESCRIPTION:
        //
        // we will use S_04 as our test subject
        // imagine, that for whatever reason, we sent AppendEntries for the three NoopEntry objects as follows:
        //
        // N(1, 1) + N(2, 2)
        // N(1, 1) + N(2, 2), N(3, 2)
        // N(1, 1) + N(2, 2), N(3, 2), N(4, 2)
        //
        // and we now receive responses in order
        //
        // [m1] N(1, 1) ec=2 a=T
        // [m2] N(1, 1) ec=1 a=T
        // [m3] N(1, 1) ec=3 a=T
        //
        // at the end of this, S_04 should have applied everything, even though, after the first message, nextIndex was updated
        // and the last message is definitely old

        // receive [m1]
        algorithm.onAppendEntriesReply(S_04, 2, 1, 2, true);

        // move to the heartbeat timeout to verify that we've updated nextIndex appropriately for S_04
        timer.fastForward();

        appendEntriesRequests = getRPCs(4, AppendEntries.class);
        for (AppendEntries appendEntries : appendEntriesRequests) {
            if (appendEntries.server.equals(S_04)) { // verify that we updated nextIndex appropriately for S_04
                assertThatAppendEntriesHasValues(
                        appendEntries,
                        2, 1, 3, 2,
                        NOOP(4, 2)
                );
            } else {
                assertThatAppendEntriesHasValues(
                        appendEntries,
                        2, 1, 1, 1,
                        NOOP(2, 2),
                        NOOP(3, 2),
                        NOOP(4, 2)
                );
            }
        }

        // now, receive [m2] (this is the first delayed message)
        algorithm.onAppendEntriesReply(S_04, 2, 1, 1, true);

        // move to the heartbeat timeout to verify that we haven't rolled back nextIndex
        // IOW, we should still be making _forward_ progress
        timer.fastForward();

        appendEntriesRequests = getRPCs(4, AppendEntries.class);
        for (AppendEntries appendEntries : appendEntriesRequests) {
            if (appendEntries.server.equals(S_04)) { // we haven't changed nextIndex for S_04
                assertThatAppendEntriesHasValues(
                        appendEntries,
                        2, 1, 3, 2,
                        NOOP(4, 2)
                );
            } else {
                assertThatAppendEntriesHasValues(
                        appendEntries,
                        2, 1, 1, 1,
                        NOOP(2, 2),
                        NOOP(3, 2),
                        NOOP(4, 2)
                );
            }
        }

        // finally, receive [m3] (this is the second delayed message)
        algorithm.onAppendEntriesReply(S_04, 2, 1, 3, true);

        // move to the heartbeat timeout
        // we check that even though this message is delayed, we apply it because it contains _good_, _new_ information
        // and allows us to make forward progress
        timer.fastForward();

        appendEntriesRequests = getRPCs(4, AppendEntries.class);
        for (AppendEntries appendEntries : appendEntriesRequests) {
            if (appendEntries.server.equals(S_04)) { // S_04 got all the entries
                assertThatAppendEntriesHasValues(appendEntries, 2, 1, 4, 2);
            } else {
                assertThatAppendEntriesHasValues(
                        appendEntries,
                        2, 1, 1, 1,
                        NOOP(2, 2),
                        NOOP(3, 2),
                        NOOP(4, 2)
                );
            }
        }

        // finally, verify that we're in a good state
        assertThatLogContains(
                SENTINEL(),
                NOOP(1, 1),
                NOOP(2, 2),
                NOOP(3, 2),
                NOOP(4, 2)
        );
        assertThatTermAndCommitIndexHaveValues(2, 1);
    }

    @Test
    public void shouldNotReApplyButShouldRespondToDelayedAppendEntriesThatDoNotResultInAnyNewLogEntriesBeingAdded() throws StorageException {
        // become a follower in term 3
        algorithm.becomeFollower(3, S_04);
        assertThatSelfTransitionedToFollower(3, 0, S_04, true);

        // the leader asks you to append 3 entries
        algorithm.onAppendEntries(
                S_04,
                3, 0, 0, 0,
                Lists.<LogEntry>newArrayList(
                        NOOP(1, 3),
                        NOOP(2, 3),
                        NOOP(3, 3)
                )
        );

        // check that we've appended the entries and that we're in a good state
        assertThatLogContains(
                SENTINEL(),
                NOOP(1, 3),
                NOOP(2, 3),
                NOOP(3, 3)
        );
        assertThatTermAndCommitIndexHaveValues(3, 0);

        AppendEntriesReply appendEntriesReply;

        // check that we respond properly
        appendEntriesReply = sender.nextAndRemove(AppendEntriesReply.class);
        assertThatAppendEntriesReplyHasValues(appendEntriesReply, S_04, 3, 0, 3, true);
        assertThatNoMoreRPCsWereSent();

        // get the tick just before we receive the delayed AppendEntries
        long preAppendEntriesElectionTimeout = getElectionTimeoutTick();

        // now, move time forward by _1_ tick (this will make it easy to check if we changed our election timeout)
        timer.fastForward(1);

        // now, get a delayed message from the leader, asking us to only apply one entry
        algorithm.onAppendEntries(
                S_04,
                3, 0, 0, 0,
                Lists.<LogEntry>newArrayList(
                        NOOP(1, 3)
                )
        );

        // check that we do respond (and claim that, yes, we did append that entry)
        appendEntriesReply = sender.nextAndRemove(AppendEntriesReply.class);
        assertThatAppendEntriesReplyHasValues(appendEntriesReply, S_04, 3, 0, 1, true);
        assertThatNoMoreRPCsWereSent();

        // but we don't actually touch the log at all
        // essentially, the following block verifies that we only call the log
        // once for each entry (notice I use eq instead of refEq)
        verify(log, times(1)).put(SENTINEL());
        verify(log, times(1)).put(eq(NOOP(1, 3)));
        verify(log, times(1)).put(eq(NOOP(2, 3)));
        verify(log, times(1)).put(eq(NOOP(3, 3)));

        // and, we've bumped our election timeout
        long postAppendEntriesElectionTimeout = getElectionTimeoutTick();
        assertThat(postAppendEntriesElectionTimeout, equalTo(preAppendEntriesElectionTimeout + 1)); // old election timeout, plus the 1 tick we advanced

        // and check that we're in a good final state
        assertThatLogContains(
                SENTINEL(),
                NOOP(1, 3),
                NOOP(2, 3),
                NOOP(3, 3)
        );
        assertThatTermAndCommitIndexHaveValues(3, 0);
    }

    @Test
    public void shouldNotReApplyButShouldRespondToDuplicateAppendEntries() throws StorageException {
        // become a follower in term 3
        algorithm.becomeFollower(3, S_04);
        assertThatSelfTransitionedToFollower(3, 0, S_04, true);

        // the leader asks you to append 3 entries
        algorithm.onAppendEntries(
                S_04,
                3, 0, 0, 0,
                Lists.<LogEntry>newArrayList(
                        NOOP(1, 3),
                        NOOP(2, 3),
                        NOOP(3, 3)
                )
        );

        // check that we've appended the entries and that we're in a good state
        assertThatLogContains(
                SENTINEL(),
                NOOP(1, 3),
                NOOP(2, 3),
                NOOP(3, 3)
        );
        assertThatTermAndCommitIndexHaveValues(3, 0);

        AppendEntriesReply appendEntriesReply;

        // check that we respond properly
        appendEntriesReply = sender.nextAndRemove(AppendEntriesReply.class);
        assertThatAppendEntriesReplyHasValues(appendEntriesReply, S_04, 3, 0, 3, true);
        assertThatNoMoreRPCsWereSent();

        // get the election timeout just before we receive the duplicate AppendEntries
        long preAppendEntriesElectionTimeout = getElectionTimeoutTick();

        // now, move time forward by _1_ tick (this will make it easy to check if we changed our election timeout)
        timer.fastForward(1);

        // now, get a delayed message from the leader, asking us to only apply one entry
        algorithm.onAppendEntries(
                S_04,
                3, 0, 0, 0,
                Lists.<LogEntry>newArrayList(
                        NOOP(1, 3),
                        NOOP(2, 3),
                        NOOP(3, 3)
                )
        );

        // check that we do respond (and claim that, yes, we did append that entry)
        appendEntriesReply = sender.nextAndRemove(AppendEntriesReply.class);
        assertThatAppendEntriesReplyHasValues(appendEntriesReply, S_04, 3, 0, 3, true);
        assertThatNoMoreRPCsWereSent();

        // but we don't actually touch the log at all
        // essentially, the following block verifies that we only call the log
        // once for each entry (notice I use eq instead of refEq)
        verify(log, times(1)).put(SENTINEL());
        verify(log, times(1)).put(eq(NOOP(1, 3)));
        verify(log, times(1)).put(eq(NOOP(2, 3)));
        verify(log, times(1)).put(eq(NOOP(3, 3)));

        // and, we've bumped our election timeout
        long postAppendEntriesElectionTimeout = getElectionTimeoutTick();
        assertThat(postAppendEntriesElectionTimeout, equalTo(preAppendEntriesElectionTimeout + 1)); // old election timeout, plus the 1 tick we advanced

        // and check that we're in a good final state
        assertThatLogContains(
                SENTINEL(),
                NOOP(1, 3),
                NOOP(2, 3),
                NOOP(3, 3)
        );
        assertThatTermAndCommitIndexHaveValues(3, 0);
    }

    @Test
    public void shouldNotReApplyButShouldRespondToDuplicateAppendEntriesWhereCommitIndexGreaterThanPrevLogIndex() throws StorageException {
        // become a follower in term 3
        algorithm.becomeFollower(3, S_04);
        assertThatSelfTransitionedToFollower(3, 0, S_04, true);

        // the leader asks you to append 3 entries and commit two of them
        algorithm.onAppendEntries(
                S_04,
                3, 2, 0, 0,
                Lists.<LogEntry>newArrayList(
                        NOOP(1, 3),
                        NOOP(2, 3),
                        NOOP(3, 3)
                )
        );

        // check that we've appended the entries and that we're in a good state
        assertThatLogContains(
                SENTINEL(),
                NOOP(1, 3),
                NOOP(2, 3),
                NOOP(3, 3)
        );
        assertThatTermAndCommitIndexHaveValues(3, 2);

        AppendEntriesReply appendEntriesReply;

        // check that we respond properly
        appendEntriesReply = sender.nextAndRemove(AppendEntriesReply.class);
        assertThatAppendEntriesReplyHasValues(appendEntriesReply, S_04, 3, 0, 3, true);
        assertThatNoMoreRPCsWereSent();

        // get the tick just before we receive the duplicate AppendEntries
        long preAppendEntriesElectionTimeout = getElectionTimeoutTick();

        // now, move time forward by _1_ tick (this will make it easy to check if we changed our election timeout)
        timer.fastForward(1);

        // now, get a delayed message from the leader, asking us to only apply one entry
        algorithm.onAppendEntries(
                S_04,
                3, 2, 0, 0,
                Lists.<LogEntry>newArrayList(
                        NOOP(1, 3),
                        NOOP(2, 3),
                        NOOP(3, 3)
                )
        );

        // check that we do respond (and claim that, yes, we did append those entries)
        appendEntriesReply = sender.nextAndRemove(AppendEntriesReply.class);
        assertThatAppendEntriesReplyHasValues(appendEntriesReply, S_04, 3, 0, 3, true);
        assertThatNoMoreRPCsWereSent();

        // but we don't actually touch the log at all
        // essentially, the following block verifies that we only call the log
        // once for each entry (notice I use eq instead of refEq)
        verify(log, times(1)).put(SENTINEL());
        verify(log, times(1)).put(eq(NOOP(1, 3)));
        verify(log, times(1)).put(eq(NOOP(2, 3)));
        verify(log, times(1)).put(eq(NOOP(3, 3)));

        // and, we've bumped our election timeout
        long postAppendEntriesElectionTimeout = getElectionTimeoutTick();
        assertThat(postAppendEntriesElectionTimeout, equalTo(preAppendEntriesElectionTimeout + 1)); // old election timeout, plus the 1 tick we advanced

        // and check that we're in a good final state
        assertThatLogContains(
                SENTINEL(),
                NOOP(1, 3),
                NOOP(2, 3),
                NOOP(3, 3)
        );
        assertThatTermAndCommitIndexHaveValues(3, 2);
    }

    @Test
    public void shouldNotReApplyButShouldRespondToRepeatedAppendEntriesWithIncreasedCommitIndex() throws StorageException {
        // become a follower in term 3
        algorithm.becomeFollower(3, S_04);
        assertThatSelfTransitionedToFollower(3, 0, S_04, true);

        // the leader asks you to append 3 entries
        algorithm.onAppendEntries(
                S_04,
                3, 0, 0, 0,
                Lists.<LogEntry>newArrayList(
                        NOOP(1, 3),
                        NOOP(2, 3),
                        NOOP(3, 3)
                )
        );

        // check that we've appended the entries and that we're in a good state
        assertThatLogContains(
                SENTINEL(),
                NOOP(1, 3),
                NOOP(2, 3),
                NOOP(3, 3)
        );
        assertThatTermAndCommitIndexHaveValues(3, 0);

        AppendEntriesReply appendEntriesReply;

        // check that we respond properly
        appendEntriesReply = sender.nextAndRemove(AppendEntriesReply.class);
        assertThatAppendEntriesReplyHasValues(appendEntriesReply, S_04, 3, 0, 3, true);
        assertThatNoMoreRPCsWereSent();

        // get the tick just before we receive the AppendEntries message
        long preAppendEntriesElectionTimeout = getElectionTimeoutTick();

        // now, move time forward by _1_ tick (this will make it easy to check if we changed our election timeout)
        timer.fastForward(1);

        // now, get another message from the leader asking us to apply the
        // same entries again, but with a different commit index
        algorithm.onAppendEntries(
                S_04,
                3, 2, 0, 0,
                Lists.<LogEntry>newArrayList(
                        NOOP(1, 3),
                        NOOP(2, 3),
                        NOOP(3, 3)
                )
        );

        // check that we do respond (and claim that, yes, we did append that entry)
        appendEntriesReply = sender.nextAndRemove(AppendEntriesReply.class);
        assertThatAppendEntriesReplyHasValues(appendEntriesReply, S_04, 3, 0, 3, true);
        assertThatNoMoreRPCsWereSent();

        // but we don't actually touch the log at all
        // essentially, the following block verifies that we only call the log
        // once for each entry (notice I use eq instead of refEq)
        verify(log, times(1)).put(SENTINEL());
        verify(log, times(1)).put(eq(NOOP(1, 3)));
        verify(log, times(1)).put(eq(NOOP(2, 3)));
        verify(log, times(1)).put(eq(NOOP(3, 3)));

        // and, we've bumped our election timeout
        long postAppendEntriesElectionTimeout = getElectionTimeoutTick();
        assertThat(postAppendEntriesElectionTimeout, equalTo(preAppendEntriesElectionTimeout + 1)); // old election timeout, plus the 1 tick we advanced

        // and check that we're in a good final state
        assertThatLogContains(
                SENTINEL(),
                NOOP(1, 3),
                NOOP(2, 3),
                NOOP(3, 3)
        );
        assertThatTermAndCommitIndexHaveValues(3, 2);
    }

    // TODO (AG): make the setup for these kinds of tests easier

    @Test
    public void shouldCommitAppropriateEntriesAndNotifyListenerWhenReceivingAppendEntriesReplies() throws StorageException, RPCException {
        //
        // Log (SELF) Leader:
        //
        //   0   1   2   3   4   5   6   7   8   9   10  11  12
        // +---+---+---+---+---+---+---+---+---+---+---+---+---+
        // | S | 1 | 1 | 1 | 1 | 1 | 2 | 2 | 2 | 3 | 3 | 3 | 3 |
        // +---+---+---+---+---+---+---+---+---+---+---+---+---+
        //                   ^
        //                   |
        //                   +----- commitIndex

        //                                   applied -----+
        // Log (S_01) Follower:                           |
        //                                                V
        //   0   1   2   3   4   5   6   7   8   9   10  11  12
        // +---+---+---+---+---+---+---+---+---+---+---+---+---+
        // | S | 1 | 1 | 1 | 1 | 1 | 2 | 2 | 2 | 3 | 3 | 3 | ? |
        // +---+---+---+---+---+---+---+---+---+---+---+---+---+
        //       ^
        //       |
        //       +----- nextIndex

        //                          applied -----+
        // Log (S_03) Follower:                  |
        //                                       V
        //   0   1   2   3   4   5   6   7   8   9   10  11  12
        // +---+---+---+---+---+---+---+---+---+---+---+---+---+
        // | S | 1 | 1 | 1 | 1 | 1 | 2 | 2 | 2 | 3 | ? | ? | ? |
        // +---+---+---+---+---+---+---+---+---+---+---+---+---+
        //                           ^
        //                           |
        //                           +----- nextIndex

        // As a result of above, should commit and notify listeners of 5, 6, 7, 8, 9

        UnitTestCommand command1 = new UnitTestCommand();
        UnitTestCommand command2 = new UnitTestCommand();
        UnitTestCommand command3 = new UnitTestCommand();
        UnitTestCommand command4 = new UnitTestCommand();
        UnitTestCommand command5 = new UnitTestCommand();
        UnitTestCommand command6 = new UnitTestCommand();
        UnitTestCommand command7 = new UnitTestCommand();
        UnitTestCommand command8 = new UnitTestCommand();

        // setup the log
        // only put in 11 entries because when I
        // become a leader I will automatically add
        // a NOOP for term 3
        // Also, entries 9 - 11 should be in term 3, but ... my checks
        // prevent that, so set it to be in term 2, and adjust them later
        insertIntoLog(
                NOOP(1, 1),
                NOOP(2, 1),
                CLIENT(3, 1, command1),
                CLIENT(4, 1, command2),
                CLIENT(5, 1, command3),
                CLIENT(6, 2, command4),
                CLIENT(7, 2, command5),
                CLIENT(8, 2, command6),
                CLIENT(9, 2, command7), // to be overwritten
                CLIENT(10, 2, command8), // to be overwritten
                NOOP(11, 2) // to be overwritten
        );
        store.setCommitIndex(4);

        // become leader
        // but, don't drain the heartbeat RPCs yet, because we're going to adjust the leader's internal state
        becomeLeaderInTerm(3, false);

        // now, adjust the leader's state via invasive probes
        algorithm.addOrUpdateLogEntryWhileLeaderForUnitTestsOnly(CLIENT(9, 3, command7));
        algorithm.addOrUpdateLogEntryWhileLeaderForUnitTestsOnly(CLIENT(10, 3, command8));
        algorithm.addOrUpdateLogEntryWhileLeaderForUnitTestsOnly(NOOP(11, 3));

        algorithm.setServerNextIndexWhileLeaderForUnitTestsOnly(S_01, 1);
        algorithm.setServerNextIndexWhileLeaderForUnitTestsOnly(S_03, 6);

        // drain the "I am leader" messages because they're broken
        sender.drainSentRPCs();

        // check the log state just to verify that everything's ok
        assertThatLogContains(
                SENTINEL(),
                NOOP(1, 1),
                NOOP(2, 1),
                CLIENT(3, 1, command1),
                CLIENT(4, 1, command2), // <----- committed up till here
                CLIENT(5, 1, command3),
                CLIENT(6, 2, command4),
                CLIENT(7, 2, command5),
                CLIENT(8, 2, command6),
                CLIENT(9, 3, command7),
                CLIENT(10, 3, command8),
                NOOP(11, 3),
                NOOP(12, 3)
        );

        Collection<AppendEntries> appendEntriesRequests;

        // move to the next heartbeat and check the heartbeats to make sure our surgery went OK
        timer.fastForward();

        appendEntriesRequests = getRPCs(4, AppendEntries.class);
        for (AppendEntries appendEntries : appendEntriesRequests) {
            if (appendEntries.server.equals(S_01)) {
                assertThatAppendEntriesHasValues(
                        appendEntries,
                        3, 4, 0, 0,
                        NOOP(1, 1),
                        NOOP(2, 1),
                        CLIENT(3, 1, command1),
                        CLIENT(4, 1, command2),
                        CLIENT(5, 1, command3),
                        CLIENT(6, 2, command4),
                        CLIENT(7, 2, command5),
                        CLIENT(8, 2, command6),
                        CLIENT(9, 3, command7),
                        CLIENT(10, 3, command8),
                        NOOP(11, 3),
                        NOOP(12, 3)
                );
            } else if (appendEntries.server.equals(S_03)) {
                assertThatAppendEntriesHasValues(
                        appendEntries,
                        3, 4, 5, 1,
                        CLIENT(6, 2, command4),
                        CLIENT(7, 2, command5),
                        CLIENT(8, 2, command6),
                        CLIENT(9, 3, command7),
                        CLIENT(10, 3, command8),
                        NOOP(11, 3),
                        NOOP(12, 3)
                );
            } else {
                assertThatAppendEntriesHasValues(appendEntries, 3, 4, 11, 3, NOOP(12, 3));
            }
        }
        assertThatNoMoreRPCsWereSent();

        // now that the setup is completed, I can actually run the test

        // NOTE: the actual clients won't do this oddness
        // (i.e. not applying all the entries), but...do it for
        // the sake of the test
        algorithm.onAppendEntriesReply(S_01, 3, 0, 11, true);
        algorithm.onAppendEntriesReply(S_03, 3, 5, 4, true);

        // check that we've updated our commitIndex
        assertThatTermAndCommitIndexHaveValues(3, 9);

        // and that we've notified our listener
        InOrder notificationOrder = inOrder(listener);
        notificationOrder.verify(listener).applyCommitted(argThat(isCommittedCommandAtIndex(5, command3)));
        notificationOrder.verify(listener).applyCommitted(argThat(isCommittedCommandAtIndex(6, command4)));
        notificationOrder.verify(listener).applyCommitted(argThat(isCommittedCommandAtIndex(7, command5)));
        notificationOrder.verify(listener).applyCommitted(argThat(isCommittedCommandAtIndex(8, command6)));
        notificationOrder.verify(listener).applyCommitted(argThat(isCommittedCommandAtIndex(9, command7)));
        notificationOrder.verifyNoMoreInteractions();

        // check the logs (nothing should have changed)
        assertThatLogContains(
                SENTINEL(),
                NOOP(1, 1),
                NOOP(2, 1),
                CLIENT(3, 1, command1),
                CLIENT(4, 1, command2),
                CLIENT(5, 1, command3),
                CLIENT(6, 2, command4),
                CLIENT(7, 2, command5),
                CLIENT(8, 2, command6),
                CLIENT(9, 3, command7),
                CLIENT(10, 3, command8),
                NOOP(11, 3),
                NOOP(12, 3)
        );
    }

    @Test
    public void shouldNotNotifyListenerAgainOfCommittedCommandWhenReceivingAdditionalAppendEntriesRepliesAfterQuorum() throws RPCException, StorageException, NotLeaderException {
        becomeLeaderInTerm3OnFirstBoot();

        UnitTestCommand command = new UnitTestCommand();
        algorithm.submitCommand(command);

        Collection<AppendEntries> appendEntriesRequests = getRPCs(4, AppendEntries.class);
        assertThatAppendEntriesHaveValues(appendEntriesRequests, 3, 1, 1, 3, CLIENT(2, 3, command));
        assertThatNoMoreRPCsWereSent();

        long heartbeatTimeout = getHeartbeatTimeoutTick();
        long heartbeatInterval = heartbeatTimeout - timer.getTick();

        // achieve quorum for this command
        timer.fastForward(heartbeatInterval / 2);
        algorithm.onAppendEntriesReply(S_01, 3, 1, 1, true);
        algorithm.onAppendEntriesReply(S_02, 3, 1, 1, true);

        // verify that we've notified the listener and bumped the commitIndex
        assertThatTermAndCommitIndexHaveValues(3, 2);
        verify(listener, times(1)).applyCommitted(argThat(isCommittedCommandAtIndex(2, command)));

        // after some time we get another response
        // but...should not notify the client
        timer.fastForward(heartbeatInterval / 4);
        algorithm.onAppendEntriesReply(S_03, 3, 1, 1, true);
        verifyNoMoreInteractions(listener);

        // check final state
        assertThatLogContains(
                SENTINEL(),
                NOOP(1, 3),
                CLIENT(2, 3, command)
        );
        assertThatTermAndCommitIndexHaveValues(3, 2);
    }

    @Test
    public void shouldNotCommitEntryOrNotifyListenerIfReceivingDuplicateAppliedAppendEntriesRepliesFromLessThanQuorum() throws RPCException, StorageException, NotLeaderException {
        insertIntoLog(
                NOOP(1, 1),
                NOOP(2, 2)
        );

        // become leader
        // also, drain these "I am leader" messages because I'm going to adjust nextIndex
        becomeLeaderInTerm(3, true);
        verify(listener).onLeadershipChange(SELF);

        // the only entry all the servers have is the one at index 1
        algorithm.setServerNextIndexWhileLeaderForUnitTestsOnly(S_01, 2);
        algorithm.setServerNextIndexWhileLeaderForUnitTestsOnly(S_02, 2);
        algorithm.setServerNextIndexWhileLeaderForUnitTestsOnly(S_03, 2);
        algorithm.setServerNextIndexWhileLeaderForUnitTestsOnly(S_04, 2);

        // submit a command
        UnitTestCommand command = new UnitTestCommand();
        algorithm.submitCommand(command);

        // drain out the AppendEntries requests sent when a command is submitted
        sender.drainSentRPCs();

        Collection<AppendEntries> appendEntriesRequests;

        // move to the heartbeat timeout
        fastForwardToHeartbeat();
        appendEntriesRequests = getRPCs(4, AppendEntries.class);
        assertThatAppendEntriesHaveValues(
                appendEntriesRequests,
                3, 0, 1, 1,
                NOOP(2, 2),
                NOOP(3, 3),
                CLIENT(4, 3, command)
        );
        assertThatNoMoreRPCsWereSent();

        long heartbeatTimeout = getHeartbeatTimeoutTick();
        long heartbeatInterval = heartbeatTimeout - timer.getTick();

        // move forward and get a bunch of duplicate responses
        timer.fastForward(heartbeatInterval / 3);
        algorithm.onAppendEntriesReply(S_03, 3, 1, 3, true);
        algorithm.onAppendEntriesReply(S_03, 3, 1, 3, true);
        algorithm.onAppendEntriesReply(S_03, 3, 1, 3, true);

        // now, move forward to the next heartbeat
        fastForwardToHeartbeat();

        // nothing should have changed
        appendEntriesRequests = getRPCs(4, AppendEntries.class);
        for (AppendEntries appendEntries : appendEntriesRequests) {
            if (appendEntries.server.equals(S_03)) {
                assertThatAppendEntriesHasValues(appendEntries, 3, 0, 4, 3);
            } else {
                assertThatAppendEntriesHasValues(
                        appendEntries,
                        3, 0, 1, 1,
                        NOOP(2, 2),
                        NOOP(3, 3),
                        CLIENT(4, 3, command)
                );
            }
        }
        assertThatNoMoreRPCsWereSent();

        verifyNoMoreInteractions(listener);
        assertThatLogContains(
                SENTINEL(),
                NOOP(1, 1),
                NOOP(2, 2),
                NOOP(3, 3),
                CLIENT(4, 3, command)
        );
        assertThatTermAndCommitIndexHaveValues(3, 0);
    }

    //================================================================================================================//
    //
    // Command Submission Tests
    //
    //================================================================================================================//

    @Test
    public void shouldThrowIfReplicationAttemptedOnANonLeader() throws StorageException, NotLeaderException {
        algorithm.becomeFollower(3, S_01);
        assertThatSelfTransitionedToFollower(3, 0, S_01, true);

        expectedException.expect(NotLeaderException.class);
        algorithm.submitCommand(new UnitTestCommand());
    }

    @Test
    public void shouldNotifyListenerAndTriggerFutureIfCommandCommitted() throws RPCException, StorageException, NotLeaderException {
        becomeLeaderInTerm3OnFirstBoot();

        // submit a command
        UnitTestCommand command = new UnitTestCommand();
        ListenableFuture<Void> commandFuture = algorithm.submitCommand(command);
        assertThat(commandFuture.isDone(), equalTo(false));

        // check the log and commitIndex
        assertThatLogContains(
                SENTINEL(),
                NOOP(1, 3),
                CLIENT(2, 3, command)
        );
        assertThatTermAndCommitIndexHaveValues(3, 1);

        // I've already tested the AppendEntries RPC calls
        sender.drainSentRPCs();

        // get two responses - enough to achieve quorum
        algorithm.onAppendEntriesReply(S_03, 3, 1, 1, true);
        algorithm.onAppendEntriesReply(S_04, 3, 1, 1, true);

        // bumped the commitIndex, triggered the future, and notified the listeners
        assertThatTermAndCommitIndexHaveValues(3, 2);
        assertThat(commandFuture.isDone(), equalTo(true));
        verify(listener, times(1)).applyCommitted(argThat(isCommittedCommandAtIndex(2, command)));
        verifyNoMoreInteractions(listener);

        // check final state
        assertThatLogContains(
                SENTINEL(),
                NOOP(1, 3),
                CLIENT(2, 3, command)
        );
        assertThatTermAndCommitIndexHaveValues(3, 2);
    }

    @Test
    public void shouldNotifyListenerAndTriggerFuturesIfMultipleCommandsCommitted() throws RPCException, StorageException, NotLeaderException {
        becomeLeaderInTerm3OnFirstBoot();

        // command definitions
        UnitTestCommand command1 = new UnitTestCommand();
        ListenableFuture<Void> commandFuture1;
        UnitTestCommand command2 = new UnitTestCommand();
        ListenableFuture<Void> commandFuture2;
        UnitTestCommand command3 = new UnitTestCommand();
        ListenableFuture<Void> commandFuture3;
        UnitTestCommand command4 = new UnitTestCommand();
        ListenableFuture<Void> commandFuture4;
        UnitTestCommand command5 = new UnitTestCommand();
        ListenableFuture<Void> commandFuture5;

        // pipeline commands (toss in some random NOOPs for fun)
        commandFuture1 = algorithm.submitCommand(command1);
        assertThat(commandFuture1.isDone(), equalTo(false));

        commandFuture2 = algorithm.submitCommand(command2);
        assertThat(commandFuture2.isDone(), equalTo(false));

        algorithm.addOrUpdateLogEntryWhileLeaderForUnitTestsOnly(NOOP(4, 3));
        algorithm.addOrUpdateLogEntryWhileLeaderForUnitTestsOnly(NOOP(5, 3));

        commandFuture3 = algorithm.submitCommand(command3);
        assertThat(commandFuture3.isDone(), equalTo(false));

        commandFuture4 = algorithm.submitCommand(command4);
        assertThat(commandFuture4.isDone(), equalTo(false));

        algorithm.addOrUpdateLogEntryWhileLeaderForUnitTestsOnly(NOOP(8, 3));

        commandFuture5 = algorithm.submitCommand(command5);
        assertThat(commandFuture5.isDone(), equalTo(false));

        // check the log and commitIndex
        assertThatLogContains(
                SENTINEL(),
                NOOP(1, 3),
                CLIENT(2, 3, command1),
                CLIENT(3, 3, command2),
                NOOP(4, 3),
                NOOP(5, 3),
                CLIENT(6, 3, command3),
                CLIENT(7, 3, command4),
                NOOP(8, 3),
                CLIENT(9, 3, command5)
        );
        assertThatTermAndCommitIndexHaveValues(3, 1);

        // I've already tested the AppendEntries RPC calls
        sender.drainSentRPCs();

        // get two responses - enough to achieve quorum
        algorithm.onAppendEntriesReply(S_03, 3, 1, 8, true);
        algorithm.onAppendEntriesReply(S_04, 3, 1, 8, true);

        // bumped the commitIndex, triggered the future, and notified the listeners
        assertThatTermAndCommitIndexHaveValues(3, 9);

        assertThat(commandFuture1.isDone(), equalTo(true));
        assertThat(commandFuture2.isDone(), equalTo(true));
        assertThat(commandFuture3.isDone(), equalTo(true));
        assertThat(commandFuture4.isDone(), equalTo(true));
        assertThat(commandFuture5.isDone(), equalTo(true));

        InOrder notificationOrder = inOrder(listener);
        notificationOrder.verify(listener).applyCommitted(argThat(isCommittedCommandAtIndex(2, command1)));
        notificationOrder.verify(listener).applyCommitted(argThat(isCommittedCommandAtIndex(3, command2)));
        notificationOrder.verify(listener).applyCommitted(argThat(isSkipAtIndex(4)));
        notificationOrder.verify(listener).applyCommitted(argThat(isSkipAtIndex(5)));
        notificationOrder.verify(listener).applyCommitted(argThat(isCommittedCommandAtIndex(6, command3)));
        notificationOrder.verify(listener).applyCommitted(argThat(isCommittedCommandAtIndex(7, command4)));
        notificationOrder.verify(listener).applyCommitted(argThat(isSkipAtIndex(8)));
        notificationOrder.verify(listener).applyCommitted(argThat(isCommittedCommandAtIndex(9, command5)));
        notificationOrder.verifyNoMoreInteractions();

        // check final state
        assertThatLogContains(
                SENTINEL(),
                NOOP(1, 3),
                CLIENT(2, 3, command1),
                CLIENT(3, 3, command2),
                NOOP(4, 3),
                NOOP(5, 3),
                CLIENT(6, 3, command3),
                CLIENT(7, 3, command4),
                NOOP(8, 3),
                CLIENT(9, 3, command5)
        );
        assertThatTermAndCommitIndexHaveValues(3, 9);
    }

    @Test
    public void shouldFailAllOutstandingCommandsOnLosingLeadership() throws RPCException, StorageException, NotLeaderException, ExecutionException, InterruptedException {
        becomeLeaderInTerm3OnFirstBoot();

        // submit a bunch of commands (and insert a NOOP in the middle)
        UnitTestCommand command1 = new UnitTestCommand();
        ListenableFuture<Void> commandFuture1 = algorithm.submitCommand(command1);
        assertThat(commandFuture1.isDone(), equalTo(false));

        UnitTestCommand command2 = new UnitTestCommand();
        ListenableFuture<Void> commandFuture2 = algorithm.submitCommand(command2);
        assertThat(commandFuture2.isDone(), equalTo(false));

        algorithm.addOrUpdateLogEntryWhileLeaderForUnitTestsOnly(
                NOOP(
                        log.getLast().getIndex() + 1,
                        store.getCurrentTerm()
                )
        );

        UnitTestCommand command3 = new UnitTestCommand();
        ListenableFuture<Void> commandFuture3 = algorithm.submitCommand(command3);
        assertThat(commandFuture3.isDone(), equalTo(false));

        assertThatLogContains(
                SENTINEL(),
                NOOP(1, 3),
                CLIENT(2, 3, command1),
                CLIENT(3, 3, command2),
                NOOP(4, 3),
                CLIENT(5, 3, command3)
        );
        assertThatTermAndCommitIndexHaveValues(3, 1);

        // drain out any pending RPCs from submitting all these commands, etc.
        sender.drainSentRPCs();

        // move to the heartbeat timeout
        fastForwardToHeartbeat();
        Collection<AppendEntries> heartbeats = getRPCs(4, AppendEntries.class);
        assertThatAppendEntriesHaveValues(
                heartbeats,
                3, 1, 1, 3,
                CLIENT(2, 3, command1),
                CLIENT(3, 3, command2),
                NOOP(4, 3),
                CLIENT(5, 3, command3)
        );
        assertThatNoMoreRPCsWereSent();

        AppendEntriesReply appendEntriesReply;

        // now, get someone claiming to be the leader
        algorithm.onAppendEntries(S_01, 4, 1, 5, 3, Lists.<LogEntry>newArrayList(NOOP(6, 4)));

        // verify that we change to follower
        assertThatSelfTransitionedToFollower(4, 1, S_01, true);

        // fail all outstanding futures
        try {
            assertThat(commandFuture1.isDone(), equalTo(true));
            commandFuture1.get();
        } catch (ExecutionException e) {
            assertThat(e.getCause(), instanceOf(ReplicationException.class));
        }

        try {
            assertThat(commandFuture2.isDone(), equalTo(true));
            commandFuture2.get();
        } catch (ExecutionException e) {
            assertThat(e.getCause(), instanceOf(ReplicationException.class));
        }

        try {
            assertThat(commandFuture3.isDone(), equalTo(true));
            commandFuture3.get();
        } catch (ExecutionException e) {
            assertThat(e.getCause(), instanceOf(ReplicationException.class));
        }

        // apply the changes
        appendEntriesReply = sender.nextAndRemove(AppendEntriesReply.class);
        assertThatAppendEntriesReplyHasValues(appendEntriesReply, S_01, 4, 5, 1, true);
        assertThatLogContains(
                SENTINEL(),
                NOOP(1, 3),
                CLIENT(2, 3, command1),
                CLIENT(3, 3, command2),
                NOOP(4, 3),
                CLIENT(5, 3, command3),
                NOOP(6, 4)
        );
        assertThatTermAndCommitIndexHaveValues(4, 1);

        // get notified that all the entries that were sent out were committed
        algorithm.onAppendEntries(S_01, 4, 6, 6, 4, null);

        // check that the listener was notified (this is independent of the fact that the command futures were all tripped to false!)
        InOrder notificationOrder = inOrder(listener);
        notificationOrder.verify(listener).applyCommitted(argThat(isCommittedCommandAtIndex(2, command1)));
        notificationOrder.verify(listener).applyCommitted(argThat(isCommittedCommandAtIndex(3, command2)));
        notificationOrder.verify(listener).applyCommitted(argThat(isSkipAtIndex(4)));
        notificationOrder.verify(listener).applyCommitted(argThat(isCommittedCommandAtIndex(5, command3)));
        notificationOrder.verify(listener).applyCommitted(argThat(isSkipAtIndex(6)));
        notificationOrder.verifyNoMoreInteractions();

        // verify that the final state looks good
        assertThatLogContains(
                SENTINEL(),
                NOOP(1, 3),
                CLIENT(2, 3, command1),
                CLIENT(3, 3, command2),
                NOOP(4, 3),
                CLIENT(5, 3, command3),
                NOOP(6, 4)
        );
        assertThatTermAndCommitIndexHaveValues(4, 6);
    }

    //================================================================================================================//
    //
    // Committed Loading Tests
    //
    //================================================================================================================//

    // generally we want to check the following:
    // - bounds violations
    // - indexToSearchFrom = 0
    // - indexToSearchFrom at arbitrary position < lastLogEntry
    // - indexToSearchFrom = end
    //
    // for all of these tests:
    // - don't have to start raftAlgorithm for this to work
    // - don't have to be leader
    // - it doesn't matter what the current term is
    // - it _only_ matters what the commitIndex is, and that the log has entries or a snapshot exists

    @Test
    public void shouldThrowIllegalArgumentExceptionIfCallerSpecifiesIndexLessThanZero() {
        // standard log is fine

        // when you make the call with a -'ve number it should fail immediately
        expectedException.expect(IllegalArgumentException.class);
        algorithm.getNextCommitted(-1);
    }

    @Test
    public void shouldThrowIllegalArgumentExceptionIfCallerSpecifiesIndexGreaterThanCommitIndexInCallToGetNextCommitted() throws Exception {
        final LogEntry[] entries = new LogEntry[] {
                SENTINEL(),
                NOOP(1, 1),
                CLIENT(2, 1, new UnitTestCommand()),
                CLIENT(3, 1, new UnitTestCommand()), // <-- committed until here
                CLIENT(4, 1, new UnitTestCommand()),
                CLIENT(5, 1, new UnitTestCommand()),
                CLIENT(6, 1, new UnitTestCommand())
        };
        final long currentTerm = 1;
        final long commitIndex = 3;

        // setup the starting state
        insertIntoLog(entries);
        store.setCurrentTerm(currentTerm);
        store.setCommitIndex(commitIndex);

        IllegalArgumentException callException = null;

        try {
            // act as if the caller is specifying a value > commitIndex
            // but check that we aren't accidentally triggering the other bounds check (i.e., that argument <= lastLogIndex)
            long callerLastAppliedCommandIndex = commitIndex + 1;
            assertThat(callerLastAppliedCommandIndex, lessThanOrEqualTo((long) entries.length - 1));

            // make the call
            algorithm.getNextCommitted(callerLastAppliedCommandIndex);
        } catch (IllegalArgumentException e) {
            callException = e;
        }

        assertThat(callException, notNullValue());

        // verify that the state hasn't changed
        assertThatLogContains(entries);
        assertThatTermAndCommitIndexHaveValues(currentTerm, commitIndex);
    }

    @Test
    public void shouldThrowIllegalArgumentExceptionIfCallerSpecifiesIndexGreaterThanLastLogIndexInCallToGetNextCommitted() throws Exception {
        final LogEntry[] entries = new LogEntry[] {
                SENTINEL(),
                NOOP(1, 1),
                CLIENT(2, 1, new UnitTestCommand()),
                CLIENT(3, 1, new UnitTestCommand()),
                CLIENT(4, 1, new UnitTestCommand()),
                CLIENT(5, 1, new UnitTestCommand()),
                CLIENT(6, 1, new UnitTestCommand())  // <-- committed until here (i.e. _everything_ is committed)
        };
        final long currentTerm = 2;
        final long commitIndex = entries.length - 1;

        // setup the starting state
        insertIntoLog(entries);
        store.setCurrentTerm(currentTerm);
        store.setCommitIndex(commitIndex);

        //
        // situation is as follows:
        //
        //                                    +----------- COMMITTED
        //                                    V
        //   ------------------------------------
        //   | 00 | 01 | 02 | 03 | 04 | 05 | 06 | (LOG)
        //   -----------------------------------
        //                                         ^
        //                                         +----------- INDEX TO SEARCH FROM = 7
        //

        IllegalArgumentException callException = null;

        try {
            algorithm.getNextCommitted(entries.length); // the caller is acting as if it got an entry at lastLogIndex + 1
        } catch (IllegalArgumentException e) {
            callException = e;
        }

        assertThat(callException, notNullValue());

        // verify that the state hasn't changed
        assertThatLogContains(entries);
        assertThatTermAndCommitIndexHaveValues(currentTerm, commitIndex);
    }

    @Test
    public void shouldThrowIllegalArgumentExceptionIfCallerSpecifiesIndexGreaterThanLastAppliedIndexInSnapshotInCallToGetNextCommitted() throws Exception {
        // we have a snapshot that contains data to index 6 (inclusive)
        long lastAppliedIndex = 6;
        SnapshotsStore.ExtendedSnapshot storedSnapshot = new UnitTestSnapshot(lastAppliedIndex, 2);

        // our log is empty
        clearLog(); // clear out the log completely - we don't even want the sentinel

        // set the term
        long currentTerm = 2;
        store.setCurrentTerm(currentTerm); // has to be >= snapshot term
        // set the committed index
        store.setCommitIndex(lastAppliedIndex);

        // have the snapshot store return the snapshot
        when(snapshotsStore.getLatestSnapshot()).thenReturn(storedSnapshot);

        //
        // situation is as follows:
        //
        //                   +----------- COMMITTED = 6
        //                   V
        //   ---------------------
        //  | LAST APPLIED = 6   | (SNAPSHOT)
        //  ---------------------
        //                          ^
        //                          +----------- INDEX TO SEARCH FROM = 7
        //

        IllegalArgumentException callException = null;
        try {
            algorithm.getNextCommitted(lastAppliedIndex + 1); // the caller is acting as if it got some entry after the end of the snapshot
        } catch (IllegalArgumentException e) {
            callException = e;
        }

        assertThat(callException, notNullValue());

        // verify that the state hasn't changed
        assertThatLogIsEmpty();
        assertThatTermAndCommitIndexHaveValues(currentTerm, lastAppliedIndex);
    }

    // log and snapshot
    // we do have overlap for some entries
    // initial indexToSearchFrom is after the end of the log
    @Test
    public void shouldThrowIllegalArgumentExceptionIfCallerSpecifiesIndexGreaterThanLastLogIndexInCallToGetNextCommittedAndBothLogAndSnapshotExist() throws Exception {
        // we have a snapshot that contains data to index 9 (inclusive)
        SnapshotsStore.ExtendedSnapshot storedSnapshot = new UnitTestSnapshot(9L, 2);

        // we have a log that contains entries from index 7 onwards
        LogEntry unappliedEntry0 = CLIENT(7, 2, new UnitTestCommand());
        LogEntry unAppliedEntry1 = NOOP(8, 2);
        LogEntry unappliedEntry2 = NOOP(9, 3);
        LogEntry unappliedEntry3 = CLIENT(10, 3, new UnitTestCommand());
        LogEntry unappliedEntry4 = CLIENT(11, 3, new UnitTestCommand());
        final LogEntry[] entries = new LogEntry[] {
                unappliedEntry0,
                unAppliedEntry1,
                unappliedEntry2,
                unappliedEntry3,
                unappliedEntry4, // <------ we've committed up to here
                CLIENT(12, 3, new UnitTestCommand())
        };
        clearLog(); // clear out the log completely - we don't even want the sentinel
        insertIntoLog(entries);

        // set the current term
        long currentTerm = 3;
        store.setCurrentTerm(currentTerm);

        // set the committed index
        long commitIndex = 11;
        store.setCommitIndex(commitIndex);

        // have the snapshot store return the snapshot
        when(snapshotsStore.getLatestSnapshot()).thenReturn(storedSnapshot);

        //
        // situation is as follows:
        //
        //                                             +----------- COMMITTED
        //                                             V
        //                       ------------------------------
        //    .... EMPTY ....   | 07 | 08 | 09 | 10 | 11 | 12 | (LOG)
        //                      ------------------------------
        //  ------------------------------------
        // |         LAST APPLIED = 9          | (SNAPSHOT)
        // ------------------------------------
        //                                                      ^
        //                                                      +----------- INDEX TO SEARCH FROM = 13
        //

        IllegalArgumentException callException = null;
        try {
            algorithm.getNextCommitted(entries[entries.length - 1].getIndex() + 1); // the caller is acting as if it got some entry after the end of the log
        } catch (IllegalArgumentException e) {
            callException = e;
        }

        assertThat(callException, notNullValue());

        // verify that the state hasn't changed
        assertThatLogContains(entries);
        assertThatTermAndCommitIndexHaveValues(currentTerm, commitIndex);
    }

    // empty case
    @Test
    public void shouldReturnNoUnappliedEntriesIfTheLogIsEmptyAndThereAreNoSnapshots() throws Exception {
        insertIntoLog(SENTINEL());
        store.setCommitIndex(0);

        when(snapshotsStore.getLatestSnapshot()).thenReturn(null);

        assertThat(algorithm.getNextCommitted(0), nullValue());

        assertThatLogContainsOnlySentinel();
        assertThatTermAndCommitIndexHaveValues(0, 0);
    }

    // log only
    // indexToSearchFrom starts at 0
    @Test
    public void shouldReturnCorrectSequenceOfCommittedInstancesInResponseToRepeatedGetNextCommittedCallsWithInitialIndexToSearchFromAsZero() throws Exception {
        LogEntry unappliedEntries[] = {
                NOOP(1, 1),
                CLIENT(2, 1, new UnitTestCommand()),
                CLIENT(3, 1, new UnitTestCommand()),
                CLIENT(4, 1, new UnitTestCommand()),
        };

        // starting state before the calls to "getNextCommitted"
        final long commitIndex = 4;
        final LogEntry[] entries = new LogEntry[] {
                SENTINEL(),
                unappliedEntries[0],
                unappliedEntries[1],
                unappliedEntries[2],
                unappliedEntries[3],                 // <--- only committed up to here
                CLIENT(5, 1, new UnitTestCommand()), // <--- uncommitted from this point on
        };
        insertIntoLog(entries);
        store.setCommitIndex(commitIndex);

        // set the term
        // it shouldn't matter if the current term > last term in the log
        long currentTerm = 3;
        store.setCurrentTerm(currentTerm);

        //
        // situation is as follows:
        //
        //                       +----------- COMMITTED
        //                       V
        // -------------------------------
        // | 00 | 01 | 02 | 03 | 04 | 05 | (LOG)
        // ------------------------------
        //   ^
        //   +----------- INDEX TO SEARCH FROM
        //

        // check that all the log entries were returned correctly
        checkCorrectSequenceOfLogEntriesReturnedForRepeatedGetNextCommittedCalls(0, unappliedEntries);

        // we should not have changed the internal state of the algorithm
        assertThatLogContains(entries);
        assertThatTermAndCommitIndexHaveValues(currentTerm, commitIndex);
    }

    // log only
    // indexToSearchFrom starts in the middle of the log
    @Test
    public void shouldReturnCorrectSequenceOfCommittedInstancesInResponseToRepeatedGetNextCommittedCallsWithInitialIndexToSearchFromInMiddleOfLog() throws Exception {
        LogEntry unappliedEntries[] = {
                CLIENT(4, 1, new UnitTestCommand()),
                NOOP(5, 2),
                CLIENT(6, 2, new UnitTestCommand()),
                NOOP(7, 3),
                CLIENT(8, 3, new UnitTestCommand()),
                CLIENT(9, 3, new UnitTestCommand()),
                CLIENT(10, 3, new UnitTestCommand()),
        };

        // starting state before the calls to "getNextCommitted"
        final long commitIndex = 10;
        final LogEntry[] entries = new LogEntry[] {
                SENTINEL(),
                NOOP(1, 1),
                NOOP(2, 1),
                CLIENT(3, 1, new UnitTestCommand()),  // <--- applied this command (but nothing after!)
                unappliedEntries[0],
                unappliedEntries[1],
                unappliedEntries[2],
                unappliedEntries[3],
                unappliedEntries[4],
                unappliedEntries[5],
                unappliedEntries[6],                  // <--- only committed up to here!
                CLIENT(11, 3, new UnitTestCommand()), // <--- uncommitted from this point on
                CLIENT(12, 3, new UnitTestCommand()),
                CLIENT(13, 3, new UnitTestCommand()),
                CLIENT(14, 3, new UnitTestCommand())
        };
        insertIntoLog(entries);
        store.setCommitIndex(commitIndex);

        // set the current term
        // again, shouldn't matter that the term is >= the one in the logs
        long currentTerm = 3;
        store.setCurrentTerm(currentTerm);

        //
        // situation is as follows:
        //
        //                                                      +----------- COMMITTED
        //                                                      V
        // ----------------------------------------------------------------------------
        // | 00 | 01 | 02 | 03 | 04 | 05 | 06 | 07 | 08 | 09 | 10 | 11 | 12 | 13 | 14 | (LOG)
        // ---------------------------------------------------------------------------
        //                  ^
        //                  +----------- INDEX TO SEARCH FROM
        //

        // we're going to start at index 3 and verify that all log entries from (3 -> 10]
        checkCorrectSequenceOfLogEntriesReturnedForRepeatedGetNextCommittedCalls(3, unappliedEntries);

        // the algorithm internal state should not have changed
        assertThatLogContains(entries);
        assertThatTermAndCommitIndexHaveValues(currentTerm, commitIndex);
    }

    // snapshot only
    // initial indexToSearchFrom is 0
    @Test
    public void shouldReturnSnapshotInResponseToRepeatedGetNextCommittedCallsWithInitialIndexToSearchFromAsZero() throws Exception {
        // we have a snapshot that contains data to index 6 (inclusive)
        int lastAppliedTerm = 2;
        long lastAppliedIndex = 6;
        SnapshotsStore.ExtendedSnapshot storedSnapshot = new UnitTestSnapshot(lastAppliedIndex, lastAppliedTerm);

        // our log is empty
        clearLog(); // clear out the log completely - we don't even want the sentinel

        // set the committed index
        store.setCommitIndex(lastAppliedIndex);

        // set the current term
        // this has to be >= the term in the snapshot
        store.setCurrentTerm(lastAppliedTerm);

        // have the snapshot store return the snapshot
        when(snapshotsStore.getLatestSnapshot()).thenReturn(storedSnapshot);

        //
        // situation is as follows:
        //
        //                   +----------- COMMITTED = 6
        //                   V
        //   ---------------------
        //  | LAST APPLIED = 6   | (SNAPSHOT)
        //  ---------------------
        //   ^
        //   +----------- INDEX TO SEARCH FROM = 0
        //

        Committed committed;

        // OK, now, let's start by saying that the last index we applied was '0' (IOW, we have no state and have to start from scratch)
        // we should get the snapshot back
        committed = algorithm.getNextCommitted(0);
        checkMatchingSnapshot(committed, storedSnapshot);

        // if we make a second call we should get nothing
        committed = algorithm.getNextCommitted(committed.getIndex()); // index = 7
        assertThat(committed, nullValue());

        // algorithm should have exactly the same state at the end
        assertThatLogIsEmpty();
        assertThatTermAndCommitIndexHaveValues(lastAppliedTerm, lastAppliedIndex);
    }

    // snapshot only
    // initial indexToSearchFrom is before the end of the snapshot
    @Test
    public void shouldReturnSnapshotInResponseToRepeatedGetNextCommittedCallsWithInitialIndexToSearchFromIsBeforeLastAppliedIndexInSnapshot() throws Exception {
        // we have a snapshot that contains data to index 6 (inclusive)
        long lastAppliedTerm = 2;
        long lastAppliedIndex = 6;
        SnapshotsStore.ExtendedSnapshot storedSnapshot = new UnitTestSnapshot(lastAppliedIndex, lastAppliedTerm);

        // our log is empty
        clearLog(); // clear out the log completely - we don't even want the sentinel

        // set the current term
        // has to be >= the term in the snapshot
        long currentTerm = lastAppliedTerm + 1;
        store.setCurrentTerm(currentTerm);

        // set the committed index
        store.setCommitIndex(lastAppliedIndex);

        // have the snapshot store return the snapshot
        when(snapshotsStore.getLatestSnapshot()).thenReturn(storedSnapshot);

        //
        // situation is as follows:
        //
        //                   +----------- COMMITTED = 6
        //                   V
        //   ---------------------
        //  | LAST APPLIED = 6   | (SNAPSHOT)
        //  ---------------------
        //          ^
        //          +----------- INDEX TO SEARCH FROM = 3
        //

        Committed committed;

        // OK, now, let's start by saying that the last index we applied was '3' (weird, but, whatever)
        // we should get the snapshot back
        committed = algorithm.getNextCommitted(3);
        checkMatchingSnapshot(committed, storedSnapshot);

        // if we make a second call we should get nothing
        committed = algorithm.getNextCommitted(committed.getIndex()); // index = 7
        assertThat(committed, nullValue());

        // algorithm should have exactly the same state at the end
        assertThatLogIsEmpty();
        assertThatTermAndCommitIndexHaveValues(currentTerm, lastAppliedIndex);
    }

    // log and snapshot
    // no overlap
    // initial indexToSearchFrom is 0
    @Test
    public void shouldReturnSnapshotFollowedByLogEntriesInResponseToRepeatedGetNextCommittedCallsWithInitialIndexToSearchFromAsZero() throws Exception {
        // we have a snapshot that contains data to index 6 (inclusive)
        SnapshotsStore.ExtendedSnapshot storedSnapshot = new UnitTestSnapshot((long) 6, 2);

        // we have a log that contains entries from index 7 onwards
        LogEntry unappliedEntry0 = CLIENT(7, 2, new UnitTestCommand());
        LogEntry unappliedEntry1 = NOOP(8, 2);
        final LogEntry[] entries = new LogEntry[] {
                unappliedEntry0,
                unappliedEntry1, // <------ we've committed up to here
                NOOP(9, 3),
                CLIENT(10, 3, new UnitTestCommand()),
                CLIENT(11, 3, new UnitTestCommand()),
                CLIENT(12, 3, new UnitTestCommand())
        };
        clearLog(); // clear out the log completely - we don't even want the sentinel
        insertIntoLog(entries);

        // set the current term
        long currentTerm = 8;
        store.setCurrentTerm(currentTerm);

        // set the committed index
        long commitIndex = 8;
        store.setCommitIndex(commitIndex);

        // have the snapshot store return the snapshot
        when(snapshotsStore.getLatestSnapshot()).thenReturn(storedSnapshot);

        //
        // situation is as follows:
        //
        //                                +----------- COMMITTED
        //                                V
        //                         ------------------------------
        //      .... EMPTY ....   | 07 | 08 | 09 | 10 | 11 | 12 | (LOG)
        //                        ------------------------------
        //   ---------------------
        //  | LAST APPLIED = 6   | (SNAPSHOT)
        //  ---------------------
        //   ^
        //   +----------- INDEX TO SEARCH FROM = 0
        //

        Committed committed;

        // OK, now, let's start by saying that the last index we applied was '0' (IOW, we have no state and have to start from scratch)
        // we should get the snapshot back
        committed = algorithm.getNextCommitted(0);
        checkMatchingSnapshot(committed, storedSnapshot);

        // let's check that if we make a second call we should get back the first committed, unapplied entry (command)
        committed = algorithm.getNextCommitted(committed.getIndex()); // index = 6
        checkMatchingCommittedLogEntry(committed, unappliedEntry0);

        // let's check that if we make a third call we should get back the second committed, unapplied entry (noop)
        committed = algorithm.getNextCommitted(committed.getIndex()); // index = 7
        checkMatchingCommittedLogEntry(committed, unappliedEntry1);

        // if we make a fourth call we should get nothing
        committed = algorithm.getNextCommitted(committed.getIndex()); // index = 8
        assertThat(committed, nullValue());

        // algorithm should have exactly the same state at the end
        assertThatLogContains(entries);
        assertThatTermAndCommitIndexHaveValues(currentTerm, commitIndex);
    }

    // log and snapshot
    // no overlap
    // initial indexToSearchFrom is after the end of the snapshot and before the last committed entry in the log
    @Test
    public void shouldReturnLogEntriesInResponseToRepeatedGetNextCommittedCallsWithInitialIndexToSearchFromNonZeroAndAfterTheLastAppliedIndexInTheSnapshot() throws Exception {
        // we have a snapshot that contains data to index 6 (inclusive)
        SnapshotsStore.ExtendedSnapshot storedSnapshot = new UnitTestSnapshot(6L, 2);

        // we have a log that contains entries from index 7 onwards
        LogEntry unappliedEntry0 = NOOP(8, 2);
        LogEntry unappliedEntry1 = CLIENT(9, 2, new UnitTestCommand());
        final LogEntry[] entries = new LogEntry[] {
                CLIENT(7, 2, new UnitTestCommand()),
                unappliedEntry0,
                unappliedEntry1, // <------ we've committed up to here
                CLIENT(10, 3, new UnitTestCommand()),
                CLIENT(11, 3, new UnitTestCommand()),
                CLIENT(12, 3, new UnitTestCommand())
        };
        clearLog(); // clear out the log completely - we don't even want the sentinel
        insertIntoLog(entries);

        // set the current term
        long currentTerm = 3;
        store.setCurrentTerm(currentTerm);

        // set the committed index
        long commitIndex = 9;
        store.setCommitIndex(commitIndex);

        // have the snapshot store return the snapshot
        when(snapshotsStore.getLatestSnapshot()).thenReturn(storedSnapshot);

        //
        // situation is as follows:
        //
        //                                     +----------- COMMITTED
        //                                     V
        //                         ------------------------------
        //      .... EMPTY ....   | 07 | 08 | 09 | 10 | 11 | 12 | (LOG)
        //                        ------------------------------
        //   ---------------------
        //  | LAST APPLIED = 6   | (SNAPSHOT)
        //  ---------------------
        //                         ^
        //                         +----------- INDEX TO SEARCH FROM = 7
        //

        Committed committed;

        // OK, now, let's start by saying that the last index we applied was '7'
        committed = algorithm.getNextCommitted(7);
        checkMatchingCommittedLogEntry(committed, unappliedEntry0);

        // second call (should get back the client entry)
        committed = algorithm.getNextCommitted(committed.getIndex()); // index = 8
        checkMatchingCommittedLogEntry(committed, unappliedEntry1);

        // a final call (should get nothing)
        committed = algorithm.getNextCommitted(committed.getIndex()); // index = 9
        assertThat(committed, nullValue());

        // we should not have changed our internal state
        assertThatLogContains(entries);
        assertThatTermAndCommitIndexHaveValues(currentTerm, commitIndex);
    }

    // log and snapshot
    // we do have overlap for some entries
    // initial indexToSearchFrom is 0
    @Test
    public void shouldReturnSnapshotFollowedByLogEntriesInResponseToRepeatedGetNextCommittedCallsWithInitialIndexToSearchFromAsZeroAndOverlapBetweenLogAndSnapshot() throws Exception {
        // we have a snapshot that contains data to index 9 (inclusive)
        SnapshotsStore.ExtendedSnapshot storedSnapshot = new UnitTestSnapshot(9L, 2);

        // we have a log that contains entries from index 7 onwards
        LogEntry unappliedEntries[] = {
                CLIENT(7, 2, new UnitTestCommand()),
                NOOP(8, 2),
                NOOP(9, 3),
                CLIENT(10, 3, new UnitTestCommand()),
                CLIENT(11, 3, new UnitTestCommand()),
        };
        final LogEntry[] entries = new LogEntry[] {
                unappliedEntries[0],
                unappliedEntries[1],
                unappliedEntries[2],
                unappliedEntries[3],
                unappliedEntries[4], // <------ we've committed up to here
                CLIENT(12, 3, new UnitTestCommand())
        };
        clearLog(); // clear out the log completely - we don't even want the sentinel
        insertIntoLog(entries);

        // set the current term
        // shouldn't matter if it's greater than the last term in the log
        long currentTerm = 5;
        store.setCurrentTerm(currentTerm);

        // set the committed index
        long commitIndex = 11;
        store.setCommitIndex(commitIndex);

        // have the snapshot store return the snapshot
        when(snapshotsStore.getLatestSnapshot()).thenReturn(storedSnapshot);

        //
        // situation is as follows:
        //
        //                                             +----------- COMMITTED
        //                                             V
        //                       ------------------------------
        //    .... EMPTY ....   | 07 | 08 | 09 | 10 | 11 | 12 | (LOG)
        //                      ------------------------------
        //  ------------------------------------
        // |         LAST APPLIED = 9          | (SNAPSHOT)
        // ------------------------------------
        //  ^
        //  +----------- INDEX TO SEARCH FROM = 0
        //

        Committed committed;

        // OK, now, let's start by saying that the last index we applied was '0' (IOW, we have no state and have to start from scratch)
        // we should get the snapshot back
        committed = algorithm.getNextCommitted(0);
        checkMatchingSnapshot(committed, storedSnapshot);

        // when we make the second call we should ignore the
        // overlapping log region
        // and return the first log entry with new info
        // i.e. the entry at index 10
        committed = algorithm.getNextCommitted(committed.getIndex()); // index = 9
        checkMatchingCommittedLogEntry(committed, unappliedEntries[3]);

        // when we make the third call we should get the last committed unapplied log entry
        committed = algorithm.getNextCommitted(committed.getIndex()); // index = 10
        checkMatchingCommittedLogEntry(committed, unappliedEntries[4]);

        // if we make a fourth call we should get nothing since there are no committed unapplied entries left
        committed = algorithm.getNextCommitted(committed.getIndex()); // index = 11
        assertThat(committed, nullValue());

        // algorithm should have exactly the same state at the end
        assertThatLogContains(entries);
        assertThatTermAndCommitIndexHaveValues(currentTerm, commitIndex);
    }

    // log and snapshot
    // we do have overlap for some entries
    // similar to test above, but: initial indexToSearchFrom is before the end of the snapshot, and well before the beginning of the log
    // _only_ the snapshot can be returned for the first call to getNextCommitted
    @Test
    public void shouldReturnSnapshotFollowedByLogEntriesInResponseToRepeatedGetNextCommittedCallsWithInitialIndexToSearchFromNonZeroAndOverlapBetweenLogAndSnapshot() throws Exception {
        // we have a snapshot that contains data to index 9 (inclusive)
        SnapshotsStore.ExtendedSnapshot storedSnapshot = new UnitTestSnapshot(9L, 2);

        // we have a log that contains entries from index 7 onwards
        LogEntry unappliedEntries[] = {
                CLIENT(7, 2, new UnitTestCommand()),
                NOOP(8, 2),
                NOOP(9, 3),
                CLIENT(10, 3, new UnitTestCommand()),
                CLIENT(11, 3, new UnitTestCommand()),
        };
        final LogEntry[] entries = new LogEntry[] {
                unappliedEntries[0],
                unappliedEntries[1],
                unappliedEntries[2],
                unappliedEntries[3],
                unappliedEntries[4], // <------ we've committed up to here
                CLIENT(12, 3, new UnitTestCommand())
        };
        clearLog(); // clear out the log completely - we don't even want the sentinel
        insertIntoLog(entries);

        // set the current term
        long currentTerm = 4;
        store.setCurrentTerm(currentTerm);

        // set the committed index
        long commitIndex = 11;
        store.setCommitIndex(commitIndex);

        // have the snapshot store return the snapshot
        when(snapshotsStore.getLatestSnapshot()).thenReturn(storedSnapshot);

        //
        // situation is as follows:
        //
        //                                             +----------- COMMITTED
        //                                             V
        //                       ------------------------------
        //    .... EMPTY ....   | 07 | 08 | 09 | 10 | 11 | 12 | (LOG)
        //                      ------------------------------
        //  ------------------------------------
        // |         LAST APPLIED = 9          | (SNAPSHOT)
        // ------------------------------------
        //             ^
        //             +----------- INDEX TO SEARCH FROM = 5
        //

        Committed committed;

        // OK, now, let's start by saying that the last index we applied was 5
        // we should get the snapshot back
        committed = algorithm.getNextCommitted(5);
        checkMatchingSnapshot(committed, storedSnapshot);

        // when we make the second call we should ignore the
        // overlapping log region
        // and return the first log entry with new info
        // i.e. the entry at index 10
        committed = algorithm.getNextCommitted(committed.getIndex()); // index = 9
        checkMatchingCommittedLogEntry(committed, unappliedEntries[3]);

        // when we make the third call we should get the last committed unapplied log entry
        committed = algorithm.getNextCommitted(committed.getIndex()); // index = 10
        checkMatchingCommittedLogEntry(committed, unappliedEntries[4]);

        // if we make a fourth call we should get nothing since there are no committed unapplied entries left
        committed = algorithm.getNextCommitted(committed.getIndex()); // index = 11
        assertThat(committed, nullValue());

        // we should not have changed the internal state at all
        assertThatLogContains(entries);
        assertThatTermAndCommitIndexHaveValues(currentTerm, commitIndex);
    }

    // log and snapshot
    // we do have overlap for some entries
    // initialIndexToSearchFrom is before the end of the snapshot and just before the beginning of the log
    // _either_ the snapshot _or_ a log entry can be returned for the first call to getNextCommitted (it's up to us to decide which one)
    @Test
    public void shouldReturnLogEntryInResponseToRepeatedGetNextCommittedCallsWithNonZeroInitialIndexToSearchFromAndOverlapBetweenLogAndSnapshot() throws Exception {
        // we have a snapshot that contains data to index 7 (inclusive)
        SnapshotsStore.ExtendedSnapshot snapshot = new UnitTestSnapshot(7L, 2);

        // the log only contains entries from index 7 (inclusive) onwards
        LogEntry unappliedEntry0 = NOOP(7, 2);
        final LogEntry[] entries = new LogEntry[] {
                unappliedEntry0, // <------ we've committed up to here
                NOOP(8, 2),
                NOOP(9, 3),
                CLIENT(10, 3, new UnitTestCommand()),
                CLIENT(11, 3, new UnitTestCommand()),
                CLIENT(12, 3, new UnitTestCommand())
        };
        clearLog(); // want the log to be completely clear
        insertIntoLog(entries);

        // set the current term
        long currentTerm = 3;
        store.setCurrentTerm(currentTerm);

        // set the committed index
        long commitIndex = 7;
        store.setCommitIndex(commitIndex);

        // return the snapshot defined above when asked for the latest
        when(snapshotsStore.getLatestSnapshot()).thenReturn(snapshot);

        //
        // situation is as follows:
        //
        //                                   +----------- COMMITTED
        //                                   V
        //                                 ------------------------------
        //          .... EMPTY ....        | 07 | 08 | 09 | 10 | 11 | 12 | (LOG)
        //                                 ------------------------------
        //  ------------------------------------
        // |         LAST APPLIED = 7          | (SNAPSHOT)
        // ------------------------------------
        //                             ^
        //                             +----------- INDEX TO SEARCH FROM = 6
        //
        // so, the snapshot and the log overlap for one index: 7
        // and we've committed up to index 7 as well
        //
        // if the caller says they've applied up to index 6, we have two options:
        //   1. return a snapshot: they will flush their state and load the snapshot
        //   2. return only log entry 7: they simply apply that entry as usual
        //
        // we opt for option 2
        //

        Committed committed;

        // OK, now, let's start by saying that the last index we applied was '6' (kinda weird, given that we have a snapshot with last applied = 7, but, whatever)
        committed = algorithm.getNextCommitted(6);

        // check that we got the _log entry_ back
        checkMatchingCommittedLogEntry(committed, unappliedEntry0);

        // if we make a second call we should get nothing (because index 7 was the last one committed)
        committed = algorithm.getNextCommitted(committed.getIndex()); // index = 7
        assertThat(committed, nullValue());

        // we should not have changed our internal state
        assertThatLogContains(entries);
        assertThatTermAndCommitIndexHaveValues(currentTerm, commitIndex);
    }

    // log and snapshot
    // we do have overlap for some entries
    // similar to test above, but: initial indexToSearchFrom is after the end of the snapshot, and before the last committed entry in the log
    // _only_ a log entry can be returned for the first call to getNextCommitted
    @Test
    public void shouldReturnLogEntriesInResponseToRepeatedGetNextCommittedCallsWithInitialIndexToSearchFromNonZeroAndOverlapBetweenLogAndSnapshot() throws Exception {
        // we have a snapshot that contains data to index 9 (inclusive)
        long lastAppliedIndex = 9;
        SnapshotsStore.ExtendedSnapshot storedSnapshot = new UnitTestSnapshot(lastAppliedIndex, 2);

        // we have a log that contains entries from index 7 onwards
        LogEntry unappliedEntry0 = CLIENT(11, 3, new UnitTestCommand());
        final LogEntry[] entries = new LogEntry[] {
                CLIENT(7, 2, new UnitTestCommand()),
                NOOP(8, 2),
                NOOP(9, 3),
                CLIENT(10, 3, new UnitTestCommand()),
                unappliedEntry0, // <------ we've committed up to here
                CLIENT(12, 3, new UnitTestCommand())
        };
        clearLog(); // clear out the log completely - we don't even want the sentinel
        insertIntoLog(entries);

        // set the current term
        long currentTerm = 3;
        store.setCurrentTerm(currentTerm);

        // set the committed index
        long commitIndex = 11;
        store.setCommitIndex(commitIndex);

        // have the snapshot store return the snapshot
        when(snapshotsStore.getLatestSnapshot()).thenReturn(storedSnapshot);

        //
        // situation is as follows:
        //
        //                                             +----------- COMMITTED
        //                                             V
        //                       ------------------------------
        //    .... EMPTY ....   | 07 | 08 | 09 | 10 | 11 | 12 | (LOG)
        //                      ------------------------------
        //  ------------------------------------
        // |         LAST APPLIED = 9          | (SNAPSHOT)
        // ------------------------------------
        //                                      ^
        //                                      +----------- INDEX TO SEARCH FROM = 10
        //

        Committed committed;

        // OK, now, let's start by saying that the last index we applied was 10
        // we should get the only remaining committed unapplied entry back
        committed = algorithm.getNextCommitted(10);
        checkMatchingCommittedLogEntry(committed, unappliedEntry0);

        // if we make one more call we should get nothing
        committed = algorithm.getNextCommitted(committed.getIndex()); // index = 11
        assertThat(committed, nullValue());

        // we haven't changed the state at all
        assertThatLogContains(entries);
        assertThatTermAndCommitIndexHaveValues(currentTerm, commitIndex);
    }

    private void checkCorrectSequenceOfLogEntriesReturnedForRepeatedGetNextCommittedCalls(long initialIndexToSearchFrom, LogEntry[] unappliedEntries) {
        int unappliedEntriesCounter = 0;
        long indexToSearchFrom = initialIndexToSearchFrom;
        while(true) {
            // get the next committed
            Committed committed = algorithm.getNextCommitted(indexToSearchFrom);

            // check if we can still proceed
            if (committed == null) {
                break;
            }

            // check the returned log entry and update the next index to search from
            checkMatchingCommittedLogEntry(committed, unappliedEntries[unappliedEntriesCounter]);
            indexToSearchFrom = committed.getIndex();

            // move on to the next entry we're supposed to get
            unappliedEntriesCounter++;
        }

        // there are no more unapplied entries that should have been returned to the caller
        assertThat(unappliedEntriesCounter, equalTo(unappliedEntries.length));
    }

    private static void checkMatchingSnapshot(Committed committed, Snapshot expectedSnapshot) throws IOException {
        checkArgument(committed instanceof Snapshot);

        assertThat(committed.getType(), equalTo(Committed.Type.SNAPSHOT));

        Snapshot actualSnapshot = (Snapshot) committed;
        assertThat(actualSnapshot.getIndex(), equalTo(expectedSnapshot.getIndex()));
        assertThat(actualSnapshot.getSnapshotInputStream(), notNullValue());
    }

    private static void checkMatchingCommittedLogEntry(Committed committed, LogEntry entry) {
        if (entry.getType() == LogEntry.Type.CLIENT) {
            checkArgument(committed instanceof CommittedCommand);
            checkArgument(entry instanceof LogEntry.ClientEntry);

            assertThat(committed.getType(), equalTo(Committed.Type.COMMAND));

            CommittedCommand committedCommand = (CommittedCommand) committed;
            LogEntry.ClientEntry clientEntry = (LogEntry.ClientEntry) entry;

            assertThat(committedCommand.getIndex(), equalTo(clientEntry.getIndex()));
            assertThat(committedCommand.getCommand(), sameInstance(clientEntry.getCommand()));
        } else if (entry.getType() == LogEntry.Type.NOOP) {
            checkArgument(entry instanceof LogEntry.NoopEntry);
            assertThat(committed.getType(), equalTo(Committed.Type.SKIP));
            assertThat(committed.getIndex(), equalTo(entry.getIndex()));
        } else {
            throw new IllegalArgumentException("unexpected entry type:" + entry.getType().name());
        }
    }

    //================================================================================================================//
    //
    // General Tests
    //
    //================================================================================================================//

    @Test
    public void shouldNotifyListenerWhenLeaderChangesFromOneToAnother() throws StorageException { // (can happen if you don't know about an election and suddenly receive AppendEntries from another leader)
        // imagine that you're the follower of S_01 (after election you got a heartbeat from S_01)
        algorithm.becomeFollower(3, S_01);
        assertThatSelfTransitionedToFollower(3, 0, S_01, true);

        // suddenly, receive an AppendEntries (heartbeat) from another server in a newer term
        // apparently, there was an election and you didn't know about it (network partition?)
        algorithm.onAppendEntries(S_04, 4, 0, 0, 0, null);
        assertThatSelfTransitionedToFollower(4, 0, S_04, true);

        assertThatLogContains(
                SENTINEL()
        );
        assertThatTermAndCommitIndexHaveValues(4, 0);
    }

    //================================================================================================================//
    //
    // Exception Cases
    //
    //================================================================================================================//

    @Test
    public void shouldSendRequestVotesToAllServersEvenThoughSomeThrowAnRPCException() throws RPCException, StorageException {
        // default to not throwing exceptions when RequestVotes are sent, but throw then for 2 servers
        doCallRealMethod().when(sender).requestVote(anyString(), anyLong(), anyLong(), anyLong());
        doThrow(RPCException.class).when(sender).requestVote(eq(S_01), anyLong(), anyLong(), anyLong());
        doThrow(RPCException.class).when(sender).requestVote(eq(S_03), anyLong(), anyLong(), anyLong());

        triggerElection(1);

        // check that the remainder were sent successfully
        Collection<RequestVote> requestVotes = getRPCs(2, RequestVote.class);
        assertThatRequestVotesHaveValues(requestVotes, 1, 0, 0);
        assertThat(getRPCDestinations(requestVotes), containsInAnyOrder(S_02, S_04));

        // final state
        assertThatLogContains(
                SENTINEL()
        );
        assertThatTermAndCommitIndexHaveValues(1, 0);
    }

    @Test
    public void shouldSendHeartbeatsToAllServersEvenThoughSomeThrowAnRPCException() throws RPCException, StorageException {
        becomeLeaderInTerm3OnFirstBoot();

        // IMPORTANT: DO THIS AFTER calling becomeLeaderInTerm3OnFirstBoot
        // default to not throwing exceptions when AppendEntries are sent, but throw then for 1 server
        doCallRealMethod().when(sender).requestVote(anyString(), anyLong(), anyLong(), anyLong());
        doThrow(RPCException.class).when(sender).appendEntries(eq(S_02), anyLong(), anyLong(), anyLong(), anyLong(), anyCollection());

        // move to the heartbeat timeout
        timer.fastForward();

        // check that the remainder of the heartbeats were sent successfully
        Collection<AppendEntries> appendEntriesRequests = getRPCs(3, AppendEntries.class);
        assertThatAppendEntriesHaveValues(appendEntriesRequests, 3, 1, 1, 3);
        assertThat(getRPCDestinations(appendEntriesRequests), containsInAnyOrder(S_01, S_03, S_04));

        // final state
        assertThatLogContains(
                SENTINEL(),
                NOOP(1, 3)
        );
        assertThatTermAndCommitIndexHaveValues(3, 1);
    }

    // generally, for the "continue if listener throws exception" tests we should test more than one call
    // to verify that the system can _actually continue_ after the listener call fails. otherwise, we may
    // simply catch an exception and then be in a completely broken state

    @Test
    public void shouldNotCrashIfLeadershipNotificationListenerThrowsAnException() throws StorageException {
        doThrow(IllegalStateException.class).when(listener).onLeadershipChange(anyString());

        // first leadership transition
        algorithm.becomeFollower(3, S_02);
        assertThatSelfTransitionedToFollower(3, 0, S_02, true);

        // second leadership transition
        algorithm.becomeFollower(4, S_02);
        assertThatSelfTransitionedToFollower(4, 0, S_02, true);

        assertThatLogContains(
                SENTINEL()
        );
        assertThatTermAndCommitIndexHaveValues(4, 0);
    }

    @Test
    public void shouldThrowRaftErrorIfApplyCommandListenerThrowsAnException() throws StorageException, RPCException, NotLeaderException {
        becomeLeaderInTerm3OnFirstBoot();

        IllegalStateException applyCommandException = new IllegalStateException("listener failed");

        doThrow(applyCommandException).when(listener).applyCommitted(any(CommittedCommand.class));

        // attempt to submit a command
        UnitTestCommand command0 = new UnitTestCommand();
        UnitTestCommand command1 = new UnitTestCommand();

        algorithm.submitCommand(command0);
        algorithm.submitCommand(command1);

        Collection<AppendEntries> appendEntriesRequests;

        // AppendEntries for first command
        appendEntriesRequests = getRPCs(4, AppendEntries.class);
        assertThatAppendEntriesHaveValues(
                appendEntriesRequests,
                3, 1, 1, 3,
                CLIENT(2, 3, command0)
        );

        // AppendEntries for second command
        appendEntriesRequests = getRPCs(4, AppendEntries.class);
        assertThatAppendEntriesHaveValues(
                appendEntriesRequests,
                3, 1, 1, 3,
                CLIENT(2, 3, command0),
                CLIENT(3, 3, command1)
        );

        // after which, there should be silence
        assertThatNoMoreRPCsWereSent();

        // we get enough responses for a quorum
        try {
            algorithm.onAppendEntriesReply(S_02, 3, 1, 2, true);
            algorithm.onAppendEntriesReply(S_04, 3, 1, 2, true);
        } catch (RaftError e) {
            IllegalStateException wrappedException = (IllegalStateException) e.getCause();
            assertThat(wrappedException, equalTo(applyCommandException));
        }

        // check that we attempted to call back the listener only once, and never again
        verify(listener).applyCommitted(argThat(isCommittedCommandAtIndex(2, command0)));
        verifyNoMoreInteractions(listener);

        // check that we've updated our state before calling the listener
        assertThatLogContains(
                SENTINEL(),
                NOOP(1, 3),
                CLIENT(2, 3, command0),
                CLIENT(3, 3, command1)
        );
        assertThatTermAndCommitIndexHaveValues(3, 3);
    }
}
