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

package io.libraft.agent.rpc;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.common.util.concurrent.MoreExecutors;
import io.libraft.agent.ClusterMembersFixture;
import io.libraft.agent.RaftAgentConstants;
import io.libraft.agent.RaftMember;
import io.libraft.agent.TestLoggingRule;
import io.libraft.agent.UnitTestCommand;
import io.libraft.agent.UnitTestCommandDeserializer;
import io.libraft.agent.UnitTestCommandSerializer;
import io.libraft.agent.WrappedTimer;
import io.libraft.agent.protocol.RaftRPC;
import io.libraft.algorithm.LogEntry;
import io.libraft.algorithm.RPCException;
import io.libraft.algorithm.RPCReceiver;
import io.libraft.algorithm.Timer;
import org.jboss.netty.channel.local.DefaultLocalClientChannelFactory;
import org.jboss.netty.channel.local.DefaultLocalServerChannelFactory;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashSet;
import java.util.List;
import java.util.Random;
import java.util.concurrent.Executors;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;

public final class RaftNetworkClientTest {

    private static final Logger LOGGER = LoggerFactory.getLogger(RaftNetworkClientTest.class);

    private final ObjectMapper mapper = new ObjectMapper();
    private final UnitTestCommandSerializer commandSerializer = new UnitTestCommandSerializer();
    private final UnitTestCommandDeserializer commandDeserializer = new UnitTestCommandDeserializer();
    private final Random random = new Random();
    private final WrappedTimer timer = new WrappedTimer();
    private final ListeningExecutorService nonIoExecutorService = MoreExecutors.listeningDecorator(Executors.newCachedThreadPool());
    private final DefaultLocalClientChannelFactory clientChannelFactory = new DefaultLocalClientChannelFactory();
    private final DefaultLocalServerChannelFactory serverChannelFactory = new DefaultLocalServerChannelFactory();

    private final RPCReceiver client0RPCReceiver = mock(RPCReceiver.class);
    private RaftNetworkClient client0;

    private final RPCReceiver client1RPCReceiver = mock(RPCReceiver.class);
    private RaftNetworkClient client1;

    @Rule
    public final TestLoggingRule testLoggingRule = new TestLoggingRule(LOGGER);

    @Before
    public void setupClientAndServer() {
        RaftRPC.setupCustomCommandSerializationAndDeserialization(mapper, commandSerializer, commandDeserializer);

        timer.start();

        client1 = newRaftNetworkClient(
                random,
                timer,
                mapper,
                ClusterMembersFixture.RAFT_MEMBER_1,
                Sets.newHashSet(
                        ClusterMembersFixture.RAFT_MEMBER_0,
                        ClusterMembersFixture.RAFT_MEMBER_1
                )
        );
        client1.initialize(nonIoExecutorService, serverChannelFactory, clientChannelFactory, client1RPCReceiver);
        client1.start();

        client0 = newRaftNetworkClient(
                random,
                timer,
                mapper,
                ClusterMembersFixture.RAFT_MEMBER_0,
                Sets.newHashSet(
                        ClusterMembersFixture.RAFT_MEMBER_0,
                        ClusterMembersFixture.RAFT_MEMBER_1
                )
        );
        client0.initialize(nonIoExecutorService, serverChannelFactory, clientChannelFactory, client0RPCReceiver);
        client0.start();
    }

    private static RaftNetworkClient newRaftNetworkClient(
            Random random,
            Timer timer,
            ObjectMapper mapper,
            RaftMember me,
            HashSet<RaftMember> cluster) {
        return new RaftNetworkClient(
                random,
                timer,
                mapper,
                me,
                cluster,
                RaftAgentConstants.CONNECT_TIMEOUT,
                RaftAgentConstants.MIN_RECONNECT_INTERVAL,
                RaftAgentConstants.ADDITIONAL_RECONNECT_INTERVAL_RANGE,
                RaftAgentConstants.DEFAULT_AGENT_TIME_UNIT);
    }

    @After
    public void teardownClientAndServer() {
        timer.stop();
        client0.stop();
        client1.stop();
        serverChannelFactory.releaseExternalResources();
        clientChannelFactory.releaseExternalResources();
        nonIoExecutorService.shutdownNow();
    }

    @Test
    public void shouldProduceAndConsumeRequestVote() throws RPCException {
        client0.requestVote(ClusterMembersFixture.RAFT_MEMBER_1.getId(), 10, 20, 9);

        verifyNoMoreInteractions(client0RPCReceiver);
        verify(client1RPCReceiver).onRequestVote(ClusterMembersFixture.RAFT_MEMBER_0.getId(), 10, 20, 9);
    }

    @Test
    public void shouldProduceAndConsumeRequestVoteReply() throws RPCException {
        client0.requestVoteReply(ClusterMembersFixture.RAFT_MEMBER_1.getId(), 10, false);

        verifyNoMoreInteractions(client0RPCReceiver);
        verify(client1RPCReceiver).onRequestVoteReply(ClusterMembersFixture.RAFT_MEMBER_0.getId(), 10, false);
    }

    @Test
    public void shouldProduceAndConsumeHeartbeatAppendEntries() throws RPCException {
        client0.appendEntries(ClusterMembersFixture.RAFT_MEMBER_1.getId(), 10, 299, 300, 9, null);

        verifyNoMoreInteractions(client0RPCReceiver);
        verify(client1RPCReceiver).onAppendEntries(ClusterMembersFixture.RAFT_MEMBER_0.getId(), 10, 299, 300, 9, null);
    }

    @Test
    public void shouldProduceAndConsumeNonHeartbeatAppendEntries() throws RPCException {
         List<LogEntry> entries = Lists.newArrayList(
                new LogEntry.NoopEntry(302, 10),
                new LogEntry.ClientEntry(303, 10, new UnitTestCommand())
        );

        client0.appendEntries(ClusterMembersFixture.RAFT_MEMBER_1.getId(), 10, 300, 301, 10, entries);

        verifyNoMoreInteractions(client0RPCReceiver);
        verify(client1RPCReceiver).onAppendEntries(ClusterMembersFixture.RAFT_MEMBER_0.getId(), 10, 300, 301, 10, entries);
    }

    @Test
    public void shouldProduceAndConsumeAppendEntriesReply() throws RPCException {
        client0.appendEntriesReply(ClusterMembersFixture.RAFT_MEMBER_1.getId(), 10, 301, 2, true);

        verifyNoMoreInteractions(client0RPCReceiver);
        verify(client1RPCReceiver).onAppendEntriesReply(ClusterMembersFixture.RAFT_MEMBER_0.getId(), 10, 301, 2, true);
    }
}
