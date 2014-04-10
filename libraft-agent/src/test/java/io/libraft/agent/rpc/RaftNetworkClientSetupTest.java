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
import com.google.common.collect.Sets;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.common.util.concurrent.MoreExecutors;
import io.libraft.agent.ClusterMembersFixture;
import io.libraft.agent.RaftAgentConstants;
import io.libraft.agent.RaftMember;
import io.libraft.agent.TestLoggingRule;
import io.libraft.agent.WrappedTimer;
import io.libraft.algorithm.RPCReceiver;
import org.jboss.netty.channel.local.DefaultLocalClientChannelFactory;
import org.jboss.netty.channel.local.DefaultLocalServerChannelFactory;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.mockito.Mockito;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashSet;
import java.util.Random;
import java.util.concurrent.Executors;

public final class RaftNetworkClientSetupTest {

    private static final Logger LOGGER = LoggerFactory.getLogger(RaftNetworkClientSetupTest.class);

    private static final HashSet<RaftMember> CLUSTER = Sets.newHashSet(ClusterMembersFixture.RAFT_MEMBER_0, ClusterMembersFixture.RAFT_MEMBER_1, ClusterMembersFixture.RAFT_MEMBER_2);
    private static final RaftMember SELF = ClusterMembersFixture.RAFT_MEMBER_0;
    private static final ObjectMapper MAPPER = new ObjectMapper();

    private final Random random = new Random();
    private final WrappedTimer timer = new WrappedTimer();
    private final ListeningExecutorService nonIoExecutorService = MoreExecutors.listeningDecorator(Executors.newCachedThreadPool());
    private final DefaultLocalServerChannelFactory serverChannelFactory = new DefaultLocalServerChannelFactory();
    private final DefaultLocalClientChannelFactory clientChannelFactory = new DefaultLocalClientChannelFactory();

    private RaftNetworkClient raftNetworkClient;

    @Rule
    public final TestLoggingRule testLoggingRule = new TestLoggingRule(LOGGER);

    @Before
    public void newRaftNetworkClient() {
        raftNetworkClient = new RaftNetworkClient(
                random,
                timer,
                MAPPER,
                SELF,
                CLUSTER,
                RaftAgentConstants.CONNECT_TIMEOUT,
                RaftAgentConstants.MIN_RECONNECT_INTERVAL,
                RaftAgentConstants.ADDITIONAL_RECONNECT_INTERVAL_RANGE,
                RaftAgentConstants.DEFAULT_AGENT_TIME_UNIT);

        timer.start();
    }

    @After
    public void tearDown() {
        raftNetworkClient.stop();
        serverChannelFactory.releaseExternalResources();
        clientChannelFactory.releaseExternalResources();
        nonIoExecutorService.shutdownNow();
        timer.stop();
    }

    @Test(expected = NullPointerException.class)
    public void shouldFailIfStartedWithoutRPCReceiverBeingSet() {
        raftNetworkClient.start();
    }

    @Test
    public void shouldStartSuccessfully() {
        RPCReceiver receiver = Mockito.mock(RPCReceiver.class);
        raftNetworkClient.initialize(nonIoExecutorService, serverChannelFactory, clientChannelFactory, receiver);
        raftNetworkClient.start();
        raftNetworkClient.stop();
    }
}
