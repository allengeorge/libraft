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
import io.libraft.agent.RaftRPCFixture;
import io.libraft.agent.TestLoggingRule;
import io.libraft.agent.UnitTestCommandDeserializer;
import io.libraft.agent.UnitTestCommandSerializer;
import io.libraft.agent.protocol.RaftRPC;
import org.jboss.netty.bootstrap.ClientBootstrap;
import org.jboss.netty.bootstrap.ServerBootstrap;
import org.jboss.netty.channel.Channel;
import org.jboss.netty.channel.ChannelFuture;
import org.jboss.netty.channel.ChannelHandlerContext;
import org.jboss.netty.channel.ChannelPipeline;
import org.jboss.netty.channel.ChannelPipelineFactory;
import org.jboss.netty.channel.Channels;
import org.jboss.netty.channel.MessageEvent;
import org.jboss.netty.channel.SimpleChannelUpstreamHandler;
import org.jboss.netty.channel.local.DefaultLocalClientChannelFactory;
import org.jboss.netty.channel.local.DefaultLocalServerChannelFactory;
import org.jboss.netty.channel.local.LocalAddress;
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.atomic.AtomicReference;

import static com.google.common.base.Preconditions.checkState;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;

public final class RaftRPCCommunicationTest {

    private static final Logger LOGGER = LoggerFactory.getLogger(RaftRPCCommunicationTest.class);

    private static final ObjectMapper MAPPER = new ObjectMapper();
    private static final UnitTestCommandSerializer COMMAND_SERIALIZER = new UnitTestCommandSerializer();
    private static final UnitTestCommandDeserializer COMMAND_DESERIALIZER = new UnitTestCommandDeserializer();

    private final DefaultLocalServerChannelFactory serverChannelFactory = new DefaultLocalServerChannelFactory();
    private final DefaultLocalClientChannelFactory clientChannelFactory = new DefaultLocalClientChannelFactory();
    private final RPCConverters.RPCEncoder encoder = new RPCConverters.RPCEncoder(MAPPER);
    private final RPCConverters.RPCDecoder decoder = new RPCConverters.RPCDecoder(MAPPER);
    private final FinalUpstreamHandler finalUpstreamHandler = new FinalUpstreamHandler("SELF");
    private final AtomicReference<RaftRPC> receivedRpc = new AtomicReference<RaftRPC>(null);

    @Rule
    public final TestLoggingRule testLoggingRule = new TestLoggingRule(LOGGER);

    private Channel clientChannel;

    @BeforeClass
    public static void setupSerializerAndDeserializer() {
        RaftRPC.setupCustomCommandSerializationAndDeserialization(MAPPER, COMMAND_SERIALIZER, COMMAND_DESERIALIZER);
    }

    @Before
    public void setup() {
        final LocalAddress serverListenAddress = new LocalAddress(LocalAddress.EPHEMERAL);

        ServerBootstrap serverBootstrap = new ServerBootstrap(serverChannelFactory);
        serverBootstrap.setPipelineFactory(new ChannelPipelineFactory() {
            @Override
            public ChannelPipeline getPipeline() throws Exception {
                ChannelPipeline pipeline = Channels.pipeline();
                pipeline.addLast("frame-decoder", new Framers.FrameDecoder());
                pipeline.addLast("rpc-decoder", decoder);
                pipeline.addLast("recipient", new SimpleChannelUpstreamHandler() {

                    @Override
                    public void messageReceived(ChannelHandlerContext ctx, MessageEvent e) throws Exception {
                        RaftRPC rpc = (RaftRPC) e.getMessage();
                        checkState(receivedRpc.get() == null, "previously received message:%s", receivedRpc.get());
                        receivedRpc.set(rpc);
                    }
                });
                pipeline.addLast("final", finalUpstreamHandler);
                return pipeline;
            }
        });
        serverBootstrap.bind(serverListenAddress);

        ClientBootstrap clientBootstrap = new ClientBootstrap(clientChannelFactory);
        clientBootstrap.setPipelineFactory(new ChannelPipelineFactory() {
            @Override
            public ChannelPipeline getPipeline() throws Exception {
                ChannelPipeline pipeline = Channels.pipeline();
                pipeline.addLast("frame-encoder", new Framers.FrameEncoder());
                pipeline.addLast("rpc-encoder", encoder);
                pipeline.addLast("final", finalUpstreamHandler);
                return pipeline;
            }
        });

        ChannelFuture connectFuture = clientBootstrap.connect(serverListenAddress);
        connectFuture.awaitUninterruptibly();
        clientChannel = connectFuture.getChannel();
    }

    @After
    public void teardown() {
        serverChannelFactory.releaseExternalResources();
        clientChannelFactory.releaseExternalResources();
    }

    @Test
    public void shouldEncodeAndDecodeRequestVote() throws Exception {
        sendAndCheckReceivedMessage(RaftRPCFixture.REQUEST_VOTE);
    }

    @Test
    public void shouldEncodeAndDecodeRequestVoteReply() throws Exception {
        sendAndCheckReceivedMessage(RaftRPCFixture.REQUEST_VOTE_REPLY);
    }

    @Test
    public void shouldEncodeAndDecodeAppendEntries() throws Exception {
        sendAndCheckReceivedMessage(RaftRPCFixture.APPEND_ENTRIES);
    }

    @Test
    public void shouldEncodeAndDecodeAppendEntriesReply() throws Exception {
        sendAndCheckReceivedMessage(RaftRPCFixture.APPEND_ENTRIES_REPLY);
    }

    private void sendAndCheckReceivedMessage(RaftRPC rpc) throws InterruptedException {
        ChannelFuture writeFuture = clientChannel.write(rpc);
        writeFuture.awaitUninterruptibly();

        while(receivedRpc.get() == null) { // FIXME (AG): remove this sleep and use a better approach
           Thread.sleep(10);
        }

        RaftRPC receivedRPC = receivedRpc.get();
        assertThat(receivedRPC, equalTo(rpc));
    }
}
