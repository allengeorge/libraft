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

package io.libraft.agent.rpc;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.libraft.agent.RaftRPCFixture;
import io.libraft.agent.TestLoggingRule;
import io.libraft.agent.UnitTestCommandDeserializer;
import io.libraft.agent.UnitTestCommandSerializer;
import io.libraft.agent.protocol.RaftRPC;
import org.jboss.netty.buffer.ChannelBuffer;
import org.jboss.netty.buffer.ChannelBufferFactory;
import org.jboss.netty.buffer.HeapChannelBufferFactory;
import org.jboss.netty.channel.Channel;
import org.jboss.netty.channel.ChannelConfig;
import org.jboss.netty.channel.ChannelHandlerContext;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.mockito.Mockito;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.mockito.Mockito.mock;

public final class WireConverterTest {

    private static final Logger LOGGER = LoggerFactory.getLogger(WireConverterTest.class);

    private static final ObjectMapper MAPPER = new ObjectMapper();
    private static final UnitTestCommandSerializer COMMAND_SERIALIZER = new UnitTestCommandSerializer();
    private static final UnitTestCommandDeserializer COMMAND_DESERIALIZER = new UnitTestCommandDeserializer();

    private final WireConverter.Encoder encoder = new WireConverter.Encoder(MAPPER);
    private final WireConverter.Decoder decoder = new WireConverter.Decoder(MAPPER);
    private final ChannelHandlerContext ctx = mock(ChannelHandlerContext.class);
    private final Channel channel = mock(Channel.class);

    @Rule
    public final TestLoggingRule testLoggingRule = new TestLoggingRule(LOGGER);

    @BeforeClass
    public static void setupSerializerAndDeserializer() {
        RaftRPC.setupCustomCommandSerializationAndDeserialization(MAPPER, COMMAND_SERIALIZER, COMMAND_DESERIALIZER);
    }

    @Before
    public void setup() {
        // the encoder/decoder allocate buffers from the buffer-factory specified in the channel configuration
        ChannelBufferFactory channelBufferFactory = HeapChannelBufferFactory.getInstance();
        ChannelConfig channelConfig = mock(ChannelConfig.class);
        Mockito.when(channelConfig.getBufferFactory()).thenReturn(channelBufferFactory);
        Mockito.when(channel.getConfig()).thenReturn(channelConfig);
    }

    @Test
    public void shouldEncodeAndDecodeRequestVote() throws Exception {
        decoder.decode(ctx, channel, (ChannelBuffer) encoder.encode(ctx, channel, RaftRPCFixture.REQUEST_VOTE));
    }

    @Test
    public void shouldEncodeAndDecodeRequestVoteReply() throws Exception {
        decoder.decode(ctx, channel, (ChannelBuffer) encoder.encode(ctx, channel, RaftRPCFixture.REQUEST_VOTE_REPLY));
    }

    @Test
    public void shouldEncodeAndDecodeAppendEntries() throws Exception {
        decoder.decode(ctx, channel, (ChannelBuffer) encoder.encode(ctx, channel, RaftRPCFixture.APPEND_ENTRIES));
    }

    @Test
    public void shouldEncodeAndDecodeAppendEntriesReply() throws Exception {
        decoder.decode(ctx, channel, (ChannelBuffer) encoder.encode(ctx, channel, RaftRPCFixture.APPEND_ENTRIES_REPLY));
    }
}
