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
import com.fasterxml.jackson.databind.ObjectReader;
import com.fasterxml.jackson.databind.ObjectWriter;
import io.libraft.agent.protocol.RaftRPC;
import org.jboss.netty.buffer.ChannelBuffer;
import org.jboss.netty.buffer.ChannelBufferInputStream;
import org.jboss.netty.buffer.ChannelBufferOutputStream;
import org.jboss.netty.buffer.ChannelBuffers;
import org.jboss.netty.channel.ChannelHandler;
import org.jboss.netty.channel.ChannelHandlerContext;
import org.jboss.netty.channel.Channels;
import org.jboss.netty.channel.MessageEvent;
import org.jboss.netty.channel.SimpleChannelDownstreamHandler;
import org.jboss.netty.channel.SimpleChannelUpstreamHandler;

import static com.google.common.base.Preconditions.checkArgument;

/**
 * Provides {@link org.jboss.netty.channel.ChannelHandler} implementations
 * that convert {@link RaftRPC} messages to/from {@link org.jboss.netty.buffer.ChannelBuffer}
 * instances.
 */
abstract class RPCConverters {

    private RPCConverters() { } // to prevent instantiation

    /**
     * Stateless {@link org.jboss.netty.channel.ChannelHandler} that converts
     * a {@link ChannelBuffer} into a {@link RaftRPC} message.
     */
    @ChannelHandler.Sharable
    static final class RPCDecoder extends SimpleChannelUpstreamHandler {

        private final ObjectReader reader; // immutable, thread-safe

        /**
         * Constructor.
         *
         * @param mapper instance of {@code ObjectMapper} used to map {@link RaftRPC}
         *               java properties to their corresponding JSON fields
         */
        RPCDecoder(ObjectMapper mapper) {
            this.reader = mapper.reader(RaftRPC.class);
        }

        @Override
        public void messageReceived(ChannelHandlerContext ctx, MessageEvent e) throws Exception {
            ChannelBuffer frame = (ChannelBuffer) e.getMessage();
            RaftRPC rpc = reader.readValue(new ChannelBufferInputStream(frame));

            Channels.fireMessageReceived(ctx, rpc);
        }
    }

    /**
     * Stateless {@link org.jboss.netty.channel.ChannelHandler} that converts
     * a {@link RaftRPC} message into a {@link ChannelBuffer}.
     */
    @ChannelHandler.Sharable
    static final class RPCEncoder extends SimpleChannelDownstreamHandler {

        private final ObjectWriter writer; // immutable, thread-safe

        /**
         * Constructor.
         *
         * @param mapper instance of {@code ObjectMapper} used to map JSON fields
         *               to their corresponding {@link RaftRPC} java properties
         */
        RPCEncoder(ObjectMapper mapper) {
            this.writer = mapper.writer();
        }

        @Override
        public void writeRequested(ChannelHandlerContext ctx, MessageEvent e) throws Exception {
            checkArgument(e.getMessage() instanceof RaftRPC, e.getMessage().getClass().getSimpleName());

            ChannelBuffer encoded = ChannelBuffers.dynamicBuffer();
            writer.writeValue(new ChannelBufferOutputStream(encoded), e.getMessage());

            Channels.write(ctx, e.getFuture(), encoded);
        }
    }
}
