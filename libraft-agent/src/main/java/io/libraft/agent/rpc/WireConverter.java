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
import io.libraft.agent.protocol.RaftRPC;
import org.jboss.netty.buffer.ChannelBuffer;
import org.jboss.netty.buffer.ChannelBufferInputStream;
import org.jboss.netty.buffer.ChannelBufferOutputStream;
import org.jboss.netty.buffer.ChannelBuffers;
import org.jboss.netty.channel.Channel;
import org.jboss.netty.channel.ChannelHandlerContext;
import org.jboss.netty.handler.codec.frame.LengthFieldBasedFrameDecoder;
import org.jboss.netty.handler.codec.frame.LengthFieldPrepender;

import static com.google.common.base.Preconditions.checkArgument;

/**
 * Provides {@link org.jboss.netty.channel.ChannelHandler} implementations
 * that convert a {@link RaftRPC} message to/from a length-prepended network frame.
 */
abstract class WireConverter {

    private static final int MAX_FRAME_LENGTH = 10 * 1024 * 1024; // 10 MB
    private static final int LENGTH_FIELD_OFFSET = 0;
    private static final int LENGTH_FIELD_LENGTH = 2;
    private static final int LENGTH_ADJUSTMENT = 0; // length field does not include the length of the length field itself

    private WireConverter() { } // to prevent instantiation

    /**
     * Stateful {@link org.jboss.netty.channel.ChannelHandler} that converts
     * a length-prepended network frame into a {@link RaftRPC} message.
     */
    static final class Decoder extends LengthFieldBasedFrameDecoder {

        private final ObjectMapper mapper;

        /**
         * Constructor.
         *
         * @param mapper instance of {@code ObjectMapper} used to map {@link RaftRPC}
         *               java properties to their corresponding JSON fields
         */
        Decoder(ObjectMapper mapper) {
            super(MAX_FRAME_LENGTH, LENGTH_FIELD_OFFSET, LENGTH_FIELD_LENGTH, LENGTH_ADJUSTMENT, LENGTH_FIELD_LENGTH);
            this.mapper = mapper;
        }

        @Override
        protected Object decode(ChannelHandlerContext ctx, Channel channel, ChannelBuffer buffer) throws Exception {
            Object decoded = super.decode(ctx, channel, buffer);

            if (decoded == null) {
                return null;
            }

            ChannelBuffer frame = (ChannelBuffer) decoded;
            return mapper.readValue(new ChannelBufferInputStream(frame), RaftRPC.class);
        }
    }

    /**
     * Stateful {@link org.jboss.netty.channel.ChannelHandler} that converts
     * a {@link RaftRPC} message into a length-prepended network frame.
     */
    static final class Encoder extends LengthFieldPrepender {

        private final ObjectMapper mapper;

        /**
         * Constructor.
         *
         * @param mapper instance of {@code ObjectMapper} used to map JSON fields
         *               to their corresponding {@link RaftRPC} java properties
         */
        Encoder(ObjectMapper mapper) {
            super(LENGTH_FIELD_LENGTH);
            this.mapper = mapper;
        }

        @Override
        protected Object encode(ChannelHandlerContext ctx, Channel channel, Object msg) throws Exception {
            checkArgument(msg instanceof RaftRPC, msg.getClass().getSimpleName());

            ChannelBuffer encoded = ChannelBuffers.dynamicBuffer();
            mapper.writeValue(new ChannelBufferOutputStream(encoded), msg);

            return super.encode(ctx, channel, encoded);
        }
    }
}
