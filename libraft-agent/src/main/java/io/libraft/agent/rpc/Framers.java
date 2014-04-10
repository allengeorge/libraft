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

import org.jboss.netty.handler.codec.frame.LengthFieldBasedFrameDecoder;
import org.jboss.netty.handler.codec.frame.LengthFieldPrepender;

/**
 * Provides {@link org.jboss.netty.channel.ChannelHandler} implementations
 * that convert {@code RaftNetworkClient} messages to/from length-field-prepended
 * packets suitable for network transmission.
 */
abstract class Framers {

    private static final int MAX_FRAME_LENGTH = 10 * 1024 * 1024; // 10 MB
    private static final int LENGTH_FIELD_OFFSET = 0;
    private static final int LENGTH_FIELD_LENGTH = 2;
    private static final int LENGTH_ADJUSTMENT = 0; // length field does not include the length of the length field itself

    private Framers() { } // to prevent instantiation

    /**
     * Stateful {@link org.jboss.netty.channel.ChannelHandler} that converts
     * an incoming stream of bytes into a series of discrete packets.
     *
     * @see LengthFieldBasedFrameDecoder
     */
    static final class FrameDecoder extends LengthFieldBasedFrameDecoder {

        /**
         * Constructor.
         */
        FrameDecoder() {
            super(MAX_FRAME_LENGTH, LENGTH_FIELD_OFFSET, LENGTH_FIELD_LENGTH, LENGTH_ADJUSTMENT, LENGTH_FIELD_LENGTH);
        }
    }

    /**
     * Stateful {@link org.jboss.netty.channel.ChannelHandler} that
     * prepends outgoing packets with a length-field header.
     *
     * @see LengthFieldPrepender
     */
    static final class FrameEncoder extends LengthFieldPrepender {

        /**
         * Constructor.
         */
        FrameEncoder() {
            super(LENGTH_FIELD_LENGTH);
        }
    }
}
