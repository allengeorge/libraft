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

import org.jboss.netty.channel.ChannelHandler;
import org.jboss.netty.channel.ChannelHandlerContext;
import org.jboss.netty.channel.ChannelStateEvent;
import org.jboss.netty.channel.ExceptionEvent;
import org.jboss.netty.channel.SimpleChannelHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.ConnectException;

/**
 * Stateless {@code ChannelHandler} that disconnects a
 * {@link org.jboss.netty.channel.Channel} if an exception is thrown
 * in its {@link org.jboss.netty.channel.ChannelPipeline}.
 */
@ChannelHandler.Sharable
final class FinalUpstreamHandler extends SimpleChannelHandler {

    private static final Logger LOGGER = LoggerFactory.getLogger(FinalUpstreamHandler.class);
    private final String self;

    /**
     * Constructor.
     *
     * @param self unique id of the Raft server that created this channel.
     *             This is <strong>not</strong> the unique id of the Raft server
     *             this channel is connected to.
     */
    public FinalUpstreamHandler(String self) {
        this.self = self;
    }

    @Override
    public void channelConnected(ChannelHandlerContext ctx, ChannelStateEvent e) { // discard event
    }

    @Override
    public void channelClosed(ChannelHandlerContext ctx, ChannelStateEvent e) { // discard event
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, ExceptionEvent e) {
        Object attachment = e.getChannel().getAttachment();
        Throwable channelException = e.getCause();

        if (channelException instanceof ConnectException) {
            LOGGER.warn("{}: closing channel to {}: {}", self, attachment, channelException.getMessage());
        } else {
            LOGGER.warn("{}: caught exception - closing channel to {}", self, attachment, channelException);
        }

        e.getChannel().close();
    }
}
