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

import io.libraft.agent.TestLoggingRule;
import org.jboss.netty.channel.Channel;
import org.jboss.netty.channel.ChannelHandlerContext;
import org.jboss.netty.channel.ChannelState;
import org.jboss.netty.channel.DefaultExceptionEvent;
import org.jboss.netty.channel.ExceptionEvent;
import org.jboss.netty.channel.UpstreamChannelStateEvent;
import org.junit.Rule;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetSocketAddress;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;

public final class FinalUpstreamHandlerTest {

    private static final Logger LOGGER = LoggerFactory.getLogger(FinalUpstreamHandlerTest.class);

    private final ChannelHandlerContext ctx = mock(ChannelHandlerContext.class);
    private final Channel channel = mock(Channel.class);
    private final FinalUpstreamHandler handler = new FinalUpstreamHandler("SELF");

    @Rule
    public final TestLoggingRule testLoggingRule = new TestLoggingRule(LOGGER);

    @Test
    public void shouldDropConnectedEvent() {
        UpstreamChannelStateEvent event = spy(new UpstreamChannelStateEvent(channel, ChannelState.CONNECTED, InetSocketAddress.createUnresolved("remote-host", 8888)));

        handler.channelConnected(ctx, event);

        verifyNoMoreInteractions(ctx, event);
    }

    @Test
    public void shouldDropChannelClosedEvent() {
        UpstreamChannelStateEvent event = spy(new UpstreamChannelStateEvent(channel, ChannelState.CONNECTED, null));

        handler.channelConnected(ctx, event);

        verifyNoMoreInteractions(ctx, event);
    }

    @Test
    public void shouldCloseChannelAndNotForwardEventWhenReceiveExceptionCaughtEvent() throws Exception {
        ExceptionEvent exceptionEvent = new DefaultExceptionEvent(channel, new Exception("test exception"));

        handler.exceptionCaught(ctx, exceptionEvent);

        verifyNoMoreInteractions(ctx);
        verify(channel).close();
    }
}
