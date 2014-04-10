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
import io.libraft.agent.TestLoggingRule;
import org.jboss.netty.buffer.ChannelBuffer;
import org.jboss.netty.buffer.ChannelBuffers;
import org.jboss.netty.channel.Channel;
import org.jboss.netty.channel.ChannelEvent;
import org.jboss.netty.channel.ChannelHandlerContext;
import org.jboss.netty.channel.ChannelPipeline;
import org.jboss.netty.channel.ChannelState;
import org.jboss.netty.channel.UpstreamChannelStateEvent;
import org.jboss.netty.channel.UpstreamMessageEvent;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.mockito.ArgumentCaptor;
import org.mockito.InOrder;
import org.mockito.Mockito;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.InetSocketAddress;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public final class IncomingHandshakeHandlerTest {

    private static final Logger LOGGER = LoggerFactory.getLogger(IncomingHandshakeHandlerTest.class);

    public static final String S_01 = "S_01";
    private final Channel channel = mock(Channel.class);
    private final ChannelPipeline pipeline = mock(ChannelPipeline.class);
    private final ChannelHandlerContext ctx = mock(ChannelHandlerContext.class);
    private final ObjectMapper mapper = new ObjectMapper();
    private final Handshakers.IncomingHandshakeHandler handler = new Handshakers.IncomingHandshakeHandler(mapper);

    @Rule
    public final ExpectedException expectedException = ExpectedException.none();

    @Rule
    public final TestLoggingRule loggingRule = new TestLoggingRule(LOGGER);

    @Test
    public void shouldThrowExceptionIfBadHandshakeIsReceived() throws Exception {
        final InetSocketAddress remoteAddress = new InetSocketAddress(0);

        // start off by simulating a 'channelConnected' event
        // this should set the internal state properly
        handler.channelConnected(ctx, new UpstreamChannelStateEvent(channel, ChannelState.CONNECTED, remoteAddress));

        // we shouldn't forward the event on
        Mockito.verifyNoMoreInteractions(ctx);

        // now simulate an incoming message
        // the handler is expecting a handshake message
        // but we're going to feed it something else, and we expect an exception as a result
        ChannelBuffer badHandshakeBuffer = ChannelBuffers.wrappedBuffer(new byte[]{0, 1, 3, 4});
        expectedException.expect(IOException.class);
        handler.messageReceived(ctx, new UpstreamMessageEvent(channel, badHandshakeBuffer, remoteAddress));
    }

    @Test
    public void shouldThrowExceptionIfMultipleChannelConnectedEventsAreReceived() throws Exception {
        // first channel connected event
        handler.channelConnected(ctx, new UpstreamChannelStateEvent(channel, ChannelState.CONNECTED, new InetSocketAddress(0)));

        // second channel connected event
        expectedException.expect(IllegalStateException.class);
        handler.channelConnected(ctx, new UpstreamChannelStateEvent(channel, ChannelState.CONNECTED, new InetSocketAddress(0)));
    }

    @Test
    public void shouldNotForwardChannelConnectedEventUntilHandshakeMessageReceived() throws Exception {
        // simulate a channelConnected event
        handler.channelConnected(ctx, new UpstreamChannelStateEvent(channel, ChannelState.CONNECTED, new InetSocketAddress(0)));

        // verify that no event is forwarded on (if an event is forwarded, it _must_ use the ctx object)
        Mockito.verifyNoMoreInteractions(ctx);
    }

    @Test
    public void shouldThrowExceptionIfHandshakeReceivedBeforeChannelConnectedEvent() throws Exception {
        // simulate an incoming handshake message
        // since no channelConnected event was received first the handler should fail
        ChannelBuffer handshakeBuffer = Handshakers.createHandshakeMessage(S_01, mapper);
        expectedException.expect(NullPointerException.class);
        handler.messageReceived(ctx, new UpstreamMessageEvent(channel, handshakeBuffer, new InetSocketAddress(0)));
    }

    @Test
    public void shouldProperlyHandleIncomingHandshakeMessage() throws Exception {
        // the following actions should be performed for a incoming handshake
        // 1. set attachment to "S_01"
        // 2. remove self from pipeline
        // 3. forward channelConnected event on

        when(ctx.getChannel()).thenReturn(channel);
        when(ctx.getPipeline()).thenReturn(pipeline);

        // go through the full handshake flow:

        // address we expect in the channelConnected event
        final InetSocketAddress remoteAddress = new InetSocketAddress(0);

        // start off by simulating the original incoming 'channelConnected' event
        // this should set the internal state properly
        handler.channelConnected(ctx, new UpstreamChannelStateEvent(channel, ChannelState.CONNECTED, remoteAddress));

        // we shouldn't forward the event on
        Mockito.verifyNoMoreInteractions(ctx);

        // now simulate the incoming handshake message
        ChannelBuffer handshakeBuffer = Handshakers.createHandshakeMessage(S_01, mapper);
        handler.messageReceived(ctx, new UpstreamMessageEvent(channel, handshakeBuffer, remoteAddress));

        // captor for the event that's sent in response to this handshake
        ArgumentCaptor<ChannelEvent> upstreamEventCaptor = ArgumentCaptor.forClass(ChannelEvent.class);

        // verify the actions
        InOrder inOrder = Mockito.inOrder(channel, pipeline, ctx);
        inOrder.verify(ctx).getChannel();
        inOrder.verify(channel).setAttachment(S_01);
        inOrder.verify(ctx).getPipeline();
        inOrder.verify(pipeline).remove(handler);
        inOrder.verify(ctx).sendUpstream(upstreamEventCaptor.capture());
        inOrder.verifyNoMoreInteractions();

        ChannelEvent event = upstreamEventCaptor.getValue();
        assertThat(event, instanceOf(UpstreamChannelStateEvent.class));

        // now check that the event is actually a channelConnected event
        UpstreamChannelStateEvent channelStateEvent = (UpstreamChannelStateEvent) event;
        assertThat(channelStateEvent.getChannel(), is(channel));
        assertThat(channelStateEvent.getState(), is(ChannelState.CONNECTED));
        assertThat(channelStateEvent.getValue(), instanceOf(InetSocketAddress.class));
        assertThat((InetSocketAddress) channelStateEvent.getValue(), is(remoteAddress));
    }
}
