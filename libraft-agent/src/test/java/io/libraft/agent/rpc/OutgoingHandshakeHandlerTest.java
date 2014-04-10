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
import org.jboss.netty.channel.Channel;
import org.jboss.netty.channel.ChannelFuture;
import org.jboss.netty.channel.ChannelHandlerContext;
import org.jboss.netty.channel.ChannelPipeline;
import org.jboss.netty.channel.ChannelState;
import org.jboss.netty.channel.Channels;
import org.jboss.netty.channel.DownstreamChannelStateEvent;
import org.jboss.netty.channel.DownstreamMessageEvent;
import org.jboss.netty.channel.UpstreamChannelStateEvent;
import org.junit.Before;
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
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.notNullValue;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

public final class OutgoingHandshakeHandlerTest {

    private static final String SELF = "SELF";

    private static final Logger LOGGER = LoggerFactory.getLogger(OutgoingHandshakeHandlerTest.class);

    private final Channel channel = Mockito.mock(Channel.class);
    private final ChannelFuture closeFuture = Channels.future(channel);
    private final ChannelPipeline pipeline = Mockito.mock(ChannelPipeline.class);
    private final ChannelHandlerContext ctx = Mockito.mock(ChannelHandlerContext.class);
    private final ObjectMapper mapper = new ObjectMapper();

    private Handshakers.OutgoingHandshakeHandler handler;

    @Before
    public void setup() throws Exception {
        handler = new Handshakers.OutgoingHandshakeHandler(SELF, mapper);

        when(channel.getCloseFuture()).thenReturn(closeFuture);
        when(ctx.getChannel()).thenReturn(channel);
        when(ctx.getPipeline()).thenReturn(pipeline);
    }

    @Rule
    public final TestLoggingRule testLoggingRule = new TestLoggingRule(LOGGER);

    @Rule
    public final ExpectedException expectedException = ExpectedException.none();

    @Test
    public void shouldThrowExceptionIfMultipleConnectRequestedEventsAreReceived() throws Exception {
        // first connect request
        // no issues expected
        handler.connectRequested(ctx, new DownstreamChannelStateEvent(channel, Channels.future(channel), ChannelState.CONNECTED, new InetSocketAddress(0)));

        // second connect request
        // in real life this shouldn't happen
        // but, if we blindly expect this our internal state is corrupted
        expectedException.expect(IllegalStateException.class);
        handler.connectRequested(ctx, new DownstreamChannelStateEvent(channel, Channels.future(channel), ChannelState.CONNECTED, new InetSocketAddress(0)));
    }

    @Test
    public void shouldPassOnProxyConnectRequestedEvent() throws Exception {
        ChannelFuture originalConnectFuture = Channels.future(channel);
        InetSocketAddress originalValue = new InetSocketAddress(0);

        // initiate the connect request
        handler.connectRequested(ctx, new DownstreamChannelStateEvent(channel, originalConnectFuture, ChannelState.CONNECTED, originalValue));

        // verify that a proxy event is sent instead of the original event
        ArgumentCaptor<DownstreamChannelStateEvent> downstreamEvent = ArgumentCaptor.forClass(DownstreamChannelStateEvent.class);
        verify(ctx, atLeastOnce()).getChannel();
        verify(ctx).sendDownstream(downstreamEvent.capture());
        verifyNoMoreInteractions(ctx);

        DownstreamChannelStateEvent forwardedEvent = downstreamEvent.getValue();
        assertThat(forwardedEvent.getChannel(), is(channel));
        assertThat(forwardedEvent.getFuture(), notNullValue());
        assertThat(forwardedEvent.getFuture(), not(originalConnectFuture));
        assertThat(forwardedEvent.getState(), is(ChannelState.CONNECTED));
        assertThat((InetSocketAddress) forwardedEvent.getValue(), is(originalValue));
    }

    @Test
    public void shouldFailOriginalConnectFutureWhenProxyConnectFutureIsMarkedAsFailed() throws Exception {
        ChannelFuture originalConnectFuture = Channels.future(channel);
        InetSocketAddress originalValue = new InetSocketAddress(0);

        // initiate the connect request
        handler.connectRequested(ctx, new DownstreamChannelStateEvent(channel, originalConnectFuture, ChannelState.CONNECTED, originalValue));

        // get the proxy event
        ArgumentCaptor<DownstreamChannelStateEvent> downstreamEvent = ArgumentCaptor.forClass(DownstreamChannelStateEvent.class);
        verify(ctx, atLeastOnce()).getChannel();
        verify(ctx).sendDownstream(downstreamEvent.capture());
        verifyNoMoreInteractions(ctx);

        // mark the proxy event future as having failed
        DownstreamChannelStateEvent forwardedEvent = downstreamEvent.getValue();

        Exception failureCause = new Exception();
        forwardedEvent.getFuture().setFailure(failureCause);

        // and check that we've triggered the original future as well
        assertThat(originalConnectFuture.isDone(), equalTo(true));
        assertThat(originalConnectFuture.isSuccess(), equalTo(false));
    }

    @Test
    public void shouldFailOriginalConnectFutureWhenChannelIsClosed() throws Exception {
        ChannelFuture originalConnectFuture = Channels.future(channel);
        InetSocketAddress originalValue = new InetSocketAddress(0);

        // initiate the connect request
        handler.connectRequested(ctx, new DownstreamChannelStateEvent(channel, originalConnectFuture, ChannelState.CONNECTED, originalValue));

        // trigger the close future (i.e the channel closed)
        closeFuture.setSuccess();

        // and check that we've triggered the original future as well
        assertThat(originalConnectFuture.isDone(), equalTo(true));
        assertThat(originalConnectFuture.isSuccess(), equalTo(false));
    }

    // happy path
    @Test
    public void shouldNotTriggerOriginalConnectFutureIfProxyConnectFutureSucceeds() throws Exception {
        ChannelFuture originalConnectFuture = Channels.future(channel);
        InetSocketAddress originalValue = new InetSocketAddress(0);

        // initiate the connect request
        handler.connectRequested(ctx, new DownstreamChannelStateEvent(channel, originalConnectFuture, ChannelState.CONNECTED, originalValue));

        // get the proxy event
        ArgumentCaptor<DownstreamChannelStateEvent> downstreamEvent = ArgumentCaptor.forClass(DownstreamChannelStateEvent.class);
        verify(ctx, atLeastOnce()).getChannel();
        verify(ctx).sendDownstream(downstreamEvent.capture());

        // mark the proxy event future as having succeeded
        DownstreamChannelStateEvent forwardedEvent = downstreamEvent.getValue();
        forwardedEvent.getFuture().setSuccess();

        // and check that original future _is not triggered_ and no events are forwarded!
        assertThat(originalConnectFuture.isDone(), equalTo(false));
        verifyNoMoreInteractions(ctx);
    }

    @Test
    public void shouldThrowExceptionIfOriginalConnectFutureIsNull() throws Exception {
        // this handler only works for _user-initiated_ connect requests
        expectedException.expect(NullPointerException.class);
        handler.channelConnected(ctx, new UpstreamChannelStateEvent(channel, ChannelState.CONNECTED, new InetSocketAddress(0)));
    }

    @SuppressWarnings("ThrowableResultOfMethodCallIgnored")
    @Test
    public void shouldFailOriginalConnectFutureAndCloseChannelIfHandshakeWriteFutureFails() throws Exception {
        ChannelFuture originalConnectFuture = Channels.future(channel);

        // pretend that a connectRequested event was sent over this channel
        handler.setConnectFutureForUnitTestOnly(originalConnectFuture);

        // signal that the connect succeeded
        handler.channelConnected(ctx, new UpstreamChannelStateEvent(channel, ChannelState.CONNECTED, new InetSocketAddress(0)));

        // instead of passing events upstream, the handler should
        // attempt to write an handshake out and send nothing upstream
        ArgumentCaptor<DownstreamMessageEvent> downstreamEvent = ArgumentCaptor.forClass(DownstreamMessageEvent.class);
        verify(ctx, atLeastOnce()).getChannel();
        verify(ctx).sendDownstream(downstreamEvent.capture());
        verifyNoMoreInteractions(ctx);

        // fail the write
        DownstreamMessageEvent handshakeEvent = downstreamEvent.getValue();
        IOException cause = new IOException();
        handshakeEvent.getFuture().setFailure(cause);

        // verify that the original future failed as well
        assertThat(originalConnectFuture.isDone(), equalTo(true));
        assertThat(originalConnectFuture.isSuccess(), equalTo(false));
        assertThat((IOException) originalConnectFuture.getCause(), is(cause));

        // and that the channel was closed
        verify(channel).close();
    }

    // happy path
    @Test
    public void shouldIndicateThatConnectSucceededIfHandshakeWriteSucceeds() throws Exception {
        ChannelFuture originalConnectFuture = Mockito.mock(ChannelFuture.class);
        InetSocketAddress connectedAddress = new InetSocketAddress(0);

        // pretend that a connectRequested event was sent over this channel
        handler.setConnectFutureForUnitTestOnly(originalConnectFuture);

        // signal that the connect succeeded
        handler.channelConnected(ctx, new UpstreamChannelStateEvent(channel, ChannelState.CONNECTED, connectedAddress));

        // check the order of operations for the handshake write
        ArgumentCaptor<DownstreamMessageEvent> downstreamEvent = ArgumentCaptor.forClass(DownstreamMessageEvent.class);
        InOrder preWriteEventOrder = Mockito.inOrder(ctx);
        preWriteEventOrder.verify(ctx, atLeastOnce()).getChannel();
        preWriteEventOrder.verify(ctx).sendDownstream(downstreamEvent.capture());
        preWriteEventOrder.verifyNoMoreInteractions();

        // check that the handshake is valid
        DownstreamMessageEvent handshakeEvent = downstreamEvent.getValue();
        assertThat(Handshakers.getServerIdFromHandshake((ChannelBuffer) handshakeEvent.getMessage(), mapper), equalTo(SELF));

        // mark the handshake write as having succeeded
        handshakeEvent.getFuture().setSuccess();

        // check the order of operations after the handshake write succeeded
        // the following actions must be performed, in order:
        // 1. the handler is removed
        // 2. the original connect future is triggered
        // 3. a channelConnected event is forwarded on
        ArgumentCaptor<UpstreamChannelStateEvent> upstreamEvent = ArgumentCaptor.forClass(UpstreamChannelStateEvent.class);
        InOrder postWriteEventOrder = Mockito.inOrder(originalConnectFuture, ctx, pipeline);
        postWriteEventOrder.verify(ctx).getPipeline();
        postWriteEventOrder.verify(pipeline).remove(handler);
        postWriteEventOrder.verify(originalConnectFuture).setSuccess();
        postWriteEventOrder.verify(ctx).sendUpstream(upstreamEvent.capture());
        postWriteEventOrder.verifyNoMoreInteractions();

        // and that an appropriate upstream event was sent
        UpstreamChannelStateEvent connectedEvent = upstreamEvent.getValue();
        assertThat(connectedEvent.getChannel(), is(channel));
        assertThat(connectedEvent.getState(), is(ChannelState.CONNECTED));
        assertThat((InetSocketAddress) connectedEvent.getValue(), is(connectedAddress));
    }
}
