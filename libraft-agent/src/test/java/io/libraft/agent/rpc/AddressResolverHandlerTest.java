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

import com.google.common.util.concurrent.MoreExecutors;
import io.libraft.agent.TestLoggingRule;
import org.hamcrest.Matchers;
import org.jboss.netty.channel.Channel;
import org.jboss.netty.channel.ChannelFuture;
import org.jboss.netty.channel.ChannelHandlerContext;
import org.jboss.netty.channel.ChannelPipeline;
import org.jboss.netty.channel.ChannelState;
import org.jboss.netty.channel.ChannelStateEvent;
import org.jboss.netty.channel.DefaultChannelFuture;
import org.jboss.netty.channel.DefaultExceptionEvent;
import org.jboss.netty.channel.DownstreamChannelStateEvent;
import org.jboss.netty.channel.local.LocalAddress;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetSocketAddress;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.refEq;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public final class AddressResolverHandlerTest {

    private static final Logger LOGGER = LoggerFactory.getLogger(AddressResolverHandlerTest.class);

    private final ChannelHandlerContext ctx = mock(ChannelHandlerContext.class);
    private final Channel channel = mock(Channel.class);
    private final AddressResolverHandler handler = new AddressResolverHandler(MoreExecutors.listeningDecorator(MoreExecutors.sameThreadExecutor()));

    @Rule
    public final TestLoggingRule testLoggingRule = new TestLoggingRule(LOGGER);

    @Before
    public void setup() {
        when(ctx.getChannel()).thenReturn(channel);
    }

    @Test
    public void shouldResolveAddressAndForwardEventWhenConnectIsCalledWithAnUnresolvedInetSocketAddress() throws Exception {
        DefaultChannelFuture originalEventFuture = new DefaultChannelFuture(channel, false);
        ChannelStateEvent event = new DownstreamChannelStateEvent(
                channel,
                originalEventFuture,
                ChannelState.CONNECTED,
                createUnresolvedRemoteAddress()
        );

        handler.connectRequested(ctx, event);

        ArgumentCaptor<DownstreamChannelStateEvent> eventCaptor = ArgumentCaptor.forClass(DownstreamChannelStateEvent.class);
        verify(ctx).sendDownstream(eventCaptor.capture());

        DownstreamChannelStateEvent forwardedEvent = eventCaptor.getValue();
        assertThat(forwardedEvent.getChannel(), equalTo(channel));
        assertThat(forwardedEvent.getState(), equalTo(ChannelState.CONNECTED));
        assertThat(forwardedEvent.getFuture(), Matchers.<ChannelFuture>equalTo(originalEventFuture));

        InetSocketAddress forwardedAddress = (InetSocketAddress) forwardedEvent.getValue();
        assertThat(forwardedAddress.isUnresolved(), equalTo(false));
    }

    @Test
    public void shouldSimplyForwardEventWhenConnectIsCalledWithAResolvedInetSocketAddress() throws Exception {
        ChannelStateEvent event = new DownstreamChannelStateEvent(
                channel,
                new DefaultChannelFuture(channel, false),
                ChannelState.CONNECTED,
                new InetSocketAddress("localhost", 9999)
        );

        handler.connectRequested(ctx, event);

        verify(ctx).sendDownstream(refEq(event));
    }

    @Test
    public void shouldSimplyForwardEventWhenConnectIsCalledWithNonInetSocketAddress() throws Exception {
        ChannelStateEvent event = new DownstreamChannelStateEvent(
                channel,
                new DefaultChannelFuture(channel, false),
                ChannelState.CONNECTED,
                new LocalAddress(LocalAddress.EPHEMERAL)
        );

        handler.connectRequested(ctx, event);

        verify(ctx).sendDownstream(refEq(event));
    }

    @Test
    public void shouldFireExceptionIfAddressResolutionTaskFails() throws Exception {
        setupPipelineToRunFireEventLaterInline();

        // setup the address resolver to throw an exception
        final IllegalStateException resolveFailureCause = new IllegalStateException("simulate resolve failure");
        AddressResolverHandler.ResolvedAddressProvider failingResolver = new AddressResolverHandler.ResolvedAddressProvider() {
            @Override
            public InetSocketAddress createResolved(InetSocketAddress unresolvedAddress) throws Exception {
                throw resolveFailureCause;
            }
        };

        // fire a connect request with an unresolved address
        AddressResolverHandler failingHandler = new AddressResolverHandler(MoreExecutors.listeningDecorator(MoreExecutors.sameThreadExecutor()), failingResolver);

        DefaultChannelFuture originalEventFuture = new DefaultChannelFuture(channel, false);
        ChannelStateEvent event = new DownstreamChannelStateEvent(
                channel,
                originalEventFuture,
                ChannelState.CONNECTED,
                createUnresolvedRemoteAddress()
        );

        failingHandler.connectRequested(ctx, event);

        // this should cause a "fireExceptionCaughtLater" call
        ArgumentCaptor<DefaultExceptionEvent> exceptionEventCaptor = ArgumentCaptor.forClass(DefaultExceptionEvent.class);
        verify(ctx).sendUpstream(exceptionEventCaptor.capture());

        DefaultExceptionEvent exceptionEvent = exceptionEventCaptor.getValue();
        assertThat(exceptionEvent.getCause(), Matchers.<Throwable>is(resolveFailureCause));
    }

    @Test
    public void shouldFireExceptionIfConnectEventCannotBeForwarded() throws Exception {
        setupPipelineToRunFireEventLaterInline();

        // setup the ctx to throw an exception when we attempt to send the connected event downstream
        IllegalStateException resolveFailureCause = new IllegalStateException("downstream event failed");
        doThrow(resolveFailureCause).when(ctx).sendDownstream(any(DownstreamChannelStateEvent.class));

        // initiate the connect request
        DefaultChannelFuture originalEventFuture = new DefaultChannelFuture(channel, false);
        ChannelStateEvent event = new DownstreamChannelStateEvent(
                channel,
                originalEventFuture,
                ChannelState.CONNECTED,
                createUnresolvedRemoteAddress()
        );

        handler.connectRequested(ctx, event);

        // firing the connect downstream should cause a "fireExceptionCaughtLater" call
        ArgumentCaptor<DefaultExceptionEvent> exceptionEventCaptor = ArgumentCaptor.forClass(DefaultExceptionEvent.class);
        verify(ctx).sendUpstream(exceptionEventCaptor.capture());

        DefaultExceptionEvent exceptionEvent = exceptionEventCaptor.getValue();
        assertThat(exceptionEvent.getCause(), Matchers.<Throwable>is(resolveFailureCause));
    }

    private static InetSocketAddress createUnresolvedRemoteAddress() {
        return InetSocketAddress.createUnresolved("localhost", 9999);
    }

    private void setupPipelineToRunFireEventLaterInline() {
        // setup the ctx/pipeline to run a "fire<Event>Later" task immediately, inline
        ChannelPipeline pipeline = mock(ChannelPipeline.class);
        when(ctx.getPipeline()).thenReturn(pipeline);
        when(pipeline.execute(any(Runnable.class))).then(new Answer<Void>() {
            @Override
            public Void answer(InvocationOnMock invocation) throws Throwable {
                Object[] arguments = invocation.getArguments();
                assertThat(arguments.length, equalTo(1));

                Runnable task = (Runnable) arguments[0];
                task.run();

                return null;
            }
        });
    }
}
