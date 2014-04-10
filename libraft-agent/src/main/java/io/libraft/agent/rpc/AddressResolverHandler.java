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

import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListeningExecutorService;
import org.jboss.netty.channel.ChannelHandlerContext;
import org.jboss.netty.channel.ChannelStateEvent;
import org.jboss.netty.channel.Channels;
import org.jboss.netty.channel.DownstreamChannelStateEvent;
import org.jboss.netty.channel.SimpleChannelDownstreamHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetSocketAddress;
import java.util.concurrent.Callable;

/**
 * {@code ChannelHandler} that automatically resolves
 * unresolved {@link InetSocketAddress} instances
 * on the supplied {@link ListeningExecutorService} before
 * initiating outgoing connections. The connection is established
 * immediately if the remote address is already resolved.
 * <p/>
 * Implementations may or may not be stateless depending on
 * the {@link ResolvedAddressProvider} implementation used.
 * If no {@code ResolvedAddressProvider} is specified a
 * {@link ResolvedAddressProvider#DEFAULT_PROVIDER} is used. This
 * implementation is stateless, allowing a single
 * instance of this handler to be shared across multiple
 * channels.
 */
final class AddressResolverHandler extends SimpleChannelDownstreamHandler {

    /**
     * Implemented by classes that construct a resolved
     * {@link InetSocketAddress} from an unresolved {@code InetSocketAddress}.
     * <p/>
     * Ideally, implementations <strong>should</strong> be
     * thread-safe, and stateless if possible.
     */
    static interface ResolvedAddressProvider {

        /**
         * Construct a resolved {@code InetSocketAddress} given an unresolved {@code InetSocketAddress}.
         *
         * @param unresolvedAddress unresolved {@code InetSocketAddress} instance
         * @return resolved {@code InetSocketAddress} instance
         *
         * @throws Exception if name resolution cannot be performed or a resolved
         * {@code InetSocketAddress} cannot be constructed
         */
        InetSocketAddress createResolved(InetSocketAddress unresolvedAddress) throws Exception;

        /**
         * Stateless implementation of {@link ResolvedAddressProvider}.
         * <p/>
         * This implementation relies on Java's name-resolution mechanism,
         * and creates a new {@link InetSocketAddress} using the unresolved address' host/port.
         */
        final static ResolvedAddressProvider DEFAULT_PROVIDER = new ResolvedAddressProvider() {
            @Override
            public InetSocketAddress createResolved(InetSocketAddress unresolvedAddress) throws Exception {
                return new InetSocketAddress(unresolvedAddress.getHostName(), unresolvedAddress.getPort());
            }
        };
    }

    private static final Logger LOGGER = LoggerFactory.getLogger(AddressResolverHandler.class);

    private final ListeningExecutorService resolveService;
    private final ResolvedAddressProvider resolvedAddressProvider;

    /**
     * Constructor.
     *
     * @param resolveService instance of {@code ListeningExecutorService} that can run name resolution tasks
     */
    AddressResolverHandler(ListeningExecutorService resolveService) {
        this(resolveService, ResolvedAddressProvider.DEFAULT_PROVIDER);
    }

    /**
     * Constructor.
     *
     * @param resolveService instance of {@code ListeningExecutorService} that can run name resolution tasks
     * @param resolvedAddressProvider instance of {@code ResolvedAddressProvider} that constructs a resolved
     *                                {@link InetSocketAddress} from an unresolved one
     */
    AddressResolverHandler(ListeningExecutorService resolveService, ResolvedAddressProvider resolvedAddressProvider) {
        this.resolveService = resolveService;
        this.resolvedAddressProvider = resolvedAddressProvider;
    }

    @Override
    public void connectRequested(final ChannelHandlerContext ctx, final ChannelStateEvent e) throws Exception {
        if (!(e.getValue() instanceof InetSocketAddress)) {
            super.connectRequested(ctx, e);
            return;
        }

        final InetSocketAddress originalAddress = (InetSocketAddress) e.getValue();

        if (!originalAddress.isUnresolved()) {
            super.connectRequested(ctx, e);
            return;
        }

        ListenableFuture<InetSocketAddress> resolvedFuture = resolveService.submit(new Callable<InetSocketAddress>() {
            @Override
            public InetSocketAddress call() throws Exception {
                return resolvedAddressProvider.createResolved(originalAddress);
            }
        });

        Futures.addCallback(resolvedFuture, new FutureCallback<InetSocketAddress>() {
            @Override
            public void onSuccess(InetSocketAddress resolvedAddress) {
                try {
                    DownstreamChannelStateEvent forwardedEvent = new DownstreamChannelStateEvent(e.getChannel(), e.getFuture(), e.getState(), resolvedAddress);
                    AddressResolverHandler.super.connectRequested(ctx, forwardedEvent);
                } catch (Exception cause) {
                    failConnect(cause);
                }
            }

            @Override
            public void onFailure(Throwable cause) {
                failConnect(cause);
            }

            private void failConnect(Throwable cause) {
                LOGGER.warn("fail connect to unresolved address:{}", originalAddress, cause);
                Channels.fireExceptionCaughtLater(ctx, cause);
            }
        });
    }
}
